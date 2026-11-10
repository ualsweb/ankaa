//! Warm and cold tiers (PRD F-13, F-16, F-17) behind the store's [`EncodedChunk`] trait.
//!
//! - **Warm**: every column of a sealed chunk is compressed in memory by Vortex's adaptive
//!   (`BtrBlocks`-style) compressor, which picks ALP / frame-of-reference / `FastLanes` bit-packing /
//!   dictionary / run-end encodings per column. Decoding goes through Vortex's executor back to
//!   Arrow and then to plain columns.
//! - **Cold**: the same compressed columns are written as one Vortex file per chunk
//!   (`<dir>/<table>/p<partition>-c<id>.vortex`) with the [`ChunkMeta`] stored as a metadata
//!   segment, so a restart lists segments without decoding and queries prune on metadata alone.
//!
//! The hot path never links this crate; `ankaad` drives it from the snapshot tick with a time
//! budget (see `tiering` there). Everything here is correctness-first: compression ratios and
//! decode throughput are measured by `bench/` on real hardware, never asserted here.
#![forbid(unsafe_code)]

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ankaa_store::tier::{ChunkMeta, DecodeError, EncodedChunk, NullableColumn, Tier};
use ankaa_store::{Chunk, Column, ColumnType, Schema};
use arrow::array::{
    Array as _, ArrayRef as ArrowArrayRef, Float64Array, Int64Array, UInt8Array, UInt32Array,
};
use arrow::buffer::NullBuffer;
use vortex::VortexSessionDefault;
use vortex::array::arrays::StructArray;
use vortex::array::expr::{root, select};
use vortex::array::iter::ArrayIteratorExt as _;
use vortex::array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::ByteBuffer;
use vortex::compressor::BtrBlocksCompressor;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::runtime::BlockingRuntime as _;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt as _;
use vortex::session::VortexSession;

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Metadata segment key inside a segment file.
pub const META_KEY: &str = "ankaa.chunk";
/// Segment file extension.
pub const SEGMENT_EXT: &str = "vortex";

/// Tier errors.
#[derive(Debug, thiserror::Error)]
pub enum TierError {
    /// Vortex failure.
    #[error("vortex: {0}")]
    Vortex(String),
    /// I/O.
    #[error("io {path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Error.
        #[source]
        source: std::io::Error,
    },
    /// Metadata missing or invalid.
    #[error("segment {path}: {msg}")]
    Meta {
        /// Path involved.
        path: PathBuf,
        /// Message.
        msg: String,
    },
    /// A column could not be converted.
    #[error("column {0}: unsupported type {1:?}")]
    Column(String, ColumnType),
}

impl From<vortex::error::VortexError> for TierError {
    fn from(e: vortex::error::VortexError) -> Self {
        Self::Vortex(e.to_string())
    }
}

fn io_err(path: &Path) -> impl FnOnce(std::io::Error) -> TierError + '_ {
    move |source| TierError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Shared Vortex session + compressor. One per process; cheap to clone through `Arc`.
pub struct Encoder {
    session: VortexSession,
    compressor: BtrBlocksCompressor,
    runtime: CurrentThreadRuntime,
}

impl Debug for Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Encoder(vortex)")
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder {
    /// Default session with all built-in encodings.
    #[must_use]
    pub fn new() -> Self {
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let compressor = BtrBlocksCompressor::from_session(&session);
        Self {
            session,
            compressor,
            runtime,
        }
    }

    fn column_to_arrow(col: &Column, valid: Option<&[bool]>) -> ArrowArrayRef {
        let nulls = valid.map(|v| NullBuffer::from(v.to_vec()));
        match col {
            Column::I64(v) => Arc::new(Int64Array::new(v.clone().into(), nulls)),
            Column::U32(v) => Arc::new(UInt32Array::new(v.clone().into(), nulls)),
            Column::U8(v) => Arc::new(UInt8Array::new(v.clone().into(), nulls)),
            Column::F64(v) => Arc::new(Float64Array::new(v.clone().into(), nulls)),
        }
    }

    /// Plain column plus validity (`None` when every row holds a value).
    fn arrow_to_column(
        name: &str,
        ty: ColumnType,
        a: &ArrowArrayRef,
    ) -> Result<(Column, Option<Vec<bool>>), TierError> {
        let valid = a
            .logical_nulls()
            .filter(|n| n.null_count() > 0)
            .map(|n| n.iter().collect::<Vec<bool>>());
        let any = a.as_any();
        let col = match ty {
            ColumnType::TimestampNs | ColumnType::FixedPoint { .. } | ColumnType::Int64 => any
                .downcast_ref::<Int64Array>()
                .map(|x| Column::I64(x.values().to_vec())),
            ColumnType::DictU32 => any
                .downcast_ref::<UInt32Array>()
                .map(|x| Column::U32(x.values().to_vec())),
            ColumnType::U8 => any
                .downcast_ref::<UInt8Array>()
                .map(|x| Column::U8(x.values().to_vec())),
            ColumnType::Float64 => any
                .downcast_ref::<Float64Array>()
                .map(|x| Column::F64(x.values().to_vec())),
        };
        col.map(|c| (c, valid))
            .ok_or_else(|| TierError::Column(name.to_owned(), ty))
    }

    /// Compress one plain column (`nullable` keeps its validity in the encoding).
    fn compress_column(
        &self,
        col: &Column,
        valid: Option<&[bool]>,
        nullable: bool,
    ) -> Result<ArrayRef, TierError> {
        let arrow = Self::column_to_arrow(col, valid);
        let v = self.session.arrow().from_arrow_array(arrow, nullable)?;
        let mut ctx = self.session.create_execution_ctx();
        Ok(self.compressor.compress(&v, &mut ctx)?)
    }

    /// Decode one compressed column back to plain values.
    fn decompress_column(
        &self,
        name: &str,
        ty: ColumnType,
        a: &ArrayRef,
    ) -> Result<(Column, Option<Vec<bool>>), TierError> {
        let mut ctx = self.session.create_execution_ctx();
        let arrow = self
            .session
            .arrow()
            .execute_arrow(a.clone(), None, &mut ctx)?;
        Self::arrow_to_column(name, ty, &arrow)
    }

    /// Encode a sealed chunk into the warm tier.
    pub fn encode_warm(
        self: &Arc<Self>,
        table: &str,
        partition: u32,
        chunk: &Chunk,
    ) -> Result<Arc<WarmChunk>, TierError> {
        let meta = ChunkMeta::of(table, partition, chunk);
        let mut columns = Vec::with_capacity(meta.schema.fields().len());
        for (i, f) in meta.schema.fields().iter().enumerate() {
            let col = chunk
                .column(i)
                .ok_or_else(|| TierError::Column(f.name.clone(), f.ty))?;
            columns.push(self.compress_column(col, chunk.validity(i), f.nullable)?);
        }
        let nbytes = columns
            .iter()
            .map(|a| usize::try_from(a.nbytes()).unwrap_or(usize::MAX))
            .sum();
        Ok(Arc::new(WarmChunk {
            meta,
            columns,
            nbytes,
            enc: Arc::clone(self),
        }))
    }

    /// Path of the segment file for a chunk.
    #[must_use]
    pub fn segment_path(dir: &Path, meta: &ChunkMeta) -> PathBuf {
        dir.join(&meta.table)
            .join(format!("p{}-c{}.{SEGMENT_EXT}", meta.partition, meta.id.0))
    }

    /// Write a warm chunk as a cold segment file (atomic: written to a temp name, fsynced, renamed).
    pub fn write_cold(
        self: &Arc<Self>,
        dir: &Path,
        warm: &WarmChunk,
    ) -> Result<Arc<ColdChunk>, TierError> {
        let path = Self::segment_path(dir, &warm.meta);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(io_err(parent))?;
        }
        let tmp = path.with_extension("tmp");
        let fields: Vec<(&str, ArrayRef)> = warm
            .meta
            .schema
            .fields()
            .iter()
            .zip(&warm.columns)
            .map(|(f, a)| (f.name.as_str(), a.clone()))
            .collect();
        let st = StructArray::from_fields(&fields)?.into_array();
        let meta_json = serde_json::to_vec(&warm.meta).map_err(|e| TierError::Meta {
            path: path.clone(),
            msg: e.to_string(),
        })?;
        {
            let file = std::fs::File::create(&tmp).map_err(io_err(&tmp))?;
            let dtype = st.dtype().clone();
            let mut w = self
                .session
                .write_options()
                .with_metadata_segment(META_KEY, ByteBuffer::from(meta_json))
                .blocking(&self.runtime)
                .writer(&file, dtype);
            w.push(st)?;
            w.finish()?;
            file.sync_all().map_err(io_err(&tmp))?;
        }
        std::fs::rename(&tmp, &path).map_err(io_err(&path))?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)
                .and_then(|d| d.sync_all())
                .map_err(io_err(parent))?;
        }
        let file_size = std::fs::metadata(&path).map_err(io_err(&path))?.len();
        Ok(Arc::new(ColdChunk {
            meta: warm.meta.clone(),
            path,
            file_size,
            enc: Arc::clone(self),
        }))
    }

    /// Open a segment file: reads its metadata only.
    pub fn open_cold(self: &Arc<Self>, path: &Path) -> Result<Arc<ColdChunk>, TierError> {
        let bytes = std::fs::read(path).map_err(io_err(path))?;
        let file_size = bytes.len() as u64;
        let file = self
            .session
            .open_options()
            .include_metadata()
            .open_buffer(ByteBuffer::from(bytes))?;
        let meta_bytes = file
            .metadata_segment(META_KEY)
            .ok_or_else(|| TierError::Meta {
                path: path.to_path_buf(),
                msg: format!("missing {META_KEY} metadata segment"),
            })?;
        let meta: ChunkMeta =
            serde_json::from_slice(meta_bytes.as_slice()).map_err(|e| TierError::Meta {
                path: path.to_path_buf(),
                msg: e.to_string(),
            })?;
        if meta.num_rows as u64 != file.row_count() {
            return Err(TierError::Meta {
                path: path.to_path_buf(),
                msg: format!(
                    "row count {} != metadata {}",
                    file.row_count(),
                    meta.num_rows
                ),
            });
        }
        Ok(Arc::new(ColdChunk {
            meta,
            path: path.to_path_buf(),
            file_size,
            enc: Arc::clone(self),
        }))
    }

    /// All segments under `dir` (recursively), sorted by table, partition, chunk id. Files that
    /// are not readable segments are reported through `on_bad` and skipped.
    pub fn scan_dir(
        self: &Arc<Self>,
        dir: &Path,
        mut on_bad: impl FnMut(&Path, &TierError),
    ) -> Result<Vec<Arc<ColdChunk>>, TierError> {
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d).map_err(io_err(&d))? {
                let entry = entry.map_err(io_err(&d))?;
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|e| e == SEGMENT_EXT) {
                    match self.open_cold(&p) {
                        Ok(c) => out.push(c),
                        Err(e) => on_bad(&p, &e),
                    }
                }
            }
        }
        out.sort_by(|a, b| {
            (&a.meta.table, a.meta.partition, a.meta.id).cmp(&(
                &b.meta.table,
                b.meta.partition,
                b.meta.id,
            ))
        });
        Ok(out)
    }

    /// Decode `projection` columns of a cold segment.
    fn read_cold(
        &self,
        cold: &ColdChunk,
        projection: &[usize],
    ) -> Result<Vec<NullableColumn>, TierError> {
        let bytes = std::fs::read(&cold.path).map_err(io_err(&cold.path))?;
        let file = self
            .session
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))?;
        let fields = cold.meta.schema.fields();
        let names: Vec<String> = projection.iter().map(|&i| fields[i].name.clone()).collect();
        let expr = select(names.iter().map(String::as_str).collect::<Vec<_>>(), root())
            .bind(file.dtype())?
            .optimize_recursive()?;
        let array = file
            .scan()?
            .with_projection(expr)
            .into_array_iter(&self.runtime)?
            .read_all()?;
        let mut ctx = self.session.create_execution_ctx();
        let arrow = self.session.arrow().execute_arrow(array, None, &mut ctx)?;
        let st = arrow
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .ok_or_else(|| TierError::Vortex("projection did not decode to a struct".into()))?;
        projection
            .iter()
            .zip(&names)
            .map(|(&i, name)| {
                let col = st
                    .column_by_name(name)
                    .ok_or_else(|| TierError::Column(name.clone(), fields[i].ty))?;
                Self::arrow_to_column(name, fields[i].ty, col)
            })
            .collect()
    }
}

/// A sealed chunk compressed in memory.
pub struct WarmChunk {
    meta: ChunkMeta,
    columns: Vec<ArrayRef>,
    nbytes: usize,
    enc: Arc<Encoder>,
}

impl Debug for WarmChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WarmChunk")
            .field("id", &self.meta.id)
            .field("rows", &self.meta.num_rows)
            .field("nbytes", &self.nbytes)
            .finish_non_exhaustive()
    }
}

impl WarmChunk {
    /// Encoding tree per column (for diagnostics and the compression report).
    #[must_use]
    pub fn encoding_trees(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|a| a.display_tree().to_string())
            .collect()
    }
}

impl EncodedChunk for WarmChunk {
    fn meta(&self) -> &ChunkMeta {
        &self.meta
    }
    fn tier(&self) -> Tier {
        Tier::Warm
    }
    fn encoded_bytes(&self) -> usize {
        self.nbytes
    }
    fn decode(&self, projection: &[usize]) -> Result<Vec<Column>, DecodeError> {
        Ok(self
            .decode_nullable(projection)?
            .into_iter()
            .map(|(c, _)| c)
            .collect())
    }
    fn decode_nullable(&self, projection: &[usize]) -> Result<Vec<NullableColumn>, DecodeError> {
        let fields = self.meta.schema.fields();
        projection
            .iter()
            .map(|&i| {
                let f = fields.get(i).ok_or_else(|| DecodeError {
                    tier: Tier::Warm,
                    id: self.meta.id,
                    msg: format!("column {i} out of range"),
                })?;
                self.enc
                    .decompress_column(&f.name, f.ty, &self.columns[i])
                    .map_err(|e| DecodeError {
                        tier: Tier::Warm,
                        id: self.meta.id,
                        msg: e.to_string(),
                    })
            })
            .collect()
    }
}

/// A sealed chunk persisted as a segment file; decoded on demand.
pub struct ColdChunk {
    meta: ChunkMeta,
    path: PathBuf,
    file_size: u64,
    enc: Arc<Encoder>,
}

impl Debug for ColdChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdChunk")
            .field("id", &self.meta.id)
            .field("rows", &self.meta.num_rows)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl ColdChunk {
    /// Segment file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl EncodedChunk for ColdChunk {
    fn meta(&self) -> &ChunkMeta {
        &self.meta
    }
    fn tier(&self) -> Tier {
        Tier::Cold
    }
    fn encoded_bytes(&self) -> usize {
        usize::try_from(self.file_size).unwrap_or(usize::MAX)
    }
    fn decode(&self, projection: &[usize]) -> Result<Vec<Column>, DecodeError> {
        Ok(self
            .decode_nullable(projection)?
            .into_iter()
            .map(|(c, _)| c)
            .collect())
    }
    fn decode_nullable(&self, projection: &[usize]) -> Result<Vec<NullableColumn>, DecodeError> {
        self.enc
            .read_cold(self, projection)
            .map_err(|e| DecodeError {
                tier: Tier::Cold,
                id: self.meta.id,
                msg: e.to_string(),
            })
    }
}

/// Plain-column bytes of a chunk (for ratio reporting).
#[must_use]
pub fn raw_bytes(chunk: &Chunk) -> usize {
    (0..chunk.schema().fields().len())
        .filter_map(|i| chunk.column(i))
        .map(|c| c.len() * c.value_size())
        .sum()
}

/// Schema helper re-export for callers building chunks.
pub fn schema_of(chunk: &Chunk) -> &Schema {
    chunk.schema()
}

#[cfg(test)]
#[allow(
    clippy::inconsistent_digit_grouping,
    clippy::similar_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
mod tests {
    use super::*;
    use ankaa_store::{ChunkId, Value};

    fn chunk(rows: usize) -> Chunk {
        let mut c = Chunk::new(ChunkId(7), Schema::trades());
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        let mut px = 65_000_00000000i64;
        for i in 0..rows {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            px += (s % 5) as i64 * 1_000_000 - 2_000_000;
            c.append_row(&[
                Value::I64(1_700_000_000_000_000_000 + i as i64 * 1_000),
                Value::U32((i % 3) as u32),
                Value::I64(px),
                Value::I64((s % 1_000) as i64 * 1_000_000),
                Value::U8((s % 2) as u8),
                Value::I64(i as i64),
            ])
            .unwrap();
        }
        c.note_lsn(10);
        c.note_lsn(42);
        c.seal();
        c
    }

    fn same(a: &Column, b: &Column) -> bool {
        match (a, b) {
            (Column::I64(x), Column::I64(y)) => x == y,
            (Column::U32(x), Column::U32(y)) => x == y,
            (Column::U8(x), Column::U8(y)) => x == y,
            (Column::F64(x), Column::F64(y)) => x == y,
            _ => false,
        }
    }

    #[test]
    fn warm_round_trip_is_exact_and_smaller() {
        let enc = Arc::new(Encoder::new());
        let c = chunk(10_000);
        let warm = enc.encode_warm("trades", 3, &c).unwrap();
        assert_eq!(warm.meta().num_rows, 10_000);
        assert_eq!(warm.meta().lsn_range, (10, 42));
        assert!(
            warm.encoded_bytes() < raw_bytes(&c),
            "{} vs {}",
            warm.encoded_bytes(),
            raw_bytes(&c)
        );
        let cols = warm.decode(&[0, 1, 2, 3, 4, 5]).unwrap();
        for (i, col) in cols.iter().enumerate() {
            assert!(same(col, c.column(i).unwrap()), "column {i}");
        }
        // projection order is preserved
        let cols = warm.decode(&[2, 0]).unwrap();
        assert!(same(&cols[0], c.column(2).unwrap()));
        assert!(same(&cols[1], c.column(0).unwrap()));
    }

    #[test]
    fn cold_segment_round_trip_and_scan() {
        let enc = Arc::new(Encoder::new());
        let dir = tempfile::tempdir().unwrap();
        let c = chunk(5_000);
        let warm = enc.encode_warm("trades", 1, &c).unwrap();
        let cold = enc.write_cold(dir.path(), &warm).unwrap();
        assert!(cold.path().ends_with("trades/p1-c7.vortex"));
        assert_eq!(cold.meta(), warm.meta());
        let cols = cold.decode(&[0, 2, 5]).unwrap();
        assert!(same(&cols[0], c.column(0).unwrap()));
        assert!(same(&cols[1], c.column(2).unwrap()));
        assert!(same(&cols[2], c.column(5).unwrap()));
        // a second table + a junk file
        let warm2 = enc.encode_warm("quotes_like", 0, &c).unwrap();
        enc.write_cold(dir.path(), &warm2).unwrap();
        std::fs::write(dir.path().join("trades").join("junk.vortex"), b"nope").unwrap();
        let mut bad = Vec::new();
        let all = enc
            .scan_dir(dir.path(), |p, _| bad.push(p.to_path_buf()))
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(bad.len(), 1);
        assert_eq!(all[0].meta().table, "quotes_like");
        assert_eq!(all[1].meta().table, "trades");
        let reopened = enc.open_cold(cold.path()).unwrap();
        assert_eq!(reopened.meta(), cold.meta());
        assert!(same(
            &reopened.decode(&[1]).unwrap()[0],
            c.column(1).unwrap()
        ));
    }

    #[test]
    fn nulls_survive_warm_and_cold() {
        let schema = Schema::new(vec![
            ankaa_store::Field::new("ts", ColumnType::TimestampNs),
            ankaa_store::Field::new("device", ColumnType::DictU32),
            ankaa_store::Field::nullable("temp", ColumnType::Float64),
            ankaa_store::Field::nullable("count", ColumnType::Int64),
            ankaa_store::Field::nullable("state", ColumnType::U8),
        ])
        .unwrap();
        let mut c = Chunk::new(ChunkId(3), schema);
        for i in 0..3_000i64 {
            let temp = if i % 7 == 0 {
                Value::Null
            } else {
                Value::F64(i as f64 / 10.0)
            };
            let count = if i % 11 == 0 {
                Value::Null
            } else {
                Value::I64(i)
            };
            c.append_row(&[
                Value::I64(1_000 + i),
                Value::U32((i % 4) as u32),
                temp,
                count,
                Value::U8(1),
            ])
            .unwrap();
        }
        c.seal();
        let enc = Arc::new(Encoder::new());
        let dir = tempfile::tempdir().unwrap();
        let warm = enc.encode_warm("sensors", 0, &c).unwrap();
        let cold = enc.write_cold(dir.path(), &warm).unwrap();
        for decoded in [
            warm.decode_nullable(&[2, 3, 4]).unwrap(),
            cold.decode_nullable(&[2, 3, 4]).unwrap(),
        ] {
            for (k, (col, valid)) in decoded.iter().enumerate() {
                let i = k + 2;
                assert_eq!(valid.as_deref(), c.validity(i), "column {i}");
                for row in 0..3_000 {
                    let ok = valid.as_ref().is_none_or(|v| v[row]);
                    if ok {
                        assert_eq!(col.get(row), c.value(i, row), "column {i} row {row}");
                    } else {
                        assert_eq!(c.value(i, row), Some(Value::Null));
                    }
                }
            }
        }
        // a column with no NULL in this chunk reports no validity
        assert!(warm.decode_nullable(&[4]).unwrap()[0].1.is_none());
    }
}
