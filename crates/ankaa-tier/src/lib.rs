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
    cache: SegmentCache,
}

/// Segment files kept in memory after a read, compressed as they are on disk, with their footer
/// parsed: the most recently used ones, up to a budget in bytes.
///
/// Reading a cold chunk costs a file read, a footer parse and the decode of the columns asked
/// for; for a chunk of some hundred kilobytes the first two are most of it (measured: 17 µs and
/// 21 µs of 55 to 75 µs). A query that returns to a segment (a dashboard refreshing, a scan of
/// the same day by another user) finds it here and pays the decode only. The operating
/// system's page cache already holds the bytes; this saves the copy out of it and the parse.
///
/// Entries are keyed by an id given to each [`ColdChunk`] when it is created and removed when
/// it is dropped, so a file replaced or deleted is never served from here.
#[derive(Default)]
struct SegmentCache {
    budget: usize,
    state: std::sync::Mutex<CacheState>,
    next_id: std::sync::atomic::AtomicU64,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct CacheState {
    files: std::collections::HashMap<u64, Cached>,
    bytes: usize,
    tick: u64,
}

struct Cached {
    file: vortex::file::VortexFile,
    bytes: usize,
    used: u64,
}

/// Counters of the segment cache ([`Encoder::segment_cache`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentCacheStats {
    /// Bytes of segment files held.
    pub bytes: usize,
    /// Segment files held.
    pub files: usize,
    /// Reads answered from the cache.
    pub hits: u64,
    /// Reads that had to open the file.
    pub misses: u64,
}

impl SegmentCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        // the state is consistent after every statement below: a panic elsewhere while the lock
        // was held leaves nothing half done
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn get(&self, id: u64) -> Option<vortex::file::VortexFile> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.budget == 0 {
            return None;
        }
        let mut s = self.lock();
        s.tick += 1;
        let tick = s.tick;
        let found = s.files.get_mut(&id).map(|c| {
            c.used = tick;
            c.file.clone()
        });
        drop(s);
        if found.is_some() {
            self.hits.fetch_add(1, Relaxed);
        } else {
            self.misses.fetch_add(1, Relaxed);
        }
        found
    }

    /// Keep `file` (`bytes` long). When the budget is exceeded the least recently used eighth
    /// of it goes at once, so a workload larger than the cache pays for an eviction pass once
    /// in many inserts, not on each.
    fn put(&self, id: u64, file: vortex::file::VortexFile, bytes: usize) {
        if bytes > self.budget {
            return;
        }
        let mut s = self.lock();
        s.tick += 1;
        let used = s.tick;
        if let Some(old) = s.files.insert(id, Cached { file, bytes, used }) {
            s.bytes -= old.bytes;
        }
        s.bytes += bytes;
        if s.bytes > self.budget {
            let keep = self.budget - self.budget / 8;
            let mut by_age: Vec<(u64, u64)> = s.files.iter().map(|(&i, c)| (c.used, i)).collect();
            by_age.sort_unstable();
            for (_, old) in by_age {
                if s.bytes <= keep {
                    break;
                }
                if old != id
                    && let Some(c) = s.files.remove(&old)
                {
                    s.bytes -= c.bytes;
                }
            }
        }
    }

    fn remove(&self, id: u64) {
        if self.budget == 0 {
            return;
        }
        let mut s = self.lock();
        if let Some(c) = s.files.remove(&id) {
            s.bytes -= c.bytes;
        }
    }
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
            cache: SegmentCache::default(),
        }
    }

    /// Keep up to `bytes` of recently read segment files in memory (0: none, the default):
    /// a cold chunk read again is decoded without reading its file or parsing its footer.
    #[must_use]
    pub fn with_segment_cache(mut self, bytes: usize) -> Self {
        self.cache.budget = bytes;
        self
    }

    /// What the segment cache holds and how often it answered.
    #[must_use]
    pub fn segment_cache(&self) -> SegmentCacheStats {
        use std::sync::atomic::Ordering::Relaxed;
        let s = self.cache.lock();
        SegmentCacheStats {
            bytes: s.bytes,
            files: s.files.len(),
            hits: self.cache.hits.load(Relaxed),
            misses: self.cache.misses.load(Relaxed),
        }
    }

    fn next_cache_id(&self) -> u64 {
        self.cache
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
            cache_id: self.next_cache_id(),
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
            cache_id: self.next_cache_id(),
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
        rows: Option<core::ops::Range<usize>>,
    ) -> Result<Vec<NullableColumn>, TierError> {
        let file = self.open_segment(cold)?;
        self.scan_segment(&file, cold, projection, rows)
    }

    /// The segment file of `cold`, opened: from the cache, or read and its footer parsed (and
    /// then kept, within the cache's budget).
    fn open_segment(&self, cold: &ColdChunk) -> Result<vortex::file::VortexFile, TierError> {
        if let Some(file) = self.cache.get(cold.cache_id) {
            return Ok(file);
        }
        let bytes = std::fs::read(&cold.path).map_err(io_err(&cold.path))?;
        let len = bytes.len();
        let file = self
            .session
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))?;
        if self.cache.budget > 0 {
            self.cache.put(cold.cache_id, file.clone(), len);
        }
        Ok(file)
    }

    /// Decode `projection` (all rows, or the rows `rows`) of an opened segment.
    fn scan_segment(
        &self,
        file: &vortex::file::VortexFile,
        cold: &ColdChunk,
        projection: &[usize],
        rows: Option<core::ops::Range<usize>>,
    ) -> Result<Vec<NullableColumn>, TierError> {
        let fields = cold.meta.schema.fields();
        let names: Vec<String> = projection.iter().map(|&i| fields[i].name.clone()).collect();
        let expr = select(names.iter().map(String::as_str).collect::<Vec<_>>(), root())
            .bind(file.dtype())?
            .optimize_recursive()?;
        let mut scan = file.scan()?.with_projection(expr);
        if let Some(rows) = rows {
            scan = scan.with_row_range(rows.start as u64..rows.end as u64);
        }
        let array = scan.into_array_iter(&self.runtime)?.read_all()?;
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
        self.decode_rows(projection, 0..self.meta.num_rows)
    }
    /// Only the rows asked for are decoded: the slice of a compressed array is taken in constant
    /// time and decoding it touches the blocks it covers.
    fn decode_rows(
        &self,
        projection: &[usize],
        rows: core::ops::Range<usize>,
    ) -> Result<Vec<NullableColumn>, DecodeError> {
        let fields = self.meta.schema.fields();
        let err = |msg: String| DecodeError {
            tier: Tier::Warm,
            id: self.meta.id,
            msg,
        };
        projection
            .iter()
            .map(|&i| {
                let (Some(f), Some(column)) = (fields.get(i), self.columns.get(i)) else {
                    return Err(err(format!("column {i} out of range")));
                };
                let rows = ankaa_store::clamp_rows(&rows, column.len());
                let part = column.slice(rows).map_err(|e| err(e.to_string()))?;
                self.enc
                    .decompress_column(&f.name, f.ty, &part)
                    .map_err(|e| err(e.to_string()))
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
    /// Key of this chunk in the encoder's segment cache.
    cache_id: u64,
}

impl Drop for ColdChunk {
    /// The chunk is gone (retention, a rewritten segment): so is its cached file.
    fn drop(&mut self) {
        self.enc.cache.remove(self.cache_id);
    }
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
            .read_cold(self, projection, None)
            .map_err(|e| DecodeError {
                tier: Tier::Cold,
                id: self.meta.id,
                msg: e.to_string(),
            })
    }
    /// The file's layout is asked for the row range: only the pieces of each projected column
    /// that hold those rows are decoded.
    fn decode_rows(
        &self,
        projection: &[usize],
        rows: core::ops::Range<usize>,
    ) -> Result<Vec<NullableColumn>, DecodeError> {
        let rows = ankaa_store::clamp_rows(&rows, self.meta.num_rows);
        self.enc
            .read_cold(self, projection, Some(rows))
            .map_err(|e| DecodeError {
                tier: Tier::Cold,
                id: self.meta.id,
                msg: e.to_string(),
            })
    }
    /// The segment file is read and its footer parsed once for both steps (finding the rows in
    /// the timestamps, decoding the projection for them): opening costs more than either.
    fn decode_time_range(
        &self,
        projection: &[usize],
        lo: i64,
        hi: i64,
        limit: usize,
    ) -> Result<(core::ops::Range<usize>, Vec<NullableColumn>), DecodeError> {
        let err = |e: TierError| DecodeError {
            tier: Tier::Cold,
            id: self.meta.id,
            msg: e.to_string(),
        };
        let total = self.meta.num_rows;
        let inside = lo <= self.meta.ts_min && self.meta.ts_max <= hi;
        if inside && projection.is_empty() {
            return Ok((0..total.min(limit), Vec::new()));
        }
        let file = self.enc.open_segment(self).map_err(err)?;
        let rows = if inside {
            0..total.min(limit)
        } else {
            let ts = self
                .enc
                .scan_segment(&file, self, &[0], None)
                .map_err(err)?;
            let ts = ts
                .first()
                .and_then(|(c, _)| c.as_i64())
                .ok_or_else(|| err(TierError::Vortex("column 0 is not the timestamp".into())))?;
            ankaa_store::time_rows(ts, lo, hi, limit)
        };
        if rows.is_empty() || projection.is_empty() {
            return Ok((rows, Vec::new()));
        }
        let part = (rows.len() < total).then(|| rows.clone());
        let cols = self
            .enc
            .scan_segment(&file, self, projection, part)
            .map_err(err)?;
        Ok((rows, cols))
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

    /// Cold chunks read again come from the segment cache, the same values as from the file;
    /// the cache stays inside its budget, forgets a dropped chunk and is off by default.
    #[test]
    fn the_segment_cache_serves_repeated_reads_within_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let chunks: Vec<Chunk> = (0..6).map(|_| chunk(4_000)).collect();
        let plain = Arc::new(Encoder::new());
        let one = plain
            .write_cold(
                dir.path(),
                &plain.encode_warm("size", 0, &chunks[0]).unwrap(),
            )
            .unwrap();
        let size = usize::try_from(one.file_size).unwrap();
        one.decode(&[0]).unwrap();
        one.decode(&[0]).unwrap();
        assert_eq!(plain.segment_cache(), SegmentCacheStats::default());

        // room for three segments and a half
        let enc = Arc::new(Encoder::new().with_segment_cache(size * 7 / 2));
        let mut cold: Vec<Arc<ColdChunk>> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let warm = enc.encode_warm("t", u32::try_from(i).unwrap(), c).unwrap();
                enc.write_cold(dir.path(), &warm).unwrap()
            })
            .collect();
        let want = chunks[0].column(2).unwrap();
        for _ in 0..3 {
            assert!(same(&cold[0].decode(&[2]).unwrap()[0], want));
            let part = cold[0].decode_rows(&[2], 10..20).unwrap();
            assert!(same(&part[0].0, &want.copy_range(10..20)));
        }
        let s = enc.segment_cache();
        assert_eq!((s.files, s.misses, s.hits), (1, 1, 5), "{s:?}");
        // more segments than fit: the budget holds, the most recently used stay
        for c in &cold {
            c.decode(&[0]).unwrap();
        }
        let s = enc.segment_cache();
        assert!(
            s.bytes <= size * 7 / 2 && s.files >= 2 && s.files <= 3,
            "{s:?}"
        );
        let hits = s.hits;
        cold[5].decode(&[1]).unwrap();
        assert_eq!(enc.segment_cache().hits, hits + 1);
        // a dropped chunk leaves the cache
        let before = enc.segment_cache();
        cold.pop();
        let after = enc.segment_cache();
        assert_eq!(after.files, before.files - 1);
        assert!(after.bytes < before.bytes);
    }

    /// A sensors chunk with `NULL`s in three columns.
    fn sensors(rows: i64) -> Chunk {
        let schema = Schema::new(vec![
            ankaa_store::Field::new("ts", ColumnType::TimestampNs),
            ankaa_store::Field::new("device", ColumnType::DictU32),
            ankaa_store::Field::nullable("temp", ColumnType::Float64),
            ankaa_store::Field::nullable("count", ColumnType::Int64),
            ankaa_store::Field::nullable("state", ColumnType::U8),
        ])
        .unwrap();
        let mut c = Chunk::new(ChunkId(3), schema);
        for i in 0..rows {
            c.append_row(&[
                Value::I64(1_000 + i * 10),
                Value::U32((i % 4) as u32),
                if i % 7 == 0 {
                    Value::Null
                } else {
                    Value::F64(i as f64 / 10.0)
                },
                if i % 11 == 0 {
                    Value::Null
                } else {
                    Value::I64(i * i)
                },
                if (2_000..2_100).contains(&i) {
                    Value::Null
                } else {
                    Value::U8((i % 3) as u8)
                },
            ])
            .unwrap();
        }
        c.seal();
        c
    }

    /// Decoding a row range gives exactly those rows of the whole decode, values and `NULL`s,
    /// in the warm and the cold tier and through the schema-evolution adapter: at block edges
    /// (1,024 rows), for one row, for the last row, for an empty range and beyond the end.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn a_row_range_decodes_like_the_rows_of_the_whole() {
        let enc = Arc::new(Encoder::new());
        let dir = tempfile::tempdir().unwrap();
        let ranges = [
            0..0,
            0..1,
            0..1_024,
            1..1_023,
            1_023..1_025,
            1_024..2_048,
            1_999..2_101,
            2_050..2_051,
            4_999..5_000,
            3_000..5_000,
            0..5_000,
            4_990..9_000,
            7_000..8_000,
        ];
        for (table, chunk) in [("trades", chunk(5_000)), ("sensors", sensors(5_000))] {
            let all: Vec<usize> = (0..chunk.schema().fields().len()).collect();
            let warm = enc.encode_warm(table, 0, &chunk).unwrap();
            let cold = enc.write_cold(dir.path(), &warm).unwrap();
            // read as a table that gained a nullable column since the chunk was written
            let mut fields = chunk.schema().fields().to_vec();
            fields.push(ankaa_store::Field::nullable("added", ColumnType::Float64));
            let evolved = ankaa_store::Evolved::wrap(
                Arc::clone(&warm) as Arc<dyn EncodedChunk>,
                &Schema::new(fields).unwrap(),
            )
            .unwrap();
            let mut wide = all.clone();
            wide.push(all.len());
            let tiers: [(&str, &dyn EncodedChunk, &[usize]); 3] = [
                ("warm", warm.as_ref(), &all),
                ("cold", cold.as_ref(), &all),
                ("evolved", evolved.as_ref(), &wide),
            ];
            for (tier, e, projection) in tiers {
                let whole = e.decode_nullable(projection).unwrap();
                for r in &ranges {
                    let part = e.decode_rows(projection, r.clone()).unwrap();
                    let rows = ankaa_store::clamp_rows(r, 5_000);
                    assert_eq!(part.len(), whole.len());
                    for (i, ((col, valid), (wcol, wvalid))) in part.iter().zip(&whole).enumerate() {
                        let what = format!("{table} {tier} column {i} rows {r:?}");
                        assert_eq!(col.len(), rows.len(), "{what}");
                        assert!(same(col, &wcol.copy_range(rows.clone())), "{what}");
                        // a range without a NULL may report no validity at all
                        let want: Vec<bool> = wvalid
                            .as_ref()
                            .map_or_else(|| vec![true; rows.len()], |v| v[rows.clone()].to_vec());
                        let got = valid.clone().unwrap_or_else(|| vec![true; rows.len()]);
                        assert_eq!(got, want, "{what}");
                    }
                }
                // by time: the rows a filter over the whole decode keeps, cut to the limit
                let ts = whole[0].0.as_i64().unwrap().to_vec();
                let (first, last) = (ts[0], ts[ts.len() - 1]);
                for (lo, hi, limit) in [
                    (i64::MIN, i64::MAX, usize::MAX),
                    (first, last, usize::MAX),
                    (first, last, 10),
                    (ts[1_000], ts[1_359], usize::MAX),
                    (ts[1_000] + 1, ts[1_359] - 1, usize::MAX),
                    (ts[4_000], i64::MAX, 7),
                    (i64::MIN, ts[3], usize::MAX),
                    (last, last, usize::MAX),
                    (last + 1, i64::MAX, usize::MAX),
                    (i64::MIN, first - 1, usize::MAX),
                    (ts[200], ts[100], usize::MAX),
                ] {
                    let what = format!("{table} {tier} ts in [{lo}, {hi}] limit {limit}");
                    let want: Vec<usize> = (0..ts.len())
                        .filter(|&i| lo <= ts[i] && ts[i] <= hi)
                        .take(limit)
                        .collect();
                    let (rows, cols) = e.decode_time_range(&[2, 0], lo, hi, limit).unwrap();
                    assert_eq!(rows.len(), want.len(), "{what}");
                    if let (Some(&a), Some(&b)) = (want.first(), want.last()) {
                        assert_eq!(rows, a..b + 1, "{what}");
                        assert!(
                            same(&cols[0].0, &whole[2].0.copy_range(rows.clone())),
                            "{what}"
                        );
                        assert!(
                            same(&cols[1].0, &whole[0].0.copy_range(rows.clone())),
                            "{what}"
                        );
                    } else {
                        assert!(cols.is_empty(), "{what}");
                    }
                    // no column asked for: the rows are still found
                    let (rows, cols) = e.decode_time_range(&[], lo, hi, limit).unwrap();
                    assert_eq!((rows.len(), cols.len()), (want.len(), 0), "{what}");
                }
                // the newest row is in the metadata, as the chunk holds it
                let meta = e.meta();
                assert_eq!(meta.last_row.len(), projection.len(), "{table} {tier}");
                for (i, bits) in meta.last_row.iter().enumerate() {
                    let stored = chunk.value(i, 4_999).and_then(ankaa_store::value_bits);
                    assert_eq!(
                        *bits,
                        if i < all.len() { stored } else { None },
                        "{table} {tier} column {i}"
                    );
                }
                // a projection in another order, a column twice
                let part = e.decode_rows(&[2, 0, 2], 100..200).unwrap();
                assert!(same(&part[0].0, &whole[2].0.copy_range(100..200)));
                assert!(same(&part[1].0, &whole[0].0.copy_range(100..200)));
                assert!(same(&part[2].0, &part[0].0));
            }
        }
    }
}
