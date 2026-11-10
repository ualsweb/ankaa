//! Chunk ↔ Arrow conversion.
//!
//! Columns are handed to Arrow **in place**: an Arrow buffer points at the chunk's own vector
//! and holds a reference that keeps it alive, so a scan copies no column data. A hot chunk
//! behind an `Arc` never changes (the open chunk is exposed through a sealed copy), which is
//! what makes sharing it sound; decoded columns of a warm or cold chunk are moved in.
//! Every batch is built from its own byte range, so its reported memory is its own size and
//! not the size of the chunk it was cut from (`DataFusion` charges operators by that number).
//!
//! The price of sharing: a batch that is still referenced keeps its whole chunk in memory, also
//! after the store has tiered or evicted that chunk. Results are short-lived (a query deadline
//! bounds them on the server), so this is a bounded delay in freeing memory, not a leak.

use std::sync::Arc;

use ankaa_store::{Chunk, Column, ColumnType, Schema as AnkaaSchema};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

/// Arrow schema for an ANKAA schema. Fixed-point columns are exposed as `Int64` ticks with the
/// scale recorded in field metadata (`ankaa.scale`); a `Decimal128` view is a later option.
#[must_use]
pub fn arrow_schema(s: &AnkaaSchema) -> SchemaRef {
    let fields = s
        .fields()
        .iter()
        .map(|f| {
            let (dt, meta) = match f.ty {
                ColumnType::TimestampNs => (
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    None,
                ),
                ColumnType::FixedPoint { scale } => (
                    DataType::Int64,
                    Some(("ankaa.scale".to_string(), scale.to_string())),
                ),
                ColumnType::Int64 => (DataType::Int64, None),
                ColumnType::DictU32 => (DataType::UInt32, None),
                ColumnType::U8 => (DataType::UInt8, None),
                ColumnType::Float64 => (DataType::Float64, None),
            };
            let mut field = Field::new(&f.name, dt, f.nullable);
            if let Some((k, v)) = meta {
                field = field.with_metadata([(k, v)].into_iter().collect());
            }
            field
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Plain values of one column as bytes, with whatever keeps them alive.
enum Owner {
    /// One column (by index) of a shared hot chunk.
    Hot(Arc<Chunk>, usize),
    /// A decoded column, owned.
    Decoded(Column),
}

fn column_bytes(c: &Column) -> &[u8] {
    match c {
        Column::I64(v) => bytemuck::cast_slice(v),
        Column::U32(v) => bytemuck::cast_slice(v),
        Column::U8(v) => v,
        Column::F64(v) => bytemuck::cast_slice(v),
    }
}

impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Hot(chunk, i) => chunk.column(*i).map_or(&[], column_bytes),
            Self::Decoded(c) => column_bytes(c),
        }
    }
}

/// Physical layout of a column's values.
#[derive(Debug, Clone, Copy)]
enum Phys {
    I64,
    U32,
    U8,
    F64,
}

impl Phys {
    fn of(c: &Column) -> Self {
        match c {
            Column::I64(_) => Self::I64,
            Column::U32(_) => Self::U32,
            Column::U8(_) => Self::U8,
            Column::F64(_) => Self::F64,
        }
    }

    const fn width(self) -> usize {
        match self {
            Self::I64 | Self::F64 => 8,
            Self::U32 => 4,
            Self::U8 => 1,
        }
    }
}

/// One projected column: values in place, plus which rows hold one (`None`: all).
struct Col {
    ty: ColumnType,
    phys: Phys,
    bytes: bytes::Bytes,
    nulls: Option<NullBuffer>,
}

fn null_buffer(valid: Option<&[bool]>) -> Option<NullBuffer> {
    valid.filter(|v| v.contains(&false)).map(NullBuffer::from)
}

/// The projected columns of one chunk, restricted to a row range, ready to be cut into
/// batches without copying.
pub struct ChunkColumns {
    cols: Vec<Col>,
    rows: core::ops::Range<usize>,
    schema: SchemaRef,
}

impl std::fmt::Debug for ChunkColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkColumns")
            .field("columns", &self.cols.len())
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

impl ChunkColumns {
    /// Columns `projection` of a hot chunk, shared with the chunk.
    pub fn hot(
        chunk: &Arc<Chunk>,
        projection: &[usize],
        rows: core::ops::Range<usize>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let mut cols = Vec::with_capacity(projection.len());
        for &ci in projection {
            let col = chunk
                .column(ci)
                .ok_or_else(|| DataFusionError::Internal(format!("column {ci} missing")))?;
            check_rows(&rows, col.len())?;
            let ty = chunk.schema().fields()[ci].ty;
            let bytes = bytes::Bytes::from_owner(Owner::Hot(Arc::clone(chunk), ci));
            cols.push(Col {
                ty,
                phys: Phys::of(col),
                bytes,
                nulls: null_buffer(chunk.validity(ci)),
            });
        }
        Ok(Self { cols, rows, schema })
    }

    /// Already-selected `(type, column, validity)` triples (the decoded columns of an encoded
    /// chunk), taken over without a copy of the values.
    pub fn decoded(
        columns: Vec<(ColumnType, Column, Option<Vec<bool>>)>,
        rows: core::ops::Range<usize>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let mut cols = Vec::with_capacity(columns.len());
        for (ty, col, valid) in columns {
            check_rows(&rows, col.len())?;
            if valid.as_ref().is_some_and(|v| v.len() != col.len()) {
                return Err(DataFusionError::Internal(
                    "validity length differs from the column".into(),
                ));
            }
            cols.push(Col {
                ty,
                phys: Phys::of(&col),
                nulls: null_buffer(valid.as_deref()),
                bytes: bytes::Bytes::from_owner(Owner::Decoded(col)),
            });
        }
        Ok(Self { cols, rows, schema })
    }

    /// Rows in the range.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// The same columns without the first `skip` rows of the range.
    #[must_use]
    pub fn tail(self, skip: usize) -> Self {
        let start = self.rows.start + skip.min(self.rows.len());
        Self {
            rows: start..self.rows.end,
            ..self
        }
    }

    /// Batch of `len` rows starting `offset` rows into the range (clamped to the range).
    pub fn batch(&self, offset: usize, len: usize) -> Result<RecordBatch> {
        let start = self.rows.start + offset.min(self.rows.len());
        let len = len.min(self.rows.end - start);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.cols.len());
        for c in &self.cols {
            let w = c.phys.width();
            // a byte range of its own per batch: no copy, and the batch accounts for exactly
            // the bytes it shows
            let buf = Buffer::from(c.bytes.slice(start * w..(start + len) * w));
            let nulls = c
                .nulls
                .as_ref()
                .map(|n| n.slice(start, len))
                .filter(|n| n.null_count() > 0);
            let arr: ArrayRef = match (c.phys, c.ty) {
                (Phys::I64, ColumnType::TimestampNs) => Arc::new(
                    TimestampNanosecondArray::new(ScalarBuffer::from(buf), None)
                        .with_timezone("UTC"),
                ),
                (Phys::I64, _) => Arc::new(Int64Array::new(ScalarBuffer::from(buf), nulls)),
                (Phys::U32, _) => Arc::new(UInt32Array::new(ScalarBuffer::from(buf), nulls)),
                (Phys::U8, _) => Arc::new(UInt8Array::new(ScalarBuffer::from(buf), nulls)),
                (Phys::F64, _) => Arc::new(Float64Array::new(ScalarBuffer::from(buf), nulls)),
            };
            arrays.push(arr);
        }
        // `SELECT count(*)` projects no columns: the batch must still carry its row count.
        let opts = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(len));
        RecordBatch::try_new_with_options(self.schema.clone(), arrays, &opts)
            .map_err(DataFusionError::from)
    }
}

fn check_rows(rows: &core::ops::Range<usize>, len: usize) -> Result<()> {
    if rows.start > rows.end || rows.end > len {
        return Err(DataFusionError::Internal(format!(
            "row range {rows:?} beyond column length {len}"
        )));
    }
    Ok(())
}

/// Convert `rows` of the given columns of a hot chunk into one `RecordBatch` (no copy).
pub fn chunk_to_batch(
    chunk: &Arc<Chunk>,
    projection: &[usize],
    rows: core::ops::Range<usize>,
    out_schema: SchemaRef,
) -> Result<RecordBatch> {
    let cols = ChunkColumns::hot(chunk, projection, rows, out_schema)?;
    cols.batch(0, cols.num_rows())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankaa_store::{Field as AnkaaField, Value};
    use arrow::array::Array;

    fn chunk(n: i64) -> Arc<Chunk> {
        let schema = AnkaaSchema::new(vec![
            AnkaaField::new("ts", ColumnType::TimestampNs),
            AnkaaField::new("symbol", ColumnType::DictU32),
            AnkaaField::new("flag", ColumnType::U8),
            AnkaaField::new("x", ColumnType::Float64),
        ])
        .unwrap();
        let mut c = Chunk::new(ankaa_store::ChunkId(1), schema);
        for i in 0..n {
            c.append_row(&[
                Value::I64(1_000 + i),
                Value::U32(u32::try_from(i % 7).unwrap()),
                Value::U8(u8::try_from(i % 3).unwrap()),
                Value::F64(0.5 * f64::from(i32::try_from(i).unwrap())),
            ])
            .unwrap();
        }
        c.seal();
        Arc::new(c)
    }

    #[test]
    fn batches_share_the_chunk_and_report_their_own_size() {
        let c = chunk(1_000);
        let schema = arrow_schema(c.schema());
        let cols = ChunkColumns::hot(&c, &[0, 1, 2, 3], 100..900, schema).unwrap();
        assert_eq!(cols.num_rows(), 800);
        let b = cols.batch(10, 50).unwrap();
        assert_eq!(b.num_rows(), 50);
        let ts = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_110);
        assert_eq!(ts.value(49), 1_159);
        // in place: the Arrow values live inside the chunk's own vector
        let Column::I64(v) = c.column(0).unwrap() else {
            panic!("ts is i64")
        };
        assert!(core::ptr::eq(ts.values().as_ptr(), &raw const v[110]));
        // 50 rows of 8 + 4 + 1 + 8 bytes, not the 1,000-row chunk
        assert_eq!(b.get_array_memory_size(), 50 * 21 + array_overhead(&b));
        let sym = b.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(sym.value(0), 110 % 7);
        let x = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((x.value(1) - 55.5).abs() < f64::EPSILON);
        // a batch outlives the chunk handle it was made from
        drop(cols);
        drop(c);
        assert_eq!(ts.value(49), 1_159);
    }

    fn array_overhead(b: &RecordBatch) -> usize {
        b.columns()
            .iter()
            .map(|a| a.get_array_memory_size() - a.to_data().get_buffer_memory_size())
            .sum()
    }

    #[test]
    fn clamps_and_rejects_bad_ranges() {
        let c = chunk(100);
        let schema = arrow_schema(c.schema());
        let cols =
            ChunkColumns::hot(&c, &[0], 0..100, Arc::new(schema.project(&[0]).unwrap())).unwrap();
        assert_eq!(cols.batch(90, 50).unwrap().num_rows(), 10);
        assert_eq!(cols.batch(500, 50).unwrap().num_rows(), 0);
        assert!(ChunkColumns::hot(&c, &[0], 0..101, schema.clone()).is_err());
        assert!(ChunkColumns::hot(&c, &[9], 0..10, schema.clone()).is_err());
        // no columns: the row count survives
        let none =
            ChunkColumns::hot(&c, &[], 0..100, Arc::new(schema.project(&[]).unwrap())).unwrap();
        assert_eq!(none.batch(0, 64).unwrap().num_rows(), 64);
    }

    #[test]
    fn decoded_columns_move_in() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let cols = ChunkColumns::decoded(
            vec![(ColumnType::Int64, Column::I64((0..1_000).collect()), None)],
            10..20,
            schema,
        )
        .unwrap();
        let b = cols.batch(0, 100).unwrap();
        drop(cols);
        let v = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(v.values().to_vec(), (10..20).collect::<Vec<i64>>());
    }
}
