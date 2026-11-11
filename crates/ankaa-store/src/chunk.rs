//! Append-only chunk with watermark, sealing and zone map.

use crate::column::{Column, ColumnStats, Value};
use crate::schema::Schema;
use crate::{CHUNK_ROWS, VECTOR_LEN};

/// Chunk identifier (monotonic per partition).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ChunkId(pub u64);

/// Lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    /// Accepting appends.
    Open,
    /// Immutable; zone map frozen.
    Sealed,
}

/// Minimum, maximum and wrapping sum of every 1,024-row block of an `i64` column (small
/// materialised aggregates): 24 bytes per 8 KiB of data. An aggregate over a row range reads
/// the blocks inside the range from here and only the two partial blocks at its ends from the
/// column, so its cost does not grow with the range.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockStats {
    min: Vec<i64>,
    max: Vec<i64>,
    sum: Vec<i64>,
}

impl BlockStats {
    /// Rows per block (the vector size of the kernels).
    pub const ROWS: usize = ankaa_simd::VECTOR_LEN;

    fn of(values: &[i64]) -> Self {
        let n = values.len().div_ceil(Self::ROWS);
        let mut s = Self {
            min: Vec::with_capacity(n),
            max: Vec::with_capacity(n),
            sum: Vec::with_capacity(n),
        };
        for block in values.chunks(Self::ROWS) {
            let (lo, hi, sum) = scan(block);
            s.min.push(lo);
            s.max.push(hi);
            s.sum.push(sum);
        }
        s
    }

    /// `(min, max, wrapping sum)` of the whole column (`(i64::MAX, i64::MIN, 0)` when empty).
    #[must_use]
    pub fn total(&self) -> (i64, i64, i64) {
        self.blocks(0..self.sum.len())
    }

    fn blocks(&self, blocks: core::ops::Range<usize>) -> (i64, i64, i64) {
        let lo = self.min[blocks.clone()]
            .iter()
            .copied()
            .min()
            .unwrap_or(i64::MAX);
        let hi = self.max[blocks.clone()]
            .iter()
            .copied()
            .max()
            .unwrap_or(i64::MIN);
        let sum = self.sum[blocks]
            .iter()
            .fold(0i64, |a, &b| a.wrapping_add(b));
        (lo, hi, sum)
    }

    /// `(min, max, wrapping sum)` of rows `rows` of `values`, the column these statistics
    /// were built from: whole blocks from the statistics, the partial blocks at both ends
    /// from the values.
    #[must_use]
    pub fn range(&self, values: &[i64], rows: core::ops::Range<usize>) -> (i64, i64, i64) {
        let rows = rows.start.min(values.len())..rows.end.min(values.len());
        let first = rows.start.div_ceil(Self::ROWS);
        // the last block may be short: it counts as whole when the range ends with the column
        let last = if rows.end == values.len() {
            self.sum.len()
        } else {
            rows.end / Self::ROWS
        };
        if first >= last {
            return scan(&values[rows]);
        }
        let head = scan(&values[rows.start..first * Self::ROWS]);
        let middle = self.blocks(first..last);
        let tail_from = (last * Self::ROWS).min(rows.end);
        let tail = scan(&values[tail_from..rows.end]);
        (
            head.0.min(middle.0).min(tail.0),
            head.1.max(middle.1).max(tail.1),
            head.2.wrapping_add(middle.2).wrapping_add(tail.2),
        )
    }
}

/// `(min, max, wrapping sum)` of a slice, with the SIMD kernels.
fn scan(values: &[i64]) -> (i64, i64, i64) {
    let kernels = ankaa_simd::backend();
    values
        .chunks(ankaa_simd::VECTOR_LEN)
        .fold((i64::MAX, i64::MIN, 0i64), |(lo, hi, sum), v| {
            let (a, b) = kernels.min_max_i64(v);
            (lo.min(a), hi.max(b), sum.wrapping_add(kernels.sum_i64(v)))
        })
}

/// Statistics per column, frozen at seal time.
#[derive(Debug, Clone, PartialEq)]
pub struct ZoneMap {
    /// Min/max: one entry per schema field, in order.
    pub per_column: Vec<ColumnStats>,
    /// Per-block aggregates of the `i64` columns (`None` for the others), same order.
    pub blocks: Vec<Option<BlockStats>>,
    /// `Some((first, last))` values of the last column when it is an integer and the rows are
    /// in `(timestamp, last column)` order: the order of arrival of a feed whose last column is
    /// a sequence number. A scan may then promise that order instead of sorting for it.
    pub tie_order: Option<(i64, i64)>,
}

impl ZoneMap {
    /// Statistics of `columns` (all values valid).
    #[must_use]
    pub fn of(columns: &[Column]) -> Self {
        Self::of_valid(columns, &[])
    }

    /// Statistics of `columns`, whose `validity[i]` (when given) marks the rows of column `i`
    /// that hold a value: minimum and maximum over those; no block aggregates for a column
    /// with a `NULL` (an aggregate over it reads its rows).
    #[must_use]
    pub fn of_valid(columns: &[Column], validity: &[Option<Vec<bool>>]) -> Self {
        let valid = |i: usize| validity.get(i).and_then(Option::as_deref);
        let tie_order = match (
            columns.first().and_then(Column::as_i64),
            columns.last().and_then(Column::as_i64),
        ) {
            (Some(ts), Some(tie)) if columns.len() > 1 && !ts.is_empty() => {
                let ordered = (1..ts.len()).all(|i| (ts[i - 1], tie[i - 1]) <= (ts[i], tie[i]));
                ordered.then(|| (tie[0], tie[tie.len() - 1]))
            }
            _ => None,
        };
        Self {
            per_column: columns
                .iter()
                .enumerate()
                .map(|(i, c)| c.stats_where(valid(i)))
                .collect(),
            blocks: columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let complete = valid(i).is_none_or(|v| v.iter().all(|&ok| ok));
                    c.as_i64().filter(|_| complete).map(BlockStats::of)
                })
                .collect(),
            tie_order,
        }
    }

    /// Per-block aggregates of `i64` column `col`.
    #[must_use]
    pub fn block_stats(&self, col: usize) -> Option<&BlockStats> {
        self.blocks.get(col).and_then(Option::as_ref)
    }

    /// True when an `i64` predicate range `[lo, hi]` on column `col` may match rows here.
    #[must_use]
    pub fn may_overlap_i64(&self, col: usize, lo: i64, hi: i64) -> bool {
        match self.per_column.get(col) {
            Some(ColumnStats::I64 { min, max }) => !(hi < *min || lo > *max),
            _ => true, // unknown type or missing stats: cannot prune
        }
    }
}

/// Append errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AppendError {
    /// The chunk is sealed.
    #[error("chunk is sealed")]
    Sealed,
    /// The chunk already holds `CHUNK_ROWS` rows.
    #[error("chunk is full ({0} rows)")]
    Full(usize),
    /// Row has the wrong number of values.
    #[error("row arity {got} does not match schema arity {want}")]
    Arity {
        /// Values supplied.
        got: usize,
        /// Values expected.
        want: usize,
    },
    /// A value's type does not match its column.
    #[error("type mismatch at column {0}")]
    Type(usize),
    /// Timestamp earlier than the watermark (rows must arrive in order per partition).
    #[error("timestamp {ts} is below the watermark {watermark}")]
    OutOfOrder {
        /// Offending timestamp.
        ts: i64,
        /// Current watermark.
        watermark: i64,
    },
    /// First value must be an `i64` timestamp.
    #[error("first value must be an i64 timestamp")]
    NoTimestamp,
}

/// The columns of some rows and, per column, which rows hold a value (`None`: all of them).
pub type ChunkPart = (Vec<Column>, Vec<Option<Vec<bool>>>);

/// An append-only columnar chunk.
#[derive(Debug, Clone)]
pub struct Chunk {
    id: ChunkId,
    schema: Schema,
    columns: Vec<Column>,
    /// Per column: which rows hold a value (`None` = all of them; created at the first `NULL`).
    validity: Vec<Option<Vec<bool>>>,
    rows: usize,
    watermark: i64,
    state: ChunkState,
    zone_map: Option<ZoneMap>,
    /// WAL LSN range of the frames that produced this chunk (`0` = unknown / no WAL).
    first_lsn: u64,
    last_lsn: u64,
    /// Rows contributed by `last_lsn` (a record may straddle two chunks; recovery needs the split).
    last_lsn_rows: u32,
}

impl Chunk {
    /// New open chunk (up to `CHUNK_ROWS` rows). Columns start empty and grow with the data:
    /// a table with many partitions (one per key) must not reserve a full chunk for every key
    /// that has only a handful of rows.
    #[must_use]
    pub fn new(id: ChunkId, schema: Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| Column::with_capacity(f.ty, 0))
            .collect();
        let validity = vec![None; schema.len()];
        Self {
            id,
            schema,
            columns,
            validity,
            rows: 0,
            watermark: i64::MIN,
            state: ChunkState::Open,
            zone_map: None,
            first_lsn: 0,
            last_lsn: 0,
            last_lsn_rows: 0,
        }
    }

    /// One sealed chunk holding the rows of `parts`, in that order: what compaction makes of
    /// several small chunks of a partition. Every part gives its columns (the schema's, all of
    /// one length) and its validity per column (`None`: no `NULL`). `lsn` is the WAL
    /// bookkeeping of the result: `(first, last, rows of the last record)`.
    ///
    /// Refused (`Err` says why) when the parts do not fit the schema, hold more than
    /// [`crate::CHUNK_ROWS`] rows together, or their timestamps are not in order across the
    /// whole: a chunk's rows are in time order, and every reader relies on it.
    pub fn merged(
        id: ChunkId,
        schema: Schema,
        parts: &[ChunkPart],
        lsn: (u64, u64, u32),
    ) -> Result<Self, String> {
        let total: usize = parts
            .iter()
            .map(|(c, _)| c.first().map_or(0, Column::len))
            .sum();
        if total > crate::CHUNK_ROWS {
            return Err(format!("{total} rows do not fit one chunk"));
        }
        let mut columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|f| Column::with_capacity(f.ty, total))
            .collect();
        let mut validity: Vec<Option<Vec<bool>>> = vec![None; schema.len()];
        let mut rows = 0usize;
        for (cols, valid) in parts {
            if cols.len() != columns.len() || valid.len() != columns.len() {
                return Err("a part has another number of columns".into());
            }
            let n = cols.first().map_or(0, Column::len);
            for (i, (col, v)) in cols.iter().zip(valid).enumerate() {
                if col.len() != n || !columns[i].extend_from(col) {
                    return Err(format!("column {i} of a part does not fit the schema"));
                }
                match (v, &mut validity[i]) {
                    (Some(v), _) if v.len() != n => {
                        return Err(format!("validity of column {i} has another length"));
                    }
                    (Some(v), Some(all)) => all.extend_from_slice(v),
                    (Some(v), slot @ None) => {
                        // the rows before this part all hold a value
                        let mut all = vec![true; rows];
                        all.extend_from_slice(v);
                        *slot = Some(all);
                    }
                    (None, Some(all)) => all.resize(rows + n, true),
                    (None, None) => {}
                }
            }
            rows += n;
        }
        for (i, v) in validity.iter_mut().enumerate() {
            if v.as_ref().is_some_and(|v| !v.contains(&false)) {
                *v = None;
            } else if v.is_some() && !schema.fields()[i].nullable {
                return Err(format!("column {i} holds NULL and is not nullable"));
            }
        }
        let ts = columns
            .first()
            .and_then(Column::as_i64)
            .ok_or("the first column is not the timestamp")?;
        if ts.windows(2).any(|w| w[0] > w[1]) {
            return Err("the rows are not in time order".into());
        }
        let watermark = ts.last().copied().unwrap_or(i64::MIN);
        let zone_map = Some(ZoneMap::of_valid(&columns, &validity));
        Ok(Self {
            id,
            schema,
            columns,
            validity,
            rows,
            watermark,
            state: ChunkState::Sealed,
            zone_map,
            first_lsn: lsn.0,
            last_lsn: lsn.1,
            last_lsn_rows: lsn.2,
        })
    }

    /// Record that rows appended from now on came from WAL record `lsn` (ignored when 0).
    pub fn note_lsn(&mut self, lsn: u64) {
        if lsn == 0 {
            return;
        }
        if self.first_lsn == 0 {
            self.first_lsn = lsn;
        }
        if lsn != self.last_lsn {
            self.last_lsn = self.last_lsn.max(lsn);
            self.last_lsn_rows = 0;
        }
        self.last_lsn_rows = self.last_lsn_rows.saturating_add(1);
    }

    /// Rows in this chunk that came from the last WAL record (`lsn_range().1`).
    #[must_use]
    pub fn last_lsn_rows(&self) -> u32 {
        self.last_lsn_rows
    }

    /// `(first, last)` WAL LSNs that contributed rows (`(0, 0)` when unknown).
    #[must_use]
    pub fn lsn_range(&self) -> (u64, u64) {
        (self.first_lsn, self.last_lsn)
    }

    /// Identifier.
    #[must_use]
    pub fn id(&self) -> ChunkId {
        self.id
    }

    /// Schema.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Row count.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.rows
    }

    /// True when no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Number of complete 1,024-row vectors.
    #[must_use]
    pub fn num_full_vectors(&self) -> usize {
        self.rows / VECTOR_LEN
    }

    /// Maximum accepted timestamp (`i64::MIN` when empty).
    #[must_use]
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// Raise the watermark of an empty open chunk (used when rolling chunks so ordering carries over).
    pub fn set_watermark(&mut self, wm: i64) {
        debug_assert!(self.is_empty());
        self.watermark = self.watermark.max(wm);
    }

    /// Lifecycle state.
    #[must_use]
    pub fn state(&self) -> ChunkState {
        self.state
    }

    /// True when the chunk cannot take more rows.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.rows >= CHUNK_ROWS
    }

    /// Column `i` (slice over all rows).
    #[must_use]
    pub fn column(&self, i: usize) -> Option<&Column> {
        self.columns.get(i)
    }

    /// Column by name.
    #[must_use]
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.schema.index_of(name).and_then(|i| self.columns.get(i))
    }

    /// Zone map (only after sealing).
    #[must_use]
    pub fn zone_map(&self) -> Option<&ZoneMap> {
        self.zone_map.as_ref()
    }

    /// Append one row. Values must match the schema; timestamps must be `>= watermark`.
    pub fn append_row(&mut self, row: &[Value]) -> Result<(), AppendError> {
        if self.state == ChunkState::Sealed {
            return Err(AppendError::Sealed);
        }
        if self.is_full() {
            return Err(AppendError::Full(self.rows));
        }
        if row.len() != self.schema.len() {
            return Err(AppendError::Arity {
                got: row.len(),
                want: self.schema.len(),
            });
        }
        let Some(Value::I64(ts)) = row.first().copied() else {
            return Err(AppendError::NoTimestamp);
        };
        if ts < self.watermark {
            return Err(AppendError::OutOfOrder {
                ts,
                watermark: self.watermark,
            });
        }
        // Type-check everything before mutating anything, so a failed append leaves the
        // chunk unchanged (all columns stay the same length).
        self.check_types(row)?;
        let rows = self.rows;
        for (i, ((col, valid), v)) in self
            .columns
            .iter_mut()
            .zip(&mut self.validity)
            .zip(row)
            .enumerate()
        {
            if *v == Value::Null {
                col.push_null();
                valid.get_or_insert_with(|| vec![true; rows]).push(false);
            } else {
                let pushed = col.push(*v);
                debug_assert!(pushed, "column {i}");
                if let Some(valid) = valid {
                    valid.push(true);
                }
            }
        }
        self.rows += 1;
        self.watermark = ts;
        Ok(())
    }

    /// Add a row at its place in time, also when it is older than rows already here: after
    /// the last row with a timestamp at or below its own, so the chunk stays in time order and
    /// rows with equal timestamps stay in arrival order. Returns the row's position. The
    /// watermark stays the largest timestamp. Same checks as [`Self::append_row`] except the
    /// order.
    pub fn insert_row(&mut self, row: &[Value]) -> Result<usize, AppendError> {
        if self.state == ChunkState::Sealed {
            return Err(AppendError::Sealed);
        }
        if self.is_full() {
            return Err(AppendError::Full(self.rows));
        }
        if row.len() != self.schema.len() {
            return Err(AppendError::Arity {
                got: row.len(),
                want: self.schema.len(),
            });
        }
        let Some(Value::I64(ts)) = row.first().copied() else {
            return Err(AppendError::NoTimestamp);
        };
        self.check_types(row)?;
        let at = self.timestamps().partition_point(|&t| t <= ts);
        let rows = self.rows;
        for (i, ((col, valid), v)) in self
            .columns
            .iter_mut()
            .zip(&mut self.validity)
            .zip(row)
            .enumerate()
        {
            if *v == Value::Null {
                col.insert_null(at);
                valid
                    .get_or_insert_with(|| vec![true; rows])
                    .insert(at, false);
            } else {
                let inserted = col.insert(at, *v);
                debug_assert!(inserted, "column {i}");
                if let Some(valid) = valid {
                    valid.insert(at, true);
                }
            }
        }
        self.rows += 1;
        self.watermark = self.watermark.max(ts);
        Ok(at)
    }

    /// Every value of `row` fits its column: the type of the column, or `NULL` where the
    /// column is nullable.
    fn check_types(&self, row: &[Value]) -> Result<(), AppendError> {
        for (i, ((col, field), v)) in self
            .columns
            .iter()
            .zip(self.schema.fields())
            .zip(row)
            .enumerate()
        {
            let ok = match (col, v) {
                (_, Value::Null) => field.nullable,
                (Column::I64(_), Value::I64(_))
                | (Column::U32(_), Value::U32(_))
                | (Column::U8(_), Value::U8(_))
                | (Column::F64(_), Value::F64(_)) => true,
                _ => false,
            };
            if !ok {
                return Err(AppendError::Type(i));
            }
        }
        Ok(())
    }

    /// Which rows of column `i` hold a value (`None` = all of them).
    #[must_use]
    pub fn validity(&self, i: usize) -> Option<&[bool]> {
        self.validity.get(i).and_then(Option::as_deref)
    }

    /// Value of column `i` at `row`, `Value::Null` where there is none.
    #[must_use]
    pub fn value(&self, i: usize, row: usize) -> Option<Value> {
        if self
            .validity(i)
            .is_some_and(|v| !v.get(row).copied().unwrap_or(true))
        {
            return Some(Value::Null);
        }
        self.columns.get(i).and_then(|c| c.get(row))
    }

    /// Seal: no further appends; compute and freeze the zone map. Idempotent.
    pub fn seal(&mut self) -> &ZoneMap {
        if self.state == ChunkState::Open {
            if self.rows < CHUNK_ROWS {
                // sealed early (shutdown, forced checkpoint): give the unused capacity back
                for c in &mut self.columns {
                    c.shrink_to_fit();
                }
            }
            for valid in self.validity.iter_mut().flatten() {
                valid.shrink_to_fit();
            }
            self.zone_map = Some(ZoneMap::of_valid(&self.columns, &self.validity));
            self.state = ChunkState::Sealed;
        }
        self.zone_map.as_ref().expect("sealed chunk has a zone map")
    }

    /// A sealed copy of rows `rows` of this chunk, with a zone map of its own: one piece of
    /// the view queries get of a chunk that is still being written. It carries the chunk's id
    /// and no WAL bookkeeping (a piece is never persisted).
    #[must_use]
    pub fn copy_rows(&self, rows: core::ops::Range<usize>) -> Self {
        let rows = rows.start.min(self.rows)..rows.end.min(self.rows);
        let columns: Vec<Column> = self
            .columns
            .iter()
            .map(|c| c.copy_range(rows.clone()))
            .collect();
        let watermark = columns
            .first()
            .and_then(Column::as_i64)
            .and_then(|ts| ts.last().copied())
            .unwrap_or(i64::MIN);
        let validity: Vec<Option<Vec<bool>>> = self
            .validity
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|v| v[rows.clone()].to_vec())
                    .filter(|v| v.contains(&false))
            })
            .collect();
        let zone_map = Some(ZoneMap::of_valid(&columns, &validity));
        Self {
            id: self.id,
            schema: self.schema.clone(),
            columns,
            validity,
            rows: rows.len(),
            watermark,
            state: ChunkState::Sealed,
            zone_map,
            first_lsn: 0,
            last_lsn: 0,
            last_lsn_rows: 0,
        }
    }

    /// Timestamp column as a slice.
    #[must_use]
    pub fn timestamps(&self) -> &[i64] {
        self.columns[0]
            .as_i64()
            .expect("column 0 is the i64 timestamp")
    }

    /// Row range `[start, end)` whose timestamps fall in `[lo, hi]`, by binary search
    /// (timestamps are non-decreasing by construction).
    #[must_use]
    pub fn time_range_rows(&self, lo: i64, hi: i64) -> core::ops::Range<usize> {
        let ts = self.timestamps();
        let start = ts.partition_point(|&t| t < lo);
        let end = ts.partition_point(|&t| t <= hi);
        start..end.max(start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Block aggregates give the same answer as reading the rows, for every kind of range:
    /// inside one block, across blocks, aligned or not, to the end of a short last block.
    #[test]
    fn block_stats_match_the_rows() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        // under Miri (an interpreter) the short lengths cover every boundary case and the
        // kernels; the long ones and the many random ranges are for the native run
        let (lens, rounds): (&[usize], usize) = if cfg!(miri) {
            (&[0, 1, 1023, 1024, 1025], 10)
        } else {
            (&[0, 1, 1023, 1024, 1025, 5000, 65_536], 300)
        };
        for &len in lens {
            let values: Vec<i64> = (0..len)
                .map(|_| (next() % 2_000_001).cast_signed() - 1_000_000)
                .collect();
            let stats = BlockStats::of(&values);
            let naive = |r: core::ops::Range<usize>| {
                values[r]
                    .iter()
                    .fold((i64::MAX, i64::MIN, 0i64), |(lo, hi, s), &v| {
                        (lo.min(v), hi.max(v), s.wrapping_add(v))
                    })
            };
            assert_eq!(stats.total(), naive(0..len));
            let mut ranges = vec![
                0..len,
                0..0,
                len..len,
                0..len.min(1),
                len.saturating_sub(1)..len,
            ];
            for _ in 0..rounds {
                let a = (next() as usize) % (len + 1);
                let b = a + (next() as usize) % (len - a + 1);
                ranges.push(a..b);
                // on block boundaries too
                ranges.push(a / 1024 * 1024..(b / 1024 * 1024).max(a / 1024 * 1024));
            }
            for r in ranges {
                assert_eq!(
                    stats.range(&values, r.clone()),
                    naive(r.clone()),
                    "len {len}, rows {r:?}"
                );
            }
        }
        // sums wrap, as the kernels and the SQL sum do
        let big = vec![i64::MAX; 3000];
        assert_eq!(
            BlockStats::of(&big).range(&big, 5..2999).2,
            big[5..2999].iter().fold(0i64, |s, &v| s.wrapping_add(v))
        );
    }
    use crate::schema::Schema;

    fn row(ts: i64, sym: u32, px: i64, qty: i64, side: u8, seq: i64) -> Vec<Value> {
        vec![
            Value::I64(ts),
            Value::U32(sym),
            Value::I64(px),
            Value::I64(qty),
            Value::U8(side),
            Value::I64(seq),
        ]
    }

    #[test]
    fn append_seal_zone_map() {
        let mut c = Chunk::new(ChunkId(1), Schema::trades());
        for i in 0..10 {
            c.append_row(&row(
                1_000 + i,
                7,
                5_000_000_000_000 + i,
                1,
                (i % 2) as u8,
                i,
            ))
            .unwrap();
        }
        assert_eq!(c.num_rows(), 10);
        assert_eq!(c.watermark(), 1_009);
        assert_eq!(c.time_range_rows(1_002, 1_004), 2..5);
        assert_eq!(c.time_range_rows(5_000, 6_000), 10..10);
        let zm = c.seal().clone();
        assert_eq!(
            zm.per_column[0],
            ColumnStats::I64 {
                min: 1_000,
                max: 1_009
            }
        );
        assert!(zm.may_overlap_i64(0, 1_005, 2_000));
        assert!(!zm.may_overlap_i64(0, 0, 999));
        assert_eq!(
            c.append_row(&row(2_000, 7, 1, 1, 0, 11)),
            Err(AppendError::Sealed)
        );
    }

    #[test]
    fn rejects_out_of_order_and_type_mismatch_atomically() {
        let mut c = Chunk::new(ChunkId(2), Schema::trades());
        c.append_row(&row(100, 1, 1, 1, 0, 0)).unwrap();
        assert_eq!(
            c.append_row(&row(99, 1, 1, 1, 0, 1)),
            Err(AppendError::OutOfOrder {
                ts: 99,
                watermark: 100
            })
        );
        let mut bad = row(101, 1, 1, 1, 0, 2);
        bad[4] = Value::I64(0);
        assert_eq!(c.append_row(&bad), Err(AppendError::Type(4)));
        assert_eq!(c.num_rows(), 1);
        for col in 0..6 {
            assert_eq!(
                c.column(col).unwrap().len(),
                1,
                "column {col} length unchanged after failed append"
            );
        }
    }
}
