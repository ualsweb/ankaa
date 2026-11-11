//! `ChunkTable`: a `DataFusion` `TableProvider` over in-memory chunks with time-range pruning.

use std::fmt;
use std::sync::Arc;

use ankaa_store::{Chunk, ChunkRef, Schema as AnkaaSchema, TableSnapshot};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ColumnStatistics, Result, ScalarValue, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, StatisticsArgs,
};
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;

use crate::arrow_conv::{ChunkColumns, arrow_schema, arrow_schema_named};
use crate::names::{Names, is_dict};

/// Inclusive time bounds extracted from pushed-down filters on the timestamp column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeBounds {
    /// Lower bound (inclusive), ns.
    pub lo: i64,
    /// Upper bound (inclusive), ns.
    pub hi: i64,
}

impl Default for TimeBounds {
    fn default() -> Self {
        Self {
            lo: i64::MIN,
            hi: i64::MAX,
        }
    }
}

impl TimeBounds {
    pub(crate) fn tighten_lo(&mut self, v: i64) {
        self.lo = self.lo.max(v);
    }
    pub(crate) fn tighten_hi(&mut self, v: i64) {
        self.hi = self.hi.min(v);
    }
}

/// Inclusive `[lo, hi]` range on one dictionary-id column, extracted from pushed-down filters
/// (`col = n`, comparisons, `col IN (…)`). Used to skip whole chunks by their statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyBounds {
    /// Column index in the table schema.
    pub col: usize,
    /// Lower bound (inclusive).
    pub lo: u32,
    /// Upper bound (inclusive).
    pub hi: u32,
}

/// The statistics a scan prunes on, of one chunk list, laid out as plain arrays: the time range
/// of every chunk, and the id range of every chunk for the `dict` columns queries have filtered
/// on so far.
///
/// Pruning used to ask each chunk in turn, which is a pointer to follow per chunk (two for an
/// encoded one: the chunk, then its metadata) and a cache miss each at the first touch: for a
/// table of tens of thousands of chunks that was a quarter of a small query. Here the first
/// scan of a chunk list pays that walk once per column it filters on, and every later scan of
/// the same list compares numbers that sit next to each other. A chunk list is immutable; a
/// new snapshot gets a new index.
struct PruneIndex {
    ts_min: Vec<i64>,
    ts_max: Vec<i64>,
    /// Per column, built at the first filter on it.
    ids: Vec<std::sync::OnceLock<IdRanges>>,
}

impl fmt::Debug for PruneIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PruneIndex")
            .field("chunks", &self.ts_min.len())
            .finish_non_exhaustive()
    }
}

/// Smallest and largest id of one column per chunk (`lo > hi`: the chunk has no statistics for
/// the column and cannot be judged).
struct IdRanges {
    lo: Vec<u32>,
    hi: Vec<u32>,
}

impl IdRanges {
    fn of(chunks: &[ChunkRef], col: usize) -> Self {
        let (mut lo, mut hi) = (
            Vec::with_capacity(chunks.len()),
            Vec::with_capacity(chunks.len()),
        );
        for c in chunks {
            let (a, b) = c.u32_range(col).unwrap_or((1, 0));
            lo.push(a);
            hi.push(b);
        }
        Self { lo, hi }
    }

    /// The id range of chunk `i`, when it is known.
    fn range(&self, i: usize) -> Option<(u32, u32)> {
        let (lo, hi) = (*self.lo.get(i)?, *self.hi.get(i)?);
        (lo <= hi).then_some((lo, hi))
    }
}

impl PruneIndex {
    fn new(chunks: &[ChunkRef], columns: usize) -> Self {
        let (mut ts_min, mut ts_max) = (
            Vec::with_capacity(chunks.len()),
            Vec::with_capacity(chunks.len()),
        );
        for c in chunks {
            // an empty chunk overlaps nothing
            let (a, b) = c.time_range().unwrap_or((i64::MAX, i64::MIN));
            ts_min.push(a);
            ts_max.push(b);
        }
        Self {
            ts_min,
            ts_max,
            ids: (0..columns).map(|_| std::sync::OnceLock::new()).collect(),
        }
    }

    fn ids(&self, chunks: &[ChunkRef], col: usize) -> Option<&IdRanges> {
        Some(self.ids.get(col)?.get_or_init(|| IdRanges::of(chunks, col)))
    }
}

/// A chunk list and its [`PruneIndex`].
type Indexed = (Arc<Vec<ChunkRef>>, Arc<PruneIndex>);

/// A table backed by a fixed set of chunks sharing one schema.
#[derive(Debug, Clone)]
pub struct ChunkTable {
    /// Current chunk set; swapped atomically by [`ChunkTable::swap`] so a registered table can
    /// follow a live store without deregister/register races.
    chunks: Arc<RwLock<Arc<Vec<ChunkRef>>>>,
    ankaa_schema: AnkaaSchema,
    schema: SchemaRef,
    ts_name: String,
    /// Per column: the published names of a `dict` column shown as strings (`None`: the column
    /// is not a dictionary, or is shown as ids).
    names: Vec<Option<Arc<Names>>>,
    /// The index of the chunk list scanned last (the list is held with it, so that "the same
    /// list" can be told by identity).
    prune: Arc<parking_lot::Mutex<Option<Indexed>>>,
}

/// `column = 'name'`, `column <> 'name'` or `column [NOT] IN ('a', 'b')` on a `dict` column
/// shown as strings, translated to dictionary ids (names nobody ever stored have no id and are
/// left out: an equality with one matches nothing).
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyPred {
    /// Column index in the table schema.
    col: usize,
    ids: Vec<u32>,
    negated: bool,
}

/// Rows taken from chunk metadata ([`ankaa_store::ChunkMeta::last_row`]), column by column.
struct MetaRows {
    cols: Vec<(ankaa_store::ColumnType, ankaa_store::Column, Vec<bool>)>,
    rows: usize,
}

impl MetaRows {
    fn new(fields: &[ankaa_store::Field], projection: &[usize]) -> Self {
        Self {
            cols: projection
                .iter()
                .filter_map(|&c| fields.get(c))
                .map(|f| {
                    (
                        f.ty,
                        ankaa_store::Column::with_capacity(f.ty, 0),
                        Vec::new(),
                    )
                })
                .collect(),
            rows: 0,
        }
    }

    /// Add the last row of the chunk `meta` describes; `false` (nothing added) when the
    /// metadata does not hold it (a segment written before it was recorded).
    fn push(&mut self, meta: &ankaa_store::ChunkMeta, projection: &[usize]) -> bool {
        if meta.last_row.len() != meta.schema.fields().len() || self.cols.len() != projection.len()
        {
            return false;
        }
        // every value first: a row is added whole or not at all
        let mut row: Vec<Option<ankaa_store::Value>> = Vec::with_capacity(projection.len());
        for (&c, (ty, _, _)) in projection.iter().zip(&self.cols) {
            match meta.last_row.get(c) {
                Some(None) => row.push(None),
                Some(Some(bits)) => match ankaa_store::value_of_bits(*ty, *bits) {
                    Some(v) => row.push(Some(v)),
                    None => return false,
                },
                None => return false,
            }
        }
        for (v, (ty, col, valid)) in row.into_iter().zip(&mut self.cols) {
            // a NULL keeps a placeholder in the column, as everywhere else
            let stored = v.or_else(|| ankaa_store::value_of_bits(*ty, 0));
            if let Some(stored) = stored {
                col.push(stored);
            }
            valid.push(v.is_some());
        }
        self.rows += 1;
        true
    }

    fn finish(
        self,
        schema: &SchemaRef,
        dicts: &[Option<arrow::array::ArrayRef>],
    ) -> Result<Option<ChunkColumns>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let typed = self
            .cols
            .into_iter()
            .map(|(ty, col, valid)| {
                let valid = valid.contains(&false).then_some(valid);
                (ty, col, valid)
            })
            .collect();
        ChunkColumns::decoded(typed, 0..self.rows, schema.clone(), dicts).map(Some)
    }
}

/// Rows per batch of a [`ChunkTable::latest`] result.
const LATEST_BATCH_ROWS: usize = 8192;

/// What a [`KeyPred`] says about a chunk from the range of ids it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// No row matches: the chunk is not read.
    Skip,
    /// Every row matches: the chunk is read as it is.
    All,
    /// Some rows may match: the predicate has to be applied row by row.
    Rows,
}

impl KeyPred {
    fn verdict(&self, range: Option<(u32, u32)>) -> Verdict {
        let Some((lo, hi)) = range else {
            return Verdict::Rows;
        };
        let any_in = self.ids.iter().any(|&i| lo <= i && i <= hi);
        match (self.negated, lo == hi, any_in) {
            (false, _, false) | (true, true, true) => Verdict::Skip,
            (false, true, true) | (true, _, false) => Verdict::All,
            (_, false, true) => Verdict::Rows,
        }
    }
}

fn literal_str(v: &ScalarValue) -> Option<&str> {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s),
        ScalarValue::Dictionary(_, inner) => literal_str(inner),
        _ => None,
    }
}

impl ChunkTable {
    /// Build from owned chunks (all must share `schema`).
    #[must_use]
    pub fn new(schema: AnkaaSchema, chunks: Vec<Chunk>) -> Self {
        Self::from_shared(
            schema,
            chunks
                .into_iter()
                .map(|c| ChunkRef::Hot(Arc::new(c)))
                .collect(),
        )
    }

    /// Build from shared chunks in any tier, e.g. a [`TableSnapshot`]'s chunks (no copy).
    #[must_use]
    pub fn from_shared(schema: AnkaaSchema, chunks: Vec<ChunkRef>) -> Self {
        let ts_name = schema.fields()[0].name.clone();
        let arrow = arrow_schema(&schema);
        Self {
            chunks: Arc::new(RwLock::new(Arc::new(chunks))),
            ankaa_schema: schema,
            schema: arrow,
            ts_name,
            names: Vec::new(),
            prune: Arc::default(),
        }
    }

    /// The current chunk list with its pruning index (built now if this list has none yet).
    fn indexed(&self) -> Indexed {
        let chunks = self.chunks.read().clone();
        let mut held = self.prune.lock();
        if let Some((list, index)) = held.as_ref()
            && Arc::ptr_eq(list, &chunks)
        {
            return (chunks, index.clone());
        }
        let index = Arc::new(PruneIndex::new(&chunks, self.ankaa_schema.fields().len()));
        *held = Some((chunks.clone(), index.clone()));
        (chunks, index)
    }

    /// Show `dict` columns as strings: `names[i]` holds the published names of column `i`
    /// (`None` leaves a column as it is). The column's SQL type becomes a dictionary-encoded
    /// string whose keys are the stored ids; `=`, `<>` and `IN` with string literals are
    /// answered on the ids (RFC-0002).
    #[must_use]
    pub fn with_names(mut self, mut names: Vec<Option<Arc<Names>>>) -> Self {
        let fields = self.ankaa_schema.fields();
        names.resize(fields.len(), None);
        for (n, f) in names.iter_mut().zip(fields) {
            if f.ty != ankaa_store::ColumnType::DictU32 {
                *n = None;
            }
        }
        let as_text: Vec<bool> = names.iter().map(Option::is_some).collect();
        self.schema = arrow_schema_named(&self.ankaa_schema, &as_text);
        self.names = names;
        self
    }

    /// The published names of column `col`, when it is shown as strings.
    #[must_use]
    pub fn names_of(&self, col: usize) -> Option<&Arc<Names>> {
        self.names.get(col).and_then(Option::as_ref)
    }

    /// A `dict` column shown as strings behind `e` (also under a cast to a string type, which
    /// the planner may add around it).
    fn named_col(&self, e: &Expr) -> Option<(usize, &Arc<Names>)> {
        let e = match e {
            Expr::Cast(c) if c.field.data_type().is_string() => c.expr.as_ref(),
            Expr::TryCast(c) if c.field.data_type().is_string() => c.expr.as_ref(),
            other => other,
        };
        let Expr::Column(c) = e else { return None };
        let (i, _) = self.schema.column_with_name(&c.name)?;
        Some((i, self.names_of(i)?))
    }

    /// `f` as a predicate on dictionary ids, when it has one of the supported shapes.
    fn key_pred(&self, f: &Expr) -> Option<KeyPred> {
        match f {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => {
                // the planner writes short IN lists as a chain of ORs
                let (l, r) = (self.key_pred(left)?, self.key_pred(right)?);
                if l.col != r.col || l.negated || r.negated {
                    return None;
                }
                let mut ids = l.ids;
                ids.extend(r.ids);
                ids.sort_unstable();
                ids.dedup();
                Some(KeyPred {
                    col: l.col,
                    ids,
                    negated: false,
                })
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::Eq | Operator::NotEq) =>
            {
                // the literal on either side
                let ((col, names), lit) = match (left.as_ref(), right.as_ref()) {
                    (c, Expr::Literal(v, _)) | (Expr::Literal(v, _), c) => (self.named_col(c)?, v),
                    _ => return None,
                };
                Some(KeyPred {
                    col,
                    ids: names.id_of(literal_str(lit)?).into_iter().collect(),
                    negated: *op == Operator::NotEq,
                })
            }
            Expr::InList(il) => {
                let (col, names) = self.named_col(il.expr.as_ref())?;
                let mut ids = Vec::with_capacity(il.list.len());
                for e in &il.list {
                    let Expr::Literal(v, _) = e else { return None };
                    ids.extend(names.id_of(literal_str(v)?));
                }
                ids.sort_unstable();
                ids.dedup();
                Some(KeyPred {
                    col,
                    ids,
                    negated: il.negated,
                })
            }
            _ => None,
        }
    }

    /// Replace the chunk set (e.g. with a fresh [`TableSnapshot`]); running scans keep the old set.
    pub fn swap(&self, chunks: Vec<ChunkRef>) {
        *self.chunks.write() = Arc::new(chunks);
    }

    /// Replace the chunk set from a snapshot of the same table.
    pub fn swap_snapshot(&self, snap: TableSnapshot) {
        self.swap(snap.chunks);
    }

    /// Current number of chunks.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.chunks.read().len()
    }

    /// Build from a table snapshot.
    #[must_use]
    pub fn from_snapshot(snap: TableSnapshot) -> Self {
        Self::from_shared(snap.schema, snap.chunks)
    }

    /// Timestamp column name.
    #[must_use]
    pub fn timestamp_column(&self) -> &str {
        &self.ts_name
    }

    /// The ANKAA (physical) schema behind the Arrow schema.
    #[must_use]
    pub fn ankaa_schema(&self) -> &AnkaaSchema {
        &self.ankaa_schema
    }

    /// The newest row of every value of the `dict` column `key`: the row with the greatest
    /// timestamp, the last stored one among equals. `projection` selects the columns (`None`:
    /// all). The rows come in no particular order, in batches of at most 8,192.
    ///
    /// The cost follows the number of key values, not the rows of the table. A chunk that holds
    /// one key value (every chunk of a table partitioned by the key) is judged from its
    /// statistics: its last row is its newest, since the rows of a chunk are in time order.
    /// Only the chunks that hold a winner are read, and of those only the winning row
    /// ([`ankaa_store::EncodedChunk::decode_rows`]). A chunk that mixes key values has its key
    /// and timestamp columns read to find the newest row of each.
    pub fn latest(&self, key: usize, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        let fields = self.ankaa_schema.fields();
        if !fields
            .get(key)
            .is_some_and(|f| f.ty == ankaa_store::ColumnType::DictU32)
        {
            return datafusion::common::plan_err!("latest: the key is not a dict column");
        }
        let projection: Vec<usize> =
            projection.map_or_else(|| (0..fields.len()).collect(), <[usize]>::to_vec);
        let out_schema = Arc::new(self.schema.project(&projection)?);
        let dicts: Vec<Option<arrow::array::ArrayRef>> = projection
            .iter()
            .map(|&c| self.names_of(c).map(|n| n.values()))
            .collect();
        let chunks = self.chunks.read().clone();
        let wanted = self.newest_rows(&chunks, key)?;
        if projection.is_empty() {
            // `count(*)`: the rows are counted, none of their columns is read
            let rows =
                arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(wanted.len()));
            return Ok(vec![RecordBatch::try_new_with_options(
                out_schema,
                Vec::new(),
                &rows,
            )?]);
        }
        let mut parts: Vec<RecordBatch> = Vec::new();
        let mut from_meta = MetaRows::new(fields, &projection);
        let mut from = 0;
        while from < wanted.len() {
            let ci = wanted[from].0;
            let to = from + wanted[from..].partition_point(|&(c, _)| c == ci);
            let rows: Vec<usize> = wanted[from..to].iter().map(|&(_, r)| r).collect();
            from = to;
            let (Some(&first), Some(&last), Some(chunk)) =
                (rows.first(), rows.last(), chunks.get(ci))
            else {
                continue;
            };
            // the newest row of a sealed chunk is in its metadata: nothing is decoded
            if let ChunkRef::Encoded(e) = chunk
                && rows.len() == 1
                && first + 1 == e.meta().num_rows
                && from_meta.push(e.meta(), &projection)
            {
                continue;
            }
            let span = first..last + 1;
            let cols = match chunk {
                ChunkRef::Hot(c) => {
                    ChunkColumns::hot(c, &projection, span.clone(), out_schema.clone(), &dicts)?
                }
                ChunkRef::Encoded(e) => {
                    encoded_range(e.as_ref(), &projection, span.clone(), &out_schema, &dicts)?
                }
            };
            let batch = cols.batch(0, span.len())?;
            parts.push(if rows.len() == span.len() {
                batch
            } else {
                let pick: arrow::array::UInt32Array = rows
                    .iter()
                    .map(|&r| u32::try_from(r - first).unwrap_or(u32::MAX))
                    .collect();
                arrow::compute::take_record_batch(&batch, &pick)?
            });
        }
        if let Some(cols) = from_meta.finish(&out_schema, &dicts)? {
            parts.push(cols.batch(0, cols.num_rows())?);
        }
        // one-row pieces make poor batches for whatever reads the result: put them together
        let mut out = Vec::with_capacity(parts.len() / LATEST_BATCH_ROWS + 1);
        let (mut start, mut held) = (0, 0);
        for (i, p) in parts.iter().enumerate() {
            held += p.num_rows();
            if held >= LATEST_BATCH_ROWS || i + 1 == parts.len() {
                out.push(arrow::compute::concat_batches(
                    &out_schema,
                    &parts[start..=i],
                )?);
                (start, held) = (i + 1, 0);
            }
        }
        Ok(out)
    }

    /// `(chunk, row)` of the newest row of every value of the `dict` column `key`, sorted.
    fn newest_rows(&self, chunks: &[ChunkRef], key: usize) -> Result<Vec<(usize, usize)>> {
        use arrow::array::AsArray;
        use datafusion::error::DataFusionError;
        // per key id, the newest row seen so far: (timestamp, chunk, row)
        let mut newest: Vec<Option<(i64, usize, usize)>> = Vec::new();
        let mut consider = |id: u32, ts: i64, chunk: usize, row: usize| {
            let id = id as usize;
            if id >= newest.len() {
                newest.resize(id + 1, None);
            }
            // `>=`: among equal timestamps the row stored last wins
            if newest[id].is_none_or(|(best, _, _)| ts >= best) {
                newest[id] = Some((ts, chunk, row));
            }
        };
        let ids_schema = Arc::new(arrow_schema(&self.ankaa_schema).project(&[0, key])?);
        for (ci, chunk) in chunks.iter().enumerate() {
            let Some((_, ts_max)) = chunk.time_range() else {
                continue; // empty
            };
            match chunk.u32_range(key) {
                Some((lo, hi)) if lo == hi => consider(lo, ts_max, ci, chunk.num_rows() - 1),
                _ => {
                    let Some(cols) = chunk_columns(
                        chunk,
                        &[0, key],
                        TimeBounds::default(),
                        usize::MAX,
                        &ids_schema,
                        &[],
                    )?
                    else {
                        continue;
                    };
                    let batch = cols.batch(0, cols.num_rows())?;
                    let ts = batch
                        .column(0)
                        .as_primitive_opt::<arrow::datatypes::TimestampNanosecondType>()
                        .ok_or_else(|| DataFusionError::Internal("latest: ts type".into()))?
                        .values();
                    let ids = batch
                        .column(1)
                        .as_primitive_opt::<arrow::datatypes::UInt32Type>()
                        .ok_or_else(|| DataFusionError::Internal("latest: key type".into()))?
                        .values();
                    for (row, (&t, &id)) in ts.iter().zip(ids.iter()).enumerate() {
                        consider(id, t, ci, row);
                    }
                }
            }
        }
        let mut wanted: Vec<(usize, usize)> = newest
            .iter()
            .flatten()
            .map(|&(_, chunk, row)| (chunk, row))
            .collect();
        wanted.sort_unstable();
        Ok(wanted)
    }

    /// Extract `[lo, hi]` from filters of the form `ts <op> literal` / `literal <op> ts` /
    /// `ts BETWEEN a AND b` (`DataFusion` rewrites BETWEEN into two comparisons). Unknown
    /// filters are ignored here and re-evaluated by `DataFusion` (we report `Inexact`).
    #[must_use]
    pub fn time_bounds(&self, filters: &[Expr]) -> TimeBounds {
        let mut b = TimeBounds::default();
        for f in filters {
            self.apply_filter(f, &mut b);
        }
        b
    }

    fn apply_filter(&self, f: &Expr, b: &mut TimeBounds) {
        match f {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                self.apply_filter(left, b);
                self.apply_filter(right, b);
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (col_left, lit) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v, _)) if c.name == self.ts_name => (true, v),
                    (Expr::Literal(v, _), Expr::Column(c)) if c.name == self.ts_name => (false, v),
                    _ => return,
                };
                let Some(ns) = literal_ns(lit) else { return };
                // Normalise to `ts <op> ns`.
                let op = if col_left { *op } else { swap(*op) };
                match op {
                    Operator::Gt => b.tighten_lo(ns.saturating_add(1)),
                    Operator::GtEq => b.tighten_lo(ns),
                    Operator::Lt => b.tighten_hi(ns.saturating_sub(1)),
                    Operator::LtEq => b.tighten_hi(ns),
                    Operator::Eq => {
                        b.tighten_lo(ns);
                        b.tighten_hi(ns);
                    }
                    _ => {}
                }
            }
            Expr::Between(bt)
                if matches!(bt.expr.as_ref(), Expr::Column(c) if c.name == self.ts_name)
                    && !bt.negated =>
            {
                if let (Expr::Literal(lo, _), Expr::Literal(hi, _)) =
                    (bt.low.as_ref(), bt.high.as_ref())
                    && let (Some(lo), Some(hi)) = (literal_ns(lo), literal_ns(hi))
                {
                    b.tighten_lo(lo);
                    b.tighten_hi(hi);
                }
            }
            _ => {}
        }
    }
}

impl ChunkTable {
    /// True when `f` is nothing but comparisons of the timestamp column with nanosecond
    /// timestamp literals (joined by `AND`, or a `BETWEEN`): then [`Self::time_bounds`]
    /// describes it completely and the scan, which returns exactly the rows inside the
    /// bounds, is the whole filter.
    fn exact_time_filter(&self, f: &Expr) -> bool {
        let ts = |e: &Expr| matches!(e, Expr::Column(c) if c.name == self.ts_name);
        let ns = |e: &Expr| match e {
            Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _), _) => Some(*v),
            _ => None,
        };
        match f {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => self.exact_time_filter(left) && self.exact_time_filter(right),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (op, v) = match (ts(left), ns(right), ns(left), ts(right)) {
                    (true, Some(v), _, _) => (*op, v),
                    (_, _, Some(v), true) => (swap(*op), v),
                    _ => return false,
                };
                match op {
                    // the bound is `v + 1` / `v - 1`: not representable at the ends of the range
                    Operator::Gt => v != i64::MAX,
                    Operator::Lt => v != i64::MIN,
                    Operator::GtEq | Operator::LtEq | Operator::Eq => true,
                    _ => false,
                }
            }
            Expr::Between(bt) => {
                !bt.negated && ts(&bt.expr) && ns(&bt.low).is_some() && ns(&bt.high).is_some()
            }
            _ => false,
        }
    }

    /// Ranges on dictionary-id columns implied by the filters (conjunctions only). Several
    /// predicates on one column intersect. An unsatisfiable range is kept (`lo > hi`) and
    /// prunes every chunk.
    #[must_use]
    pub fn key_bounds(&self, filters: &[Expr]) -> Vec<KeyBounds> {
        let mut out: Vec<KeyBounds> = Vec::new();
        for f in filters {
            self.apply_key_filter(f, &mut out);
        }
        out
    }

    fn key_col(&self, e: &Expr) -> Option<usize> {
        let Expr::Column(c) = e else { return None };
        let (i, f) = self.schema.column_with_name(&c.name)?;
        (*f.data_type() == arrow::datatypes::DataType::UInt32).then_some(i)
    }

    fn apply_key_filter(&self, f: &Expr, out: &mut Vec<KeyBounds>) {
        let mut tighten = |col: usize, lo: u32, hi: u32| {
            if let Some(b) = out.iter_mut().find(|b| b.col == col) {
                b.lo = b.lo.max(lo);
                b.hi = b.hi.min(hi);
            } else {
                out.push(KeyBounds { col, lo, hi });
            }
        };
        match f {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                self.apply_key_filter(left, out);
                self.apply_key_filter(right, out);
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => {
                // a column bounded on both sides of an OR is bounded by the hull of the two
                // ranges (the planner rewrites short IN lists into this shape)
                let (mut l, mut r) = (Vec::new(), Vec::new());
                self.apply_key_filter(left, &mut l);
                self.apply_key_filter(right, &mut r);
                for a in &l {
                    if let Some(b) = r.iter().find(|b| b.col == a.col) {
                        let hull = match (a.lo <= a.hi, b.lo <= b.hi) {
                            (true, true) => (a.lo.min(b.lo), a.hi.max(b.hi)),
                            (true, false) => (a.lo, a.hi),
                            (false, true) => (b.lo, b.hi),
                            (false, false) => (1, 0),
                        };
                        tighten(a.col, hull.0, hull.1);
                    }
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (col, lit, col_left) = match (left.as_ref(), right.as_ref()) {
                    (c, Expr::Literal(v, _)) => (self.key_col(c), v, true),
                    (Expr::Literal(v, _), c) => (self.key_col(c), v, false),
                    _ => return,
                };
                let (Some(col), Some(v)) = (col, literal_i128(lit)) else {
                    return;
                };
                let op = if col_left { *op } else { swap(*op) };
                // work in i128 so out-of-range literals clamp instead of wrapping
                let (lo, hi) = match op {
                    Operator::Eq => (v, v),
                    Operator::Gt => (v + 1, i128::from(u32::MAX)),
                    Operator::GtEq => (v, i128::from(u32::MAX)),
                    Operator::Lt => (0, v - 1),
                    Operator::LtEq => (0, v),
                    _ => return,
                };
                let (lo, hi) = clamp_u32(lo, hi);
                tighten(col, lo, hi);
            }
            Expr::InList(il) if !il.negated => {
                let Some(col) = self.key_col(il.expr.as_ref()) else {
                    return;
                };
                let mut lo = i128::MAX;
                let mut hi = i128::MIN;
                for e in &il.list {
                    let Expr::Literal(v, _) = e else { return };
                    let Some(v) = literal_i128(v) else { return };
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
                if lo <= hi {
                    let (lo, hi) = clamp_u32(lo, hi);
                    tighten(col, lo, hi);
                }
            }
            _ => {}
        }
    }
}

/// Clamp an `i128` range to `u32`; a range entirely outside becomes empty (`lo > hi`).
fn clamp_u32(lo: i128, hi: i128) -> (u32, u32) {
    let max = i128::from(u32::MAX);
    if hi < 0 || lo > max {
        return (1, 0);
    }
    (
        u32::try_from(lo.max(0)).unwrap_or(u32::MAX),
        u32::try_from(hi.min(max)).unwrap_or(u32::MAX),
    )
}

fn literal_i128(v: &ScalarValue) -> Option<i128> {
    match v {
        ScalarValue::UInt8(Some(x)) => Some(i128::from(*x)),
        ScalarValue::UInt16(Some(x)) => Some(i128::from(*x)),
        ScalarValue::UInt32(Some(x)) => Some(i128::from(*x)),
        ScalarValue::UInt64(Some(x)) => Some(i128::from(*x)),
        ScalarValue::Int8(Some(x)) => Some(i128::from(*x)),
        ScalarValue::Int16(Some(x)) => Some(i128::from(*x)),
        ScalarValue::Int32(Some(x)) => Some(i128::from(*x)),
        ScalarValue::Int64(Some(x)) => Some(i128::from(*x)),
        _ => None,
    }
}

pub(crate) fn swap(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        o => o,
    }
}

pub(crate) fn literal_ns(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::TimestampNanosecond(Some(ns), _) | ScalarValue::Int64(Some(ns)) => Some(*ns),
        ScalarValue::TimestampMicrosecond(Some(us), _) => us.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(ms), _) => ms.checked_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(s), _) => s.checked_mul(1_000_000_000),
        _ => None,
    }
}

#[async_trait]
impl TableProvider for ChunkTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // A comparison of the timestamp with a literal is applied exactly: the scan cuts every
        // chunk to the time bounds by binary search, so DataFusion need not evaluate it again
        // on every row. So is `=`, `<>` or `IN` between a dictionary column shown as strings
        // and string literals: it is answered on the ids, per chunk from the range of ids the
        // chunk holds (and row by row only in a chunk that mixes matching and other ids).
        // Everything else only prunes chunks and stays in the plan.
        Ok(filters
            .iter()
            .map(|f| {
                if self.exact_time_filter(f) || self.key_pred(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let bounds = self.time_bounds(filters);
        let keys = self.key_bounds(filters);
        let projection: Vec<usize> = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let preds: Vec<KeyPred> = filters.iter().filter_map(|f| self.key_pred(f)).collect();
        let (all, index) = self.indexed();
        // prune on statistics now, so the plan shows what will be read and the partitions
        // below only hold chunks that can contribute rows
        let key_ranges: Vec<Option<&IdRanges>> =
            keys.iter().map(|k| index.ids(&all, k.col)).collect();
        let pred_ranges: Vec<Option<&IdRanges>> =
            preds.iter().map(|p| index.ids(&all, p.col)).collect();
        let mut by_row = false;
        let mut candidates: Vec<ChunkRef> = Vec::new();
        'chunks: for (i, c) in all.iter().enumerate() {
            let (min, max) = (index.ts_min[i], index.ts_max[i]);
            if bounds.hi < min || bounds.lo > max {
                continue;
            }
            for (k, ranges) in keys.iter().zip(&key_ranges) {
                // without statistics for the column the chunk cannot be ruled out
                if let Some((lo, hi)) = ranges.and_then(|r| r.range(i))
                    && (k.hi < lo || k.lo > hi)
                {
                    continue 'chunks;
                }
            }
            let mut rows = false;
            for (p, ranges) in preds.iter().zip(&pred_ranges) {
                match p.verdict(ranges.and_then(|r| r.range(i))) {
                    Verdict::Skip => continue 'chunks,
                    Verdict::Rows => rows = true,
                    Verdict::All => {}
                }
            }
            by_row |= rows;
            candidates.push(c.clone());
        }
        let pruned = all.len() - candidates.len();
        let dicts_of = |cols: &[usize]| -> Vec<Option<arrow::array::ArrayRef>> {
            cols.iter()
                .map(|&c| self.names_of(c).map(|n| n.values()))
                .collect()
        };
        if by_row {
            // A chunk holds matching and other ids: the name predicates are applied to its
            // rows by DataFusion's own filter over a scan that also returns their columns.
            // (Chunks are single-valued on a partition key and on anything that depends on
            // it, so this is the rare path; the dedicated operators leave such plans alone.)
            let mut wide = projection.clone();
            for p in &preds {
                if !wide.contains(&p.col) {
                    wide.push(p.col);
                }
            }
            let wide_schema = Arc::new(self.schema.project(&wide)?);
            let parts = state.config().target_partitions();
            let groups = match split_by_runs(&candidates, parts) {
                Some(groups) => groups,
                None => split_by_rows(candidates, parts),
            };
            let dicts = dicts_of(&wide);
            let scan = ChunkScanExec::new(
                groups,
                pruned,
                wide,
                bounds,
                None,
                wide_schema.clone(),
                dicts,
            );
            let name_filters: Vec<Expr> = filters
                .iter()
                .filter(|f| self.key_pred(f).is_some())
                .cloned()
                .collect();
            let Some(predicate) = datafusion::logical_expr::utils::conjunction(name_filters) else {
                return datafusion::common::internal_err!("row filter without a predicate");
            };
            let df_schema = datafusion::common::DFSchema::try_from(wide_schema.as_ref().clone())?;
            let predicate = state.create_physical_expr(predicate, &df_schema)?;
            let keep: Vec<usize> = (0..projection.len()).collect();
            let filter = datafusion::physical_plan::filter::FilterExecBuilder::new(
                predicate,
                Arc::new(scan),
            )
            .apply_projection(Some(keep))?
            .with_fetch(limit)
            .build()?;
            return Ok(Arc::new(filter));
        }
        let out_schema = Arc::new(self.schema.project(&projection)?);
        let dicts = dicts_of(&projection);
        let parts = state.config().target_partitions();
        // Partitions that are each in time order let the planner merge instead of sorting
        // (`ORDER BY ts`); a table whose chunk list has too many separate time-ordered runs
        // (thousands of keys) is split by rows alone.
        let groups = match split_by_runs(&candidates, parts) {
            Some(groups) => groups,
            None => split_by_rows(candidates, parts),
        };
        Ok(Arc::new(ChunkScanExec::new(
            groups, pruned, projection, bounds, limit, out_schema, dicts,
        )))
    }
}

/// Minimum rows per scan partition: below this, splitting costs more than it saves. A scan
/// copies nothing, so one chunk per partition is already worth a worker.
const MIN_ROWS_PER_PARTITION: usize = ankaa_store::CHUNK_ROWS;

/// Cut the chunk list into at most `parts` **consecutive** runs with similar row totals.
/// Always returns at least one group.
///
/// The order is kept on purpose: a snapshot lists each partition's chunks in time order, so
/// reading the groups one after another gives every key's rows in the order they were stored.
/// The table functions rely on that to skip a sort (and check it, so a different order costs
/// time, never correctness).
fn split_by_rows(chunks: Vec<ChunkRef>, parts: usize) -> Vec<Vec<ChunkRef>> {
    let total: usize = chunks.iter().map(ChunkRef::num_rows).sum();
    let n = parts
        .min(total / MIN_ROWS_PER_PARTITION)
        .min(chunks.len())
        .max(1);
    if n == 1 {
        return vec![chunks];
    }
    let mut groups: Vec<Vec<ChunkRef>> = (0..n).map(|_| Vec::new()).collect();
    let (mut g, mut seen) = (0usize, 0usize);
    for c in chunks {
        // close a group once the rows before this chunk reach its share of the total
        while g + 1 < n && !groups[g].is_empty() && seen * n >= (g + 1) * total {
            g += 1;
        }
        seen += c.num_rows();
        groups[g].push(c);
    }
    groups.retain(|g| !g.is_empty());
    groups
}

/// Most partitions a scan is cut into to keep each of them in time order.
const MAX_ORDERED_PARTITIONS: usize = 64;

/// True when `next` holds no row earlier than the last row of `prev`.
fn follows(prev: &ChunkRef, next: &ChunkRef) -> bool {
    match (prev.time_range(), next.time_range()) {
        (Some((_, last)), Some((first, _))) => last <= first,
        _ => true, // an empty chunk orders with anything
    }
}

/// True when, in addition, rows with the timestamp the two chunks share are in order of the
/// table's last column across them (both chunks being in that order inside).
fn follows_in_tie_order(prev: &ChunkRef, next: &ChunkRef) -> bool {
    match (
        prev.time_range(),
        next.time_range(),
        prev.tie_order(),
        next.tie_order(),
    ) {
        (Some((_, last)), Some((first, _)), Some((_, prev_tie)), Some((next_tie, _))) => {
            last < first || (last == first && prev_tie <= next_tie)
        }
        _ => false,
    }
}

/// Cut the chunk list into groups that are each in time order: the list is a sequence of
/// runs (the chunks of one partition of the table follow each other in time; the next
/// partition starts earlier again), and a group never spans two runs. Runs larger than a
/// worker's share are cut further. `None` when that takes more than
/// [`MAX_ORDERED_PARTITIONS`] groups.
fn split_by_runs(chunks: &[ChunkRef], parts: usize) -> Option<Vec<Vec<ChunkRef>>> {
    let total: usize = chunks.iter().map(ChunkRef::num_rows).sum();
    let share = (total / parts.max(1)).max(MIN_ROWS_PER_PARTITION);
    let mut groups: Vec<Vec<ChunkRef>> = Vec::new();
    let mut rows = 0usize;
    for chunk in chunks {
        let start = match groups.last().and_then(|g| g.last()) {
            Some(prev) => !follows(prev, chunk) || rows >= share,
            None => true,
        };
        if start {
            if groups.len() == MAX_ORDERED_PARTITIONS {
                return None;
            }
            groups.push(Vec::new());
            rows = 0;
        }
        rows += chunk.num_rows();
        if let Some(g) = groups.last_mut() {
            g.push(chunk.clone());
        }
    }
    if groups.is_empty() {
        groups.push(Vec::new());
    }
    Some(groups)
}

/// Physical scan over chunks already pruned by statistics. One output partition per chunk
/// group, so the engine converts and decodes chunks on several cores; each partition is a lazy
/// stream (one batch per chunk, produced when polled).
#[derive(Debug)]
pub struct ChunkScanExec {
    groups: Vec<Arc<Vec<ChunkRef>>>,
    pruned: usize,
    projection: Arc<Vec<usize>>,
    bounds: TimeBounds,
    limit: Option<usize>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    /// Every partition's chunks follow each other in time.
    ordered: bool,
    /// Table column that orders rows with equal timestamps in every partition, if any.
    tie: Option<usize>,
    /// Rows are returned latest first (chunks and rows within them in reverse).
    reverse: bool,
    /// Per output column: the names of a `dict` column shown as strings.
    dicts: Arc<Vec<Option<arrow::array::ArrayRef>>>,
}

impl ChunkScanExec {
    /// Output order of a scan whose partitions are in time order: the timestamp, ascending or
    /// (for a reverse scan) descending, followed by the table's last column when every
    /// partition is also in that order among equal timestamps (`tie` = its table column).
    /// None when the timestamp is not returned.
    fn ordering(
        schema: &SchemaRef,
        projection: &[usize],
        ordered: bool,
        tie: Option<usize>,
        reverse: bool,
    ) -> EquivalenceProperties {
        let key = |at: usize| {
            PhysicalSortExpr::new(
                Arc::new(Column::new(schema.field(at).name(), at)),
                arrow::compute::SortOptions {
                    descending: reverse,
                    nulls_first: false,
                },
            )
        };
        match projection.iter().position(|&c| c == 0) {
            Some(ts) if ordered => {
                let mut keys = vec![key(ts)];
                if let Some(at) = tie.and_then(|t| projection.iter().position(|&c| c == t))
                    && at != ts
                {
                    keys.push(key(at));
                }
                EquivalenceProperties::new_with_orderings(schema.clone(), [keys])
            }
            _ => EquivalenceProperties::new(schema.clone()),
        }
    }

    /// The same scan returning rows latest first.
    fn reversed(&self) -> Self {
        let props = Arc::new(PlanProperties::new(
            Self::ordering(&self.schema, &self.projection, self.ordered, self.tie, true),
            Partitioning::UnknownPartitioning(self.groups.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            groups: self.groups.clone(),
            pruned: self.pruned,
            projection: self.projection.clone(),
            bounds: self.bounds,
            limit: self.limit,
            schema: self.schema.clone(),
            props,
            ordered: self.ordered,
            tie: self.tie,
            reverse: true,
            dicts: self.dicts.clone(),
        }
    }

    fn new(
        groups: Vec<Vec<ChunkRef>>,
        pruned: usize,
        projection: Vec<usize>,
        bounds: TimeBounds,
        limit: Option<usize>,
        schema: SchemaRef,
        dicts: Vec<Option<arrow::array::ArrayRef>>,
    ) -> Self {
        // rows come out in time order when every partition's chunks follow each other in time
        // (and the timestamp is among the columns returned)
        let ordered = groups
            .iter()
            .all(|g| g.windows(2).all(|w| follows(&w[0], &w[1])));
        // ... and, among equal timestamps, in order of the table's last column (a sequence
        // number): known per chunk since it was sealed
        let tie = (ordered
            && groups.iter().all(|g| {
                g.iter().all(|c| c.is_empty() || c.tie_order().is_some())
                    && g.windows(2).all(|w| follows_in_tie_order(&w[0], &w[1]))
            }))
        .then(|| {
            groups
                .iter()
                .filter_map(|g| g.first())
                .map(|c| c.schema().fields().len() - 1)
                .next()
        })
        .flatten();
        let eq = Self::ordering(&schema, &projection, ordered, tie, false);
        let props = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(groups.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            groups: groups.into_iter().map(Arc::new).collect(),
            pruned,
            projection: Arc::new(projection),
            bounds,
            limit,
            schema,
            props,
            ordered,
            tie,
            reverse: false,
            dicts: Arc::new(dicts),
        }
    }
}

impl ChunkScanExec {
    /// Chunk groups, one per output partition.
    pub(crate) fn groups(&self) -> &[Arc<Vec<ChunkRef>>] {
        &self.groups
    }

    /// Table column of each output column.
    pub(crate) fn projection(&self) -> &Arc<Vec<usize>> {
        &self.projection
    }

    /// Time bounds every returned row satisfies (rows are cut to them exactly).
    pub(crate) fn bounds(&self) -> TimeBounds {
        self.bounds
    }

    /// Row limit per partition, if one was pushed down.
    pub(crate) fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Output schema.
    pub(crate) fn output_schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Per output column: the names of a `dict` column shown as strings.
    pub(crate) fn dicts(&self) -> &Arc<Vec<Option<arrow::array::ArrayRef>>> {
        &self.dicts
    }
}

/// The projected columns of one chunk in any tier, cut to the time bounds (`None`: no row).
pub(crate) fn chunk_columns(
    chunk: &ChunkRef,
    projection: &[usize],
    bounds: TimeBounds,
    remaining: usize,
    schema: &SchemaRef,
    dicts: &[Option<arrow::array::ArrayRef>],
) -> Result<Option<ChunkColumns>> {
    match chunk {
        ChunkRef::Hot(c) => hot_columns(c, projection, bounds, remaining, schema, dicts),
        ChunkRef::Encoded(e) => {
            encoded_columns(e.as_ref(), projection, bounds, remaining, schema, dicts)
        }
    }
}

/// Columns of one hot chunk: the row range by binary search on the timestamps.
fn hot_columns(
    c: &Arc<Chunk>,
    projection: &[usize],
    bounds: TimeBounds,
    remaining: usize,
    schema: &SchemaRef,
    dicts: &[Option<arrow::array::ArrayRef>],
) -> Result<Option<ChunkColumns>> {
    let mut rows = c.time_range_rows(bounds.lo, bounds.hi);
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() > remaining {
        rows.end = rows.start + remaining;
    }
    Ok(Some(ChunkColumns::hot(
        c,
        projection,
        rows,
        schema.clone(),
        dicts,
    )?))
}

/// Columns of one encoded chunk, cut to the time bounds and to `remaining` rows.
///
/// The timestamps of a chunk are sorted, so the bounds are a row range, and the columns are
/// decoded **for that range only** ([`ankaa_store::EncodedChunk::decode_time_range`]). A query
/// for an hour of one device out of a chunk that holds three days decodes that hour, not the
/// three days; a `LIMIT` without bounds decodes the first rows only.
fn encoded_columns(
    e: &dyn ankaa_store::EncodedChunk,
    projection: &[usize],
    bounds: TimeBounds,
    remaining: usize,
    schema: &SchemaRef,
    dicts: &[Option<arrow::array::ArrayRef>],
) -> Result<Option<ChunkColumns>> {
    let want = distinct(projection);
    let (rows, cols) = e
        .decode_time_range(&want, bounds.lo, bounds.hi, remaining)
        .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;
    if rows.is_empty() {
        return Ok(None);
    }
    typed_columns(e, projection, &want, cols, rows.len(), schema, dicts).map(Some)
}

/// The projected columns of rows `rows` of an encoded chunk.
fn encoded_range(
    e: &dyn ankaa_store::EncodedChunk,
    projection: &[usize],
    rows: core::ops::Range<usize>,
    schema: &SchemaRef,
    dicts: &[Option<arrow::array::ArrayRef>],
) -> Result<ChunkColumns> {
    let want = distinct(projection);
    let cols = e
        .decode_rows(&want, rows.clone())
        .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;
    typed_columns(e, projection, &want, cols, rows.len(), schema, dicts)
}

/// The columns of a projection, each once (a projection may name a column twice).
fn distinct(projection: &[usize]) -> Vec<usize> {
    let mut want: Vec<usize> = Vec::with_capacity(projection.len());
    for &ci in projection {
        if !want.contains(&ci) {
            want.push(ci);
        }
    }
    want
}

/// Decoded columns `cols` (those of `want`, `rows` values each) laid out as `projection`.
fn typed_columns(
    e: &dyn ankaa_store::EncodedChunk,
    projection: &[usize],
    want: &[usize],
    cols: Vec<(ankaa_store::Column, Option<Vec<bool>>)>,
    rows: usize,
    schema: &SchemaRef,
    dicts: &[Option<arrow::array::ArrayRef>],
) -> Result<ChunkColumns> {
    use datafusion::error::DataFusionError;
    if cols.len() != want.len() {
        return Err(DataFusionError::Internal(format!(
            "segment decoded {} of {} columns",
            cols.len(),
            want.len()
        )));
    }
    let fields = e.meta().schema.fields();
    let mut decoded: Vec<Option<(ankaa_store::Column, Option<Vec<bool>>)>> =
        cols.into_iter().map(Some).collect();
    let mut typed: Vec<(
        ankaa_store::ColumnType,
        ankaa_store::Column,
        Option<Vec<bool>>,
    )> = Vec::with_capacity(projection.len());
    for (n, &ci) in projection.iter().enumerate() {
        let again = projection[n + 1..].contains(&ci);
        let slot = want
            .iter()
            .position(|&w| w == ci)
            .and_then(|pos| decoded.get_mut(pos));
        let col = match slot {
            // the last use of a column takes it; an earlier one (rare) copies
            Some(slot) if again => slot.clone(),
            Some(slot) => slot.take(),
            None => None,
        };
        let (Some(f), Some((col, valid))) = (fields.get(ci), col) else {
            return Err(DataFusionError::Internal(format!(
                "segment has no column {ci}"
            )));
        };
        if col.len() != rows {
            return Err(DataFusionError::Internal(format!(
                "segment column {ci} decoded {} rows for a range of {rows}",
                col.len()
            )));
        }
        typed.push((f.ty, col, valid));
    }
    ChunkColumns::decoded(typed, 0..rows, schema.clone(), dicts)
}

impl DisplayAs for ChunkScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChunkScanExec: chunks={} pruned={} partitions={} projection={:?} ts∈[{}, {}] limit={:?}{}",
            self.groups.iter().map(|g| g.len()).sum::<usize>(),
            self.pruned,
            self.groups.len(),
            self.projection,
            self.bounds.lo,
            self.bounds.hi,
            self.limit,
            if self.reverse { " reverse" } else { "" }
        )
    }
}

impl ExecutionPlan for ChunkScanExec {
    fn name(&self) -> &'static str {
        "ChunkScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // This scan holds no physical expressions (time bounds are plain integers).
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Row counts and key ranges of the chunks this scan will read. Without them the planner
    /// knows nothing about a table: it cannot tell that a `GROUP BY symbol` yields a handful
    /// of rows, so it hash-partitions a whole table to join it with twelve.
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let groups: Vec<&Arc<Vec<ChunkRef>>> = match args.partition() {
            Some(p) => self.groups.get(p).into_iter().collect(),
            None => self.groups.iter().collect(),
        };
        let chunks = || groups.iter().flat_map(|g| g.iter());
        // exact only when every stored row is returned: no time bounds, no limit (a limit
        // applies to each partition's stream)
        let whole = self.bounds == TimeBounds::default() && self.limit.is_none();
        let rows: usize = groups
            .iter()
            .map(|g| {
                let stored: usize = g.iter().map(ChunkRef::num_rows).sum();
                self.limit.map_or(stored, |l| l.min(stored))
            })
            .sum();
        let precise = |n: usize| {
            if whole {
                Precision::Exact(n)
            } else {
                Precision::Inexact(n)
            }
        };
        let mut bytes = 0usize;
        let mut columns = Vec::with_capacity(self.projection.len());
        for (field, &col) in self.schema.fields().iter().zip(self.projection.iter()) {
            bytes += rows.saturating_mul(field.data_type().primitive_width().unwrap_or(8));
            let mut stats = ColumnStatistics::new_unknown();
            if !field.is_nullable() {
                stats.null_count = Precision::Exact(0);
            }
            // a dictionary-id column: its ids lie in the union of the chunks' ranges, which
            // bounds how many distinct keys there can be
            let as_ids = field.data_type() == &arrow::datatypes::DataType::UInt32;
            if as_ids || is_dict(field.data_type()) {
                let range = chunks().try_fold(None, |acc: Option<(u32, u32)>, c| {
                    let (lo, hi) = c.u32_range(col)?;
                    Some(Some(acc.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi)))))
                });
                if let Some(Some((lo, hi))) = range {
                    // (shown as strings, the ids are not the column's minimum and maximum)
                    if as_ids {
                        stats.min_value = Precision::Inexact(ScalarValue::UInt32(Some(lo)));
                        stats.max_value = Precision::Inexact(ScalarValue::UInt32(Some(hi)));
                    }
                    let span = usize::try_from(hi - lo)
                        .unwrap_or(usize::MAX)
                        .saturating_add(1);
                    stats.distinct_count = Precision::Inexact(span.min(rows));
                }
            }
            columns.push(stats);
        }
        Ok(Arc::new(Statistics {
            num_rows: precise(rows),
            total_byte_size: precise(bytes),
            column_statistics: columns,
        }))
    }

    /// `ORDER BY ts` needs no sort when the partitions are in time order, and `ORDER BY ts
    /// DESC` is the same scan run backwards: with a `LIMIT`, the planner then reads only the
    /// end of each partition ("the latest N rows" without touching the rest of the table).
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let ts = self.projection.iter().position(|&c| c == 0);
        let tie = self
            .tie
            .and_then(|t| self.projection.iter().position(|&c| c == t));
        let column = |k: &PhysicalSortExpr| k.expr.downcast_ref::<Column>().map(Column::index);
        // the timestamp alone, or followed by the tie-break column in the same direction
        let key = match order {
            [key] if column(key) == ts && ts.is_some() => key,
            [key, second]
                if column(key) == ts
                    && ts.is_some()
                    && tie.is_some()
                    && column(second) == tie
                    && second.options.descending == key.options.descending =>
            {
                key
            }
            _ => return Ok(SortOrderPushdownResult::Unsupported),
        };
        if !self.ordered || self.reverse {
            return Ok(SortOrderPushdownResult::Unsupported);
        }
        let inner: Arc<dyn ExecutionPlan> = if key.options.descending {
            Arc::new(self.reversed())
        } else {
            Arc::new(Self {
                groups: self.groups.clone(),
                pruned: self.pruned,
                projection: self.projection.clone(),
                bounds: self.bounds,
                limit: self.limit,
                schema: self.schema.clone(),
                props: self.props.clone(),
                ordered: self.ordered,
                tie: self.tie,
                reverse: false,
                dicts: self.dicts.clone(),
            })
        };
        Ok(SortOrderPushdownResult::Exact { inner })
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // A chunk holds up to 65,536 rows. Handing it over as one batch leaves DataFusion with a
        // handful of batches to spread over its workers and operators working on oversized
        // batches; batches of the session batch size (8,192 by default) point into the chunk's
        // own memory and cost nothing to make.
        let batch_rows = ctx.session_config().batch_size().max(1);
        let Some(group) = self.groups.get(partition).cloned() else {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "ChunkScanExec has {} partitions, asked for {partition}",
                self.groups.len()
            )));
        };
        let projection = Arc::clone(&self.projection);
        let bounds = self.bounds;
        let schema = self.schema.clone();
        let dicts = Arc::clone(&self.dicts);
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        let reverse = self.reverse;
        let mut next = 0usize;
        // the chunk being handed out in slices, and how many of its rows were handed out
        let mut current: Option<(ChunkColumns, usize)> = None;
        // lazy: a chunk is opened (hot) or decoded (warm/cold) only when the consumer polls,
        // so a partition holds one chunk at a time and a LIMIT stops the scan early
        let batches = std::iter::from_fn(move || {
            loop {
                // slices of the chunk already loaded first: the row budget was charged when it
                // was loaded (and the loaded chunk is already cut to that budget)
                if let Some((cols, done)) = &mut current {
                    let rows = cols.num_rows();
                    if *done < rows {
                        let len = batch_rows.min(rows - *done);
                        // latest first: slices from the end of the chunk, rows turned round
                        let batch = if reverse {
                            cols.batch(rows - *done - len, len)
                                .and_then(|b| reverse_rows(&b))
                        } else {
                            cols.batch(*done, len)
                        };
                        *done += len;
                        return Some(batch);
                    }
                    current = None;
                }
                if remaining == 0 {
                    return None;
                }
                let at = if reverse {
                    group.len().checked_sub(next + 1)?
                } else {
                    next
                };
                let c = group.get(at)?;
                next += 1;
                // a reverse scan wants the last rows of a chunk: the loader's budget cuts from
                // the front, so the chunk is loaded whole and the budget applied below
                let budget = if reverse { usize::MAX } else { remaining };
                match chunk_columns(c, &projection, bounds, budget, &schema, &dicts) {
                    Ok(Some(cols)) => {
                        let rows = cols.num_rows();
                        let skip = if reverse {
                            rows.saturating_sub(remaining)
                        } else {
                            0
                        };
                        remaining -= (rows - skip).min(remaining);
                        // `skip` rows at the front of the chunk are beyond the budget (they
                        // would come last in reverse): cut them off
                        current = Some((if skip > 0 { cols.tail(skip) } else { cols }, 0));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        remaining = 0;
                        return Some(Err(e));
                    }
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(batches),
        )))
    }
}

/// The rows of a batch in reverse order.
fn reverse_rows(batch: &RecordBatch) -> Result<RecordBatch> {
    let n = u32::try_from(batch.num_rows())
        .map_err(|_| datafusion::error::DataFusionError::Internal("batch too large".into()))?;
    let indices = arrow::array::UInt32Array::from_iter_values((0..n).rev());
    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let opts =
        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &opts,
    )?)
}

/// Register a [`ChunkTable`] under `name` in a `SessionContext`.
pub fn register_chunks(
    ctx: &SessionContext,
    name: &str,
    schema: AnkaaSchema,
    chunks: Vec<Chunk>,
) -> Result<()> {
    ctx.register_table(name, Arc::new(ChunkTable::new(schema, chunks)))?;
    Ok(())
}

/// Register a table snapshot under its own name (replacing any previous registration).
pub fn register_snapshot(ctx: &SessionContext, snap: TableSnapshot) -> Result<()> {
    let name = snap.name.clone();
    let _ = ctx.deregister_table(&name)?;
    ctx.register_table(&name, Arc::new(ChunkTable::from_snapshot(snap)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankaa_store::{ChunkId, Value};

    fn chunk(id: u64, rows: usize) -> ChunkRef {
        let mut c = Chunk::new(ChunkId(id), AnkaaSchema::trades());
        for i in 0..rows {
            let i = i64::try_from(i).unwrap();
            c.append_row(&[
                Value::I64(i),
                Value::U32(0),
                Value::I64(1),
                Value::I64(1),
                Value::U8(0),
                Value::I64(i),
            ])
            .unwrap();
        }
        c.seal();
        ChunkRef::Hot(Arc::new(c))
    }

    fn ids(groups: &[Vec<ChunkRef>]) -> Vec<Vec<u64>> {
        groups
            .iter()
            .map(|g| g.iter().map(|c| c.id().0).collect())
            .collect()
    }

    #[test]
    fn split_keeps_chunk_order_and_balances_rows() {
        let full = ankaa_store::CHUNK_ROWS;
        // a short chunk in the middle (sealed early) must not be moved
        let sizes = [full, full, 100, full, full, full, 7, full];
        let chunks: Vec<ChunkRef> = sizes
            .iter()
            .enumerate()
            .map(|(i, &n)| chunk(u64::try_from(i).unwrap(), n))
            .collect();
        for parts in 1..=8 {
            let groups = split_by_rows(chunks.clone(), parts);
            assert!(!groups.is_empty() && groups.len() <= parts);
            assert!(groups.iter().all(|g| !g.is_empty()));
            let flat: Vec<u64> = ids(&groups).into_iter().flatten().collect();
            assert_eq!(
                flat,
                (0..8).collect::<Vec<u64>>(),
                "order kept with {parts} parts"
            );
            let rows: Vec<usize> = groups
                .iter()
                .map(|g| g.iter().map(ChunkRef::num_rows).sum())
                .collect();
            let (lo, hi) = (rows.iter().min().unwrap(), rows.iter().max().unwrap());
            assert!(hi - lo <= 2 * full, "balanced with {parts} parts: {rows:?}");
        }
        assert_eq!(split_by_rows(chunks.clone(), 3).len(), 3);
        // too few rows to be worth splitting
        assert_eq!(split_by_rows(vec![chunk(0, 10), chunk(1, 10)], 4).len(), 1);
        assert_eq!(split_by_rows(Vec::new(), 4).len(), 1);
    }
}
