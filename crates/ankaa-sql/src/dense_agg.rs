//! Aggregation without hashing for group keys with a small, known domain.
//!
//! `GROUP BY symbol`, `GROUP BY symbol, side` and plain aggregates without `GROUP BY` are what
//! a tick store answers all day. Their keys are dictionary ids and flag bytes: a few values,
//! whose range the chunks' statistics give exactly. [`DenseAggExec`] turns a row's keys into a
//! group number with a subtraction and a multiplication and adds the row to arrays indexed by
//! that number; no key is hashed, compared or copied, and nothing is exchanged between
//! partitions. The aggregate functions themselves are `DataFusion`'s own group accumulators,
//! so every function gives exactly what the hash aggregate gives.
//!
//! When the aggregate reads a chunk table directly (at most a time range in between), chunks
//! are not even turned into rows: if the keys are constant in a chunk (a table partitioned by
//! symbol stores one symbol per chunk), `count(*)` is the chunk's row count, `min`/`max` of a
//! column come from its zone map and `sum` is one SIMD pass over the column.
//!
//! [`DenseAggregation`] is the physical optimiser rule that puts the operator in place of a
//! partial + final hash aggregate when all of this is known to hold; any other plan is left
//! alone.

use std::fmt;
use std::sync::Arc;

use ankaa_store::{ChunkRef, ColumnStats};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int64Array, RecordBatch, TimestampNanosecondArray,
    UInt8Array, UInt32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit, UInt32Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator, Operator};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::StreamExt;

use crate::provider::{ChunkScanExec, TimeBounds, chunk_columns, literal_ns, swap};

/// Most key combinations the dense layout is used for. Each task keeps one `u32` per
/// combination (4 MiB at the limit); accumulators only grow with the groups that have rows.
pub const MAX_DENSE_GROUPS: usize = 1 << 20;

/// One group key: a column whose values lie in `lo .. lo + size`.
#[derive(Debug, Clone)]
struct Key {
    /// Column in the operator's input (for a time bucket: the timestamp column).
    col: usize,
    /// Table column behind it (statistics are per table column).
    table_col: usize,
    lo: i64,
    size: usize,
    /// Weight of this key in the group number.
    stride: usize,
    /// `Some(width)`: the key is `date_bin(width, ts)`. Its value is `lo + n * width` for
    /// bucket number `n`, and `lo` is the start of the first bucket that can hold a row.
    bucket: Option<i64>,
    /// A dictionary column shown as strings: the names of its ids (the groups are still
    /// counted on the ids; the result carries the same names array).
    names: Option<ArrayRef>,
}

/// An aggregate a chunk can answer without producing rows.
#[derive(Debug, Clone, Copy)]
enum Simple {
    /// `count(*)` or `count` of a column that cannot hold `NULL`.
    Rows,
    /// `sum` of an `i64` table column.
    Sum(usize),
    /// `min` of an `i64` table column.
    Min(usize),
    /// `max` of an `i64` table column.
    Max(usize),
}

/// Reading the chunk table directly, without the plan between the aggregate and the scan.
#[derive(Debug, Clone)]
struct Direct {
    scan: Arc<dyn ExecutionPlan>,
    /// Scan column of each input column.
    cols: Vec<usize>,
    /// Per aggregate, how a chunk answers it on its own; `None` when some aggregate needs rows.
    simple: Option<Vec<Simple>>,
}

/// Hash-free aggregation over dense group keys. See the module documentation.
pub struct DenseAggExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<Key>,
    groups: usize,
    aggs: Vec<Arc<AggregateFunctionExpr>>,
    direct: Option<Direct>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for DenseAggExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DenseAggExec")
            .field("keys", &self.keys)
            .field("groups", &self.groups)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DenseAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let input = self.input.schema();
        let keys: Vec<String> = self
            .keys
            .iter()
            .map(|k| match k.bucket {
                Some(width) => format!("date_bin({width} ns, {})", input.field(k.col).name()),
                None => input.field(k.col).name().clone(),
            })
            .collect();
        let aggs: Vec<&str> = self.aggs.iter().map(|a| a.name()).collect();
        let source = match &self.direct {
            Some(d) if d.simple.is_some() => {
                "chunks (statistics and kernels where keys are constant)"
            }
            Some(_) => "chunks",
            None => "input",
        };
        write!(
            f,
            "DenseAggExec: gby=[{}], aggr=[{}], dense_groups={}, source={source}",
            keys.join(", "),
            aggs.join(", "),
            self.groups
        )
    }
}

impl DenseAggExec {
    fn scan(&self) -> Option<&ChunkScanExec> {
        self.direct
            .as_ref()
            .and_then(|d| d.scan.downcast_ref::<ChunkScanExec>())
    }
}

impl ExecutionPlan for DenseAggExec {
    fn name(&self) -> &'static str {
        "DenseAggExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for agg in &self.aggs {
            for e in agg.expressions() {
                if f(&e)? == TreeNodeRecursion::Stop {
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children)
            .map_err(|_| DataFusionError::Internal("DenseAggExec has one child".into()))?;
        // a different child is no longer the plan the chunk shortcut was derived from
        let direct = if Arc::ptr_eq(&input, &self.input) {
            self.direct.clone()
        } else {
            None
        };
        Ok(Arc::new(Self {
            input,
            keys: self.keys.clone(),
            groups: self.groups,
            aggs: self.aggs.clone(),
            direct,
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DenseAggExec has one partition, asked for {partition}");
        }
        let shared = Arc::new(Shared {
            keys: self.keys.clone(),
            groups: self.groups,
            aggs: self.aggs.clone(),
            args: self.aggs.iter().map(|a| a.expressions()).collect(),
            input_schema: self.input.schema(),
            batch_rows: ctx.session_config().batch_size().max(1),
        });
        // one task per partition of the source; each fills its own arrays
        let mut tasks: Vec<SpawnedTask<Result<Partial>>> = Vec::new();
        match (&self.direct, self.scan()) {
            (Some(direct), Some(scan)) => {
                // Chunks are dealt out to as many tasks as the session has workers, largest
                // first: the scan groups chunks for row streams (few partitions for a small
                // table), while a task here costs next to nothing and order does not matter.
                let mut chunks: Vec<ChunkRef> = scan
                    .groups()
                    .iter()
                    .flat_map(|g| g.iter().cloned())
                    .collect();
                chunks.sort_by_key(|c| std::cmp::Reverse(c.num_rows()));
                let workers = ctx
                    .session_config()
                    .target_partitions()
                    .clamp(1, chunks.len().max(1));
                let mut dealt: Vec<(usize, Vec<ChunkRef>)> = vec![(0, Vec::new()); workers];
                for chunk in chunks {
                    if let Some(lightest) = dealt.iter_mut().min_by_key(|d| d.0) {
                        lightest.0 += chunk.num_rows();
                        lightest.1.push(chunk);
                    }
                }
                for (_, group) in dealt {
                    let work = ChunkWork {
                        shared: shared.clone(),
                        chunks: Arc::new(group),
                        projection: scan.projection().clone(),
                        bounds: scan.bounds(),
                        scan_schema: scan.output_schema().clone(),
                        dicts: scan.dicts().clone(),
                        cols: direct.cols.clone(),
                        simple: direct.simple.clone(),
                    };
                    tasks.push(SpawnedTask::spawn(async move { work.run().await }));
                }
            }
            _ => {
                for p in 0..self.input.output_partitioning().partition_count() {
                    let mut stream = self.input.execute(p, ctx.clone())?;
                    let shared = shared.clone();
                    tasks.push(SpawnedTask::spawn(async move {
                        let mut part = Partial::new(&shared)?;
                        while let Some(batch) = stream.next().await {
                            part.rows(&shared, &batch?)?;
                        }
                        Ok(part)
                    }));
                }
            }
        }
        let schema = self.schema.clone();
        let out = futures::stream::once(async move {
            let mut total = Partial::new(&shared)?;
            for task in tasks {
                let part = task.join().await.map_err(|e| {
                    DataFusionError::Execution(format!("aggregation task failed: {e}"))
                })??;
                total.merge(part)?;
            }
            total.finish(&shared, &schema)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            out,
        )))
    }
}

/// What every task of one execution needs.
struct Shared {
    keys: Vec<Key>,
    groups: usize,
    aggs: Vec<Arc<AggregateFunctionExpr>>,
    /// Argument expressions of each aggregate, over the input schema.
    args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    input_schema: SchemaRef,
    batch_rows: usize,
}

/// No group has this key yet.
const UNSEEN: u32 = u32::MAX;

/// The time-bucket key, its width and the timestamps of `batch` when that key is the only one
/// that varies in the batch (every other key is fixed): the common shape over a table
/// partitioned by its key column.
fn only_bucket<'a>(
    shared: &'a Shared,
    batch: &'a RecordBatch,
    fixed: &[Option<usize>],
) -> Option<(&'a Key, i64, &'a [i64])> {
    let mut open = shared
        .keys
        .iter()
        .enumerate()
        .filter(|(k, _)| !fixed.get(*k).is_some_and(Option::is_some));
    let (Some((_, key)), None) = (open.next(), open.next()) else {
        return None;
    };
    let ts = batch
        .column(key.col)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .filter(|ts| ts.null_count() == 0)?;
    Some((key, key.bucket?, ts.values()))
}

/// The aggregation state of one task (or of the whole query, after merging).
///
/// Groups are numbered in the order their keys first appear, as a hash aggregate numbers
/// them: `DataFusion`'s accumulators rely on every group below the count they are given
/// having rows (it is how they tell "no value yet" from a value). Only the lookup differs:
/// the dense key indexes `slot_of` directly.
struct Partial {
    accs: Vec<Box<dyn GroupsAccumulator>>,
    /// Without group keys there is one group and nothing to index: the functions' plain
    /// accumulators take whole batches (`count(*)` is then the batch's row count, not a pass
    /// over a column of group numbers). Empty when there are keys.
    plain: Vec<Box<dyn Accumulator>>,
    /// Group number of each dense key, or [`UNSEEN`].
    slot_of: Vec<u32>,
    /// Dense key of each group, in group order.
    key_of: Vec<u32>,
    /// Scratch: the dense key of each row of the batch at hand.
    idx: Vec<usize>,
    /// Scratch: the group number of each row.
    groups: Vec<usize>,
}

impl Partial {
    fn new(shared: &Shared) -> Result<Self> {
        let (mut accs, mut plain) = (Vec::new(), Vec::new());
        for agg in &shared.aggs {
            if shared.keys.is_empty() {
                plain.push(agg.create_accumulator()?);
            } else {
                accs.push(agg.create_groups_accumulator()?);
            }
        }
        Ok(Self {
            accs,
            plain,
            slot_of: vec![UNSEEN; shared.groups],
            key_of: Vec::new(),
            idx: Vec::new(),
            groups: Vec::new(),
        })
    }

    /// The group of a dense key, created on first use.
    #[inline]
    fn group(&mut self, dense: usize) -> usize {
        let slot = self.slot_of[dense];
        if slot != UNSEEN {
            return slot as usize;
        }
        let g = self.key_of.len();
        // dense < MAX_DENSE_GROUPS, so both fit
        #[allow(clippy::cast_possible_truncation)]
        {
            self.slot_of[dense] = g as u32;
            self.key_of.push(dense as u32);
        }
        g
    }

    /// Add the rows of a batch of the input.
    fn rows(&mut self, shared: &Shared, batch: &RecordBatch) -> Result<()> {
        self.rows_with(shared, batch, &[])
    }

    /// [`Self::rows`] for rows whose key `k` is known to be `fixed[k]` (as a dense offset)
    /// when that is `Some`: such a key is not read row by row (a chunk of a table partitioned
    /// by symbol holds one symbol).
    fn rows_with(
        &mut self,
        shared: &Shared,
        batch: &RecordBatch,
        fixed: &[Option<usize>],
    ) -> Result<()> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }
        if shared.keys.is_empty() {
            for (acc, args) in self.plain.iter_mut().zip(&shared.args) {
                acc.update_batch(&eval(args, batch)?)?;
            }
            return Ok(());
        }
        let base: usize = fixed.iter().flatten().sum();
        if let Some((key, width, ts)) = only_bucket(shared, batch, fixed) {
            self.groups_by_bucket_runs(ts, key, width, base)?;
            return self.accumulate(shared, batch);
        }
        self.idx.clear();
        self.idx.resize(n, base);
        for (k, key) in shared.keys.iter().enumerate() {
            if fixed.get(k).is_some_and(Option::is_some) {
                continue;
            }
            let col = batch.column(key.col);
            if col.null_count() != 0 {
                return internal_err!("dense aggregation: null group key");
            }
            // a value outside the range the statistics promised is an error, never an
            // out-of-bounds slot: the whole batch is checked before any slot is touched
            let mut outside = false;
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            if let Some(width) = key.bucket {
                let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() else {
                    return internal_err!(
                        "dense aggregation: bucket key of type {}",
                        col.data_type()
                    );
                };
                // rows come in time order: a division only when a row leaves the bucket of
                // the row before it
                let (mut from, mut to, mut d) = (1i64, 0i64, 0u64);
                for (slot, &t) in self.idx.iter_mut().zip(ts.values().iter()) {
                    if t < from || t >= to {
                        let n = t.wrapping_sub(key.lo).div_euclid(width);
                        d = n as u64;
                        from = key.lo.wrapping_add(n.wrapping_mul(width));
                        to = from.saturating_add(width);
                    }
                    outside |= d >= key.size as u64;
                    *slot = slot.wrapping_add((d as usize).wrapping_mul(key.stride));
                }
            } else if let Some(v) = col.as_any().downcast_ref::<UInt32Array>().or_else(|| {
                // shown as strings: the ids are the dictionary keys
                col.as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .map(DictionaryArray::keys)
            }) {
                for (slot, &x) in self.idx.iter_mut().zip(v.values().iter()) {
                    let d = (i64::from(x) - key.lo) as u64;
                    outside |= d >= key.size as u64;
                    *slot = slot.wrapping_add((d as usize).wrapping_mul(key.stride));
                }
            } else if let Some(v) = col.as_any().downcast_ref::<UInt8Array>() {
                for (slot, &x) in self.idx.iter_mut().zip(v.values().iter()) {
                    let d = (i64::from(x) - key.lo) as u64;
                    outside |= d >= key.size as u64;
                    *slot = slot.wrapping_add((d as usize).wrapping_mul(key.stride));
                }
            } else {
                return internal_err!("dense aggregation: key column of type {}", col.data_type());
            }
            if outside {
                return internal_err!(
                    "dense aggregation: a key value lies outside the range of its statistics"
                );
            }
        }
        // dense key -> group number: one table lookup per row, no branch on the data. A key
        // seen for the first time shows up as UNSEEN; only then are groups created, in a
        // second pass (after the first batches every key has its group)
        self.groups.clear();
        let mut unseen = false;
        for &dense in &self.idx {
            let g = self.slot_of[dense];
            unseen |= g == UNSEEN;
            self.groups.push(g as usize);
        }
        if unseen {
            for i in 0..n {
                if self.groups[i] == UNSEEN as usize {
                    self.groups[i] = self.group(self.idx[i]);
                }
            }
        }
        self.accumulate(shared, batch)
    }

    /// Feed the batch to the accumulators, with the group of every row in `self.groups`.
    fn accumulate(&mut self, shared: &Shared, batch: &RecordBatch) -> Result<()> {
        let total = self.key_of.len();
        for (acc, args) in self.accs.iter_mut().zip(&shared.args) {
            acc.update_batch(&eval(args, batch)?, &self.groups, None, total)?;
        }
        Ok(())
    }

    /// The group of every row when the only key that varies in the batch is the time bucket
    /// `key`: rows of one bucket follow each other (a chunk is in time order), so the work is
    /// per run of rows, not per row. A run is found by comparing timestamps with the bucket's
    /// end (a loop with nothing carried from one row to the next), its group is looked up
    /// once, and the rows are filled with it. Rows out of time order only make shorter runs.
    fn groups_by_bucket_runs(
        &mut self,
        ts: &[i64],
        key: &Key,
        width: i64,
        base: usize,
    ) -> Result<()> {
        self.groups.clear();
        self.groups.resize(ts.len(), 0);
        let mut i = 0;
        while let Some(&t) = ts.get(i) {
            let nth = t.wrapping_sub(key.lo).div_euclid(width);
            // a value outside the range the statistics promised is an error, never an
            // out-of-bounds slot
            let Some(d) = usize::try_from(nth).ok().filter(|d| *d < key.size) else {
                return internal_err!(
                    "dense aggregation: a key value lies outside the range of its statistics"
                );
            };
            let from = key.lo.wrapping_add(nth.wrapping_mul(width));
            let to = from.saturating_add(width);
            let len = ts[i..]
                .iter()
                .take_while(|&&x| x >= from && x < to)
                .count()
                .max(1);
            let dense = base.wrapping_add(d.wrapping_mul(key.stride));
            let Some(&slot) = self.slot_of.get(dense) else {
                return internal_err!("dense aggregation: key outside the table of groups");
            };
            let group = if slot == UNSEEN {
                self.group(dense)
            } else {
                slot as usize
            };
            self.groups[i..i + len].fill(group);
            i += len;
        }
        Ok(())
    }

    fn merge(&mut self, mut other: Self) -> Result<()> {
        for (acc, part) in self.plain.iter_mut().zip(&mut other.plain) {
            let state: Vec<ArrayRef> = part
                .state()?
                .iter()
                .map(ScalarValue::to_array)
                .collect::<Result<_>>()?;
            acc.merge_batch(&state)?;
        }
        if other.key_of.is_empty() {
            return Ok(());
        }
        let keys = std::mem::take(&mut other.key_of);
        let mine: Vec<usize> = keys.iter().map(|&k| self.group(k as usize)).collect();
        let total = self.key_of.len();
        for (acc, part) in self.accs.iter_mut().zip(&mut other.accs) {
            acc.merge_batch(&part.state(EmitTo::All)?, &mine, total)?;
        }
        Ok(())
    }

    /// The result: one row per group (always one row without group keys).
    fn finish(mut self, shared: &Shared, schema: &SchemaRef) -> Result<RecordBatch> {
        if shared.keys.is_empty() {
            // one row, also over nothing (`count` 0, `sum` NULL, ...): the plain accumulators
            // give exactly what the functions define
            let columns = self
                .plain
                .iter_mut()
                .map(|acc| acc.evaluate()?.to_array())
                .collect::<Result<Vec<ArrayRef>>>()?;
            return Ok(RecordBatch::try_new(schema.clone(), columns)?);
        }
        if self.key_of.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for (k, key) in shared.keys.iter().enumerate() {
            let values = self.key_of.iter().map(|&dense| {
                key.lo + i64::try_from((dense as usize / key.stride) % key.size).unwrap_or(0)
            });
            let array: ArrayRef = match schema.field(k).data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, tz) if key.bucket.is_some() => {
                    let width = key.bucket.unwrap_or(1);
                    let starts = self.key_of.iter().map(|&dense| {
                        let n =
                            i64::try_from((dense as usize / key.stride) % key.size).unwrap_or(0);
                        key.lo + n * width
                    });
                    let a = TimestampNanosecondArray::from_iter_values(starts);
                    Arc::new(match tz {
                        Some(tz) => a.with_timezone(tz.clone()),
                        None => a,
                    })
                }
                DataType::UInt32 => Arc::new(UInt32Array::from_iter_values(
                    values.map(|v| u32::try_from(v).unwrap_or(u32::MAX)),
                )),
                t if crate::names::is_dict(t) => {
                    let ids = UInt32Array::from_iter_values(
                        values.map(|v| u32::try_from(v).unwrap_or(u32::MAX)),
                    );
                    let Some(names) = &key.names else {
                        return internal_err!("dense aggregation: dictionary key without names");
                    };
                    Arc::new(DictionaryArray::<UInt32Type>::try_new(ids, names.clone())?)
                }
                DataType::UInt8 => Arc::new(UInt8Array::from_iter_values(
                    values.map(|v| u8::try_from(v).unwrap_or(u8::MAX)),
                )),
                other => return internal_err!("dense aggregation: key output type {other}"),
            };
            columns.push(array);
        }
        for acc in &mut self.accs {
            columns.push(acc.evaluate(EmitTo::All)?);
        }
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

fn eval(args: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    args.iter()
        .map(|e| e.evaluate(batch)?.into_array(batch.num_rows()))
        .collect()
}

/// One task of the direct path: a group of chunks.
struct ChunkWork {
    shared: Arc<Shared>,
    chunks: Arc<Vec<ChunkRef>>,
    projection: Arc<Vec<usize>>,
    bounds: TimeBounds,
    scan_schema: SchemaRef,
    dicts: Arc<Vec<Option<ArrayRef>>>,
    cols: Vec<usize>,
    simple: Option<Vec<Simple>>,
}

impl ChunkWork {
    async fn run(self) -> Result<Partial> {
        let shared = &self.shared;
        let mut part = Partial::new(shared)?;
        let identity = self
            .cols
            .iter()
            .copied()
            .eq(0..self.scan_schema.fields().len());
        for chunk in self.chunks.iter() {
            if let Some(simple) = &self.simple
                && self.answer_from_chunk(&mut part, chunk, simple)?
            {
                continue;
            }
            // keys this chunk holds a single value of, from its statistics
            let fixed: Vec<Option<usize>> = shared
                .keys
                .iter()
                .map(|key| {
                    if key.bucket.is_some() {
                        return None;
                    }
                    let (lo, hi) = chunk.u32_range(key.table_col)?;
                    let d = usize::try_from(i64::from(lo) - key.lo).ok()?;
                    (lo == hi && d < key.size).then_some(d * key.stride)
                })
                .collect();
            let Some(cols) = chunk_columns(
                chunk,
                &self.projection,
                self.bounds,
                usize::MAX,
                &self.scan_schema,
                &self.dicts,
            )?
            else {
                continue;
            };
            let mut at = 0;
            while at < cols.num_rows() {
                let batch = cols.batch(at, shared.batch_rows)?;
                at += shared.batch_rows;
                if identity {
                    part.rows_with(shared, &batch, &fixed)?;
                } else {
                    part.rows_with(shared, &batch.project(&self.cols)?, &fixed)?;
                }
            }
            // let other tasks of the runtime (ingest, other queries) in between chunks
            tokio::task::yield_now().await;
        }
        Ok(part)
    }

    /// Hand a chunk's partial results (`rows` rows of the group with key `dense`) to the
    /// accumulators.
    fn feed(
        &self,
        part: &mut Partial,
        dense: usize,
        rows: usize,
        simple: &[Simple],
        answers: &[i64],
    ) -> Result<()> {
        let grouped = !self.shared.keys.is_empty();
        let group = if grouped { part.group(dense) } else { 0 };
        let total = part.key_of.len();
        for (i, (how, &value)) in simple.iter().zip(answers).enumerate() {
            if matches!(how, Simple::Rows) {
                // a count's state is the count: merged in, not counted row by row
                let n: ArrayRef = Arc::new(Int64Array::from(vec![
                    i64::try_from(rows).unwrap_or(i64::MAX),
                ]));
                if grouped {
                    part.accs[i].merge_batch(&[n], &[group], total)?;
                } else {
                    part.plain[i].merge_batch(&[n])?;
                }
                continue;
            }
            // one row carrying the chunk's partial result, in the argument's own type
            let mut one: ArrayRef = Arc::new(Int64Array::from(vec![value]));
            if let Some(arg) = self.shared.args[i].first() {
                let ty = arg.data_type(&self.shared.input_schema)?;
                if ty != DataType::Int64 {
                    one = cast(&one, &ty)?;
                }
            }
            if grouped {
                part.accs[i].update_batch(&[one], &[group], None, total)?;
            } else {
                part.plain[i].update_batch(&[one])?;
            }
        }
        Ok(())
    }

    /// Answer every aggregate for one chunk whose keys are constant, without producing rows:
    /// from its statistics, and for a hot chunk cut by the time range, from the per-block
    /// aggregates plus the two partial blocks at the ends of the range. `false`: this chunk
    /// needs the row path (nothing was added).
    fn answer_from_chunk(
        &self,
        part: &mut Partial,
        chunk: &ChunkRef,
        simple: &[Simple],
    ) -> Result<bool> {
        let mut dense = 0usize;
        for key in &self.shared.keys {
            if key.bucket.is_some() {
                return Ok(false); // a chunk spans many buckets: its rows are needed
            }
            let value = match (chunk.u32_range(key.table_col), chunk) {
                (Some((min, max)), _) if min == max => i64::from(min),
                (None, ChunkRef::Hot(c)) => {
                    match c.zone_map().and_then(|z| z.per_column.get(key.table_col)) {
                        Some(ColumnStats::U8 { min, max }) if min == max => i64::from(*min),
                        _ => return Ok(false),
                    }
                }
                _ => return Ok(false),
            };
            let d = usize::try_from(value - key.lo).unwrap_or(usize::MAX);
            if d >= key.size {
                return internal_err!("dense aggregation: chunk statistics outside the key range");
            }
            dense += d * key.stride;
        }
        // every answer first: a chunk that cannot give one of them goes to the row path whole
        let mut answers: Vec<i64> = Vec::with_capacity(simple.len());
        let rows = match chunk {
            ChunkRef::Hot(c) => {
                let Some(zone) = c.zone_map() else {
                    return Ok(false);
                };
                let rows = c.time_range_rows(self.bounds.lo, self.bounds.hi);
                if rows.is_empty() {
                    return Ok(true);
                }
                for how in simple {
                    let (Simple::Sum(col) | Simple::Min(col) | Simple::Max(col)) = *how else {
                        answers.push(0);
                        continue;
                    };
                    let (Some(values), Some(blocks)) = (
                        c.column(col).and_then(ankaa_store::Column::as_i64),
                        zone.block_stats(col),
                    ) else {
                        return Ok(false);
                    };
                    let (lo, hi, sum) = blocks.range(values, rows.clone());
                    answers.push(match how {
                        Simple::Min(_) => lo,
                        Simple::Max(_) => hi,
                        _ => sum,
                    });
                }
                rows.len()
            }
            ChunkRef::Encoded(e) => {
                let meta = e.meta();
                if meta.num_rows == 0 {
                    return Ok(true);
                }
                // only a chunk that lies inside the time range: its statistics are for all rows
                if meta.ts_min < self.bounds.lo || meta.ts_max > self.bounds.hi {
                    return Ok(false);
                }
                for how in simple {
                    let (Simple::Sum(col) | Simple::Min(col) | Simple::Max(col)) = *how else {
                        answers.push(0);
                        continue;
                    };
                    let Some((lo, hi, sum)) = chunk.i64_stats(col) else {
                        return Ok(false);
                    };
                    answers.push(match how {
                        Simple::Min(_) => lo,
                        Simple::Max(_) => hi,
                        _ => sum,
                    });
                }
                meta.num_rows
            }
        };
        self.feed(part, dense, rows, simple, &answers)?;
        Ok(true)
    }
}

// ------------------------------------------------------------------------------------- rule

/// Replaces a partial + final hash aggregate by a [`DenseAggExec`] when every group key is a
/// dictionary-id or byte column of a chunk scan whose range is known and small.
#[derive(Debug, Default)]
pub struct DenseAggregation;

impl PhysicalOptimizerRule for DenseAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            Ok(match rewrite(&node) {
                Some(dense) => Transformed::yes(dense),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "ankaa_dense_aggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Nodes that pass rows through unchanged (possibly regrouped into other batches or partitions).
fn passthrough(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    if let Some(p) = plan.downcast_ref::<RepartitionExec>() {
        return Some(p.input());
    }
    if let Some(p) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return Some(p.input());
    }
    plan.downcast_ref::<CooperativeExec>()
        .map(CooperativeExec::input)
}

fn plain(agg: &AggregateExec) -> bool {
    !agg.group_expr().has_grouping_set()
        && agg.limit_options().is_none()
        && agg.filter_expr().iter().all(Option::is_none)
}

fn rewrite(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let top = plan.downcast_ref::<AggregateExec>()?;
    let partial = match top.mode() {
        AggregateMode::Single | AggregateMode::SinglePartitioned => top,
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let mut node = top.input();
            loop {
                if let Some(agg) = node.downcast_ref::<AggregateExec>() {
                    break (*agg.mode() == AggregateMode::Partial).then_some(agg)?;
                }
                node = passthrough(node)?;
            }
        }
        AggregateMode::Partial | AggregateMode::PartialReduce => return None,
    };
    if !plain(top) || !plain(partial) {
        return None;
    }
    let aggs = partial.aggr_expr();
    if !aggs
        .iter()
        .all(|a| !a.is_distinct() && a.order_bys().is_empty() && a.groups_accumulator_supported())
    {
        return None;
    }
    let input = partial.input();
    let input_schema = input.schema();
    // keys: plain columns of the input that are dictionary ids or bytes of one chunk scan
    let mut keys = Vec::new();
    let mut groups = 1usize;
    for (expr, _) in partial.group_expr().expr() {
        let key = group_key(expr, input, &input_schema, groups)?;
        groups = groups
            .checked_mul(key.size)
            .filter(|&g| g <= MAX_DENSE_GROUPS)?;
        keys.push(key);
    }
    let source = strip_round_robin(input)?;
    let direct = direct(&source, aggs, &input_schema);
    let schema = top.schema();
    let props = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    Some(Arc::new(DenseAggExec {
        input: source,
        keys,
        groups,
        aggs: aggs.to_vec(),
        direct,
        schema,
        props,
    }))
}

/// The dense form of one group expression: a dictionary-id or byte column of a chunk scan, or
/// `date_bin` over the scan's timestamp. `None` when the expression is something else or its
/// range is not known from the chunks' statistics.
fn group_key(
    expr: &Arc<dyn PhysicalExpr>,
    input: &Arc<dyn ExecutionPlan>,
    input_schema: &SchemaRef,
    stride: usize,
) -> Option<Key> {
    let (col, width) = expr
        .downcast_ref::<Column>()
        .map(|c| (c.index(), None))
        .or_else(|| time_bucket(expr).map(|(col, width)| (col, Some(width))))?;
    let (scan, scan_col) = trace(input, col)?;
    let scan = scan.downcast_ref::<ChunkScanExec>()?;
    let table_col = *scan.projection().get(scan_col)?;
    if scan.output_schema().field(scan_col).is_nullable() {
        return None; // a NULL key is a group of its own: the hash aggregate handles it
    }
    let chunks = || scan.groups().iter().flat_map(|g| g.iter());
    let (lo, size) = match (width, input_schema.field(col).data_type()) {
        // `date_bin(width, ts)` on the table's timestamp: the buckets between the first and
        // the last row the scan can return
        (Some(width), DataType::Timestamp(TimeUnit::Nanosecond, _)) if table_col == 0 => {
            let mut range: Option<(i64, i64)> = None;
            for chunk in chunks() {
                if let Some((lo, hi)) = chunk.time_range() {
                    range = Some(range.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
                }
            }
            let bounds = scan.bounds();
            let (lo, hi) = match range {
                Some((lo, hi)) => (lo.max(bounds.lo), hi.min(bounds.hi)),
                None => (0, 0),
            };
            if hi < lo {
                (0, 1) // no row can come: one unused slot
            } else {
                let first = lo.div_euclid(width).checked_mul(width)?;
                let buckets = hi.checked_sub(first)?.div_euclid(width).checked_add(1)?;
                (first, usize::try_from(buckets).ok()?)
            }
        }
        (None, DataType::UInt8) => (0, 256),
        (None, t) if *t == DataType::UInt32 || crate::names::is_dict(t) => {
            let mut range: Option<(u32, u32)> = None;
            for chunk in chunks() {
                let (lo, hi) = chunk.u32_range(table_col)?;
                range = Some(range.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
            }
            let (lo, hi) = range.unwrap_or((0, 0));
            (
                i64::from(lo),
                usize::try_from(hi - lo).ok()?.checked_add(1)?,
            )
        }
        _ => return None,
    };
    Some(Key {
        col,
        table_col,
        lo,
        size,
        stride,
        bucket: width,
        names: scan.dicts().get(scan_col).cloned().flatten(),
    })
}

/// `date_bin(interval, column)` with a fixed-width interval and the default origin (the Unix
/// epoch): `(column index, width in ns)`. Buckets are then `floor(ts / width) * width`, which
/// is what the function computes (checked by the differential tests).
fn time_bucket(expr: &Arc<dyn PhysicalExpr>) -> Option<(usize, i64)> {
    let f = expr.downcast_ref::<ScalarFunctionExpr>()?;
    if f.fun().name() != "date_bin" {
        return None;
    }
    let [interval, column] = f.args() else {
        return None; // an explicit origin: left to the hash aggregate
    };
    let width = match interval.downcast_ref::<Literal>()?.value() {
        ScalarValue::IntervalMonthDayNano(Some(i)) if i.months == 0 => i64::from(i.days)
            .checked_mul(86_400_000_000_000)?
            .checked_add(i.nanoseconds)?,
        ScalarValue::IntervalDayTime(Some(i)) => i64::from(i.days)
            .checked_mul(86_400_000_000_000)?
            .checked_add(i64::from(i.milliseconds).checked_mul(1_000_000)?)?,
        _ => return None,
    };
    (width > 0).then_some((column.downcast_ref::<Column>()?.index(), width))
}

/// Follow input column `col` of `plan` down to the chunk scan it is read from.
fn trace(plan: &Arc<dyn ExecutionPlan>, col: usize) -> Option<(&Arc<dyn ExecutionPlan>, usize)> {
    if plan.downcast_ref::<ChunkScanExec>().is_some() {
        return Some((plan, col));
    }
    if let Some(f) = plan.downcast_ref::<FilterExec>() {
        let below = match f.projection() {
            Some(p) => *p.get(col)?,
            None => col,
        };
        return trace(f.input(), below);
    }
    if let Some(p) = plan.downcast_ref::<ProjectionExec>() {
        let below = p.expr().get(col)?.expr.downcast_ref::<Column>()?.index();
        return trace(p.input(), below);
    }
    trace(passthrough(plan)?, col)
}

/// The same plan without the round-robin exchanges that do not add partitions: every
/// partition of the source gets its own task here, so such an exchange only moves batches.
/// `None` when the plan has a shape this module does not know.
fn strip_round_robin(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<ChunkScanExec>().is_some() {
        return Some(plan.clone());
    }
    if let Some(r) = plan.downcast_ref::<RepartitionExec>() {
        return match r.partitioning() {
            // kept when it widens the plan: a small table scanned in two partitions still
            // deserves all workers for an expensive filter
            Partitioning::RoundRobinBatch(n)
                if r.input().output_partitioning().partition_count() >= *n =>
            {
                strip_round_robin(r.input())
            }
            Partitioning::RoundRobinBatch(_) => Some(plan.clone()),
            _ => None,
        };
    }
    let [child] = <[_; 1]>::try_from(plan.children()).ok()?;
    let below = strip_round_robin(child)?;
    if Arc::ptr_eq(&below, child) {
        Some(plan.clone())
    } else {
        datafusion::physical_plan::execution_plan::replace_children_if_necessary(
            plan.clone(),
            vec![below],
        )
        .ok()
    }
}

/// The direct path applies when nothing but a time range lies between the aggregate and the
/// scan: then the scan's own row ranges are the whole filter.
fn direct(
    source: &Arc<dyn ExecutionPlan>,
    aggs: &[Arc<AggregateFunctionExpr>],
    input_schema: &SchemaRef,
) -> Option<Direct> {
    let mut node = source;
    // scan column of each input column, composed on the way down
    let mut cols: Vec<usize> = (0..input_schema.fields().len()).collect();
    let mut filtered = false;
    let scan = loop {
        if let Some(scan) = node.downcast_ref::<ChunkScanExec>() {
            break scan;
        }
        if let Some(f) = node.downcast_ref::<FilterExec>() {
            if filtered {
                return None;
            }
            filtered = true;
            if let Some(p) = f.projection() {
                cols = cols
                    .iter()
                    .map(|&c| p.get(c).copied())
                    .collect::<Option<_>>()?;
            }
            let child = f.input();
            let scan = leaf_scan(child)?;
            if time_range(f.predicate(), scan)? != scan.bounds() {
                return None;
            }
            node = child;
            continue;
        }
        node = passthrough(node)?;
    };
    if scan.limit().is_some() {
        return None;
    }
    let simple = aggs
        .iter()
        .map(|agg| {
            let inputs = agg.expressions();
            let name = agg.fun().name();
            let column = || {
                let [arg] = inputs.as_slice() else {
                    return None;
                };
                let scan_col = *cols.get(arg.downcast_ref::<Column>()?.index())?;
                let field = scan.output_schema().field(scan_col).clone();
                if field.is_nullable() {
                    return None; // the statistics cover values, not which rows hold one
                }
                match field.data_type() {
                    DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        scan.projection().get(scan_col).copied()
                    }
                    _ => None,
                }
            };
            match name {
                "count" => {
                    // rows are counted when the argument cannot be null: a literal or a column
                    let [arg] = inputs.as_slice() else {
                        return None;
                    };
                    let non_null = arg
                        .downcast_ref::<Literal>()
                        .is_some_and(|l| !l.value().is_null())
                        || arg.downcast_ref::<Column>().is_some_and(|c| {
                            cols.get(c.index())
                                .is_some_and(|&sc| !scan.output_schema().field(sc).is_nullable())
                        });
                    let state = agg.state_fields().ok()?;
                    (non_null && state.len() == 1 && state[0].data_type() == &DataType::Int64)
                        .then_some(Simple::Rows)
                }
                "sum" => column().map(Simple::Sum),
                "min" => column().map(Simple::Min),
                "max" => column().map(Simple::Max),
                _ => None,
            }
        })
        .collect::<Option<Vec<Simple>>>();
    Some(Direct {
        scan: node.clone(),
        cols,
        simple,
    })
}

fn leaf_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&ChunkScanExec> {
    let mut node = plan;
    loop {
        if let Some(scan) = node.downcast_ref::<ChunkScanExec>() {
            return Some(scan);
        }
        node = passthrough(node)?;
    }
}

/// The time range a predicate describes when it is nothing but comparisons of the scan's
/// timestamp column with literals (`None` otherwise). Equal to the scan's bounds, it means
/// the scan already returns exactly the rows the predicate keeps.
fn time_range(predicate: &Arc<dyn PhysicalExpr>, scan: &ChunkScanExec) -> Option<TimeBounds> {
    fn leaf(e: &Arc<dyn PhysicalExpr>, ts: usize, out: &mut TimeBounds) -> Option<()> {
        let b = e.downcast_ref::<BinaryExpr>()?;
        if *b.op() == Operator::And {
            leaf(b.left(), ts, out)?;
            return leaf(b.right(), ts, out);
        }
        let (op, lit) = match (
            b.left().downcast_ref::<Column>(),
            b.right().downcast_ref::<Literal>(),
            b.left().downcast_ref::<Literal>(),
            b.right().downcast_ref::<Column>(),
        ) {
            (Some(c), Some(l), _, _) if c.index() == ts => (*b.op(), l),
            (_, _, Some(l), Some(c)) if c.index() == ts => (swap(*b.op()), l),
            _ => return None,
        };
        let ns = literal_ns(lit.value())?;
        match op {
            Operator::Gt => out.tighten_lo(ns.checked_add(1)?),
            Operator::GtEq => out.tighten_lo(ns),
            Operator::Lt => out.tighten_hi(ns.checked_sub(1)?),
            Operator::LtEq => out.tighten_hi(ns),
            Operator::Eq => {
                out.tighten_lo(ns);
                out.tighten_hi(ns);
            }
            _ => return None,
        }
        Some(())
    }
    // the timestamp is table column 0
    let ts = scan.projection().iter().position(|&c| c == 0)?;
    let mut bounds = TimeBounds::default();
    leaf(predicate, ts, &mut bounds)?;
    Some(bounds)
}
