//! Merging partitions that are each sorted on one or two 64-bit integer keys.
//!
//! `DataFusion`'s merge of sorted partitions turns every row's sort key into a byte string when
//! the order has more than one column, and compares those strings in a loser tree: profiled on
//! `ORDER BY ts, seq` over 196,619 rows in 17 partitions, 55 % of the query went to that tree
//! and to `memcmp`. [`OrderedMergeExec`] compares the two `i64` values directly and copies rows
//! out in runs: the partition whose head is smallest keeps giving rows as long as they do not
//! pass the next partition's head, so the heap is touched once per run, not once per row.
//!
//! It reads its inputs completely before it emits (charged to the session's memory pool), so
//! [`OrderedMerge`] uses it only without a `LIMIT`, where the result is read entirely anyway; a
//! merge with a `LIMIT` stays `DataFusion`'s, which stops early.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int64Type, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;

/// See the module documentation.
pub struct OrderedMergeExec {
    input: Arc<dyn ExecutionPlan>,
    /// Input columns of the keys, most significant first (one or two).
    keys: Vec<usize>,
    descending: bool,
    ordering: LexOrdering,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for OrderedMergeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedMergeExec")
            .field("keys", &self.keys)
            .field("descending", &self.descending)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for OrderedMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrderedMergeExec: [{}]", self.ordering)
    }
}

/// A sort key as two `i64` (the second is 0 for a one-column key).
type Key = (i64, i64);

fn i64_values(a: &ArrayRef) -> Result<&[i64]> {
    use arrow::datatypes::{
        TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    // every timestamp unit is an i64 underneath
    Ok(match a.data_type() {
        DataType::Int64 => a.as_primitive::<Int64Type>().values(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            a.as_primitive::<TimestampNanosecondType>().values()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            a.as_primitive::<TimestampMicrosecondType>().values()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            a.as_primitive::<TimestampMillisecondType>().values()
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            a.as_primitive::<TimestampSecondType>().values()
        }
        other => return internal_err!("ordered merge: key of type {other}"),
    })
}

impl ExecutionPlan for OrderedMergeExec {
    fn name(&self) -> &'static str {
        "OrderedMergeExec"
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
        for key in &self.ordering {
            if f(&key.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children)
            .map_err(|_| DataFusionError::Internal("OrderedMergeExec has one child".into()))?;
        Ok(Arc::new(Self {
            input,
            keys: self.keys.clone(),
            descending: self.descending,
            ordering: self.ordering.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("OrderedMergeExec has one partition, asked for {partition}");
        }
        let held = Arc::new(MemoryConsumer::new("ordered merge").register(ctx.memory_pool()));
        let mut tasks: Vec<SpawnedTask<Result<Vec<RecordBatch>>>> = Vec::new();
        for p in 0..self.input.output_partitioning().partition_count() {
            let mut stream = self.input.execute(p, ctx.clone())?;
            let held = held.clone();
            tasks.push(SpawnedTask::spawn(async move {
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        held.try_grow(batch.get_array_memory_size())?;
                        batches.push(batch);
                    }
                }
                Ok(batches)
            }));
        }
        let (schema, keys, descending) = (self.schema(), self.keys.clone(), self.descending);
        let batch_rows = ctx.session_config().batch_size().max(1);
        let out = futures::stream::once(async move {
            let mut parts = Vec::with_capacity(tasks.len());
            for task in tasks {
                parts.push(task.join().await.map_err(|e| {
                    DataFusionError::Execution(format!("ordered merge: input failed: {e}"))
                })??);
            }
            let merged = merge(&schema, &parts, &keys, descending, batch_rows);
            drop(held);
            merged
        })
        .flat_map(|r: Result<Vec<RecordBatch>>| {
            futures::stream::iter(match r {
                Ok(batches) => batches.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), out)))
    }
}

/// One input: its batches and where its next row is.
struct Cursor<'a> {
    /// Key columns of every batch.
    keys: Vec<(&'a [i64], Option<&'a [i64]>)>,
    /// Index of each batch in the flat list given to `interleave`.
    ids: Vec<usize>,
    batch: usize,
    row: usize,
}

impl Cursor<'_> {
    fn key(&self) -> Option<Key> {
        let (a, b) = self.keys.get(self.batch)?;
        Some((a[self.row], b.map_or(0, |b| b[self.row])))
    }

    fn advance(&mut self) {
        self.row += 1;
        if self.row == self.keys[self.batch].0.len() {
            self.batch += 1;
            self.row = 0;
        }
    }
}

/// `true` when `x` goes before `y`, or with it (`<=` in the merge's direction).
fn not_after(x: Key, y: Key, descending: bool) -> bool {
    match x.cmp(&y) {
        Ordering::Less => !descending,
        Ordering::Equal => true,
        Ordering::Greater => descending,
    }
}

/// Heap entry ordered so that the next row of the merge is on top.
#[derive(PartialEq, Eq)]
struct Head {
    key: Key,
    input: usize,
    descending: bool,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // max-heap: the "greatest" entry is the one that goes first
        let by_key = if self.descending {
            self.key.cmp(&other.key)
        } else {
            other.key.cmp(&self.key)
        };
        // equal keys: the lower input first, as `DataFusion`'s merge does
        by_key.then_with(|| other.input.cmp(&self.input))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge(
    schema: &SchemaRef,
    parts: &[Vec<RecordBatch>],
    keys: &[usize],
    descending: bool,
    batch_rows: usize,
) -> Result<Vec<RecordBatch>> {
    let flat: Vec<&RecordBatch> = parts.iter().flatten().collect();
    let total: usize = flat.iter().map(|b| b.num_rows()).sum();
    let mut cursors = Vec::with_capacity(parts.len());
    let mut next_id = 0;
    for part in parts {
        let mut cursor = Cursor {
            keys: Vec::with_capacity(part.len()),
            ids: Vec::with_capacity(part.len()),
            batch: 0,
            row: 0,
        };
        for batch in part {
            let first = i64_values(batch.column(keys[0]))?;
            let second = match keys.get(1) {
                Some(&k) => Some(i64_values(batch.column(k))?),
                None => None,
            };
            cursor.keys.push((first, second));
            cursor.ids.push(next_id);
            next_id += 1;
        }
        cursors.push(cursor);
    }
    let mut heap = BinaryHeap::with_capacity(cursors.len());
    for (input, c) in cursors.iter().enumerate() {
        if let Some(key) = c.key() {
            heap.push(Head {
                key,
                input,
                descending,
            });
        }
    }
    // (batch, row) of every output row, in output order
    let mut order: Vec<(usize, usize)> = Vec::with_capacity(total);
    while let Some(Head { input, .. }) = heap.pop() {
        let bound = heap.peek().map(|h| (h.key, h.input));
        let c = &mut cursors[input];
        // a run: this input's rows up to the next input's head (equal keys go to the lower
        // input first, so a higher input stops before an equal head)
        while let Some(key) = c.key() {
            let goes_first = match bound {
                None => true,
                Some((next, other)) => {
                    not_after(key, next, descending) && (key != next || input < other)
                }
            };
            if !goes_first {
                heap.push(Head {
                    key,
                    input,
                    descending,
                });
                break;
            }
            order.push((c.ids[c.batch], c.row));
            c.advance();
        }
    }
    if order.len() != total {
        return internal_err!("ordered merge lost rows: {} of {total}", order.len());
    }
    let mut out = Vec::with_capacity(total.div_ceil(batch_rows));
    for piece in order.chunks(batch_rows) {
        let columns = (0..schema.fields().len())
            .map(|c| {
                let arrays: Vec<&dyn Array> = flat.iter().map(|b| b.column(c).as_ref()).collect();
                arrow::compute::interleave(&arrays, piece)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let opts = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(piece.len()));
        out.push(RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &opts,
        )?);
    }
    Ok(out)
}

/// Replaces a `SortPreservingMergeExec` without a `LIMIT` on one or two non-null 64-bit
/// integer columns (both in the same direction) by an [`OrderedMergeExec`].
#[derive(Debug, Default)]
pub struct OrderedMerge;

impl PhysicalOptimizerRule for OrderedMerge {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            Ok(match rewrite(&node) {
                Some(new) => Transformed::yes(new),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "ankaa_ordered_merge"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let merge = plan.downcast_ref::<SortPreservingMergeExec>()?;
    if merge.fetch().is_some() {
        return None;
    }
    let ordering = merge.expr();
    if ordering.len() > 2 {
        return None;
    }
    let schema = merge.input().schema();
    let descending = ordering.first().options.descending;
    let mut keys = Vec::with_capacity(2);
    for key in ordering {
        let col = key.expr.downcast_ref::<Column>()?.index();
        let field = schema.field(col);
        let integer = matches!(field.data_type(), DataType::Int64 | DataType::Timestamp(..));
        if !integer || field.is_nullable() || key.options.descending != descending {
            return None;
        }
        keys.push(col);
    }
    // the merge must read every input partition: one output partition, the same order
    Some(Arc::new(OrderedMergeExec {
        input: merge.input().clone(),
        keys,
        descending,
        ordering: ordering.clone(),
        props: merge.properties().clone(),
    }))
}
