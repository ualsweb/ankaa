//! Time-series operators exposed to SQL as **table functions** (P1-08).
//!
//! ```sql
//! SELECT * FROM asof_join('trades', 'quotes', 'symbol', 500000000)   -- tolerance in ns (optional)
//! SELECT * FROM ohlcv('trades', 60000000000)                           -- bucket width in ns
//! SELECT * FROM rolling('trades', 300000000000, 1000000000)            -- 5 min window, every second
//! SELECT * FROM percentiles('trades', 'price', '0.5,0.99,0.999', 'symbol') -- UDDSketch, 1 % rel. error
//! SELECT * FROM gapfill('power', 'kw', 60000000000, 'machine', 'avg', 'linear') -- every minute, gaps filled
//! ```
//! They read the named tables from the session (a consistent snapshot of what is registered),
//! run the streaming operators from `ankaa-ts` on plain columns, and return the result as an
//! in-memory table: suitable for the hot window and for replays.
//!
//! The input is used **where it is**: a scan hands out batches that point into chunk memory, and
//! the operators walk those batches in scan order, where the rows of a key are already in time
//! order. No table is copied into one buffer or sorted as a whole. An input that is not in that
//! order (a table registered from unordered batches, say) is detected and takes a second path
//! that copies and sorts it; both paths give the same result.
//!
//! Planning one of these functions, logically and physically (so `EXPLAIN` too), only checks
//! its arguments and derives the output schema.
//! The input is read when the query **executes** (inside the caller's deadline), its size is
//! charged to the session's memory pool (a call over more data than the pool allows fails with
//! "resources exhausted" instead of taking the process down), and the operator itself runs on
//! the blocking pool so it does not stall the async workers that also serve ingest.

use std::sync::Arc;

use std::borrow::Cow;
use std::collections::HashMap;

use ankaa_ts::asof::NO_MATCH;
use ankaa_ts::{AsofDirection, Ohlcv, UddSketch, asof_join, rolling_bars, time_buckets};
use arrow::array::Float64Array;
use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatchOptions;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion::datasource::TableType;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};

/// Relative-error bound of the `percentiles` sketch.
pub const PERCENTILE_ALPHA: f64 = 0.01;
/// Bucket budget per sketch; past it the sketch collapses (error bound doubles, reported).
pub const PERCENTILE_MAX_BUCKETS: usize = 2048;

/// Register the table functions `asof_join`, `ohlcv`, `rolling`, `percentiles` and `gapfill`
/// and the aggregate `vwap` on a session.
pub fn register_functions(ctx: &SessionContext) {
    ctx.register_udtf("asof_join", Arc::new(AsofJoinFn { ctx: ctx.clone() }));
    ctx.register_udtf("ohlcv", Arc::new(OhlcvFn { ctx: ctx.clone() }));
    ctx.register_udtf("percentiles", Arc::new(PercentilesFn { ctx: ctx.clone() }));
    ctx.register_udtf("rolling", Arc::new(RollingFn { ctx: ctx.clone() }));
    ctx.register_udtf("gapfill", Arc::new(GapfillFn { ctx: ctx.clone() }));
    ctx.register_udaf(datafusion::logical_expr::AggregateUDF::from(
        crate::vwap::Vwap::default(),
    ));
}

fn arg_str(args: &[Expr], i: usize, name: &str) -> Result<String> {
    let Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) = args.get(i) else {
        return plan_err!("{name}: argument {} must be a string literal", i + 1);
    };
    Ok(s.clone())
}

fn arg_i64(args: &[Expr], i: usize) -> Result<Option<i64>> {
    match args.get(i) {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Int64(Some(v)), _)) => Ok(Some(*v)),
        Some(Expr::Literal(ScalarValue::Int32(Some(v)), _)) => Ok(Some(i64::from(*v))),
        Some(Expr::Literal(ScalarValue::UInt64(Some(v)), _)) => Ok(i64::try_from(*v).ok()),
        Some(other) => plan_err!("argument {} must be an integer literal, got {other}", i + 1),
    }
}

/// Schema of a registered table, without touching its data (used while planning).
fn table_schema(ctx: &SessionContext, table: &str) -> Result<SchemaRef> {
    // in-memory catalogs resolve at once; anything else is not a table these functions read
    match ctx.table_provider(table).now_or_never() {
        Some(Ok(p)) => Ok(p.schema()),
        Some(Err(e)) => Err(e),
        None => plan_err!("table {table} cannot be resolved while planning"),
    }
}

/// A table as it was scanned: batches in scan order (for a chunk table they point into chunk
/// memory), with the memory reservation that covers working on them.
struct Input {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    _held: MemoryReservation,
}

impl Input {
    /// All rows in one batch (a copy): the second path, for input that is not in stored order.
    fn concat(&self) -> Result<RecordBatch> {
        // over zero batches this is an empty batch with the right columns
        Ok(concat_batches(&self.schema, &self.batches)?)
    }
}

/// Collect `columns` (in that order) of a registered table **in scan order**:
/// the scan's partitions are read side by side and put together one after another, so the rows
/// of a key come out in the order they were stored (time order, see `split_by_rows`). Nothing is
/// copied or sorted here; the operators check the order of each key as they go.
///
/// Every batch is charged to the session's memory pool at three times its size (the per-key
/// vectors an operator builds, its working space, the result), and the reservation lives as
/// long as the [`Input`].
async fn collect_columns(ctx: &SessionContext, table: &str, columns: &[&str]) -> Result<Input> {
    collect(ctx, table, columns, false, None).await
}

/// [`collect_columns`] without the rows where one of the columns is `NULL`: the input of an
/// aggregate (bars, quantiles, windows), which, like SQL aggregates, skips missing values.
async fn collect_values(ctx: &SessionContext, table: &str, columns: &[&str]) -> Result<Input> {
    collect(ctx, table, columns, true, None).await
}

/// The rows of `batch` where no column is `NULL` (the batch itself when there is none).
fn without_nulls(batch: RecordBatch) -> Result<RecordBatch> {
    let mut keep: Option<arrow::buffer::BooleanBuffer> = None;
    for c in batch.columns() {
        if let Some(n) = c.logical_nulls().filter(|n| n.null_count() > 0) {
            let valid = n.inner().clone();
            keep = Some(match keep {
                Some(k) => &k & &valid,
                None => valid,
            });
        }
    }
    match keep {
        None => Ok(batch),
        Some(k) => Ok(arrow::compute::filter_record_batch(
            &batch,
            &arrow::array::BooleanArray::new(k, None),
        )?),
    }
}

async fn collect(
    ctx: &SessionContext,
    table: &str,
    columns: &[&str],
    skip_nulls: bool,
    filter: Option<Expr>,
) -> Result<Input> {
    let mut df = ctx.table(table).await?;
    // a time range reaches the scan, which skips whole chunks outside it
    if let Some(f) = filter {
        df = df.filter(f)?;
    }
    let df = df.select_columns(columns)?;
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;
    let held = Arc::new(
        MemoryConsumer::new("table function input").register(&ctx.runtime_env().memory_pool),
    );
    let read = |partition: usize| {
        let (plan, task_ctx, held) = (plan.clone(), task_ctx.clone(), held.clone());
        async move {
            let mut stream = plan.execute(partition, task_ctx)?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = if skip_nulls {
                    without_nulls(batch?)?
                } else {
                    batch?
                };
                held.try_grow(batch.get_array_memory_size().saturating_mul(3))?;
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(batches)
        }
    };
    let partitions = plan.output_partitioning().partition_count();
    let mut batches = Vec::new();
    if partitions == 1 {
        batches = read(0).await?;
    } else {
        // (a plan without partitions has no rows: nothing is spawned)
        // one task per partition (decoding a warm or cold chunk is real work); a task is
        // cancelled when its handle is dropped, so giving up on the query stops the reads
        let tasks: Vec<SpawnedTask<Result<Vec<RecordBatch>>>> = (0..partitions)
            .map(|i| SpawnedTask::spawn(read(i)))
            .collect();
        for task in tasks {
            let part = task.join().await.map_err(|e| {
                DataFusionError::Execution(format!("table function read failed: {e}"))
            })??;
            batches.extend(part);
        }
    }
    let held = Arc::try_unwrap(held)
        .map_err(|_| DataFusionError::Internal("table function reservation still shared".into()))?;
    Ok(Input {
        schema,
        batches,
        _held: held,
    })
}

/// Run a CPU-bound operator off the async workers.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| DataFusionError::Execution(format!("table function task failed: {e}")))?
}

/// Computes a function's result. The argument is the projection the query asked for (indexes
/// into the function's full schema, `None` = every column); the batch returned holds exactly
/// those columns, so a function can skip reading and building what nobody looks at. A result
/// may come in several batches (`asof_join` returns one per input batch).
type Compute =
    Arc<dyn Fn(Option<Vec<usize>>) -> BoxFuture<'static, Result<Vec<RecordBatch>>> + Send + Sync>;

/// Cut a full result down to the projection.
fn project(batch: RecordBatch, projection: Option<&[usize]>) -> Result<RecordBatch> {
    match projection {
        Some(p) => Ok(batch.project(p)?),
        None => Ok(batch),
    }
}

/// A table whose rows are computed when it is scanned, not when the query is planned.
struct LazyTable {
    name: &'static str,
    schema: SchemaRef,
    compute: Compute,
}

impl std::fmt::Debug for LazyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyTable")
            .field("fn", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TableProvider for LazyTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // nothing is computed here: physical planning (and EXPLAIN) stays cheap
        let schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        Ok(Arc::new(LazyExec {
            name: self.name,
            projection: projection.cloned(),
            compute: self.compute.clone(),
            props: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            schema,
        }))
    }
}

/// Physical operator of a table function: computes its result when it is executed.
struct LazyExec {
    name: &'static str,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    compute: Compute,
    props: Arc<PlanProperties>,
}

impl std::fmt::Debug for LazyExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyExec")
            .field("fn", &self.name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LazyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TableFunctionExec: {} projection={:?}",
            self.name, self.projection
        )
    }
}

impl ExecutionPlan for LazyExec {
    fn name(&self) -> &'static str {
        "TableFunctionExec"
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
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (name, planned, projection) = (self.name, self.schema.clone(), self.projection.clone());
        let compute = self.compute.clone();
        let once = futures::stream::once(async move {
            let checked: Vec<Result<RecordBatch>> = match compute(projection).await {
                Ok(batches) => batches
                    .into_iter()
                    .map(|batch| {
                        if batch.schema().fields() == planned.fields() {
                            Ok(batch)
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "{name}: result schema differs from the planned schema"
                            )))
                        }
                    })
                    .collect(),
                Err(e) => vec![Err(e)],
            };
            futures::stream::iter(checked)
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            once,
        )))
    }
}

fn ts_ns(batch: &RecordBatch, col: usize) -> Result<&[i64]> {
    let a = batch.column(col);
    if let Some(t) = a.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Ok(t.values())
    } else if let Some(t) = a.as_any().downcast_ref::<Int64Array>() {
        Ok(t.values())
    } else {
        plan_err!(
            "column {} must be a nanosecond timestamp or Int64",
            batch.schema().field(col).name()
        )
    }
}

fn key_u32<'a>(batch: &'a RecordBatch, name: &str) -> Result<Cow<'a, [u32]>> {
    let idx = batch.schema().index_of(name)?;
    let a = batch.column(idx);
    if let Some(k) = a.as_any().downcast_ref::<UInt32Array>() {
        Ok(Cow::Borrowed(k.values()))
    } else if let Some(k) = a.as_any().downcast_ref::<Int64Array>() {
        Ok(Cow::Owned(
            k.values()
                .iter()
                .map(|&v| u32::try_from(v).unwrap_or(u32::MAX))
                .collect(),
        ))
    } else {
        plan_err!("key column {name} must be UInt32 or Int64")
    }
}

fn i64_values<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a [i64]> {
    batch
        .column(batch.schema().index_of(name)?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| &**a.values())
        .ok_or_else(|| DataFusionError::Plan(format!("{name} must be Int64")))
}

/// A column as `i64`, used to order rows with equal timestamps (seq / update id): borrowed
/// when it is stored that way; a column that has no integer reading orders nothing.
fn tie_values(batch: &RecordBatch, col: usize) -> Cow<'_, [i64]> {
    let a = batch.column(col);
    if let Some(v) = a.as_any().downcast_ref::<Int64Array>() {
        return Cow::Borrowed(v.values());
    }
    if let Some(v) = a.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Cow::Borrowed(v.values());
    }
    Cow::Owned(
        cast(a, &DataType::Int64)
            .ok()
            .and_then(|a| {
                a.as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.values().to_vec())
            })
            .unwrap_or_else(|| vec![0; batch.num_rows()]),
    )
}

/// End of the run of equal keys that starts at `from`.
fn run_end(keys: &[u32], from: usize) -> usize {
    let k = keys[from];
    keys[from..]
        .iter()
        .position(|&x| x != k)
        .map_or(keys.len(), |n| from + n)
}

/// Shortest average run of one key for which the in-place paths are used. They pay a lookup
/// (and `asof_join` a binary search) per run, which is nothing for a table partitioned by the
/// key (runs of thousands of rows) and too much for a table where keys alternate row by row;
/// that one is grouped per key by the second path, in one linear pass.
const MIN_AVG_RUN: usize = 64;

/// True when the rows of `batches` come in long runs of one key (see [`MIN_AVG_RUN`]).
fn long_runs(batches: &[RecordBatch], key: &str) -> Result<bool> {
    let (mut rows, mut runs) = (0usize, 0usize);
    for batch in batches {
        let keys = key_u32(batch, key)?;
        rows += keys.len();
        runs += usize::from(!keys.is_empty()) + keys.windows(2).filter(|w| w[0] != w[1]).count();
    }
    Ok(rows >= runs.saturating_mul(MIN_AVG_RUN))
}

/// True when rows `a..b` are in `(ts, tie)` order and do not come before `last`.
#[allow(clippy::needless_bitwise_bool)] // on purpose: `&`/`|` keep the loop branch-free
fn in_order(ts: &[i64], tie: &[i64], a: usize, b: usize, last: Option<(i64, i64)>) -> bool {
    if last.is_some_and(|l| l > (ts[a], tie[a])) {
        return false;
    }
    // without an early exit and without branches, so the compiler vectorises it: a pair is in
    // order when its time rises, or stays and its tie-break does not fall
    let (t, k) = (&ts[a..b], &tie[a..b]);
    let mut ok = true;
    for i in 1..t.len() {
        ok &= (t[i - 1] < t[i]) | ((t[i - 1] == t[i]) & (k[i - 1] <= k[i]));
    }
    ok
}

/// The row numbers of each key, keys ascending, rows of a key in `(ts, tie)` order.
///
/// Rows arrive in scan order, where a key's rows are already in that order (a partitioned
/// table stores them so, and equal timestamps keep their arrival order). That is checked per
/// key, and only a key that is out of order is sorted: no global sort.
fn rows_by_key(keys: &[u32], ts: &[i64], tie: Option<&[i64]>) -> Result<Vec<(u32, Vec<u32>)>> {
    if u32::try_from(keys.len()).is_err() {
        return plan_err!("table function input has more than 2^32 rows");
    }
    let mut slot_of: HashMap<u32, usize> = HashMap::new();
    let mut groups: Vec<(u32, Vec<u32>)> = Vec::new();
    // rows of a key come in long runs: look the key up only when it changes
    let mut run: Option<(u32, usize)> = None;
    for (i, &k) in keys.iter().enumerate() {
        let slot = match run {
            Some((rk, slot)) if rk == k => slot,
            _ => {
                let slot = *slot_of.entry(k).or_insert_with(|| {
                    groups.push((k, Vec::new()));
                    groups.len() - 1
                });
                run = Some((k, slot));
                slot
            }
        };
        #[allow(clippy::cast_possible_truncation)] // checked above
        groups[slot].1.push(i as u32);
    }
    let at = |r: u32| {
        let r = r as usize;
        (ts[r], tie.map_or(0, |t| t[r]))
    };
    for (_, rows) in &mut groups {
        if !rows.windows(2).all(|w| at(w[0]) <= at(w[1])) {
            rows.sort_by_key(|&r| at(r)); // stable: equal (ts, tie) keep scan order
        }
    }
    groups.sort_unstable_by_key(|g| g.0);
    Ok(groups)
}

/// `values` at `rows`: borrowed when the rows are one consecutive run (the usual case for a
/// key stored in its own partition), gathered otherwise.
fn at_rows<'a>(values: &'a [i64], rows: &[u32]) -> Cow<'a, [i64]> {
    match (rows.first(), rows.windows(2).all(|w| w[1] == w[0] + 1)) {
        (Some(&first), true) => Cow::Borrowed(&values[first as usize..first as usize + rows.len()]),
        _ => Cow::Owned(rows.iter().map(|&r| values[r as usize]).collect()),
    }
}

/// `asof_join(left_table, right_table, key_column [, tolerance_ns [, 'forward']])`
struct AsofJoinFn {
    ctx: SessionContext,
}

impl std::fmt::Debug for AsofJoinFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AsofJoinFn")
    }
}

impl TableFunctionImpl for AsofJoinFn {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let left_name = arg_str(args, 0, "asof_join")?;
        let right_name = arg_str(args, 1, "asof_join")?;
        let key = arg_str(args, 2, "asof_join")?;
        let tolerance = arg_i64(args, 3)?;
        if tolerance.is_some_and(|t| t < 0) {
            return plan_err!("asof_join: tolerance_ns must be >= 0");
        }
        let dir = match args.get(4) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _))
                if s.eq_ignore_ascii_case("forward") =>
            {
                AsofDirection::Forward
            }
            _ => AsofDirection::Backward,
        };
        let ls = table_schema(&self.ctx, &left_name)?;
        let rs = table_schema(&self.ctx, &right_name)?;
        ls.index_of(&key)?;
        rs.index_of(&key)?;
        let schema = asof_schema(&ls, &rs, &right_name, &key);
        let plan = Arc::new(AsofPlan {
            left: ls.fields().iter().map(|f| f.name().clone()).collect(),
            right: rs.fields().iter().map(|f| f.name().clone()).collect(),
            key,
            dir,
            tolerance,
            schema: schema.clone(),
        });
        let ctx = self.ctx.clone();
        let compute: Compute = Arc::new(move |projection| {
            let (ctx, plan) = (ctx.clone(), plan.clone());
            let (left_name, right_name) = (left_name.clone(), right_name.clone());
            async move {
                let wanted = plan.wanted(projection.as_deref())?;
                let lcols: Vec<&str> = wanted.left.iter().map(|&i| plan.left[i].as_str()).collect();
                let rcols: Vec<&str> = wanted
                    .right
                    .iter()
                    .map(|&i| plan.right[i].as_str())
                    .collect();
                let left = collect_columns(&ctx, &left_name, &lcols).await?;
                let right = collect_columns(&ctx, &right_name, &rcols).await?;
                // the inputs (and their reservations) move into the task: they live as long as
                // the work on them, also when the caller gives up and stops waiting for it
                blocking(move || {
                    if let Some(out) = asof_in_order(&plan, &wanted, &left, &right)? {
                        return Ok(out);
                    }
                    let batch = asof_compute(&plan, &wanted, &left.concat()?, &right.concat()?)?;
                    Ok(vec![batch])
                })
                .await
            }
            .boxed()
        });
        Ok(Arc::new(LazyTable {
            name: "asof_join",
            schema,
            compute,
        }))
    }
}

/// What one `asof_join` call was planned with.
struct AsofPlan {
    /// Column names of the left and right tables, in table order.
    left: Vec<String>,
    right: Vec<String>,
    key: String,
    dir: AsofDirection,
    tolerance: Option<i64>,
    /// Full output schema.
    schema: SchemaRef,
}

/// Where an output column comes from.
#[derive(Clone, Copy)]
enum AsofSource {
    /// Position in the left batch that was read.
    Left(usize),
    /// Position in the right batch that was read.
    Right(usize),
}

/// The columns one execution reads and returns.
struct AsofWanted {
    /// Table column indexes to read, left and right. Left starts with time and key; right with
    /// time, key and the tie-break (last) column.
    left: Vec<usize>,
    right: Vec<usize>,
    /// Position of the tie-break column in the right batch.
    right_tie: usize,
    /// Output columns (indexes into the full schema) and where each one is read from.
    out: Vec<(usize, AsofSource)>,
}

impl AsofPlan {
    /// Reduce the join to the output columns asked for: only those are read from the tables
    /// (plus time, key and tie-break) and only those right columns are gathered.
    fn wanted(&self, projection: Option<&[usize]>) -> Result<AsofWanted> {
        fn slot(read: &mut Vec<usize>, col: usize) -> usize {
            read.iter().position(|&c| c == col).unwrap_or_else(|| {
                read.push(col);
                read.len() - 1
            })
        }
        let index = |names: &[String]| {
            names.iter().position(|n| *n == self.key).ok_or_else(|| {
                DataFusionError::Internal(format!("asof_join: key {} disappeared", self.key))
            })
        };
        let (lkey, rkey) = (index(&self.left)?, index(&self.right)?);
        let mut left = vec![0];
        slot(&mut left, lkey);
        let mut right = vec![0];
        slot(&mut right, rkey);
        let right_tie = slot(&mut right, self.right.len() - 1);
        // full output: every left column, then every right column but the key
        let right_out: Vec<usize> = (0..self.right.len()).filter(|&j| j != rkey).collect();
        let out_idx: Cow<'_, [usize]> = projection.map_or_else(
            || Cow::Owned((0..self.schema.fields().len()).collect()),
            Cow::Borrowed,
        );
        let mut out = Vec::with_capacity(out_idx.len());
        for &o in out_idx.iter() {
            let src = if o < self.left.len() {
                AsofSource::Left(slot(&mut left, o))
            } else {
                let j = *right_out.get(o - self.left.len()).ok_or_else(|| {
                    DataFusionError::Internal(format!("asof_join: no output column {o}"))
                })?;
                AsofSource::Right(slot(&mut right, j))
            };
            out.push((o, src));
        }
        Ok(AsofWanted {
            left,
            right,
            right_tie,
            out,
        })
    }
}

/// Output of `asof_join`: all left columns, then the right columns (except the key) prefixed
/// with the right table name and nullable (unmatched rows).
fn asof_schema(ls: &Schema, rs: &Schema, right_name: &str, key: &str) -> SchemaRef {
    let mut fields: Vec<Field> = ls.fields().iter().map(|f| f.as_ref().clone()).collect();
    for f in rs.fields() {
        if f.name() != key {
            fields.push(Field::new(
                format!("{right_name}_{}", f.name()),
                f.data_type().clone(),
                true,
            ));
        }
    }
    Arc::new(Schema::new(fields))
}

/// Row numbers for `take`, with [`NO_MATCH`] as null: the values are used as they are and the
/// validity is one bit per row (no per-row `Option`); without any miss there is no validity
/// at all and `take` runs its plain path.
fn take_indices(mut idx: Vec<u32>) -> UInt32Array {
    let valid = arrow::buffer::BooleanBuffer::collect_bool(idx.len(), |i| idx[i] != NO_MATCH);
    if valid.count_set_bits() == idx.len() {
        return UInt32Array::new(idx.into(), None);
    }
    for i in &mut idx {
        if *i == NO_MATCH {
            *i = 0; // a null slot: any row number in range
        }
    }
    UInt32Array::new(idx.into(), Some(arrow::buffer::NullBuffer::new(valid)))
}

/// One key's right rows, in `(ts, tie)` order.
#[derive(Default)]
struct RightRows {
    ts: Vec<i64>,
    /// Row number in the right table as scanned.
    row: Vec<u32>,
    last_tie: i64,
}

/// Every key's right rows in `(ts, tie)` order, or `None` when a key is not stored that way.
fn index_right(
    key: &str,
    tie_col: usize,
    right: &Input,
) -> Result<Option<HashMap<u32, RightRows>>> {
    let right_total: usize = right.batches.iter().map(RecordBatch::num_rows).sum();
    if u32::try_from(right_total).is_err() {
        return plan_err!("asof_join: the right table has more than 2^32 rows");
    }
    let mut by_key: HashMap<u32, RightRows> = HashMap::new();
    let mut base = 0usize;
    for batch in &right.batches {
        let (ts, keys) = (ts_ns(batch, 0)?, key_u32(batch, key)?);
        let tie = tie_values(batch, tie_col);
        let mut a = 0;
        while a < keys.len() {
            let b = run_end(&keys, a);
            let rows = by_key.entry(keys[a]).or_default();
            let last = rows.ts.last().map(|&t| (t, rows.last_tie));
            if !in_order(ts, &tie, a, b, last) {
                return Ok(None);
            }
            rows.ts.extend_from_slice(&ts[a..b]);
            #[allow(clippy::cast_possible_truncation)] // the total was checked above
            rows.row.extend((base + a) as u32..(base + b) as u32);
            rows.last_tie = tie[b - 1];
            a = b;
        }
        base += batch.num_rows();
    }
    Ok(Some(by_key))
}

/// The join on input in stored order, without copying either table: the right side is indexed
/// per key (its timestamps and row numbers), then every run of one key in a left batch is
/// joined against that key's rows, starting where a binary search says its first row belongs.
/// The output is one batch per left batch, whose left columns are the input's own.
///
/// `None` when a key's rows are not in order, or keys alternate too often for this to pay
/// (then nothing was produced: the caller takes the second path).
fn asof_in_order(
    plan: &AsofPlan,
    wanted: &AsofWanted,
    left: &Input,
    right: &Input,
) -> Result<Option<Vec<RecordBatch>>> {
    if !long_runs(&left.batches, &plan.key)? || !long_runs(&right.batches, &plan.key)? {
        return Ok(None);
    }
    let Some(by_key) = index_right(&plan.key, wanted.right_tie, right)? else {
        return Ok(None);
    };
    // the right columns the query reads, each as one array to gather from
    let mut right_cols: HashMap<usize, ArrayRef> = HashMap::new();
    for &(_, src) in &wanted.out {
        if let AsofSource::Right(j) = src
            && let std::collections::hash_map::Entry::Vacant(slot) = right_cols.entry(j)
        {
            let parts: Vec<&dyn Array> =
                right.batches.iter().map(|b| b.column(j).as_ref()).collect();
            slot.insert(if parts.is_empty() {
                arrow::array::new_empty_array(right.schema.field(j).data_type())
            } else {
                arrow::compute::concat(&parts)?
            });
        }
    }
    let out_idx: Vec<usize> = wanted.out.iter().map(|o| o.0).collect();
    let schema = Arc::new(plan.schema.project(&out_idx)?);
    let mut out = Vec::with_capacity(left.batches.len());
    let mut matches: Vec<u32> = Vec::new();
    for batch in &left.batches {
        let (ts, keys) = (ts_ns(batch, 0)?, key_u32(batch, &plan.key)?);
        let mut idx = vec![NO_MATCH; ts.len()];
        let mut a = 0;
        while a < keys.len() {
            let b = run_end(&keys, a);
            let run = &ts[a..b];
            if !run.windows(2).all(|w| w[0] <= w[1]) {
                return Ok(None);
            }
            if let Some(rows) = by_key.get(&keys[a]) {
                // only the right rows between the run's first and last timestamp can match it
                let (first, last) = (run[0], run[run.len() - 1]);
                let (from, to) = match plan.dir {
                    AsofDirection::Backward => (
                        rows.ts.partition_point(|&t| t <= first).saturating_sub(1),
                        rows.ts.partition_point(|&t| t <= last),
                    ),
                    AsofDirection::Forward => (
                        rows.ts.partition_point(|&t| t < first),
                        (rows.ts.partition_point(|&t| t < last) + 1).min(rows.ts.len()),
                    ),
                };
                matches.clear();
                matches.resize(run.len(), NO_MATCH);
                asof_join(
                    run,
                    &rows.ts[from..to.max(from)],
                    plan.dir,
                    plan.tolerance,
                    &mut matches,
                );
                for (slot, &m) in idx[a..b].iter_mut().zip(&matches) {
                    if m != NO_MATCH {
                        *slot = rows.row[from + m as usize];
                    }
                }
            }
            a = b;
        }
        let take_idx = take_indices(idx);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(wanted.out.len());
        for &(_, src) in &wanted.out {
            columns.push(match src {
                AsofSource::Left(i) => batch.column(i).clone(),
                AsofSource::Right(j) => match right_cols.get(&j) {
                    Some(col) => take(col.as_ref(), &take_idx, None)?,
                    None => {
                        return Err(DataFusionError::Internal(format!(
                            "asof_join: right column {j} was not read"
                        )));
                    }
                },
            });
        }
        // no output column at all (`count(*)`): the batch still carries the row count
        let opts = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        out.push(RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &opts,
        )?);
    }
    Ok(Some(out))
}

/// The join on input in any order (the second path): each key's left and right rows are put in
/// time order (sorted only if they are not) and walked once. Output rows are the left rows in
/// the order they were read.
fn asof_compute(
    plan: &AsofPlan,
    wanted: &AsofWanted,
    left: &RecordBatch,
    right: &RecordBatch,
) -> Result<RecordBatch> {
    let (lt, rt) = (ts_ns(left, 0)?, ts_ns(right, 0)?);
    let (lk, rk) = (key_u32(left, &plan.key)?, key_u32(right, &plan.key)?);
    // right rows with equal timestamps are ordered by the table's last column (seq / update
    // id), so "the latest quote at t" is the same row whatever order the scan produced
    let rtie = tie_values(right, wanted.right_tie);
    let left_rows = rows_by_key(&lk, lt, None)?;
    let right_rows = rows_by_key(&rk, rt, Some(&rtie))?;
    let mut idx = vec![NO_MATCH; lt.len()];
    let mut matches: Vec<u32> = Vec::new();
    for (key, lrows) in &left_rows {
        let Ok(r) = right_rows.binary_search_by_key(key, |g| g.0) else {
            continue; // no right row for this key: its left rows stay unmatched
        };
        let rrows = &right_rows[r].1;
        let (lts, rts) = (at_rows(lt, lrows), at_rows(rt, rrows));
        matches.clear();
        matches.resize(lrows.len(), NO_MATCH);
        asof_join(&lts, &rts, plan.dir, plan.tolerance, &mut matches);
        for (&l, &m) in lrows.iter().zip(&matches) {
            if m != NO_MATCH {
                idx[l as usize] = rrows[m as usize];
            }
        }
    }
    let take_idx = take_indices(idx);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(wanted.out.len());
    for &(_, src) in &wanted.out {
        columns.push(match src {
            AsofSource::Left(i) => left.column(i).clone(),
            AsofSource::Right(j) => take(right.column(j).as_ref(), &take_idx, None)?,
        });
    }
    let out: Vec<usize> = wanted.out.iter().map(|o| o.0).collect();
    let schema = Arc::new(plan.schema.project(&out)?);
    // no output column at all (`count(*)`): the batch still carries the row count
    let opts = RecordBatchOptions::new().with_row_count(Some(left.num_rows()));
    Ok(RecordBatch::try_new_with_options(schema, columns, &opts)?)
}

/// `ohlcv(table, width_ns [, key_column])`: per key and bucket: open, high, low, close, volume, notional, count.
struct OhlcvFn {
    ctx: SessionContext,
}

impl std::fmt::Debug for OhlcvFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OhlcvFn")
    }
}

impl TableFunctionImpl for OhlcvFn {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table = arg_str(args, 0, "ohlcv")?;
        let width = arg_i64(args, 1)?
            .ok_or_else(|| DataFusionError::Plan("ohlcv: width_ns is required".into()))?;
        if width <= 0 {
            return plan_err!("ohlcv: width_ns must be positive");
        }
        let key = args
            .get(2)
            .map(|_| arg_str(args, 2, "ohlcv"))
            .transpose()?
            .unwrap_or_else(|| "symbol".to_owned());
        let input = table_schema(&self.ctx, &table)?;
        for col in [key.as_str(), "price", "size"] {
            input.index_of(col)?;
        }
        // read only what the bars need: time, key, price, size, and the last column, which
        // orders rows with equal timestamps (seq / update id)
        let ts_col = input.field(0).name().clone();
        let tie_col = input.field(input.fields().len() - 1).name().clone();
        let schema = ohlcv_schema(&key);
        let ctx = self.ctx.clone();
        let out = schema.clone();
        let compute: Compute = Arc::new(move |projection| {
            let (ctx, out, table, key) = (ctx.clone(), out.clone(), table.clone(), key.clone());
            let (ts_col, tie_col) = (ts_col.clone(), tie_col.clone());
            async move {
                let mut cols: Vec<&str> = vec![ts_col.as_str(), key.as_str(), "price", "size"];
                if !cols.contains(&tie_col.as_str()) {
                    cols.push(tie_col.as_str());
                }
                let tie = cols
                    .iter()
                    .position(|c| *c == tie_col.as_str())
                    .unwrap_or(0);
                let input = collect_values(&ctx, &table, &cols).await?;
                blocking(move || {
                    let bars = match ohlcv_in_order(&input, &key, width, tie)? {
                        Some(bars) => bars,
                        None => ohlcv_sorted(&input.concat()?, &key, width, tie)?,
                    };
                    Ok(vec![project(
                        ohlcv_batch(&out, &bars)?,
                        projection.as_deref(),
                    )?])
                })
                .await
            }
            .boxed()
        });
        Ok(Arc::new(LazyTable {
            name: "ohlcv",
            schema,
            compute,
        }))
    }
}

fn ohlcv_schema(key: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(key, DataType::UInt32, false),
        Field::new(
            "bucket",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("open", DataType::Int64, false),
        Field::new("high", DataType::Int64, false),
        Field::new("low", DataType::Int64, false),
        Field::new("close", DataType::Int64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("notional", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]))
}

/// Bars of every key, keys ascending.
type Bars = Vec<(u32, Vec<Ohlcv>)>;

/// Bars from input in stored order, batch by batch and without a copy: every run of one key
/// is bucketed where it lies, and a bucket that continues across two runs is merged.
///
/// `None` when a key's rows are not in `(ts, tie)` order, or keys alternate too often for this
/// to pay (the caller takes the second path).
fn ohlcv_in_order(input: &Input, key: &str, width: i64, tie_col: usize) -> Result<Option<Bars>> {
    struct State {
        key: u32,
        last: Option<(i64, i64)>,
        bars: Vec<Ohlcv>,
    }
    if !long_runs(&input.batches, key)? {
        return Ok(None);
    }
    let mut slot_of: HashMap<u32, usize> = HashMap::new();
    let mut states: Vec<State> = Vec::new();
    let mut buckets: Vec<Ohlcv> = Vec::new();
    for batch in &input.batches {
        let (ts, keys) = (ts_ns(batch, 0)?, key_u32(batch, key)?);
        let (price, size) = (i64_values(batch, "price")?, i64_values(batch, "size")?);
        // rows with equal timestamps are ordered by the tie-break column (seq / update id)
        let tie = tie_values(batch, tie_col);
        let mut a = 0;
        while a < keys.len() {
            let b = run_end(&keys, a);
            let slot = *slot_of.entry(keys[a]).or_insert_with(|| {
                states.push(State {
                    key: keys[a],
                    last: None,
                    bars: Vec::new(),
                });
                states.len() - 1
            });
            let st = &mut states[slot];
            if !in_order(ts, &tie, a, b, st.last) {
                return Ok(None);
            }
            st.last = Some((ts[b - 1], tie[b - 1]));
            time_buckets(&ts[a..b], &price[a..b], &size[a..b], width, &mut buckets);
            let mut fresh = buckets.iter();
            if let (Some(open), Some(first)) = (st.bars.last_mut(), buckets.first())
                && open.start_ns == first.start_ns
            {
                // the run starts inside the bucket the previous run of this key ended in
                open.high = open.high.max(first.high);
                open.low = open.low.min(first.low);
                open.close = first.close;
                open.volume = open.volume.wrapping_add(first.volume);
                open.notional += first.notional;
                open.count += first.count;
                fresh.next();
            }
            st.bars.extend(fresh);
            a = b;
        }
    }
    states.sort_unstable_by_key(|s| s.key);
    Ok(Some(states.into_iter().map(|s| (s.key, s.bars)).collect()))
}

/// Bars from input in any order (the second path): rows are grouped per key and a key is
/// sorted when it is not in `(ts, tie)` order.
fn ohlcv_sorted(batch: &RecordBatch, key: &str, width: i64, tie_col: usize) -> Result<Bars> {
    let ts = ts_ns(batch, 0)?;
    let keys = key_u32(batch, key)?;
    let (price, size) = (i64_values(batch, "price")?, i64_values(batch, "size")?);
    let tie = tie_values(batch, tie_col);
    let mut out = Vec::new();
    let mut buckets: Vec<Ohlcv> = Vec::new();
    for (k, rows) in rows_by_key(&keys, ts, Some(&tie))? {
        let (t, p, s) = (
            at_rows(ts, &rows),
            at_rows(price, &rows),
            at_rows(size, &rows),
        );
        time_buckets(&t, &p, &s, width, &mut buckets);
        out.push((k, buckets.clone()));
    }
    Ok(out)
}

fn ohlcv_batch(schema: &SchemaRef, bars: &Bars) -> Result<RecordBatch> {
    let n: usize = bars.iter().map(|b| b.1.len()).sum();
    let (mut out_key, mut out_start) = (Vec::with_capacity(n), Vec::with_capacity(n));
    let (mut open, mut high) = (Vec::with_capacity(n), Vec::with_capacity(n));
    let (mut low, mut close) = (Vec::with_capacity(n), Vec::with_capacity(n));
    let (mut volume, mut notional) = (Vec::with_capacity(n), Vec::with_capacity(n));
    let mut count = Vec::with_capacity(n);
    for (k, buckets) in bars {
        for b in buckets {
            out_key.push(*k);
            out_start.push(b.start_ns);
            open.push(b.open);
            high.push(b.high);
            low.push(b.low);
            close.push(b.close);
            volume.push(b.volume);
            notional.push(i64::try_from(b.notional / 100_000_000).unwrap_or(i64::MAX)); // Σ price·size / 1e8 (size scale)
            count.push(i64::from(b.count));
        }
    }
    let out = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(out_key)),
            Arc::new(TimestampNanosecondArray::from(out_start).with_timezone("UTC")),
            Arc::new(Int64Array::from(open)),
            Arc::new(Int64Array::from(high)),
            Arc::new(Int64Array::from(low)),
            Arc::new(Int64Array::from(close)),
            Arc::new(Int64Array::from(volume)),
            Arc::new(Int64Array::from(notional)),
            Arc::new(Int64Array::from(count)),
        ],
    )?;
    Ok(out)
}

/// `rolling(table, window_ns, step_ns [, key_column])`: per key and step end, the bar of the
/// trailing window `[window_end - window_ns, window_end)`: open, high, low, close, volume,
/// notional, count. One row per step end whose window holds at least one row.
///
/// Rows are bucketed once into steps; a window is then put together from its steps with a
/// sliding queue ([`ankaa_ts::Sliding`]) in constant time, so a five-minute window that moves
/// every second costs the same per output row as a one-second one.
struct RollingFn {
    ctx: SessionContext,
}

impl std::fmt::Debug for RollingFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RollingFn")
    }
}

/// Most steps a window may span.
const MAX_STEPS_PER_WINDOW: i64 = 1_000_000;

impl TableFunctionImpl for RollingFn {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table = arg_str(args, 0, "rolling")?;
        let window = arg_i64(args, 1)?
            .ok_or_else(|| DataFusionError::Plan("rolling: window_ns is required".into()))?;
        let step = arg_i64(args, 2)?
            .ok_or_else(|| DataFusionError::Plan("rolling: step_ns is required".into()))?;
        if step <= 0 || window < step || window % step != 0 {
            return plan_err!("rolling: window_ns must be a positive multiple of step_ns");
        }
        if window / step > MAX_STEPS_PER_WINDOW {
            return plan_err!("rolling: a window may span at most {MAX_STEPS_PER_WINDOW} steps");
        }
        let key = args
            .get(3)
            .map(|_| arg_str(args, 3, "rolling"))
            .transpose()?
            .unwrap_or_else(|| "symbol".to_owned());
        let input = table_schema(&self.ctx, &table)?;
        for col in [key.as_str(), "price", "size"] {
            input.index_of(col)?;
        }
        let ts_col = input.field(0).name().clone();
        let tie_col = input.field(input.fields().len() - 1).name().clone();
        let schema = rolling_schema(&key);
        let ctx = self.ctx.clone();
        let out = schema.clone();
        let compute: Compute = Arc::new(move |projection| {
            let (ctx, out, table, key) = (ctx.clone(), out.clone(), table.clone(), key.clone());
            let (ts_col, tie_col) = (ts_col.clone(), tie_col.clone());
            async move {
                let mut cols: Vec<&str> = vec![ts_col.as_str(), key.as_str(), "price", "size"];
                if !cols.contains(&tie_col.as_str()) {
                    cols.push(tie_col.as_str());
                }
                let tie = cols
                    .iter()
                    .position(|c| *c == tie_col.as_str())
                    .unwrap_or(0);
                let input = collect_values(&ctx, &table, &cols).await?;
                blocking(move || {
                    let steps = match ohlcv_in_order(&input, &key, step, tie)? {
                        Some(bars) => bars,
                        None => ohlcv_sorted(&input.concat()?, &key, step, tie)?,
                    };
                    let mut windows: Bars = Vec::with_capacity(steps.len());
                    for (k, panes) in &steps {
                        let mut rolled = Vec::new();
                        rolling_bars(panes, window, step, &mut rolled);
                        windows.push((*k, rolled));
                    }
                    Ok(vec![project(
                        ohlcv_batch(&out, &windows)?,
                        projection.as_deref(),
                    )?])
                })
                .await
            }
            .boxed()
        });
        Ok(Arc::new(LazyTable {
            name: "rolling",
            schema,
            compute,
        }))
    }
}

/// As [`ohlcv_schema`], with the window's exclusive end in place of the bucket start.
fn rolling_schema(key: &str) -> SchemaRef {
    let bars = ohlcv_schema(key);
    let fields: Vec<Field> = bars
        .fields()
        .iter()
        .map(|f| {
            if f.name() == "bucket" {
                f.as_ref().clone().with_name("window_end")
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Parse `'0.5,0.99,0.999'` into quantiles in `[0, 1]`.
fn parse_quantiles(spec: &str) -> Result<Vec<f64>> {
    let mut out = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let q: f64 = part
            .parse()
            .map_err(|_| DataFusionError::Plan(format!("percentiles: bad quantile {part:?}")))?;
        if !(0.0..=1.0).contains(&q) {
            return plan_err!("percentiles: quantile {q} outside [0, 1]");
        }
        out.push(q);
    }
    if out.is_empty() {
        return plan_err!("percentiles: no quantiles given");
    }
    out.sort_by(f64::total_cmp);
    out.dedup();
    if out.len() > 64 {
        return plan_err!("percentiles: at most 64 quantiles");
    }
    Ok(out)
}

/// Column name for a quantile: `0.5 -> p50`, `0.999 -> p99_9`, `1 -> p100`.
fn quantile_column(q: f64) -> String {
    let pct = format!("{}", (q * 100.0 * 1e6).round() / 1e6);
    format!("p{}", pct.replace('.', "_"))
}

/// `percentiles(table, value_column, 'q1,q2,...' [, key_column])`: per key (or overall), the
/// row count, min, max and the requested quantiles from a [`UddSketch`] with relative error
/// [`PERCENTILE_ALPHA`]. Values are read as `Float64` (fixed-point columns stay in raw units).
/// `collapses > 0` means the bucket budget was exceeded and the bound is `alpha·2^collapses`.
struct PercentilesFn {
    ctx: SessionContext,
}

impl std::fmt::Debug for PercentilesFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PercentilesFn")
    }
}

impl TableFunctionImpl for PercentilesFn {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table = arg_str(args, 0, "percentiles")?;
        let column = arg_str(args, 1, "percentiles")?;
        let qs = parse_quantiles(&arg_str(args, 2, "percentiles")?)?;
        let key = args
            .get(3)
            .map(|_| arg_str(args, 3, "percentiles"))
            .transpose()?;
        let input = table_schema(&self.ctx, &table)?;
        input.index_of(&column)?;
        if let Some(k) = &key {
            input.index_of(k)?;
        }
        let schema = percentiles_schema(key.as_deref(), &qs);
        let ctx = self.ctx.clone();
        let out = schema.clone();
        let compute: Compute = Arc::new(move |projection| {
            let (ctx, out, table) = (ctx.clone(), out.clone(), table.clone());
            let (column, key, qs) = (column.clone(), key.clone(), qs.clone());
            async move {
                // only the value column and the key are read
                let mut cols: Vec<&str> = vec![column.as_str()];
                if let Some(k) = key.as_deref().filter(|k| *k != column.as_str()) {
                    cols.push(k);
                }
                let input = collect_values(&ctx, &table, &cols).await?;
                blocking(move || {
                    let stats = percentiles_compute(&out, &input, &column, key.as_deref(), &qs)?;
                    Ok(vec![project(stats, projection.as_deref())?])
                })
                .await
            }
            .boxed()
        });
        Ok(Arc::new(LazyTable {
            name: "percentiles",
            schema,
            compute,
        }))
    }
}

struct GapfillFn {
    ctx: SessionContext,
}

impl std::fmt::Debug for GapfillFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GapfillFn")
    }
}

/// Most rows one `gapfill` call may return (buckets × keys).
pub const MAX_GAPFILL_ROWS: u64 = 10_000_000;

/// `gapfill('table', 'value', width_ns [, 'key' [, 'agg' [, 'method' [, start_ns, end_ns]]]])`:
/// one row per bucket of `width_ns` (aligned to the epoch, as `date_bin` with the default
/// origin) and per key, from each key's first to its last bucket with a reading, or over
/// `[start_ns, end_ns)` for every key that has a reading there. `agg` (default `avg`) is
/// applied to a bucket's readings; a bucket without readings is filled by `method` (`null`,
/// `locf`, `linear`; default `null`; with `count` it is 0). `NULL` readings are skipped.
/// Columns: the key (when given), `bucket`, `value` (`Float64`; `fixed8` values as decimals),
/// `filled` (true for a filled bucket).
impl TableFunctionImpl for GapfillFn {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        use ankaa_ts::gapfill::{Agg, Fill, bucket_count, bucket_of};
        let table = arg_str(args, 0, "gapfill")?;
        let column = arg_str(args, 1, "gapfill")?;
        let width = arg_i64(args, 2)?
            .ok_or_else(|| DataFusionError::Plan("gapfill: width_ns is required".into()))?;
        if width <= 0 {
            return plan_err!("gapfill: width_ns must be positive");
        }
        let opt_str = |i: usize| {
            args.get(i)
                .map(|_| arg_str(args, i, "gapfill"))
                .transpose()
                .map(|s| s.filter(|s| !s.is_empty()))
        };
        let key = opt_str(3)?;
        let agg_name = opt_str(4)?.unwrap_or_else(|| "avg".into());
        let agg = Agg::parse(&agg_name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "gapfill: aggregate {agg_name:?} is not one of avg, min, max, sum, count, first, last"
            ))
        })?;
        let method_name = opt_str(5)?.unwrap_or_else(|| "null".into());
        let fill = Fill::parse(&method_name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "gapfill: method {method_name:?} is not one of null, locf, linear"
            ))
        })?;
        let range = match (arg_i64(args, 6)?, arg_i64(args, 7)?) {
            (None, None) => None,
            (Some(a), Some(b)) if a < b => Some((a, b)),
            _ => return plan_err!("gapfill: start_ns and end_ns come together, start before end"),
        };
        if let Some((a, b)) = range
            && bucket_count(bucket_of(a, width), bucket_of(b - 1, width), width) > MAX_GAPFILL_ROWS
        {
            return plan_err!(
                "gapfill: the range holds more than {MAX_GAPFILL_ROWS} buckets; use wider buckets"
            );
        }
        let input = table_schema(&self.ctx, &table)?;
        let value_field = input.field(input.index_of(&column)?);
        if !value_field.data_type().is_numeric() {
            return plan_err!("gapfill: column {column} is not numeric");
        }
        // fixed-point columns hold ticks: the output is the decimal value
        let scale: i32 = value_field
            .metadata()
            .get("ankaa.scale")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if let Some(k) = &key {
            input.index_of(k)?;
        }
        let ts_col = input.field(0).name().clone();
        if !matches!(input.field(0).data_type(), DataType::Timestamp(..)) {
            return plan_err!("gapfill: the first column of {table} is not its timestamp");
        }
        let schema = gapfill_schema(key.as_deref());
        let ctx = self.ctx.clone();
        let out = schema.clone();
        let compute: Compute = Arc::new(move |projection| {
            let (ctx, out, table) = (ctx.clone(), out.clone(), table.clone());
            let (column, key, ts_col) = (column.clone(), key.clone(), ts_col.clone());
            async move {
                let mut cols: Vec<&str> = vec![ts_col.as_str(), column.as_str()];
                if let Some(k) = key.as_deref() {
                    cols.push(k);
                }
                let filter = range.map(|r| time_range(&ts_col, r));
                let input = collect(&ctx, &table, &cols, true, filter).await?;
                let pool = ctx.runtime_env().memory_pool.clone();
                blocking(move || {
                    let spec = GapfillSpec {
                        key: key.as_deref(),
                        width,
                        agg,
                        fill,
                        range,
                        scale,
                    };
                    let mut held = MemoryConsumer::new("gapfill output").register(&pool);
                    let batch = gapfill_compute(&out, &input, &spec, &mut held)?;
                    Ok(vec![project(batch, projection.as_deref())?])
                })
                .await
            }
            .boxed()
        });
        Ok(Arc::new(LazyTable {
            name: "gapfill",
            schema,
            compute,
        }))
    }
}

/// `ts_col >= start AND ts_col < end`, as the scan's time-range pushdown recognises it.
fn time_range(ts_col: &str, (start, end): (i64, i64)) -> Expr {
    let at = |ns| {
        Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(ns), Some("UTC".into())),
            None,
        )
    };
    datafusion::prelude::col(ts_col)
        .gt_eq(at(start))
        .and(datafusion::prelude::col(ts_col).lt(at(end)))
}

fn gapfill_schema(key: Option<&str>) -> SchemaRef {
    let mut fields = Vec::new();
    if let Some(k) = key {
        fields.push(Field::new(k, DataType::UInt32, false));
    }
    fields.push(Field::new(
        "bucket",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new("value", DataType::Float64, true));
    fields.push(Field::new("filled", DataType::Boolean, false));
    Arc::new(Schema::new(fields))
}

struct GapfillSpec<'a> {
    key: Option<&'a str>,
    width: i64,
    agg: ankaa_ts::gapfill::Agg,
    fill: ankaa_ts::gapfill::Fill,
    range: Option<(i64, i64)>,
    scale: i32,
}

/// Fold the readings into per-key series (rows of a key in stored order fold into the last
/// bucket with one compare; a key's runs are handed over as slices), then write the dense,
/// filled buckets of every key in key order.
fn gapfill_compute(
    schema: &SchemaRef,
    input: &Input,
    spec: &GapfillSpec<'_>,
    held: &mut MemoryReservation,
) -> Result<RecordBatch> {
    use ankaa_ts::gapfill::{Agg, Filled, Series, bucket_count, bucket_of};
    let width = spec.width;
    let mut slot_of: HashMap<u32, usize> = HashMap::new();
    let mut series: Vec<(u32, Series)> = Vec::new();
    for batch in &input.batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| DataFusionError::Plan("gapfill: timestamp column".into()))?
            .values();
        let values = cast(batch.column(1), &DataType::Float64)?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Plan("gapfill: cast failed".into()))?
            .values();
        let keys = spec.key.map(|k| key_u32(batch, k)).transpose()?;
        let mut a = 0;
        while a < ts.len() {
            let (k, b) = match &keys {
                Some(ks) => (ks[a], run_end(ks, a)),
                None => (0, ts.len()),
            };
            let slot = *slot_of.entry(k).or_insert_with(|| {
                series.push((k, Series::default()));
                series.len() - 1
            });
            let s = &mut series[slot].1;
            for i in a..b {
                s.push(bucket_of(ts[i], width), ts[i], values[i]);
            }
            a = b;
        }
    }
    series.sort_unstable_by_key(|(k, _)| *k);
    // the buckets of each key, and their total, before anything is written
    let mut spans: Vec<(i64, i64)> = Vec::with_capacity(series.len());
    let mut total: u64 = 0;
    for (_, s) in &mut series {
        let span = match spec.range {
            Some((a, b)) => (bucket_of(a, width), bucket_of(b - 1, width)),
            None => s.span().unwrap_or((0, -1)),
        };
        total = total.saturating_add(bucket_count(span.0, span.1, width));
        spans.push(span);
    }
    if total > MAX_GAPFILL_ROWS {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "gapfill: {total} buckets exceed {MAX_GAPFILL_ROWS}; use wider buckets or start_ns/end_ns"
        )));
    }
    let rows = usize::try_from(total).unwrap_or(usize::MAX);
    // bucket, value, key: 8 + 8 + 4 bytes; validity and filled flags: 2
    held.try_grow(rows.saturating_mul(22))?;
    let mut filled = Filled {
        bucket: Vec::with_capacity(rows),
        value: Vec::with_capacity(rows),
        valid: Vec::with_capacity(rows),
        filled: Vec::with_capacity(rows),
    };
    let mut keys: Vec<u32> = Vec::with_capacity(if spec.key.is_some() { rows } else { 0 });
    for ((k, s), (lo, hi)) in series.iter_mut().zip(&spans) {
        let n = filled.extend(s, *lo, *hi, width, spec.agg, spec.fill);
        if spec.key.is_some() {
            keys.extend(std::iter::repeat_n(*k, n));
        }
    }
    // ticks to decimals: one division per value, after aggregation and interpolation (both
    // commute with it); counts stay counts
    if spec.scale > 0 && spec.agg != Agg::Count {
        let div = 10f64.powi(spec.scale);
        for v in &mut filled.value {
            *v /= div;
        }
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(4);
    if spec.key.is_some() {
        columns.push(Arc::new(UInt32Array::from(keys)));
    }
    columns.push(Arc::new(
        TimestampNanosecondArray::from(filled.bucket).with_timezone("UTC"),
    ));
    columns.push(Arc::new(Float64Array::new(
        filled.value.into(),
        Some(arrow::buffer::NullBuffer::from(filled.valid)),
    )));
    columns.push(Arc::new(arrow::array::BooleanArray::from(filled.filled)));
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn percentiles_schema(key: Option<&str>, qs: &[f64]) -> SchemaRef {
    let mut fields = Vec::new();
    if let Some(k) = key {
        fields.push(Field::new(k, DataType::UInt32, false));
    }
    fields.push(Field::new("count", DataType::UInt64, false));
    fields.push(Field::new("min", DataType::Float64, false));
    fields.push(Field::new("max", DataType::Float64, false));
    for &q in qs {
        fields.push(Field::new(quantile_column(q), DataType::Float64, true));
    }
    fields.push(Field::new("collapses", DataType::UInt32, false));
    Arc::new(Schema::new(fields))
}

fn percentiles_compute(
    schema: &SchemaRef,
    input: &Input,
    column: &str,
    key: Option<&str>,
    qs: &[f64],
) -> Result<RecordBatch> {
    // one sketch per key, fed run by run (order does not matter to a sketch): a key's rows
    // come in long runs, each handed over as one slice
    let mut slot_of: HashMap<u32, usize> = HashMap::new();
    let mut sketches: Vec<(u32, UddSketch)> = Vec::new();
    let mut finite: Vec<f64> = Vec::new();
    for batch in &input.batches {
        let values = cast(
            batch.column(batch.schema().index_of(column)?),
            &DataType::Float64,
        )?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Plan("percentiles: cast failed".into()))?;
        let keys = key.map(|k| key_u32(batch, k)).transpose()?;
        let mut a = 0;
        while a < values.len() {
            let (k, b) = match &keys {
                Some(ks) => (ks[a], run_end(ks, a)),
                None => (0, values.len()),
            };
            let slot = *slot_of.entry(k).or_insert_with(|| {
                sketches.push((k, UddSketch::new(PERCENTILE_ALPHA, PERCENTILE_MAX_BUCKETS)));
                sketches.len() - 1
            });
            let sketch = &mut sketches[slot].1;
            if values.null_count() == 0 {
                sketch.add_slice(&values.values()[a..b]);
            } else {
                finite.clear();
                finite.extend(
                    (a..b)
                        .filter(|&i| values.is_valid(i))
                        .map(|i| values.value(i)),
                );
                sketch.add_slice(&finite);
            }
            a = b;
        }
    }
    sketches.sort_unstable_by_key(|e| e.0);
    let per_key: std::collections::BTreeMap<u32, (UddSketch, f64, f64)> = sketches
        .into_iter()
        .map(|(k, s)| {
            let (lo, hi) = (s.min(), s.max());
            (k, (s, lo, hi))
        })
        .collect();
    let mut columns: Vec<ArrayRef> = Vec::new();
    if key.is_some() {
        columns.push(Arc::new(UInt32Array::from(
            per_key.keys().copied().collect::<Vec<_>>(),
        )));
    }
    columns.push(Arc::new(UInt64Array::from(
        per_key.values().map(|e| e.0.count()).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        per_key.values().map(|e| e.1).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        per_key.values().map(|e| e.2).collect::<Vec<_>>(),
    )));
    for &q in qs {
        // clamp to the observed range: the sketch midpoint can sit just outside it
        columns.push(Arc::new(Float64Array::from(
            per_key
                .values()
                .map(|e| e.0.quantile(q).map(|x| x.clamp(e.1, e.2)))
                .collect::<Vec<_>>(),
        )));
    }
    columns.push(Arc::new(UInt32Array::from(
        per_key
            .values()
            .map(|e| e.0.collapses())
            .collect::<Vec<_>>(),
    )));
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_names() {
        assert_eq!(quantile_column(0.5), "p50");
        assert_eq!(quantile_column(0.99), "p99");
        assert_eq!(quantile_column(0.999), "p99_9");
        assert_eq!(quantile_column(1.0), "p100");
        assert_eq!(quantile_column(0.0), "p0");
    }

    /// Trades in runs of `block` rows per symbol, two chunks, optionally registered latest first.
    fn trades_ctx(block: i64, reversed: bool) -> SessionContext {
        use ankaa_store::{Chunk, ChunkId, Schema as S, Value};
        let ctx = SessionContext::new();
        let mut chunks = Vec::new();
        for c in 0..2i64 {
            let mut chunk = Chunk::new(ChunkId(u64::try_from(c).unwrap()), S::trades());
            for r in 0..20_000i64 {
                let i = c * 20_000 + r;
                chunk
                    .append_row(&[
                        Value::I64(i * 10),
                        Value::U32(u32::try_from((i / block) % 3).unwrap()),
                        Value::I64(100 + i % 7),
                        Value::I64(1),
                        Value::U8(0),
                        Value::I64(i),
                    ])
                    .unwrap();
            }
            chunk.seal();
            chunks.push(chunk);
        }
        if reversed {
            chunks.reverse();
        }
        crate::register_chunks(&ctx, "trades", S::trades(), chunks).unwrap();
        ctx
    }

    /// Which path an input takes: in place for long runs in stored order, the grouping path
    /// for alternating keys or rows out of order.
    #[tokio::test]
    async fn in_place_paths_are_taken_only_for_ordered_long_runs() {
        let cols = ["ts", "symbol", "price", "size", "seq"];
        for (block, reversed, in_place) in
            [(500, false, true), (500, true, false), (1, false, false)]
        {
            let ctx = trades_ctx(block, reversed);
            let input = collect_columns(&ctx, "trades", &cols).await.unwrap();
            assert!(input.batches.len() > 1, "several batches");
            let bars = ohlcv_in_order(&input, "symbol", 1_000, 4).unwrap();
            assert_eq!(
                bars.is_some(),
                in_place,
                "ohlcv, block {block}, reversed {reversed}"
            );
            let sorted = ohlcv_sorted(&input.concat().unwrap(), "symbol", 1_000, 4).unwrap();
            if let Some(bars) = bars {
                assert_eq!(bars, sorted, "both paths give the same bars");
            }
            let index = index_right("symbol", 4, &input).unwrap();
            assert_eq!(
                index.is_some(),
                !reversed,
                "right index, block {block}, reversed {reversed}"
            );
            assert_eq!(long_runs(&input.batches, "symbol").unwrap(), block > 1);
        }
    }

    #[test]
    fn quantile_spec() {
        assert_eq!(parse_quantiles("0.5, 0.99").unwrap(), vec![0.5, 0.99]);
        assert!(parse_quantiles("1.5").is_err());
        assert!(parse_quantiles("").is_err());
        assert!(parse_quantiles("x").is_err());
    }
}
