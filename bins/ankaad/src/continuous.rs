//! Continuous queries: operators from `ankaa_ts::incremental` fed row by row from the ingest
//! path, materialised into tables `cq_<name>` (closed results) and `cq_<name>_live` (the open
//! bar / window per key). Defined in `[[continuous]]`.
//!
//! The outputs are **derived state**: they live in memory only and are never written to
//! segments or the WAL. At start, after the input tables have been restored and the WAL has
//! been replayed, [`Continuous::rebuild`] feeds every retained input row through the operators
//! again in timestamp order, so results (including the bar that was open at shutdown) are the
//! same before and after a restart or a crash.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use ankaa_core::config::{ContinuousConfig, ContinuousKind};
use ankaa_store::{Chunk, ChunkRef, Column, ColumnType, Field, Schema, Table, Value};
use ankaa_ts::{IncrementalAsof, IncrementalOhlcv, IncrementalPercentiles, IncrementalRolling};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, TimestampNanosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field as AField, Schema as ASchema, TimeUnit};

/// Tables that share the exchange symbol dictionary (their `symbol` ids are comparable).
const MARKET_TABLES: [&str; 3] = ["trades", "quotes", "book_deltas"];

/// Column positions a query needs in its input table.
#[derive(Debug, Clone)]
struct Binding {
    key: Option<usize>,
    value: Option<usize>,
    size: Option<usize>,
}

fn is_i64(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::TimestampNs | ColumnType::FixedPoint { .. } | ColumnType::Int64
    )
}

/// Resolve the columns of one input table. The key must be a `dict` column: its ids are dense,
/// so they can index partitions; an arbitrary integer could ask for billions of them.
fn bind(
    schema: &Schema,
    key: &str,
    value: Option<&str>,
    size: Option<&str>,
    name: &str,
) -> anyhow::Result<Binding> {
    let pos = |col: &str| {
        schema
            .fields()
            .iter()
            .position(|f| f.name == col)
            .ok_or_else(|| {
                anyhow::anyhow!("continuous.{name}: column {col:?} not in the input table")
            })
    };
    let key = if key.is_empty() {
        None
    } else {
        let i = pos(key)?;
        anyhow::ensure!(
            schema.fields()[i].ty == ColumnType::DictU32,
            "continuous.{name}: key column {key:?} must be a dict column (or key = \"\" for a single series)"
        );
        Some(i)
    };
    Ok(Binding {
        key,
        value: value.map(pos).transpose()?,
        size: size.map(pos).transpose()?,
    })
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::I64(x) => *x,
        Value::U32(x) => i64::from(*x),
        Value::U8(x) => i64::from(*x),
        #[allow(clippy::cast_possible_truncation)]
        Value::F64(x) => *x as i64,
        Value::Null => 0,
    }
}

fn as_f64(v: &Value) -> f64 {
    match v {
        #[allow(clippy::cast_precision_loss)]
        Value::I64(x) => *x as f64,
        Value::U32(x) => f64::from(*x),
        Value::U8(x) => f64::from(*x),
        Value::F64(x) => *x,
        // not finite: the operators skip it
        Value::Null => f64::NAN,
    }
}

/// Lossless `i64` carrier for a value of any column type (ASOF keeps right rows as `i64`s).
fn carry(v: &Value) -> i64 {
    match v {
        Value::I64(x) => *x,
        Value::U32(x) => i64::from(*x),
        Value::U8(x) => i64::from(*x),
        Value::F64(x) => i64::from_ne_bytes(x.to_bits().to_ne_bytes()),
        Value::Null => 0,
    }
}

/// Inverse of [`carry`] for a column of type `ty`.
fn uncarry(ty: ColumnType, x: i64) -> Value {
    match ty {
        ColumnType::DictU32 => Value::U32(u32::try_from(x).unwrap_or(0)),
        ColumnType::U8 => Value::U8(u8::try_from(x).unwrap_or(0)),
        ColumnType::Float64 => Value::F64(f64::from_bits(u64::from_ne_bytes(x.to_ne_bytes()))),
        ColumnType::TimestampNs | ColumnType::FixedPoint { .. } | ColumnType::Int64 => {
            Value::I64(x)
        }
    }
}

fn key_of(b: &Binding, row: &[Value]) -> u32 {
    b.key.map_or(0, |i| match row[i] {
        Value::U32(k) => k,
        _ => 0,
    })
}

enum State {
    Ohlcv {
        op: IncrementalOhlcv,
        b: Binding,
    },
    Rolling {
        op: IncrementalRolling,
        b: Binding,
        /// Scratch for the windows one row completes.
        done: Vec<(u32, ankaa_ts::Ohlcv)>,
    },
    Percentiles {
        op: IncrementalPercentiles,
        b: Binding,
        quantiles: Vec<f64>,
    },
    Asof {
        op: IncrementalAsof,
        left: Binding,
        right_table: String,
        right_b: Binding,
        /// Right value columns carried into the output: `(index, type)`.
        right_cols: Vec<(usize, ColumnType)>,
        left_cols: Vec<usize>,
        /// Scratch for the right values of one row.
        scratch: Vec<i64>,
    },
}

/// One continuous query.
pub struct Query {
    /// Name (`cq_<name>` in SQL).
    pub name: String,
    /// Input table.
    pub table: String,
    /// Name of the key column in the output (empty = single series).
    key_name: String,
    state: State,
    /// Output table of closed results.
    pub out: Table,
    max_rows: usize,
    /// Late rows dropped (operator counter, copied for metrics).
    pub late_rows: u64,
    /// Output rows the table refused (schema mismatch: a bug, exported as a metric).
    pub rejected_rows: u64,
    /// Output rows appended since the size bound was last enforced.
    since_bound_check: usize,
    /// The key is the column the input tables are partitioned by: every key's rows live in
    /// one partition, so a rebuild can go partition by partition.
    aligned: bool,
}

/// All continuous queries, indexed by input table for the ingest hot path.
#[derive(Default)]
pub struct Continuous {
    queries: Vec<Query>,
    by_table: HashMap<String, Vec<usize>>,
}

/// What a rebuild did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RebuildStats {
    /// Input rows fed through the operators.
    pub rows: u64,
    /// Chunks that could not be decoded (their rows are missing from the results).
    pub bad_chunks: u64,
}

impl Continuous {
    /// Build from config; `schemas` maps table name → schema (market + user tables).
    #[allow(clippy::too_many_lines)]
    pub fn new(
        cfgs: &[ContinuousConfig],
        schemas: &HashMap<String, Schema>,
    ) -> anyhow::Result<Self> {
        Self::with_partitioning(cfgs, schemas, &HashMap::new())
    }

    /// [`Self::new`], told which column each table is partitioned by (table → column). A query
    /// keyed by that column is rebuilt one partition at a time instead of merging them all.
    #[allow(clippy::too_many_lines)]
    pub fn with_partitioning(
        cfgs: &[ContinuousConfig],
        schemas: &HashMap<String, Schema>,
        partitioned_by: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let mut c = Self::default();
        for cfg in cfgs {
            let schema = schemas.get(&cfg.table).ok_or_else(|| {
                anyhow::anyhow!("continuous.{}: unknown table {:?}", cfg.name, cfg.table)
            })?;
            anyhow::ensure!(
                !cfg.table.starts_with("cq_"),
                "continuous.{}: the input cannot be another continuous query",
                cfg.name
            );
            let b = bind(
                schema,
                &cfg.key,
                cfg.value.as_deref(),
                cfg.size.as_deref(),
                &cfg.name,
            )?;
            let key_field = if cfg.key.is_empty() { "key" } else { &cfg.key };
            let value_ty = b.value.map(|i| schema.fields()[i].ty);
            let (state, out_schema) = match cfg.kind {
                ContinuousKind::Ohlcv | ContinuousKind::Rolling => {
                    let vt = value_ty.unwrap_or(ColumnType::Int64);
                    anyhow::ensure!(
                        is_i64(vt),
                        "continuous.{}: ohlcv needs an integer or fixed-point value column (use percentiles for float64)",
                        cfg.name
                    );
                    if let Some(si) = b.size {
                        anyhow::ensure!(
                            is_i64(schema.fields()[si].ty),
                            "continuous.{}: the size column must be an integer or fixed-point column",
                            cfg.name
                        );
                    }
                    let fields = vec![
                        Field::new("ts", ColumnType::TimestampNs),
                        Field::new(key_field, ColumnType::DictU32),
                        Field::new("open", vt),
                        Field::new("high", vt),
                        Field::new("low", vt),
                        Field::new("close", vt),
                        Field::new("volume", ColumnType::Int64),
                        Field::new("notional", ColumnType::Int64),
                        Field::new("count", ColumnType::Int64),
                    ];
                    let state = if cfg.kind == ContinuousKind::Rolling {
                        // `ts` is the end of the window
                        State::Rolling {
                            op: IncrementalRolling::new(
                                cfg.width_ns.unwrap_or(1),
                                cfg.step_ns.unwrap_or(1),
                            ),
                            b,
                            done: Vec::new(),
                        }
                    } else {
                        State::Ohlcv {
                            op: IncrementalOhlcv::new(cfg.width_ns.unwrap_or(1)),
                            b,
                        }
                    };
                    (state, Schema::new(fields)?)
                }
                ContinuousKind::Percentiles => {
                    anyhow::ensure!(
                        value_ty.is_some_and(|t| t != ColumnType::DictU32),
                        "continuous.{}: percentiles needs a numeric value column",
                        cfg.name
                    );
                    let mut fields = vec![
                        Field::new("ts", ColumnType::TimestampNs),
                        Field::new(key_field, ColumnType::DictU32),
                        Field::new("count", ColumnType::Int64),
                        Field::new("min", ColumnType::Float64),
                        Field::new("max", ColumnType::Float64),
                    ];
                    for q in &cfg.quantiles {
                        let pct = format!("{}", (q * 100.0 * 1e6).round() / 1e6);
                        fields.push(Field::new(
                            format!("p{}", pct.replace('.', "_")),
                            ColumnType::Float64,
                        ));
                    }
                    (
                        State::Percentiles {
                            op: IncrementalPercentiles::new(
                                cfg.width_ns.unwrap_or(1),
                                cfg.quantiles.clone(),
                                0.01,
                            ),
                            b,
                            quantiles: cfg.quantiles.clone(),
                        },
                        Schema::new(fields)?,
                    )
                }
                ContinuousKind::Asof => {
                    let right_name = cfg.right_table.clone().unwrap_or_default();
                    let right = schemas.get(&right_name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "continuous.{}: unknown right_table {right_name:?}",
                            cfg.name
                        )
                    })?;
                    anyhow::ensure!(
                        right_name != cfg.table,
                        "continuous.{}: asof needs two different tables",
                        cfg.name
                    );
                    // keys are dictionary ids: they only mean the same thing on both sides when
                    // the tables share a dictionary, which today is the exchange symbol table
                    anyhow::ensure!(
                        cfg.key.is_empty()
                            || (MARKET_TABLES.contains(&cfg.table.as_str())
                                && MARKET_TABLES.contains(&right_name.as_str())),
                        "continuous.{}: a keyed asof needs tables that share the key dictionary (trades, quotes, book_deltas); user tables keep one dictionary each",
                        cfg.name
                    );
                    let right_b = bind(right, &cfg.key, None, None, &cfg.name)?;
                    let mut fields: Vec<Field> = schema.fields().to_vec();
                    let left_cols: Vec<usize> = (0..schema.fields().len()).collect();
                    let mut right_cols = Vec::new();
                    for (i, f) in right.fields().iter().enumerate() {
                        if i == 0 || Some(i) == right_b.key {
                            continue;
                        }
                        fields.push(Field::new(format!("{right_name}_{}", f.name), f.ty));
                        right_cols.push((i, f.ty));
                    }
                    (
                        State::Asof {
                            op: IncrementalAsof::new(),
                            left: b,
                            right_table: right_name,
                            right_b,
                            scratch: Vec::with_capacity(right_cols.len()),
                            right_cols,
                            left_cols,
                        },
                        Schema::new(fields)?,
                    )
                }
            };
            let keyed_by_partition = |table: &str| {
                !cfg.key.is_empty() && partitioned_by.get(table).is_some_and(|c| *c == cfg.key)
            };
            let aligned = keyed_by_partition(&cfg.table)
                && match &state {
                    State::Asof { right_table, .. } => keyed_by_partition(right_table),
                    _ => true,
                };
            let idx = c.queries.len();
            c.by_table.entry(cfg.table.clone()).or_default().push(idx);
            if let State::Asof { right_table, .. } = &state {
                c.by_table.entry(right_table.clone()).or_default().push(idx);
            }
            c.queries.push(Query {
                name: cfg.name.clone(),
                table: cfg.table.clone(),
                key_name: cfg.key.clone(),
                state,
                out: Table::new(format!("cq_{}", cfg.name), out_schema, 1),
                max_rows: cfg.max_rows,
                late_rows: 0,
                rejected_rows: 0,
                since_bound_check: 0,
                aligned,
            });
        }
        Ok(c)
    }

    /// Tell every query how late the rows of its input table may arrive (table → ns, from
    /// `tables.max_lateness_ms`): its buckets and windows then close that much later, so a
    /// row the table accepts is never late for the query, and results do not depend on the
    /// order rows arrived in (a rebuild from the stored rows gives what live feeding gave).
    pub fn set_lateness(&mut self, lateness: &HashMap<String, i64>) {
        for q in &mut self.queries {
            let ns = lateness.get(&q.table).copied().unwrap_or(0);
            match &mut q.state {
                State::Ohlcv { op, .. } => op.set_lateness(ns),
                State::Rolling { op, .. } => op.set_lateness(ns),
                State::Percentiles { op, .. } => op.set_lateness(ns),
                State::Asof { .. } => {}
            }
        }
    }

    /// True when some query listens to `table` (lets the ingest path skip the hook cheaply).
    #[must_use]
    pub fn listens(&self, table: &str) -> bool {
        self.by_table.contains_key(table)
    }

    /// Queries (for registration and metrics).
    pub fn queries(&self) -> &[Query] {
        &self.queries
    }

    /// Feed one row of `table` to every query that reads it.
    pub fn on_row(&mut self, table: &str, row: &[Value]) {
        let Some(idxs) = self.by_table.get(table) else {
            return;
        };
        for &i in idxs {
            self.queries[i].feed(table, row);
        }
    }

    /// Rebuild every query from the rows its input tables hold now, merged in timestamp order
    /// (ties: the right side of an ASOF first, so a quote and a trade with the same timestamp
    /// join). Call once at start, after segments are restored and the WAL has been replayed and
    /// before live ingest; the operators and output tables must be new.
    pub fn rebuild(&mut self, tables: &HashMap<&str, &Table>) -> RebuildStats {
        let mut stats = RebuildStats::default();
        for q in &mut self.queries {
            // right side first: on equal timestamps it wins the tie
            let mut inputs: Vec<(String, &Table)> = Vec::new();
            if let State::Asof { right_table, .. } = &q.state
                && let Some(t) = tables.get(right_table.as_str())
            {
                inputs.push((right_table.clone(), *t));
            }
            if let Some(t) = tables.get(q.table.as_str()) {
                inputs.push((q.table.clone(), *t));
            }
            // one run per (input, partition): rows inside a partition are already ordered.
            // Runs that have to be merged with each other form a group; groups are processed
            // one after another, so only one group's chunks are decoded at a time.
            let mut groups: std::collections::BTreeMap<u32, Vec<Run>> =
                std::collections::BTreeMap::new();
            for (ti, (_, t)) in inputs.iter().enumerate() {
                let mut parts: Vec<(u32, Vec<ChunkRef>)> = Vec::new();
                t.for_each_partition(|p| parts.push((p.key(), p.snapshot_chunks())));
                for (key, chunks) in parts {
                    if !chunks.is_empty() {
                        // aligned: a key's rows are all in its own partition, in every input
                        let group = if q.aligned { key } else { 0 };
                        groups.entry(group).or_default().push(Run::new(
                            ti,
                            chunks,
                            t.schema().fields().len(),
                        ));
                    }
                }
            }
            let mut row: Vec<Value> = Vec::new();
            for mut runs in groups.into_values() {
                let mut heap: BinaryHeap<Reverse<(i64, usize, usize)>> = BinaryHeap::new();
                for (ri, run) in runs.iter_mut().enumerate() {
                    if let Some(ts) = run.peek(&mut stats) {
                        heap.push(Reverse((ts, run.input, ri)));
                    }
                }
                while let Some(Reverse((_, input, ri))) = heap.pop() {
                    let run = &mut runs[ri];
                    run.take(&mut row, &mut stats);
                    if !row.is_empty() {
                        q.feed(&inputs[input].0, &row);
                        stats.rows += 1;
                    }
                    if let Some(ts) = run.peek(&mut stats) {
                        heap.push(Reverse((ts, input, ri)));
                    }
                }
            }
            q.enforce_bound();
        }
        stats
    }

    /// The live (open) rows of every query as Arrow batches, for `cq_<name>_live` tables.
    #[allow(clippy::too_many_lines)]
    pub fn live_batches(&self) -> Vec<(String, RecordBatch)> {
        let mut out = Vec::new();
        for q in &self.queries {
            let batch = match &q.state {
                State::Ohlcv { .. } | State::Rolling { .. } => {
                    let owned: Vec<(u32, ankaa_ts::Ohlcv)> = match &q.state {
                        State::Ohlcv { op, .. } => op.open_bars().map(|(k, b)| (k, *b)).collect(),
                        State::Rolling { op, .. } => op.open_rows(),
                        _ => Vec::new(),
                    };
                    let bars: Vec<(u32, &ankaa_ts::Ohlcv)> =
                        owned.iter().map(|(k, b)| (*k, b)).collect();
                    let key_name = if q.key_name.is_empty() {
                        "key"
                    } else {
                        q.key_name.as_str()
                    };
                    let schema = Arc::new(ASchema::new(vec![
                        AField::new(
                            "ts",
                            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                            false,
                        ),
                        AField::new(key_name, DataType::UInt32, false),
                        AField::new("open", DataType::Int64, false),
                        AField::new("high", DataType::Int64, false),
                        AField::new("low", DataType::Int64, false),
                        AField::new("close", DataType::Int64, false),
                        AField::new("volume", DataType::Int64, false),
                        AField::new("notional", DataType::Int64, false),
                        AField::new("count", DataType::Int64, false),
                    ]));
                    let cols: Vec<ArrayRef> = vec![
                        Arc::new(
                            TimestampNanosecondArray::from(
                                bars.iter().map(|(_, b)| b.start_ns).collect::<Vec<_>>(),
                            )
                            .with_timezone("UTC"),
                        ),
                        Arc::new(UInt32Array::from(
                            bars.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter().map(|(_, b)| b.open).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter().map(|(_, b)| b.high).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter().map(|(_, b)| b.low).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter().map(|(_, b)| b.close).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter().map(|(_, b)| b.volume).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter()
                                .map(|(_, b)| {
                                    i64::try_from(b.notional / 100_000_000).unwrap_or(i64::MAX)
                                })
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            bars.iter()
                                .map(|(_, b)| i64::from(b.count))
                                .collect::<Vec<_>>(),
                        )),
                    ];
                    RecordBatch::try_new(schema, cols)
                }
                State::Percentiles { op, quantiles, .. } => {
                    let rows = op.open_rows();
                    let key_name = if q.key_name.is_empty() {
                        "key"
                    } else {
                        q.key_name.as_str()
                    };
                    let mut fields = vec![
                        AField::new(
                            "ts",
                            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                            false,
                        ),
                        AField::new(key_name, DataType::UInt32, false),
                        AField::new("count", DataType::Int64, false),
                        AField::new("min", DataType::Float64, false),
                        AField::new("max", DataType::Float64, false),
                    ];
                    for q in quantiles {
                        let pct = format!("{}", (q * 100.0 * 1e6).round() / 1e6);
                        fields.push(AField::new(
                            format!("p{}", pct.replace('.', "_")),
                            DataType::Float64,
                            false,
                        ));
                    }
                    let mut cols: Vec<ArrayRef> = vec![
                        Arc::new(
                            TimestampNanosecondArray::from(
                                rows.iter().map(|r| r.start_ns).collect::<Vec<_>>(),
                            )
                            .with_timezone("UTC"),
                        ),
                        Arc::new(UInt32Array::from(
                            rows.iter().map(|r| r.key).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            rows.iter()
                                .map(|r| i64::try_from(r.count).unwrap_or(i64::MAX))
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(Float64Array::from(
                            rows.iter().map(|r| r.min).collect::<Vec<_>>(),
                        )),
                        Arc::new(Float64Array::from(
                            rows.iter().map(|r| r.max).collect::<Vec<_>>(),
                        )),
                    ];
                    for qi in 0..quantiles.len() {
                        cols.push(Arc::new(Float64Array::from(
                            rows.iter().map(|r| r.values[qi]).collect::<Vec<_>>(),
                        )));
                    }
                    RecordBatch::try_new(Arc::new(ASchema::new(fields)), cols)
                }
                State::Asof { .. } => continue,
            };
            if let Ok(b) = batch {
                out.push((format!("cq_{}_live", q.name), b));
            }
        }
        out
    }
}

impl Query {
    /// Append one result row, counting a refusal instead of hiding it.
    fn emit(&mut self, key: u32, row: &[Value]) {
        let p = self.out.partition_for(key);
        let (ok, err) = self.out.append_rows(p, &[row]);
        if ok == 1 {
            self.since_bound_check += 1;
        } else {
            if self.rejected_rows == 0 {
                tracing::warn!(query = %self.name, error = ?err, "continuous query: result row refused by its output table");
            }
            self.rejected_rows += 1;
        }
    }

    /// Feed one row of `table`.
    fn feed(&mut self, table: &str, row: &[Value]) {
        let ts = as_i64(&row[0]);
        let mut emitted: Option<(u32, Vec<Value>)> = None;
        match &mut self.state {
            State::Ohlcv { op, b } => {
                if self.table != table {
                    return;
                }
                let Some(vi) = b.value else { return };
                if row[vi] == Value::Null || b.size.is_some_and(|si| row[si] == Value::Null) {
                    return;
                }
                let size = b.size.map_or(1, |si| as_i64(&row[si]));
                let key = key_of(b, row);
                let mut closed = Vec::new();
                op.push(key, ts, as_i64(&row[vi]), size, &mut closed);
                self.late_rows = op.late_rows;
                for (k, bar) in closed {
                    self.emit(k, &bar_row(k, &bar));
                    self.since_bound_check_tick();
                }
                return;
            }
            State::Rolling { op, b, done } => {
                if self.table != table {
                    return;
                }
                let Some(vi) = b.value else { return };
                // a row without a price or a size is not a trade of the bar
                if row[vi] == Value::Null || b.size.is_some_and(|si| row[si] == Value::Null) {
                    return;
                }
                let size = b.size.map_or(1, |si| as_i64(&row[si]));
                let key = key_of(b, row);
                done.clear();
                op.push(key, ts, as_i64(&row[vi]), size, done);
                self.late_rows = op.late_rows();
                let rows: Vec<(u32, Vec<Value>)> =
                    done.iter().map(|(k, bar)| (*k, bar_row(*k, bar))).collect();
                for (k, r) in rows {
                    self.emit(k, &r);
                    self.since_bound_check_tick();
                }
                return;
            }
            State::Percentiles { op, b, .. } => {
                if self.table != table {
                    return;
                }
                let Some(vi) = b.value else { return };
                let key = key_of(b, row);
                let mut closed = Vec::new();
                op.push(key, ts, as_f64(&row[vi]), &mut closed);
                self.late_rows = op.late_rows;
                for w in closed {
                    let mut r = vec![
                        Value::I64(w.start_ns),
                        Value::U32(w.key),
                        Value::I64(i64::try_from(w.count).unwrap_or(i64::MAX)),
                        Value::F64(w.min),
                        Value::F64(w.max),
                    ];
                    r.extend(w.values.iter().map(|v| Value::F64(*v)));
                    self.emit(w.key, &r);
                    self.since_bound_check_tick();
                }
                return;
            }
            State::Asof {
                op,
                left,
                right_table,
                right_b,
                right_cols,
                left_cols,
                scratch,
            } => {
                if table == right_table {
                    let key = key_of(right_b, row);
                    scratch.clear();
                    scratch.extend(right_cols.iter().map(|(i, _)| carry(&row[*i])));
                    op.push_right(key, ts, scratch.clone());
                    self.late_rows = op.late_rows;
                } else if table == self.table {
                    let key = key_of(left, row);
                    if let Some(vals) = op.join(key, ts) {
                        let mut r: Vec<Value> = left_cols.iter().map(|&i| row[i]).collect();
                        r.extend(
                            vals.iter()
                                .zip(right_cols.iter())
                                .map(|(v, (_, ty))| uncarry(*ty, *v)),
                        );
                        emitted = Some((key, r));
                    }
                    self.late_rows = op.late_rows;
                }
            }
        }
        if let Some((key, r)) = emitted {
            self.emit(key, &r);
            self.since_bound_check_tick();
        }
    }

    /// Enforcing the bound walks the table: done once per batch of results, not per row.
    fn since_bound_check_tick(&mut self) {
        if self.since_bound_check >= (self.max_rows / 16).max(4096) {
            self.since_bound_check = 0;
            self.enforce_bound();
        }
    }

    /// Keep about `max_rows` of the newest results: seal the open chunks, then drop the chunks
    /// that are entirely older than the newest `max_rows` rows.
    fn enforce_bound(&mut self) {
        let rows =
            |t: &Table| -> usize { t.snapshot().chunks.iter().map(ChunkRef::num_rows).sum() };
        if rows(&self.out) <= self.max_rows {
            return;
        }
        self.out.seal_all();
        let mut by_age: Vec<(i64, usize)> = self
            .out
            .snapshot()
            .chunks
            .iter()
            .map(|c| (c.watermark(), c.num_rows()))
            .collect();
        by_age.sort_unstable_by_key(|(wm, _)| Reverse(*wm));
        let mut kept = 0usize;
        let mut cutoff = None;
        for (wm, n) in by_age {
            kept += n;
            if kept >= self.max_rows {
                cutoff = Some(wm);
                break;
            }
        }
        if let Some(cutoff) = cutoff {
            self.out.for_each_partition(|p| {
                p.evict_sealed_before(cutoff);
            });
        }
    }
}

/// An output row of an OHLCV-shaped query (`ts` first: bucket start, or window end).
fn bar_row(key: u32, bar: &ankaa_ts::Ohlcv) -> Vec<Value> {
    vec![
        Value::I64(bar.start_ns),
        Value::U32(key),
        Value::I64(bar.open),
        Value::I64(bar.high),
        Value::I64(bar.low),
        Value::I64(bar.close),
        Value::I64(bar.volume),
        Value::I64(i64::try_from(bar.notional / 100_000_000).unwrap_or(i64::MAX)),
        Value::I64(i64::from(bar.count)),
    ]
}

/// A decoded chunk of an input table.
enum Loaded {
    Hot(Arc<Chunk>),
    /// Decoded columns with the rows that hold a value (`None` = all).
    Decoded(Vec<(Column, Option<Vec<bool>>)>),
}

impl Loaded {
    fn column(&self, i: usize) -> Option<&Column> {
        match self {
            Self::Hot(c) => c.column(i),
            Self::Decoded(cols) => cols.get(i).map(|(c, _)| c),
        }
    }

    fn is_null(&self, i: usize, row: usize) -> bool {
        let valid = match self {
            Self::Hot(c) => c.validity(i),
            Self::Decoded(cols) => cols.get(i).and_then(|(_, v)| v.as_deref()),
        };
        valid.is_some_and(|v| !v.get(row).copied().unwrap_or(true))
    }
}

/// The chunks of one partition of one input, read row by row in order. A chunk is decoded
/// only when its first row is actually due: until then its position in the merge comes from
/// its metadata (the timestamp of its first row).
struct Run {
    input: usize,
    chunks: std::iter::Peekable<std::vec::IntoIter<ChunkRef>>,
    width: usize,
    cur: Option<(Loaded, usize, usize)>,
}

impl Run {
    fn new(input: usize, chunks: Vec<ChunkRef>, width: usize) -> Self {
        Self {
            input,
            chunks: chunks.into_iter().peekable(),
            width,
            cur: None,
        }
    }

    /// Timestamp of the next row.
    fn peek(&mut self, stats: &mut RebuildStats) -> Option<i64> {
        loop {
            if let Some((loaded, row, rows)) = &self.cur {
                if row < rows
                    && let Some(t) = loaded
                        .column(0)
                        .and_then(Column::as_i64)
                        .and_then(|ts| ts.get(*row))
                {
                    return Some(*t);
                }
                self.cur = None;
            }
            match self.chunks.peek() {
                None => return None,
                Some(c) if c.num_rows() == 0 => {
                    self.chunks.next();
                }
                // rows are in timestamp order inside a chunk: its first row is its minimum
                Some(c) => match c.time_range() {
                    Some((first, _)) => return Some(first),
                    None => {
                        // no time range in the metadata: decode it to find out (a chunk that
                        // cannot be read is skipped by `load`)
                        self.load(stats);
                    }
                },
            }
        }
    }

    /// Decode the next chunk into `cur`. `false` when it could not be read (it is skipped).
    fn load(&mut self, stats: &mut RebuildStats) -> bool {
        let Some(chunk) = self.chunks.next() else {
            return false;
        };
        let rows = chunk.num_rows();
        let loaded = match &chunk {
            ChunkRef::Hot(c) => Loaded::Hot(Arc::clone(c)),
            ChunkRef::Encoded(e) => {
                let all: Vec<usize> = (0..self.width).collect();
                match e.decode_nullable(&all) {
                    Ok(cols) if cols.len() == self.width => Loaded::Decoded(cols),
                    Ok(_) => {
                        stats.bad_chunks += 1;
                        return false;
                    }
                    Err(err) => {
                        stats.bad_chunks += 1;
                        tracing::warn!(error = ?err, "continuous rebuild: chunk could not be decoded; its rows are skipped");
                        return false;
                    }
                }
            }
        };
        self.cur = Some((loaded, 0, rows));
        true
    }

    /// Copy the next row into `row` and advance (call after a successful [`Self::peek`]).
    /// `row` is left empty when the chunk that was due turned out to be unreadable.
    fn take(&mut self, row: &mut Vec<Value>, stats: &mut RebuildStats) {
        row.clear();
        if self.cur.is_none() && !self.load(stats) {
            return;
        }
        let Some((loaded, at, _)) = &mut self.cur else {
            return;
        };
        for i in 0..self.width {
            if loaded.is_null(i, *at) {
                row.push(Value::Null);
                continue;
            }
            let v = match loaded.column(i) {
                Some(Column::I64(v)) => Value::I64(v.get(*at).copied().unwrap_or(0)),
                Some(Column::U32(v)) => Value::U32(v.get(*at).copied().unwrap_or(0)),
                Some(Column::U8(v)) => Value::U8(v.get(*at).copied().unwrap_or(0)),
                Some(Column::F64(v)) => Value::F64(v.get(*at).copied().unwrap_or(0.0)),
                None => Value::I64(0),
            };
            row.push(v);
        }
        *at += 1;
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]
mod tests {
    use super::*;

    fn cfg(name: &str, kind: ContinuousKind, table: &str) -> ContinuousConfig {
        ContinuousConfig {
            name: name.into(),
            kind,
            table: table.into(),
            key: "symbol".into(),
            value: Some("price".into()),
            size: Some("size".into()),
            width_ns: Some(1_000),
            step_ns: Some(250),
            quantiles: vec![0.5],
            right_table: None,
            max_rows: 1_000_000,
        }
    }

    fn schemas() -> HashMap<String, Schema> {
        let mut m = HashMap::new();
        m.insert("trades".to_owned(), Schema::trades());
        m.insert("quotes".to_owned(), crate::tables::quotes_schema());
        m.insert(
            "telemetry".to_owned(),
            Schema::new(vec![
                Field::new("ts", ColumnType::TimestampNs),
                Field::new("rack", ColumnType::DictU32),
                Field::new("temp", ColumnType::Float64),
                Field::new("n", ColumnType::Int64),
            ])
            .unwrap(),
        );
        m
    }

    fn trade(ts: i64, sym: u32, price: i64) -> [Value; 6] {
        [
            Value::I64(ts),
            Value::U32(sym),
            Value::I64(price),
            Value::I64(2),
            Value::U8(1),
            Value::I64(ts),
        ]
    }

    fn quote(ts: i64, sym: u32, bid: i64) -> [Value; 7] {
        [
            Value::I64(ts),
            Value::U32(sym),
            Value::I64(bid),
            Value::I64(1),
            Value::I64(bid + 1),
            Value::I64(1),
            Value::I64(ts),
        ]
    }

    fn rows_of(t: &Table) -> usize {
        t.snapshot().chunks.iter().map(ChunkRef::num_rows).sum()
    }

    #[test]
    fn configuration_mistakes_fail_at_start() {
        let s = schemas();
        let mut bad_key = cfg("a", ContinuousKind::Ohlcv, "trades");
        bad_key.key = "price".into();
        assert!(Continuous::new(&[bad_key], &s).is_err(), "non-dict key");
        let mut missing = cfg("a", ContinuousKind::Ohlcv, "trades");
        missing.key = "nope".into();
        assert!(Continuous::new(&[missing], &s).is_err(), "unknown key");
        let mut float = cfg("a", ContinuousKind::Ohlcv, "telemetry");
        float.key = "rack".into();
        float.value = Some("temp".into());
        float.size = None;
        assert!(
            Continuous::new(&[float.clone()], &s).is_err(),
            "float ohlcv"
        );
        float.kind = ContinuousKind::Percentiles;
        Continuous::new(&[float], &s).unwrap();
        let mut selfjoin = cfg("a", ContinuousKind::Asof, "trades");
        selfjoin.right_table = Some("trades".into());
        assert!(Continuous::new(&[selfjoin], &s).is_err(), "self join");
        let mut user = cfg("a", ContinuousKind::Asof, "trades");
        user.right_table = Some("telemetry".into());
        assert!(
            Continuous::new(&[user], &s).is_err(),
            "no shared dictionary"
        );
        // a single series needs no key
        let mut nokey = cfg("a", ContinuousKind::Ohlcv, "telemetry");
        nokey.key = String::new();
        nokey.value = Some("n".into());
        nokey.size = None;
        Continuous::new(&[nokey], &s).unwrap();
    }

    #[test]
    fn asof_carries_every_right_column_type() {
        let s = schemas();
        let mut a = cfg("tq", ContinuousKind::Asof, "quotes");
        a.right_table = Some("trades".into()); // trades has a u8 `side` column
        a.value = None;
        a.size = None;
        let mut c = Continuous::new(&[a], &s).unwrap();
        c.on_row("trades", &trade(10, 3, 500));
        c.on_row("quotes", &quote(11, 3, 499));
        let q = &c.queries()[0];
        assert_eq!(q.rejected_rows, 0);
        assert_eq!(rows_of(&q.out), 1);
        // left (quotes) columns + trades price, size, side, seq
        assert_eq!(q.out.schema().fields().len(), 7 + 4);
        let snap = q.out.snapshot();
        let ChunkRef::Hot(h) = &snap.chunks[0] else {
            panic!("hot chunk expected")
        };
        assert_eq!(h.column(7).unwrap().as_i64().unwrap(), &[500]);
        assert_eq!(h.column(9).unwrap().as_u8().unwrap(), &[1]);
    }

    #[test]
    fn rebuild_reproduces_live_results() {
        let s = schemas();
        let mut asof = cfg("tq", ContinuousKind::Asof, "trades");
        asof.right_table = Some("quotes".into());
        let cfgs = [
            cfg("bars", ContinuousKind::Ohlcv, "trades"),
            cfg("pct", ContinuousKind::Percentiles, "trades"),
            asof,
            cfg("roll", ContinuousKind::Rolling, "trades"),
        ];
        // live: rows arrive interleaved across symbols and tables
        let trades = Table::new("trades", Schema::trades(), 1);
        let quotes = Table::new("quotes", crate::tables::quotes_schema(), 1);
        let mut live = Continuous::new(&cfgs, &s).unwrap();
        for i in 0..20_000i64 {
            let sym = (i % 3) as u32;
            let ts = i * 7;
            if i % 5 == 0 {
                let q = quote(ts, sym, 1_000 + i);
                let p = quotes.partition_for(sym);
                assert_eq!(quotes.append_rows(p, &[&q]).0, 1);
                live.on_row("quotes", &q);
            }
            let t = trade(ts, sym, 1_000 + (i % 97));
            let p = trades.partition_for(sym);
            assert_eq!(trades.append_rows(p, &[&t]).0, 1);
            live.on_row("trades", &t);
        }
        // restart: new operators, rebuilt from what the tables hold
        let mut inputs: HashMap<&str, &Table> = HashMap::new();
        inputs.insert("trades", &trades);
        inputs.insert("quotes", &quotes);
        // partition by partition (the key is the partition column) and as one global merge:
        // both give what live feeding gave
        let by_symbol: HashMap<String, String> = [("trades", "symbol"), ("quotes", "symbol")]
            .into_iter()
            .map(|(t, c)| (t.to_owned(), c.to_owned()))
            .collect();
        let mut aligned = Continuous::with_partitioning(&cfgs, &s, &by_symbol).unwrap();
        assert!(aligned.queries().iter().all(|q| q.aligned));
        let st_aligned = aligned.rebuild(&inputs);
        let mut rebuilt = Continuous::new(&cfgs, &s).unwrap();
        assert!(rebuilt.queries().iter().all(|q| !q.aligned));
        let st = rebuilt.rebuild(&inputs);
        assert_eq!(st_aligned, st);
        for (a, b) in aligned.queries().iter().zip(rebuilt.queries()) {
            assert_eq!(rows_of(&a.out), rows_of(&b.out), "{}", a.name);
        }
        for ((n1, b1), (n2, b2)) in aligned.live_batches().iter().zip(&rebuilt.live_batches()) {
            assert_eq!(n1, n2);
            assert_eq!(b1, b2, "{n1}");
        }
        assert_eq!(st.bad_chunks, 0);
        // bars, percentiles and rolling read trades once each; the asof reads trades and quotes
        assert_eq!(st.rows, 20_000 * 4 + 4_000);
        for (a, b) in live.queries().iter().zip(rebuilt.queries()) {
            assert_eq!(rows_of(&a.out), rows_of(&b.out), "{}", a.name);
            assert!(rows_of(&a.out) > 100, "{}", a.name);
            assert_eq!(b.rejected_rows, 0);
        }
        // the open bars are the same too
        let live_open = live.live_batches();
        let rebuilt_open = rebuilt.live_batches();
        assert_eq!(live_open.len(), 3);
        for ((n1, b1), (n2, b2)) in live_open.iter().zip(&rebuilt_open) {
            assert_eq!(n1, n2);
            assert_eq!(b1, b2, "{n1}");
        }
    }

    #[test]
    fn output_is_bounded_even_when_no_chunk_fills() {
        let s = schemas();
        let mut c1 = cfg("bars", ContinuousKind::Ohlcv, "trades");
        c1.max_rows = 10_000;
        let mut c = Continuous::new(&[c1], &s).unwrap();
        // 50 keys, one closed bar per row: far fewer than a chunk per key
        for i in 0..200_000i64 {
            c.on_row("trades", &trade(i * 1_000, (i % 50) as u32, 100));
        }
        let rows = rows_of(&c.queries()[0].out);
        assert!(rows >= 10_000, "{rows}");
        assert!(rows <= 10_000 + 2 * 4096 + 50, "{rows}");
    }
}
