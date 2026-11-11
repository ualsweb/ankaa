//! Aggregating before a join gives the same groups as joining first, in every case the rule
//! takes (and the rule stays out of the cases it must not take). Compared with a session
//! without ANKAA's rules on the same tables.

use std::sync::Arc;

use ankaa_sql::ChunkTable;
use ankaa_store::{Chunk, ChunkId, ChunkRef, Schema, Value};
use arrow::array::{Array, AsArray, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Float64Type};
use datafusion::prelude::*;

fn trades() -> Vec<ChunkRef> {
    (0..8u64)
        .map(|id| {
            let mut c = Chunk::new(ChunkId(id), Schema::trades());
            for i in 0..5_000i64 {
                let n = i64::try_from(id).unwrap() * 5_000 + i;
                c.append_row(&[
                    Value::I64(1_000_000 + n),
                    // ids 0..=6 appear; id 6 has no name; ids 7 and 8 have names but no trades
                    Value::U32(u32::try_from(n % 7).unwrap()),
                    Value::I64(100 + (n * 13) % 997),
                    Value::I64(1 + n % 9),
                    Value::U8(u8::try_from(n % 2).unwrap()),
                    Value::I64(n),
                ])
                .unwrap();
            }
            c.seal();
            ChunkRef::Hot(Arc::new(c))
        })
        .collect()
}

/// Names, one per id, except that id 2 has two names and ids 3 and 4 share one: a join that
/// multiplies rows and a group that merges keys must both come out right.
fn names() -> RecordBatch {
    let ids = UInt32Array::from(vec![0, 1, 2, 2, 3, 4, 5, 7, 8]);
    let names = StringArray::from(vec!["A", "B", "C1", "C2", "D", "D", "E", "F", "G"]);
    let regions = StringArray::from(vec!["eu", "eu", "us", "us", "asia", "eu", "us", "eu", "us"]);
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("symbol", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ])),
        vec![Arc::new(ids), Arc::new(names), Arc::new(regions)],
    )
    .unwrap()
}

fn register(ctx: &SessionContext) {
    ctx.register_table(
        "t",
        Arc::new(ChunkTable::from_shared(Schema::trades(), trades())),
    )
    .unwrap();
    ctx.register_batch("s", names()).unwrap();
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let out = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut lines = Vec::new();
    for b in &out {
        for r in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let col = b.column(c);
                    if col.is_null(r) {
                        "NULL".into()
                    } else if col.data_type() == &DataType::Float64 {
                        format!("{:.6e}", col.as_primitive::<Float64Type>().value(r))
                    } else {
                        arrow::util::display::array_value_to_string(col, r).unwrap()
                    }
                })
                .collect();
            lines.push(cells.join("|"));
        }
    }
    lines.sort();
    lines
}

const QUERIES: &[(&str, bool)] = &[
    (
        "SELECT s.name, count(*), min(t.price), max(t.price), sum(t.size) FROM t JOIN s ON s.symbol = t.symbol GROUP BY s.name",
        true,
    ),
    (
        "SELECT s.name, count(*) AS n FROM t JOIN s ON s.symbol = t.symbol \
      WHERE t.ts >= to_timestamp_nanos(1003000) AND t.ts < to_timestamp_nanos(1031234) GROUP BY s.name",
        true,
    ),
    (
        "SELECT s.region, count(t.seq), sum(t.price * t.size) FROM s JOIN t ON t.symbol = s.symbol WHERE t.size > 4 GROUP BY s.region",
        true,
    ),
    (
        "SELECT s.name, t.side, count(*), max(t.seq) FROM t JOIN s ON s.symbol = t.symbol GROUP BY s.name, t.side",
        true,
    ),
    (
        "SELECT count(*), sum(t.size) FROM t JOIN s ON s.symbol = t.symbol",
        true,
    ),
    // no row survives: the original says count 0 (not NULL), and so must the rewrite
    (
        "SELECT count(*), count(t.seq), sum(t.size) FROM t JOIN s ON s.symbol = t.symbol WHERE t.size < 0",
        true,
    ),
    (
        "SELECT t.symbol, s.name, min(t.ts) FROM t JOIN s ON s.symbol = t.symbol GROUP BY t.symbol, s.name",
        true,
    ),
    (
        "SELECT s.name, sum(CAST(t.price AS DOUBLE) / 7.0) FROM t JOIN s ON s.symbol = t.symbol GROUP BY s.name",
        true,
    ),
    // not taken: avg, DISTINCT, aggregates over both sides, an outer join, a residual filter
    (
        "SELECT s.name, avg(t.price) FROM t JOIN s ON s.symbol = t.symbol GROUP BY s.name",
        false,
    ),
    (
        "SELECT s.name, count(DISTINCT t.side) FROM t JOIN s ON s.symbol = t.symbol GROUP BY s.name",
        false,
    ),
    (
        "SELECT s.name, count(*), max(length(s.region) + t.side) FROM t JOIN s ON s.symbol = t.symbol GROUP BY s.name",
        false,
    ),
    (
        "SELECT s.name, count(t.seq) FROM t LEFT JOIN s ON s.symbol = t.symbol GROUP BY s.name",
        false,
    ),
    (
        "SELECT s.name, count(*) FROM t JOIN s ON s.symbol = t.symbol AND t.price > s.symbol GROUP BY s.name",
        false,
    ),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregating_before_the_join_changes_no_group() {
    let ankaa = ankaa_sql::default_session();
    let reference = SessionContext::new();
    register(&ankaa);
    register(&reference);
    for (sql, taken) in QUERIES {
        let explain = ankaa
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = arrow::util::pretty::pretty_format_batches(&explain)
            .unwrap()
            .to_string();
        assert_eq!(plan.contains("__ankaa_partial_"), *taken, "{sql}\n{plan}");
        assert_eq!(
            rows(&ankaa, sql).await,
            rows(&reference, sql).await,
            "{sql}"
        );
    }
}
