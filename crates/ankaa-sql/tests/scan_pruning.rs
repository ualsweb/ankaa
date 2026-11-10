//! Key pruning and partitioned scans: results must not change, only the chunks read.

use ankaa_sql::register_chunks;
use ankaa_store::{CHUNK_ROWS, Chunk, ChunkId, Schema, Value};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use datafusion::prelude::*;

/// One sealed chunk per symbol, `rows` rows each, timestamps interleaved across symbols.
fn per_symbol_chunks(symbols: u32, rows: i64) -> Vec<Chunk> {
    (0..symbols)
        .map(|sym| {
            let mut c = Chunk::new(ChunkId(u64::from(sym)), Schema::trades());
            for i in 0..rows {
                c.append_row(&[
                    Value::I64(1_000 + i * i64::from(symbols) + i64::from(sym)),
                    Value::U32(sym),
                    Value::I64(100 + i),
                    Value::I64(1),
                    Value::U8(0),
                    Value::I64(i),
                ])
                .unwrap();
            }
            c.seal();
            c
        })
        .collect()
}

async fn one_i64(ctx: &SessionContext, sql: &str) -> i64 {
    let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let b = arrow::compute::concat_batches(&b[0].schema(), &b).unwrap();
    b.column(0).as_primitive::<Int64Type>().value(0)
}

async fn plan(ctx: &SessionContext, sql: &str) -> String {
    let b = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&b)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn key_predicates_prune_chunks_and_keep_results() {
    let ctx = SessionContext::new();
    register_chunks(&ctx, "trades", Schema::trades(), per_symbol_chunks(8, 100)).unwrap();

    for (pred, want_rows, want_pruned) in [
        ("symbol = 3", 100, 7),
        ("3 = symbol", 100, 7),
        ("symbol IN (1, 6)", 200, 2), // range [1, 6] keeps 6 chunks; the filter does the rest
        ("symbol >= 6", 200, 6),
        ("symbol < 2 AND symbol > 0", 100, 7),
        ("symbol = 99", 0, 8),
        ("symbol = 3 OR symbol = 4", 200, 6),
        ("symbol = 3 OR price > 0", 800, 0), // one side unbounded: nothing to prune
        ("symbol = 2 OR symbol = 99", 100, 2), // hull [2, 99] keeps chunks 2..7
        ("(symbol = 1 OR symbol = 2) AND symbol >= 2", 100, 7),
    ] {
        let sql = format!("SELECT count(*) FROM trades WHERE {pred}");
        assert_eq!(one_i64(&ctx, &sql).await, want_rows, "{pred}");
        // the plan of a query that needs the rows: a bare count can be answered from the
        // scan's statistics without a scan
        let p = plan(&ctx, &format!("SELECT seq FROM trades WHERE {pred}")).await;
        assert!(
            p.contains(&format!("pruned={want_pruned} ")),
            "{pred}: expected pruned={want_pruned} in\n{p}"
        );
    }
}

#[tokio::test]
async fn partitioned_scan_matches_single_partition() {
    let rows = i64::try_from(CHUNK_ROWS).unwrap();
    let chunks = per_symbol_chunks(16, rows);
    let queries = [
        "SELECT count(*) FROM trades",
        "SELECT sum(price) FROM trades WHERE symbol < 9",
        "SELECT sum(seq) FROM trades WHERE ts >= to_timestamp_nanos(5000) AND ts < to_timestamp_nanos(900000)",
        "SELECT count(*) FROM (SELECT * FROM trades LIMIT 70000) t",
    ];
    let mut results = Vec::new();
    for parts in [1usize, 4] {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(parts));
        register_chunks(&ctx, "trades", Schema::trades(), chunks.clone()).unwrap();
        // one partition per symbol at least (each in time order), more when workers allow
        let p = plan(&ctx, "SELECT price FROM trades").await;
        assert!(p.contains("partitions=16 "), "{p}");
        let mut r = Vec::new();
        for q in queries {
            r.push(one_i64(&ctx, q).await);
        }
        results.push(r);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0][0], 16 * rows);
    assert_eq!(results[0][3], 70_000);
}

/// The scan tells the planner how many rows and how many distinct keys it holds, so a join of
/// a table with its own per-key aggregate builds the hash table on the aggregate (a few rows)
/// instead of hash-partitioning the whole table. Results are the same as without the hint.
#[tokio::test]
async fn scan_statistics_steer_the_join_and_keep_results() {
    let rows = i64::try_from(CHUNK_ROWS).unwrap();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    register_chunks(&ctx, "trades", Schema::trades(), per_symbol_chunks(8, rows)).unwrap();
    let latest = "SELECT t.symbol, t.seq FROM trades t \
                  JOIN (SELECT symbol, max(seq) AS seq FROM trades GROUP BY symbol) m \
                  ON m.symbol = t.symbol AND m.seq = t.seq";
    let p = plan(&ctx, latest).await;
    assert!(p.contains("mode=CollectLeft"), "{p}");
    assert_eq!(
        one_i64(&ctx, &format!("SELECT count(*) FROM ({latest}) x")).await,
        8
    );
    assert_eq!(
        one_i64(&ctx, &format!("SELECT sum(seq) FROM ({latest}) x")).await,
        8 * (rows - 1)
    );
    // exact row counts may answer count(*) on their own: they must be right, also after a
    // filter or a limit made them an upper bound
    assert_eq!(one_i64(&ctx, "SELECT count(*) FROM trades").await, 8 * rows);
    assert_eq!(
        one_i64(&ctx, "SELECT count(*) FROM trades WHERE symbol = 3").await,
        rows
    );
    assert_eq!(
        one_i64(
            &ctx,
            "SELECT count(*) FROM trades WHERE ts < to_timestamp_nanos(1080)"
        )
        .await,
        80
    );
    assert_eq!(
        one_i64(
            &ctx,
            "SELECT count(*) FROM (SELECT * FROM trades LIMIT 100) x"
        )
        .await,
        100
    );
}

/// Time comparisons are declared exact, so `DataFusion` drops them from the plan and the scan
/// alone decides which rows come out. The same predicates on a copy of the rows in a plain
/// in-memory table (no pushdown of any kind) must select the same rows.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn exact_time_filters_select_the_same_rows_as_a_plain_table() {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
    // several chunks per symbol, timestamps shared between rows and between chunks
    let mut chunks = Vec::new();
    for id in 0..9u64 {
        let mut c = Chunk::new(ChunkId(id), Schema::trades());
        let base = i64::try_from(id / 3).unwrap() * 4_000;
        for i in 0..4_000i64 {
            c.append_row(&[
                Value::I64(1_000 + (base + i) / 3 * 7),
                Value::U32(u32::try_from(id % 3).unwrap()),
                Value::I64(100 + i % 11),
                Value::I64(1 + i % 5),
                Value::U8(0),
                Value::I64(base + i),
            ])
            .unwrap();
        }
        c.seal();
        chunks.push(c);
    }
    register_chunks(&ctx, "trades", Schema::trades(), chunks).unwrap();
    let all = ctx
        .sql("SELECT * FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plain = datafusion::datasource::MemTable::try_new(all[0].schema(), vec![all]).unwrap();
    ctx.register_table("plain", std::sync::Arc::new(plain))
        .unwrap();

    let t = |ns: i64| format!("to_timestamp_nanos({ns})");
    let (a, b, last) = (1_000 + 700 * 7, 1_000 + 2_345 * 7, 1_000 + 3_999 * 7);
    let mut predicates = Vec::new();
    for v in [
        a,
        a + 1,
        a - 1,
        b,
        1_000,
        last,
        last + 1,
        0,
        i64::MAX,
        i64::MIN + 1,
    ] {
        for op in [">", ">=", "<", "<=", "="] {
            predicates.push(format!("ts {op} {}", t(v)));
            predicates.push(format!("{} {op} ts", t(v)));
        }
    }
    predicates.extend([
        format!("ts >= {} AND ts < {}", t(a), t(b)),
        format!("ts > {} AND ts <= {}", t(a), t(b)),
        format!("ts BETWEEN {} AND {}", t(a), t(b)),
        format!("ts BETWEEN {} AND {}", t(b), t(a)),
        format!("ts > {} AND {} >= ts AND ts <> {}", t(a), t(b), t(a + 7)),
        format!("ts >= {} AND symbol = 1", t(a)),
        format!("ts < {} AND price > 105", t(b)),
        format!("ts >= {} OR ts < {}", t(b), t(a)),
        format!("NOT (ts >= {})", t(a)),
        format!("ts >= {} AND ts < {} AND seq % 2 = 0", t(a), t(b)),
    ]);
    for p in &predicates {
        for select in ["count(*)", "sum(seq)", "sum(price * size)"] {
            let q = |table: &str| format!("SELECT coalesce({select}, -1) FROM {table} WHERE {p}");
            assert_eq!(
                one_i64(&ctx, &q("trades")).await,
                one_i64(&ctx, &q("plain")).await,
                "{select} WHERE {p}"
            );
        }
    }
    // a pure time range leaves no filter in the plan; anything else keeps it
    let p = plan(
        &ctx,
        &format!(
            "SELECT seq FROM trades WHERE ts >= {} AND ts < {}",
            t(a),
            t(b)
        ),
    )
    .await;
    assert!(!p.contains("FilterExec"), "{p}");
    let p = plan(
        &ctx,
        &format!(
            "SELECT seq FROM trades WHERE ts >= {} AND price > 105",
            t(a)
        ),
    )
    .await;
    assert!(p.contains("FilterExec: price"), "{p}");
    let p = plan(
        &ctx,
        &format!(
            "SELECT seq FROM trades WHERE ts >= {} OR ts < {}",
            t(b),
            t(a)
        ),
    )
    .await;
    assert!(p.contains("FilterExec"), "{p}");
}
