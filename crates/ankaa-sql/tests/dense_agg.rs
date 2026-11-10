//! The hash-free aggregate must return exactly what `DataFusion`'s hash aggregate returns:
//! every query here runs in a session with the rule and in a plain one, on tables in several
//! layouts (one symbol per chunk, symbols mixed inside chunks, hot and encoded tiers, empty).

use std::sync::Arc;

use ankaa_sql::{ChunkTable, default_session};
use ankaa_store::{Chunk, ChunkId, ChunkRef, ColumnType, Field, Schema, Value};
use ankaa_tier::Encoder;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use datafusion::prelude::*;

fn deltas_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("symbol", ColumnType::DictU32),
        Field::new("side", ColumnType::U8),
        Field::new("price", ColumnType::FixedPoint { scale: 8 }),
        Field::new("size", ColumnType::FixedPoint { scale: 8 }),
        Field::new("update_id", ColumnType::Int64),
        Field::new("weight", ColumnType::Float64),
    ])
    .unwrap()
}

const SYMBOLS: i64 = 7;
const FIRST_SYMBOL: i64 = 3; // ids do not start at zero: the key range has an offset

/// `rows` rows from `from`; `mixed` puts every symbol in every chunk, otherwise a chunk holds
/// one symbol (`from / rows`-th).
fn chunk(id: u64, from: i64, rows: i64, mixed: bool) -> Chunk {
    let mut c = Chunk::new(ChunkId(id), deltas_schema());
    let mut x = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(id + 1);
    for i in from..from + rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let sym = FIRST_SYMBOL
            + if mixed {
                i % SYMBOLS
            } else {
                i64::try_from(id).unwrap() % SYMBOLS
            };
        c.append_row(&[
            Value::I64(1_000_000 + i * 10),
            Value::U32(u32::try_from(sym).unwrap()),
            Value::U8(u8::try_from(x % 3).unwrap()),
            Value::I64(50_000 + i64::try_from(x % 9_973).unwrap() - 4_000),
            Value::I64(if x % 5 == 1 {
                0
            } else {
                i64::try_from(x % 1_000).unwrap()
            }),
            Value::I64(i),
            Value::F64(f64::from(u32::try_from(x % 1_000).unwrap()) / 7.0),
        ])
        .unwrap();
    }
    c.seal();
    c
}

/// Tables under one name in two sessions: with the dense aggregate, and plain `DataFusion`.
fn sessions(chunks: &[ChunkRef]) -> (SessionContext, SessionContext) {
    let config = || SessionConfig::new().with_target_partitions(4);
    let dense = ankaa_sql::session(
        config(),
        Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
    );
    let plain = SessionContext::new_with_config(config());
    for ctx in [&dense, &plain] {
        ankaa_sql::register_functions(ctx);
        ctx.register_table(
            "deltas",
            Arc::new(ChunkTable::from_shared(deltas_schema(), chunks.to_vec())),
        )
        .unwrap();
    }
    (dense, plain)
}

/// Result rows as sorted text, floats rounded (their sums depend on the order of addition).
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
                        "NULL".to_owned()
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

/// Queries the rule takes over (`true`) and queries it must leave to the hash aggregate.
const QUERIES: &[(&str, bool)] = &[
    // no keys: always one row
    (
        "SELECT count(*), sum(size), min(price), max(price), min(ts), max(ts) FROM deltas",
        true,
    ),
    (
        "SELECT count(*), sum(size), min(price), max(price) FROM deltas \
      WHERE ts >= to_timestamp_nanos(1050000) AND ts < to_timestamp_nanos(1900000)",
        true,
    ),
    (
        "SELECT count(*), max(update_id) FROM deltas WHERE ts > to_timestamp_nanos(99000000)",
        true,
    ),
    (
        "SELECT count(*), sum(size) FROM deltas WHERE size > 500",
        true,
    ),
    (
        "SELECT count(*), avg(price), sum(weight), min(weight), max(weight) FROM deltas",
        true,
    ),
    // one key, the partition key
    (
        "SELECT symbol, count(*), sum(size), min(price), max(price), min(ts), max(ts) FROM deltas GROUP BY symbol",
        true,
    ),
    (
        "SELECT symbol, count(*), sum(size), min(price), max(update_id) FROM deltas \
      WHERE ts >= to_timestamp_nanos(1012345) AND ts <= to_timestamp_nanos(1400000) GROUP BY symbol",
        true,
    ),
    (
        "SELECT symbol, sum(price / 100.0 * size) / sum(size + 1) AS vwap, avg(price - size) FROM deltas GROUP BY symbol",
        true,
    ),
    (
        "SELECT symbol, count(*), sum(CASE WHEN size = 0 THEN 1 ELSE 0 END), sum(CASE WHEN size > 990 THEN price END) \
      FROM deltas WHERE symbol <> 5 GROUP BY symbol",
        true,
    ),
    // two keys, a flag byte, a filter on something else
    (
        "SELECT symbol, side, count(*), sum(CASE WHEN size = 0 THEN 1 ELSE 0 END) AS deletes, max(update_id) FROM deltas GROUP BY symbol, side",
        true,
    ),
    (
        "SELECT side, symbol, count(*), min(weight), avg(weight) FROM deltas WHERE price > 50000 AND side < 2 GROUP BY side, symbol",
        true,
    ),
    (
        "SELECT side, count(*), sum(size) FROM deltas WHERE symbol = 4 GROUP BY side",
        true,
    ),
    (
        "SELECT symbol, count(*) FROM deltas WHERE symbol > 100 GROUP BY symbol",
        true,
    ),
    (
        "SELECT symbol, count(size), count(CASE WHEN size > 0 THEN size END) FROM deltas GROUP BY symbol",
        true,
    ),
    // DISTINCT over a small key is planned as an inner GROUP BY (symbol, side): dense as well
    (
        "SELECT symbol, count(DISTINCT side) FROM deltas GROUP BY symbol",
        true,
    ),
    (
        "SELECT DISTINCT side, symbol FROM deltas WHERE size < 3",
        true,
    ),
    // time buckets: `date_bin` over the timestamp, alone and with other keys
    (
        "SELECT date_bin(INTERVAL '1 millisecond', ts) AS b, count(*), sum(size), max(price) FROM deltas GROUP BY b",
        true,
    ),
    (
        "SELECT symbol, date_bin(INTERVAL '0.000137 seconds', ts) AS b, count(*), avg(weight), min(update_id) FROM deltas \
         WHERE ts >= to_timestamp_nanos(1012345) AND ts < to_timestamp_nanos(1777777) GROUP BY symbol, b",
        true,
    ),
    (
        "SELECT date_bin(INTERVAL '1 day', ts) AS b, side, count(*) FROM deltas WHERE size > 10 GROUP BY b, side",
        true,
    ),
    // an explicit origin, a month interval: left to the hash aggregate (results still equal)
    (
        "SELECT date_bin(INTERVAL '1 millisecond', ts, TIMESTAMP '1970-01-01T00:00:00.0005Z') AS b, count(*) FROM deltas GROUP BY b",
        false,
    ),
    (
        "SELECT date_bin(INTERVAL '1 month', ts) AS b, count(*) FROM deltas GROUP BY b",
        false,
    ),
    // the native vwap aggregate: with and without keys, a filter, a time range, no rows
    (
        "SELECT symbol, vwap(price, size), count(*) FROM deltas GROUP BY symbol",
        true,
    ),
    (
        "SELECT vwap(price, size), vwap(price, update_id) FROM deltas WHERE ts < to_timestamp_nanos(1400000)",
        true,
    ),
    (
        "SELECT symbol, side, vwap(price, size) FROM deltas WHERE size < 3 GROUP BY symbol, side",
        true,
    ),
    ("SELECT vwap(price, size) FROM deltas WHERE size = 0", true),
    (
        "SELECT price, vwap(price, size) FROM deltas GROUP BY price",
        false,
    ),
    // not for this operator: a wide key, a computed key, DISTINCT on one, an ordered aggregate
    ("SELECT price, count(*) FROM deltas GROUP BY price", false),
    (
        "SELECT symbol + 1 AS s, count(*) FROM deltas GROUP BY symbol + 1",
        false,
    ),
    (
        "SELECT symbol, count(DISTINCT price) FROM deltas GROUP BY symbol",
        false,
    ),
    (
        "SELECT symbol, first_value(price ORDER BY ts) FROM deltas GROUP BY symbol",
        false,
    ),
];

async fn check(label: &str, chunks: &[ChunkRef], expect_rule: bool) {
    let (dense, plain) = sessions(chunks);
    for (sql, taken) in QUERIES {
        let p = plan(&dense, sql).await;
        assert_eq!(
            p.contains("DenseAggExec"),
            *taken && expect_rule,
            "{label}: {sql}\n{p}"
        );
        assert!(!plan(&plain, sql).await.contains("DenseAggExec"));
        assert_eq!(
            rows(&dense, sql).await,
            rows(&plain, sql).await,
            "{label}: {sql}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_aggregate_matches_hash_aggregate() {
    // one symbol per chunk (a partitioned table), chunks of several sizes, more than one per symbol
    let partitioned: Vec<ChunkRef> = (0..20u64)
        .map(|id| {
            let rows = [9_000, 70, 20_000, 1][usize::try_from(id % 4).unwrap()];
            ChunkRef::Hot(Arc::new(chunk(
                id,
                i64::try_from(id).unwrap() * 3_000,
                rows,
                false,
            )))
        })
        .collect();
    check("one symbol per chunk", &partitioned, true).await;

    // every symbol in every chunk: keys are not constant, rows are aggregated one by one
    let mixed: Vec<ChunkRef> = (0..6u64)
        .map(|id| {
            ChunkRef::Hot(Arc::new(chunk(
                id,
                i64::try_from(id).unwrap() * 15_000,
                15_000,
                true,
            )))
        })
        .collect();
    check("symbols mixed", &mixed, true).await;

    // hot, warm and cold chunks together
    let enc = Arc::new(Encoder::new());
    let dir = tempfile::tempdir().unwrap();
    let mut tiered = Vec::new();
    for (i, c) in partitioned.iter().enumerate() {
        let ChunkRef::Hot(hot) = c else {
            unreachable!()
        };
        tiered.push(match i % 3 {
            0 => c.clone(),
            1 => ChunkRef::Encoded(enc.encode_warm("deltas", 0, hot).unwrap()),
            _ => ChunkRef::Encoded(
                enc.write_cold(dir.path(), &enc.encode_warm("deltas", 0, hot).unwrap())
                    .unwrap(),
            ),
        });
    }
    check("tiered", &tiered, true).await;

    // nothing at all: one row without keys, none with keys
    check("empty", &[], true).await;
}

/// The chunk shortcut (statistics and kernels instead of rows) is announced in the plan when it
/// can apply, and a chunk whose rows are cut by the time range still gives the right answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunk_shortcut_is_used_and_exact_at_time_boundaries() {
    let chunks: Vec<ChunkRef> = (0..8u64)
        .map(|id| {
            ChunkRef::Hot(Arc::new(chunk(
                id,
                i64::try_from(id).unwrap() * 10_000,
                10_000,
                false,
            )))
        })
        .collect();
    let (dense, plain) = sessions(&chunks);
    let shortcut =
        "SELECT symbol, count(*), sum(size), min(price), max(price), min(ts), max(ts) FROM deltas";
    let p = plan(&dense, &format!("{shortcut} GROUP BY symbol")).await;
    assert!(p.contains("source=chunks (statistics and kernels"), "{p}");
    // an expression argument needs rows
    let p = plan(
        &dense,
        "SELECT symbol, sum(size * 2) FROM deltas GROUP BY symbol",
    )
    .await;
    assert!(
        p.contains("source=chunks,") || p.contains("source=chunks "),
        "{p}"
    );
    // a filter on a value column goes through the filter operator
    let p = plan(
        &dense,
        "SELECT symbol, sum(size) FROM deltas WHERE size > 3 GROUP BY symbol",
    )
    .await;
    assert!(p.contains("source=input"), "{p}");
    // every cut of the time range: inside one chunk, across chunks, on exact timestamps, empty
    let ts = |row: i64| 1_000_000 + row * 10;
    for (lo, hi) in [
        (ts(0), ts(79_999)),
        (ts(5), ts(9_998)),
        (ts(9_999), ts(10_000)),
        (ts(12_345) + 3, ts(61_000) - 7),
        (ts(40_000), ts(40_000)),
        (ts(70_000), ts(900_000)),
        (ts(900_000), ts(900_001)),
    ] {
        for bounds in [
            format!("ts >= to_timestamp_nanos({lo}) AND ts <= to_timestamp_nanos({hi})"),
            format!("ts > to_timestamp_nanos({lo}) AND ts < to_timestamp_nanos({hi})"),
            format!("to_timestamp_nanos({hi}) > ts AND to_timestamp_nanos({lo}) <= ts"),
        ] {
            for tail in ["GROUP BY symbol", ""] {
                let select = if tail.is_empty() {
                    shortcut.replacen("symbol, ", "", 1)
                } else {
                    shortcut.to_owned()
                };
                let sql = format!("{select} WHERE {bounds} {tail}");
                let p = plan(&dense, &sql).await;
                assert!(p.contains("statistics and kernels"), "{sql}\n{p}");
                assert_eq!(rows(&dense, &sql).await, rows(&plain, &sql).await, "{sql}");
            }
        }
    }
    // the default session carries the rule
    let ctx = default_session();
    ctx.register_table(
        "deltas",
        Arc::new(ChunkTable::from_shared(deltas_schema(), Vec::new())),
    )
    .unwrap();
    assert!(
        plan(
            &ctx,
            "SELECT count(*), sum(size) FROM deltas WHERE size > 1"
        )
        .await
        .contains("DenseAggExec")
    );
}
