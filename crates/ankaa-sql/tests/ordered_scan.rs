//! Scans in time order: `ORDER BY ts` without a sort, `ORDER BY ts DESC` as a reverse scan,
//! and "the latest N rows that match" stopping early. Every query is compared with the same
//! rows in a plain in-memory table, which knows nothing about order.

use std::sync::Arc;

use ankaa_sql::{ChunkTable, default_session};
use ankaa_store::{Chunk, ChunkId, ChunkRef, Schema, Value};
use ankaa_tier::Encoder;
use datafusion::prelude::*;

const SYMBOLS: u64 = 5;

/// Chunks of 3,000 rows, three per symbol; every timestamp is unique across the table, so a
/// `LIMIT` has one right answer.
fn chunk(id: u64) -> Chunk {
    let (sym, part) = (id % SYMBOLS, i64::try_from(id / SYMBOLS).unwrap());
    let mut c = Chunk::new(ChunkId(id), Schema::trades());
    for i in 0..3_000i64 {
        let n = part * 3_000 + i;
        c.append_row(&[
            Value::I64(1_000_000 + n * 10 + i64::try_from(sym).unwrap()),
            Value::U32(u32::try_from(sym).unwrap()),
            Value::I64(100 + (n * 7) % 13),
            Value::I64(1 + n % 5),
            Value::U8(u8::try_from(n % 2).unwrap()),
            Value::I64(n),
        ])
        .unwrap();
    }
    c.seal();
    c
}

async fn text(ctx: &SessionContext, sql: &str) -> String {
    let b = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    arrow::util::pretty::pretty_format_batches(&b)
        .unwrap()
        .to_string()
}

async fn session(chunks: Vec<ChunkRef>, partitions: usize) -> SessionContext {
    let ctx = ankaa_sql::session(
        SessionConfig::new().with_target_partitions(partitions),
        Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()),
    );
    ctx.register_table(
        "t",
        Arc::new(ChunkTable::from_shared(Schema::trades(), chunks)),
    )
    .unwrap();
    let all = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plain = datafusion::datasource::MemTable::try_new(all[0].schema(), vec![all]).unwrap();
    ctx.register_table("plain", Arc::new(plain)).unwrap();
    ctx
}

const QUERIES: &[&str] = &[
    "SELECT ts, symbol, seq FROM {t} ORDER BY ts LIMIT 7",
    "SELECT ts, symbol, seq FROM {t} ORDER BY ts DESC LIMIT 7",
    "SELECT ts, symbol, seq FROM {t} ORDER BY ts DESC LIMIT 4000",
    "SELECT ts, symbol, seq FROM {t} ORDER BY ts DESC LIMIT 5 OFFSET 3",
    "SELECT seq, price FROM {t} WHERE symbol = 3 ORDER BY ts DESC LIMIT 9",
    "SELECT ts, seq FROM {t} WHERE price > 108 ORDER BY ts DESC LIMIT 11",
    "SELECT ts, seq FROM {t} WHERE price > 108 AND side = 1 ORDER BY ts LIMIT 11",
    "SELECT ts, seq FROM {t} WHERE price > 1000 ORDER BY ts DESC LIMIT 3",
    "SELECT ts, seq FROM {t} WHERE ts >= to_timestamp_nanos(1030000) AND ts < to_timestamp_nanos(1061234) \
     AND size = 2 ORDER BY ts DESC LIMIT 20",
    "SELECT ts, seq FROM {t} WHERE ts < to_timestamp_nanos(1045005) ORDER BY ts DESC LIMIT 6",
    "SELECT ts, symbol, price, size, side, seq FROM {t} WHERE size >= 4 ORDER BY ts, seq",
    "SELECT ts, seq FROM {t} WHERE symbol IN (1, 4) ORDER BY ts DESC",
    "SELECT symbol, max(ts) AS latest, count(*) AS n FROM {t} GROUP BY symbol ORDER BY symbol",
    // merges on two integer keys, both directions, with ties on the first key and on both
    "SELECT ts, seq, symbol FROM {t} WHERE size >= 3 ORDER BY ts, seq",
    "SELECT ts, seq, symbol FROM {t} WHERE side = 0 ORDER BY ts DESC, seq DESC",
    "SELECT seq, symbol, ts FROM {t} ORDER BY seq, ts",
    "SELECT ts, seq FROM {t} WHERE symbol IN (0, 2) ORDER BY ts, seq",
    "SELECT seq * 2 AS s2, seq, symbol FROM {t} WHERE price > 106 ORDER BY ts, seq",
    "SELECT * FROM (SELECT ts, seq FROM {t} ORDER BY ts DESC LIMIT 50) x ORDER BY seq LIMIT 5",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_and_reverse_scans_match_a_plain_table() {
    let hot: Vec<ChunkRef> = (0..3 * SYMBOLS)
        .map(|id| ChunkRef::Hot(Arc::new(chunk(id))))
        .collect();
    // as a partitioned table lists them: all chunks of a symbol, in time order, then the next
    let mut by_symbol = hot.clone();
    by_symbol.sort_by_key(|c| (c.id().0 % SYMBOLS, c.id().0));
    let enc = Arc::new(Encoder::new());
    let dir = tempfile::tempdir().unwrap();
    let tiered: Vec<ChunkRef> = by_symbol
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let ChunkRef::Hot(h) = c else { unreachable!() };
            match i % 3 {
                0 => c.clone(),
                1 => ChunkRef::Encoded(enc.encode_warm("t", 0, h).unwrap()),
                _ => ChunkRef::Encoded(
                    enc.write_cold(dir.path(), &enc.encode_warm("t", 0, h).unwrap())
                        .unwrap(),
                ),
            }
        })
        .collect();
    // a list in no time order at all: nothing may be assumed
    let mut shuffled = by_symbol.clone();
    shuffled.reverse();
    shuffled.swap(1, 7);
    for (label, chunks) in [
        ("by symbol", by_symbol),
        ("tiered", tiered),
        ("shuffled", shuffled),
    ] {
        for partitions in [1, 4] {
            let ctx = session(chunks.clone(), partitions).await;
            for q in QUERIES {
                assert_eq!(
                    text(&ctx, &q.replace("{t}", "t")).await,
                    text(&ctx, &q.replace("{t}", "plain")).await,
                    "{label}, {partitions} partitions: {q}"
                );
            }
        }
    }
}

/// The plans: no sort for `ORDER BY ts`, a reverse scan for `DESC`, and a limit under the
/// merge that stops each partition early, also below a filter.
#[tokio::test]
async fn latest_rows_do_not_sort_the_table() {
    let chunks: Vec<ChunkRef> = {
        let mut v: Vec<ChunkRef> = (0..3 * SYMBOLS)
            .map(|id| ChunkRef::Hot(Arc::new(chunk(id))))
            .collect();
        v.sort_by_key(|c| (c.id().0 % SYMBOLS, c.id().0));
        v
    };
    let ctx = default_session();
    ctx.register_table(
        "t",
        Arc::new(ChunkTable::from_shared(Schema::trades(), chunks.clone())),
    )
    .unwrap();
    let plan = |sql: &'static str| {
        let ctx = ctx.clone();
        async move { text(&ctx, &format!("EXPLAIN {sql}")).await }
    };
    let p = plan("SELECT ts, seq FROM t ORDER BY ts LIMIT 10").await;
    assert!(!p.contains("SortExec") && !p.contains("reverse"), "{p}");
    let p = plan("SELECT ts, seq FROM t ORDER BY ts DESC LIMIT 10").await;
    assert!(!p.contains("SortExec") && p.contains("reverse"), "{p}");
    let p =
        plan("SELECT ts, seq FROM t WHERE price > 105 AND symbol = 2 ORDER BY ts DESC LIMIT 10")
            .await;
    assert!(
        !p.contains("SortExec") && p.contains("reverse") && p.contains("LocalLimitExec"),
        "{p}"
    );
    // not on the timestamp alone: sorted as before
    let p = plan("SELECT ts, seq FROM t ORDER BY ts DESC, seq LIMIT 10").await;
    assert!(p.contains("SortExec"), "{p}");
    let p = plan("SELECT ts, seq FROM t ORDER BY price LIMIT 10").await;
    assert!(p.contains("SortExec"), "{p}");
    // a chunk list in another order is cut into runs that are each in time order (here every
    // chunk on its own), so the answer is the same; the first test checks the rows
    let mut reversed = chunks;
    reversed.reverse();
    let ctx = default_session();
    ctx.register_table(
        "t",
        Arc::new(ChunkTable::from_shared(Schema::trades(), reversed)),
    )
    .unwrap();
    let latest = text(
        &ctx,
        "SELECT seq FROM t WHERE symbol = 4 ORDER BY ts DESC LIMIT 2",
    )
    .await;
    assert!(
        latest.contains("| 8999 |") && latest.contains("| 8998 |"),
        "{latest}"
    );
}
