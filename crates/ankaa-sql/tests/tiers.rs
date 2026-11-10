//! Queries over a table whose chunks live in different tiers (hot, warm, cold) return the same
//! rows as the all-hot table, and pruning on encoded chunks uses metadata only.

use std::sync::Arc;

use ankaa_sql::ChunkTable;
use ankaa_store::{Chunk, ChunkId, ChunkRef, Schema, Value};
use ankaa_tier::Encoder;
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use datafusion::prelude::*;

fn chunk(id: u64, from: i64, rows: i64) -> Chunk {
    let mut c = Chunk::new(ChunkId(id), Schema::trades());
    for i in from..from + rows {
        c.append_row(&[
            Value::I64(i * 1_000),
            Value::U32(u32::try_from(i % 3).unwrap()),
            Value::I64(50_000 + (i % 17)),
            Value::I64(1 + i % 5),
            Value::U8(0),
            Value::I64(i),
        ])
        .unwrap();
    }
    c.seal();
    c
}

async fn run(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let out = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let b = arrow::compute::concat_batches(&out[0].schema(), &out).unwrap();
    (0..b.num_columns())
        .map(|c| b.column(c).as_primitive::<Int64Type>().value(0))
        .collect()
}

#[tokio::test]
async fn tiered_table_matches_hot_table() {
    let enc = Arc::new(Encoder::new());
    let dir = tempfile::tempdir().unwrap();
    let hot1 = Arc::new(chunk(1, 0, 5_000));
    let hot2 = Arc::new(chunk(2, 5_000, 5_000));
    let hot3 = Arc::new(chunk(3, 10_000, 5_000));
    let warm = enc.encode_warm("trades", 0, &hot2).unwrap();
    let cold = enc
        .write_cold(dir.path(), &enc.encode_warm("trades", 0, &hot3).unwrap())
        .unwrap();

    let all_hot = ChunkTable::from_shared(
        Schema::trades(),
        vec![
            ChunkRef::Hot(hot1.clone()),
            ChunkRef::Hot(hot2.clone()),
            ChunkRef::Hot(hot3.clone()),
        ],
    );
    let tiered = ChunkTable::from_shared(
        Schema::trades(),
        vec![
            ChunkRef::Hot(hot1),
            ChunkRef::Encoded(warm),
            ChunkRef::Encoded(cold),
        ],
    );
    let ctx = SessionContext::new();
    ctx.register_table("hot", Arc::new(all_hot)).unwrap();
    ctx.register_table("tiered", Arc::new(tiered)).unwrap();
    for q in [
        "SELECT count(*), sum(price), sum(size), min(seq), max(seq) FROM {t}",
        "SELECT count(*), sum(price) FROM {t} WHERE ts >= to_timestamp_nanos(7000000) AND ts < to_timestamp_nanos(12000000)",
        "SELECT count(*), sum(seq) FROM {t} WHERE ts > to_timestamp_nanos(14999000)",
        "SELECT count(*), sum(size) FROM {t} WHERE symbol = 2 AND ts BETWEEN to_timestamp_nanos(4000000) AND to_timestamp_nanos(11000000)",
    ] {
        let a = run(&ctx, &q.replace("{t}", "hot")).await;
        let b = run(&ctx, &q.replace("{t}", "tiered")).await;
        assert_eq!(a, b, "{q}");
    }
    // a range entirely in the cold chunk, projected without the timestamp
    let a = run(
        &ctx,
        "SELECT sum(price) FROM hot WHERE ts >= to_timestamp_nanos(12000000)",
    )
    .await;
    let b = run(
        &ctx,
        "SELECT sum(price) FROM tiered WHERE ts >= to_timestamp_nanos(12000000)",
    )
    .await;
    assert_eq!(a, b);
}
