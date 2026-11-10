//! P0-05 gate: SQL over in-memory chunks through `DataFusion` with time-range pushdown.

use ankaa_sql::register_chunks;
use ankaa_store::{Chunk, ChunkId, Schema, Value};
use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, UInt32Type};
use datafusion::prelude::*;

/// Two chunks of synthetic trades: symbol 1 and 2 alternate, price increases by 1 tick per row.
fn make_chunks() -> Vec<Chunk> {
    let schema = Schema::trades();
    let mut chunks = Vec::new();
    let mut ts = 1_700_000_000_000_000_000i64; // 2023-11-14T22:13:20Z
    let mut seq = 0i64;
    for cid in 0..2u64 {
        let mut c = Chunk::new(ChunkId(cid), schema.clone());
        for i in 0..1_000i64 {
            ts += 1_000_000; // 1 ms
            let sym = u32::try_from(1 + (i % 2)).unwrap();
            let px = 5_000_000_000_000 + seq; // 50_000.00000000 + ticks
            c.append_row(&[
                Value::I64(ts),
                Value::U32(sym),
                Value::I64(px),
                Value::I64(100_000_000),
                Value::U8(u8::try_from(i % 2).unwrap()),
                Value::I64(seq),
            ])
            .unwrap();
            seq += 1;
        }
        if cid == 0 {
            c.seal();
        }
        chunks.push(c);
    }
    chunks
}

#[tokio::test]
async fn group_by_symbol_avg_price_over_time_range() {
    let ctx = SessionContext::new();
    register_chunks(&ctx, "trades", Schema::trades(), make_chunks()).unwrap();

    // rows 500..1500 overall (ms 501..1500 after the base) → 500 rows per symbol
    let lo = 1_700_000_000_000_000_000i64 + 501 * 1_000_000;
    let hi = 1_700_000_000_000_000_000i64 + 1_500 * 1_000_000;
    let sql = format!(
        "SELECT symbol, count(*) AS n, avg(CAST(price AS DOUBLE)) / 1e8 AS avg_px \
         FROM trades WHERE ts >= to_timestamp_nanos({lo}) AND ts <= to_timestamp_nanos({hi}) \
         GROUP BY symbol ORDER BY symbol"
    );
    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let rows: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2, "two symbols");
    let b = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let syms = b.column(0).as_primitive::<UInt32Type>();
    let ns = b.column(1).as_primitive::<arrow::datatypes::Int64Type>();
    let avgs = b.column(2).as_primitive::<Float64Type>();
    assert_eq!(syms.values(), &[1, 2]);
    assert_eq!(ns.values(), &[500, 500]);
    // seq 500..1499; symbol 1 has even i → seq even in [500,1498], mean 999; symbol 2 mean 1000.
    let want1 = (5_000_000_000_000f64 + 999.0) / 1e8;
    let want2 = (5_000_000_000_000f64 + 1000.0) / 1e8;
    assert!(
        (avgs.value(0) - want1).abs() < 1e-9,
        "{} vs {want1}",
        avgs.value(0)
    );
    assert!(
        (avgs.value(1) - want2).abs() < 1e-9,
        "{} vs {want2}",
        avgs.value(1)
    );
}

#[tokio::test]
async fn time_pushdown_prunes_sealed_chunk() {
    let ctx = SessionContext::new();
    register_chunks(&ctx, "trades", Schema::trades(), make_chunks()).unwrap();
    // Only the second (open) chunk overlaps this range.
    let lo = 1_700_000_000_000_000_000i64 + 1_900 * 1_000_000;
    let df = ctx
        .sql(&format!(
            "SELECT count(*) FROM trades WHERE ts > to_timestamp_nanos({lo})"
        ))
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert!(text.contains("ChunkScanExec"), "{text}");
    assert!(
        text.contains(&format!("ts∈[{}, {}]", lo + 1, i64::MAX)),
        "pushdown bounds missing in plan:\n{text}"
    );
    let batches = df.collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0);
    assert_eq!(n, 100);
}

#[tokio::test]
async fn projection_pushdown_only_converts_requested_columns() {
    let ctx = SessionContext::new();
    register_chunks(&ctx, "trades", Schema::trades(), make_chunks()).unwrap();
    let df = ctx
        .sql("SELECT seq FROM trades ORDER BY seq DESC LIMIT 3")
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    assert!(text.contains("projection=[5]"), "{text}");
    let b = df.collect().await.unwrap();
    let v = b[0].column(0).as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(v.values(), &[1999, 1998, 1997]);
}

#[tokio::test]
async fn count_star_without_columns() {
    let ctx = SessionContext::new();
    let mut c = Chunk::new(ChunkId(1), Schema::trades());
    for i in 0..7i64 {
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
    register_chunks(&ctx, "trades", Schema::trades(), vec![c]).unwrap();
    let out = ctx
        .sql("SELECT count(*) AS n FROM trades")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        out[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        7
    );
}
