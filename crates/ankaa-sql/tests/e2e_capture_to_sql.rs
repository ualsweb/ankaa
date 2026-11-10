//! End to end: captured Binance frames → JSON decoder → trades chunk → `DataFusion` SQL.

use ankaa_feeds::binance::BinanceJsonDecoder;
use ankaa_feeds::capture::{FrameKind, decode_all};
use ankaa_ingest::{Decoder, EventBatch};
use ankaa_sql::register_chunks;
use ankaa_store::{Chunk, ChunkId, Schema};
use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type};
use datafusion::prelude::*;

#[tokio::test]
async fn trades_from_capture_are_queryable() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ankaa-feeds/tests/fixtures/binance-btcusdt-trade-depth-60.ankc"
    );
    let (_, frames) = decode_all(&std::fs::read(path).unwrap()).unwrap();
    let mut dec = BinanceJsonDecoder::new();
    let mut batch = EventBatch::with_capacity(1024);
    for f in frames.iter().filter(|f| f.kind == FrameKind::Text) {
        dec.decode(&f.payload, f.recv_ns, &mut batch).unwrap();
    }
    // Trades may arrive with equal timestamps; the chunk requires non-decreasing ts, which
    // exchange trade time guarantees within one stream.
    let mut chunk = Chunk::new(ChunkId(1), Schema::trades());
    let (n, err) = batch.append_trades(&mut chunk);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(n, batch.trades.len());
    assert!(n > 0);
    let want_count = i64::try_from(n).unwrap();
    let want_vwap = {
        let (pq, q): (i128, i128) = batch.trades.iter().fold((0, 0), |(pq, q), t| {
            (
                pq + i128::from(t.price) * i128::from(t.size),
                q + i128::from(t.size),
            )
        });
        (pq as f64 / q as f64) / 1e8
    };

    let ctx = SessionContext::new();
    register_chunks(&ctx, "trades", Schema::trades(), vec![chunk]).unwrap();
    let df = ctx
        .sql(
            "SELECT count(*) AS n, \
                    sum(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) / sum(CAST(size AS DOUBLE)) / 1e8 AS vwap, \
                    min(price) / 1e8 AS lo, max(price) / 1e8 AS hi \
             FROM trades",
        )
        .await
        .unwrap();
    let b = df.collect().await.unwrap();
    let b = &b[0];
    assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), want_count);
    let vwap = b.column(1).as_primitive::<Float64Type>().value(0);
    assert!(
        (vwap - want_vwap).abs() / want_vwap < 1e-9,
        "vwap {vwap} vs {want_vwap}"
    );
    let lo = b.column(2).as_primitive::<Float64Type>().value(0);
    let hi = b.column(3).as_primitive::<Float64Type>().value(0);
    assert!(lo <= vwap && vwap <= hi);
}
