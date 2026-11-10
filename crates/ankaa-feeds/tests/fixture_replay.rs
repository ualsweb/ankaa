//! Replay the captured Binance fixture through the JSON decoder.

use ankaa_feeds::binance::BinanceJsonDecoder;
use ankaa_feeds::capture::{FrameKind, decode_all};
use ankaa_ingest::{Decoder, EventBatch};

#[test]
fn fixture_decodes_fully_with_no_gaps() {
    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/binance-btcusdt-trade-depth-60.ankc"
    ))
    .unwrap();
    let (hdr, frames) = decode_all(&bytes).unwrap();
    assert!(hdr.source.contains("btcusdt@trade"));
    let mut d = BinanceJsonDecoder::new();
    let mut out = EventBatch::with_capacity(1024);
    let mut text_frames = 0;
    for f in &frames {
        match f.kind {
            FrameKind::Text => {
                text_frames += 1;
                d.decode(&f.payload, f.recv_ns, &mut out).unwrap();
            }
            FrameKind::Control => assert!(matches!(
                f.payload.as_ref(),
                b"connect" | b"disconnect" | b"shutdown"
            )),
            FrameKind::Binary | FrameKind::User => {
                panic!("no binary/user frames expected in a JSON capture")
            }
            FrameKind::Snapshot => {
                d.decode_snapshot(&f.payload, f.recv_ns, &mut out).unwrap();
            }
        }
    }
    let s = d.stats();
    assert_eq!(text_frames, 60);
    assert_eq!(s.frames_ok, 60);
    assert_eq!(s.frames_bad, 0);
    assert_eq!(
        s.gaps, 0,
        "depth sequence must be contiguous within one connection"
    );
    assert!(s.trades > 0 && s.deltas > 0, "{s:?}");
    assert_eq!(out.trades.len() as u64, s.trades);
    assert_eq!(out.deltas.len() as u64, s.deltas);
    // trades: ids strictly increasing, prices sane for BTCUSDT in the capture (10k..1M USDT)
    assert!(out.trades.windows(2).all(|w| w[1].seq > w[0].seq));
    assert!(
        out.trades
            .iter()
            .all(|t| (1_000_000_000_000..=100_000_000_000_000).contains(&t.price))
    );
    // receive timestamps are non-decreasing (host clock)
    assert!(frames.windows(2).all(|w| w[1].recv_ns >= w[0].recv_ns));
}
