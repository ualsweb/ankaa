#![no_main]
use ankaa_feeds::binance::BinanceJsonDecoder;
use ankaa_ingest::{Decoder, EventBatch};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut d = BinanceJsonDecoder::new();
    let mut out = EventBatch::default();
    // Must never panic; result is irrelevant.
    let _ = d.decode(data, 0, &mut out);
    // Feed the same bytes twice: state (symbols, update ids) must not break on repeats.
    let _ = d.decode(data, 1, &mut out);
});
