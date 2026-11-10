#![no_main]
//! Binance SBE binary decoder must never panic on arbitrary frames; ids and symbols are shared
//! with the JSON decoder's dictionary.
use ankaa_feeds::binance::BinanceSbeDecoder;
use ankaa_ingest::{EventBatch, SymbolDict};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut dec = BinanceSbeDecoder::new();
    let mut symbols = SymbolDict::new();
    let mut out = EventBatch::with_capacity(64);
    // feed the whole input, then split it in two to exercise sequence state across frames
    let _ = dec.decode(data, 0, &mut symbols, &mut out);
    if data.len() > 8 {
        let (a, b) = data.split_at(data.len() / 2);
        out.clear();
        let _ = dec.decode(a, 1, &mut symbols, &mut out);
        let _ = dec.decode(b, 2, &mut symbols, &mut out);
    }
});
