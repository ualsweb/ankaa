#![no_main]
//! Databento DBN reader over arbitrary bytes (through the `dbn` crate): no panics, bounded work.
use ankaa_feeds::databento::DbnReplay;
use ankaa_ingest::{EventBatch, SymbolDict};
use libfuzzer_sys::fuzz_target;
use std::io::Write as _;

fuzz_target!(|data: &[u8]| {
    let mut f = tempfile::Builder::new().suffix(".dbn").tempfile().unwrap();
    f.write_all(data).unwrap();
    let Ok(mut r) = DbnReplay::open(f.path()) else { return };
    let mut symbols = SymbolDict::new();
    let mut out = EventBatch::with_capacity(64);
    for _ in 0..64 {
        match r.next_batch(&mut symbols, &mut out, 256) {
            Ok(None) | Err(_) => break,
            Ok(Some(_)) => out.clear(),
        }
    }
});
