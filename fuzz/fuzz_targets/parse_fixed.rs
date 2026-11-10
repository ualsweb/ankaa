#![no_main]
use ankaa_ingest::parse_fixed;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (Vec<u8>, u8)| {
    let (bytes, scale) = input;
    let _ = parse_fixed(&bytes, scale % 19);
});
