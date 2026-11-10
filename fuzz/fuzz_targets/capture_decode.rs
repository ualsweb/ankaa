#![no_main]
use ankaa_feeds::capture::decode_all;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_all(data);
});
