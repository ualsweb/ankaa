#![no_main]
//! F-03: the generic length-prefixed framer must never panic and must respect `max_frame`.
use ankaa_ingest::framing::{Framer, LengthPrefixed, Prefix, split_all};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let prefix = match data[0] % 5 {
        0 => Prefix::U8,
        1 => Prefix::U16Le,
        2 => Prefix::U16Be,
        3 => Prefix::U32Le,
        _ => Prefix::U32Be,
    };
    let mut layout = LengthPrefixed::new(prefix)
        .with_offset((data[1] % 4) as usize)
        .with_trailer_header((data[2] % 4) as usize);
    if data[3] & 1 == 1 {
        layout = layout.payload_only();
    }
    let max = 4096usize;
    let body = &data[4..];
    let mut f = Framer::new(layout, max);
    for seg in body.chunks(1 + (data[3] as usize % 17)) {
        f.push(seg);
        while let Some(fr) = f.next_frame() {
            assert!(fr.len() <= max);
        }
    }
    let _ = split_all(layout, max, body, |fr| assert!(fr.len() <= max));
});
