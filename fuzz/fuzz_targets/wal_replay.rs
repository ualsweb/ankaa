#![no_main]
use libfuzzer_sys::fuzz_target;

// Replay an arbitrary byte blob as a single WAL segment: must terminate without panicking,
// returning either records, a torn-tail stop, or a Corrupt error. The replication tail reader
// reads the same bytes: it must not panic either, and whatever it delivers is a prefix of what
// replay delivers.
fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("wal-00000000000000000001.log"), data).expect("write");
    let mut replayed = Vec::new();
    if let Ok(it) = ankaa_wal::replay(dir.path()) {
        for r in it {
            match r {
                Ok(rec) => replayed.push(rec),
                Err(_) => break,
            }
        }
    }
    let from = replayed.first().map_or(1, |r| r.lsn);
    let mut tail = ankaa_wal::Tail::new(dir.path(), from);
    let mut tailed = Vec::new();
    for _ in 0..1_000 {
        match tail.poll_bytes(64, 1 << 16) {
            Ok(v) if v.is_empty() => break,
            Ok(v) => tailed.extend(v),
            Err(_) => break,
        }
    }
    assert!(tailed.len() <= replayed.len());
    assert_eq!(tailed[..], replayed[..tailed.len()]);
});
