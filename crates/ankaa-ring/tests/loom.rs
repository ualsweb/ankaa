//! loom model checks of the SPSC protocol. Run with:
//! `RUSTFLAGS="--cfg loom" cargo test -p ankaa-ring --test loom --release`
#![cfg(loom)]

use ankaa_ring::ring;
use loom::thread;

#[test]
fn loom_spsc_two_items_capacity_two() {
    loom::model(|| {
        let (mut p, mut c) = ring::<u32>(2);
        let t = thread::spawn(move || {
            while p.try_push(1).is_err() {
                thread::yield_now();
            }
            while p.try_push(2).is_err() {
                thread::yield_now();
            }
        });
        let mut got = Vec::new();
        while got.len() < 2 {
            if let Some(v) = c.try_pop() {
                got.push(v);
            } else {
                thread::yield_now();
            }
        }
        t.join().unwrap();
        assert_eq!(got, vec![1, 2]);
    });
}

#[test]
fn loom_spsc_wraparound_capacity_two_three_items() {
    loom::model(|| {
        let (mut p, mut c) = ring::<u32>(2);
        let t = thread::spawn(move || {
            for v in 1..=3u32 {
                while p.try_push(v).is_err() {
                    thread::yield_now();
                }
            }
        });
        let mut got = Vec::new();
        while got.len() < 3 {
            if let Some(v) = c.try_pop() {
                got.push(v);
            } else {
                thread::yield_now();
            }
        }
        t.join().unwrap();
        assert_eq!(got, vec![1, 2, 3]);
    });
}

#[test]
fn loom_full_is_reported_not_overwritten() {
    loom::model(|| {
        let (mut p, mut c) = ring::<u32>(2);
        p.try_push(1).unwrap();
        p.try_push(2).unwrap();
        let t = thread::spawn(move || {
            // Either full (Err) or the consumer freed a slot; never silent overwrite.
            let r = p.try_push(3);
            r.is_ok()
        });
        let a = c.try_pop();
        let pushed = t.join().unwrap();
        assert_eq!(a, Some(1));
        let b = c.try_pop();
        assert_eq!(b, Some(2));
        let third = c.try_pop();
        if pushed {
            assert_eq!(third, Some(3));
        } else {
            assert_eq!(third, None);
        }
    });
}
