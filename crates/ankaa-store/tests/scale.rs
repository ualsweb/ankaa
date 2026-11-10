//! Many keys: a table with one partition per device (100,000 devices). Measures ingest, the
//! snapshot a query takes, and retained memory per idle device. Not a correctness test: run
//! with `cargo test --release -p ankaa-store --test scale -- --ignored --nocapture`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Live heap bytes (the test counts what the table retains, not what the process reserved).
struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
// SAFETY: forwards to the system allocator; only adds bookkeeping.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size(), Ordering::Relaxed);
        // SAFETY: same layout contract as the caller's.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        // SAFETY: `p` was allocated by `System` with `l`.
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static A: Counting = Counting;

fn live_mib() -> f64 {
    LIVE.load(Ordering::Relaxed) as f64 / 1_048_576.0
}

use ankaa_store::{ColumnType, Field, Schema, Table, Value};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("device", ColumnType::DictU32),
        Field::nullable("value", ColumnType::Float64),
        Field::new("status", ColumnType::U8),
    ])
    .unwrap()
}

#[test]
#[ignore = "measurement; run explicitly in release"]
fn hundred_thousand_devices() {
    const DEVICES: u32 = 100_000;
    const ROUNDS: i64 = 20;
    let t = Table::new("telemetry", schema(), 0);
    let t0 = Instant::now();
    for r in 0..ROUNDS {
        for d in 0..DEVICES {
            let idx = t.partition_for(d);
            let row = [
                Value::I64(r * 1_000_000_000 + i64::from(d)),
                Value::U32(d),
                Value::F64(f64::from(d % 100)),
                Value::U8(0),
            ];
            t.append_rows(idx, &[&row]);
        }
    }
    let rows = u64::from(DEVICES) * u64::try_from(ROUNDS).unwrap();
    let ingest = t0.elapsed();
    println!(
        "heap after ingest: {:.1} MiB ({:.0} B per device, raw data {:.0} B per device)",
        live_mib(),
        live_mib() * 1_048_576.0 / f64::from(DEVICES),
        21.0 * ROUNDS as f64
    );
    println!(
        "ingest: {rows} rows over {DEVICES} partitions in {ingest:?} ({:.2} M rows/s)",
        rows as f64 / ingest.as_secs_f64() / 1e6
    );
    // first snapshot copies every open chunk; later ones only what changed
    for i in 0..3 {
        let t1 = Instant::now();
        let s = t.snapshot();
        println!(
            "snapshot {i}: {} chunks in {:?}",
            s.chunks.len(),
            t1.elapsed()
        );
    }
    let held = t.snapshot();
    println!("heap with a snapshot held: {:.1} MiB", live_mib());
    drop(held);
    // one more reading for 1% of the devices, then a snapshot
    for d in (0..DEVICES).step_by(100) {
        let idx = t.partition_for(d);
        let row = [
            Value::I64(ROUNDS * 1_000_000_000 + i64::from(d)),
            Value::U32(d),
            Value::Null,
            Value::U8(1),
        ];
        t.append_rows(idx, &[&row]);
    }
    let t2 = Instant::now();
    let s = t.snapshot();
    println!(
        "snapshot after 1% of devices sent: {} chunks in {:?}",
        s.chunks.len(),
        t2.elapsed()
    );
    assert_eq!(t.num_partitions(), DEVICES as usize);
}
