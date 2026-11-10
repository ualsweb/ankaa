//! Process-wide source counters, updated as events happen so `/metrics` can report them while
//! the server runs (per-task statistics are only returned when a source stops).

use std::sync::atomic::{AtomicU64, Ordering};

static DROPPED_FULL: AtomicU64 = AtomicU64::new(0);
static OVERSIZED: AtomicU64 = AtomicU64::new(0);
static KAFKA_OFFSET_RESETS: AtomicU64 = AtomicU64::new(0);

/// Frames dropped because the ingest channel was full (WebSocket and NATS sources; file and
/// Kafka sources wait instead).
#[must_use]
pub fn dropped_full_total() -> u64 {
    DROPPED_FULL.load(Ordering::Relaxed)
}

/// Messages dropped because they were larger than the source's `max_message_bytes`.
#[must_use]
pub fn oversized_total() -> u64 {
    OVERSIZED.load(Ordering::Relaxed)
}

/// Kafka partitions whose saved position was no longer retained by the broker, so the source
/// jumped to the earliest retained offset (messages in between were lost at the broker).
#[must_use]
pub fn kafka_offset_resets_total() -> u64 {
    KAFKA_OFFSET_RESETS.load(Ordering::Relaxed)
}

#[allow(dead_code)]
pub(crate) fn note_dropped_full() {
    DROPPED_FULL.fetch_add(1, Ordering::Relaxed);
}

#[allow(dead_code)]
pub(crate) fn note_oversized() {
    OVERSIZED.fetch_add(1, Ordering::Relaxed);
}

#[allow(dead_code)]
pub(crate) fn note_kafka_offset_reset() {
    KAFKA_OFFSET_RESETS.fetch_add(1, Ordering::Relaxed);
}
