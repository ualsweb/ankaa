//! Prometheus text metrics over a tiny HTTP/1.1 responder (loopback by default, bearer token).

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use ankaa_flight::QueryMetrics;
use ankaa_ts::LatencyHistogram;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Quantiles reported for every histogram.
const QUANTILES: [(&str, f64); 4] = [("0.5", 0.5), ("0.9", 0.9), ("0.99", 0.99), ("0.999", 0.999)];

/// Fixed-point latency histogram behind a mutex: `record` is called once per exchange message
/// (not per event), off the decode critical section.
#[derive(Debug)]
pub struct Histogram {
    inner: Mutex<LatencyHistogram>,
    sum_ns: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        // 1 µs quanta up to 10 s: 10,000,000 buckets would be too many; use 1 µs up to 1 s
        // (1,000,000 × 8 B = 8 MiB is still too much for a scrape path) → 10 µs quanta to 1 s.
        Self {
            inner: Mutex::new(LatencyHistogram::new(1_000_000_000, 10_000)),
            sum_ns: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    /// Record one latency sample in nanoseconds.
    pub fn record(&self, ns: u64) {
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.inner.lock().record(ns);
    }

    fn render(&self, s: &mut String, name: &str, help: &str) {
        let h = self.inner.lock();
        let _ = writeln!(s, "# HELP {name} {help}\n# TYPE {name} summary");
        for (label, q) in QUANTILES {
            if let Some(v) = h.quantile(q) {
                let _ = writeln!(s, "{name}{{quantile=\"{label}\"}} {v}");
            }
        }
        let _ = writeln!(s, "{name}_sum {}", self.sum_ns.load(Ordering::Relaxed));
        let _ = writeln!(s, "{name}_count {}", h.count());
    }
}

/// Storage tier gauges (set by the tiering step).
#[derive(Debug, Default)]
pub struct TierMetrics {
    /// Hot (uncompressed) sealed chunks.
    pub hot_chunks: AtomicU64,
    /// Warm (encoded, in memory) chunks.
    pub warm_chunks: AtomicU64,
    /// Cold (on disk) segments.
    pub cold_segments: AtomicU64,
    /// Bytes in hot chunks.
    pub hot_bytes: AtomicU64,
    /// Bytes in warm chunks (encoded).
    pub warm_bytes: AtomicU64,
    /// Bytes in cold segments (on disk).
    pub cold_bytes: AtomicU64,
    /// Chunks encoded hot → warm.
    pub moves_to_warm: AtomicU64,
    /// Chunks written warm → cold.
    pub moves_to_cold: AtomicU64,
    /// Chunks out on the encode threads (being compressed, written or merged).
    pub encode_jobs: AtomicU64,
    /// Encodes, segment writes and merges that failed (the chunk stays as it was).
    pub errors: AtomicU64,
    /// Merges of small chunks done, and chunks they replaced.
    pub compactions: AtomicU64,
    /// See [`Self::compactions`].
    pub compacted_chunks: AtomicU64,
    /// Bytes of cold segment files held in memory by the segment cache.
    pub segment_cache_bytes: AtomicU64,
    /// Cold chunk reads answered from the segment cache.
    pub segment_cache_hits: AtomicU64,
    /// Cold chunk reads that opened the segment file.
    pub segment_cache_misses: AtomicU64,
    /// Checkpoints written.
    pub checkpoints: AtomicU64,
    /// WAL segments truncated after checkpoints.
    pub wal_truncated_segments: AtomicU64,
    /// Open chunks sealed because their first row was older than `storage.max_open_age_s`.
    pub sealed_for_age: AtomicU64,
    /// Open chunks sealed to keep uncompressed rows within `storage.hot_budget_mib`.
    pub sealed_for_memory: AtomicU64,
    /// Rows dropped by retention.
    pub expired_rows: AtomicU64,
    /// Chunks dropped by retention.
    pub expired_chunks: AtomicU64,
    /// Segment files deleted by retention (once no query held them).
    pub segments_deleted: AtomicU64,
}

/// Process-wide counters (relaxed atomics; scraped, never on the hot path's critical section).
#[derive(Debug, Default)]
pub struct Metrics {
    /// Raw frames received from the source.
    pub frames: AtomicU64,
    /// Frames the decoder rejected.
    pub frames_bad: AtomicU64,
    /// Frames ignored (acks, unknown types).
    pub frames_ignored: AtomicU64,
    /// Events appended.
    pub events: AtomicU64,
    /// Rows dropped by appenders (out of order / invalid).
    pub dropped: AtomicU64,
    /// Rows accepted late (older than their partition's watermark, within the table's
    /// `max_lateness_ms`) and inserted at their place in time.
    pub late_rows: AtomicU64,
    /// Trades decoded.
    pub trades: AtomicU64,
    /// Book deltas decoded.
    pub deltas: AtomicU64,
    /// Depth sequence gaps.
    pub gaps: AtomicU64,
    /// WebSocket (re)connects.
    pub connects: AtomicU64,
    /// Frames dropped at the source channel (backpressure).
    pub source_dropped_full: AtomicU64,
    /// Snapshots registered for queries.
    pub snapshots: AtomicU64,
    /// Sealed chunks across tables.
    pub sealed_chunks: AtomicU64,
    /// Rows in tables.
    pub rows: AtomicU64,
    /// Nanoseconds from frame receive to watermark advance, last observed (informative, software clock).
    pub last_ingest_latency_ns: AtomicU64,
    /// Highest WAL LSN known durable.
    pub wal_durable_lsn: AtomicU64,
    /// Bytes appended but not yet committed.
    pub wal_pending_bytes: AtomicU64,
    /// Size of the WAL directory's segments, bytes (refreshed every few seconds).
    pub wal_bytes: AtomicU64,
    /// Group commits performed.
    pub wal_commits: AtomicU64,
    /// Frames the WAL did not take within the wait (stalled disk): dropped, not ingested.
    pub wal_dropped: AtomicU64,
    /// WAL append/commit errors.
    pub wal_errors: AtomicU64,
    /// Frames recovered from the WAL at startup.
    pub wal_recovered_frames: AtomicU64,
    /// Book snapshots fetched.
    pub snapshots_fetched: AtomicU64,
    /// Book resynchronisations requested.
    pub book_resyncs: AtomicU64,
    /// Depth messages after which a book was crossed (best bid >= best ask).
    pub book_crossed: AtomicU64,
    /// Depth messages ignored as stale (at or before the book's last update id).
    pub book_stale: AtomicU64,
    /// Receive-to-watermark latency per exchange message.
    pub ingest_latency: Histogram,
    /// Standby: highest primary LSN received.
    pub replication_lsn: AtomicU64,
    /// Standby: (re)connections to the primary.
    pub replication_connects: AtomicU64,
    /// Standby: 1 while connected to the primary.
    pub replication_connected: AtomicU64,
    /// User sources: messages mapped into rows.
    pub user_rows: AtomicU64,
    /// User sources: messages rejected by the mapper.
    pub user_rejected: AtomicU64,
    /// Continuous queries: late rows dropped.
    pub cq_late_rows: AtomicU64,
    /// Continuous queries: result rows refused by their output table (expected 0).
    pub cq_rejected_rows: AtomicU64,
    /// Standby: highest primary LSN applied to the tables.
    pub replication_applied_lsn: AtomicU64,
    /// Standby: wall-clock milliseconds since the epoch of the last batch received (0 = never).
    pub replication_last_receive_ms: AtomicU64,
    /// Frames too large for the WAL (dropped, not ingested).
    pub wal_oversized: AtomicU64,
    /// Mapping workers, runs of frames out on them, and runs handed out since the start.
    pub ingest_workers: AtomicU64,
    /// See [`Self::ingest_workers`].
    pub map_jobs: AtomicU64,
    /// See [`Self::ingest_workers`].
    pub map_jobs_total: AtomicU64,
    /// Values in user-table dictionaries.
    pub dict_entries: AtomicU64,
    /// Source positions saved (file offsets, Kafka offsets).
    pub source_positions_saved: AtomicU64,
    /// Query-side counters shared with the Flight service.
    pub query: Arc<QueryMetrics>,
    /// Tier gauges.
    pub tier: TierMetrics,
}

impl Metrics {
    fn render_tiers(&self, s: &mut String) {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let t = &self.tier;
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_chunks Chunks or segments per storage tier\n# TYPE ankaa_tier_chunks gauge"
        );
        let _ = writeln!(s, "ankaa_tier_chunks{{tier=\"hot\"}} {}", g(&t.hot_chunks));
        let _ = writeln!(
            s,
            "ankaa_tier_chunks{{tier=\"warm\"}} {}",
            g(&t.warm_chunks)
        );
        let _ = writeln!(
            s,
            "ankaa_tier_chunks{{tier=\"cold\"}} {}",
            g(&t.cold_segments)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_bytes Bytes per storage tier (encoded size for warm/cold)\n# TYPE ankaa_tier_bytes gauge"
        );
        let _ = writeln!(s, "ankaa_tier_bytes{{tier=\"hot\"}} {}", g(&t.hot_bytes));
        let _ = writeln!(s, "ankaa_tier_bytes{{tier=\"warm\"}} {}", g(&t.warm_bytes));
        let _ = writeln!(s, "ankaa_tier_bytes{{tier=\"cold\"}} {}", g(&t.cold_bytes));
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_moves_total Chunk moves between tiers\n# TYPE ankaa_tier_moves_total counter"
        );
        let _ = writeln!(
            s,
            "ankaa_tier_moves_total{{to=\"warm\"}} {}",
            g(&t.moves_to_warm)
        );
        let _ = writeln!(
            s,
            "ankaa_tier_moves_total{{to=\"cold\"}} {}",
            g(&t.moves_to_cold)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_encode_jobs Chunks out on the encode threads (compress, write a segment, merge)\n# TYPE ankaa_tier_encode_jobs gauge\nankaa_tier_encode_jobs {}",
            g(&t.encode_jobs)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_errors_total Encodes, segment writes and merges that failed (the chunk stays as it was; see the log)\n# TYPE ankaa_tier_errors_total counter\nankaa_tier_errors_total {}",
            g(&t.errors)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_compactions_total Merges of small chunks of a partition into one\n# TYPE ankaa_tier_compactions_total counter\nankaa_tier_compactions_total {}",
            g(&t.compactions)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_tier_compacted_chunks_total Small chunks replaced by merged ones\n# TYPE ankaa_tier_compacted_chunks_total counter\nankaa_tier_compacted_chunks_total {}",
            g(&t.compacted_chunks)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_segment_cache_bytes Bytes of cold segment files kept in memory (storage.segment_cache_mib)\n# TYPE ankaa_segment_cache_bytes gauge\nankaa_segment_cache_bytes {}",
            g(&t.segment_cache_bytes)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_segment_cache_reads_total Cold chunk reads by where the segment came from\n# TYPE ankaa_segment_cache_reads_total counter\nankaa_segment_cache_reads_total{{from=\"cache\"}} {}\nankaa_segment_cache_reads_total{{from=\"file\"}} {}",
            g(&t.segment_cache_hits),
            g(&t.segment_cache_misses)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_checkpoints_total Checkpoints written\n# TYPE ankaa_checkpoints_total counter\nankaa_checkpoints_total {}",
            g(&t.checkpoints)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_retention_expired_rows_total Rows dropped by retention\n# TYPE ankaa_retention_expired_rows_total counter\nankaa_retention_expired_rows_total {}",
            g(&t.expired_rows)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_retention_expired_chunks_total Chunks dropped by retention\n# TYPE ankaa_retention_expired_chunks_total counter\nankaa_retention_expired_chunks_total {}",
            g(&t.expired_chunks)
        );
        let _ = writeln!(
            s,
            "# HELP ankaa_retention_segments_deleted_total Segment files deleted by retention\n# TYPE ankaa_retention_segments_deleted_total counter\nankaa_retention_segments_deleted_total {}",
            g(&t.segments_deleted)
        );
    }

    #[allow(clippy::too_many_lines)]
    fn render(&self, started: Instant) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = String::with_capacity(4096);
        let mut m = |name: &str, help: &str, kind: &str, v: u64| {
            let _ = writeln!(
                s,
                "# HELP {name} {help}
# TYPE {name} {kind}
{name} {v}"
            );
        };
        m(
            "ankaa_uptime_seconds",
            "Seconds since start",
            "gauge",
            started.elapsed().as_secs(),
        );
        m(
            "ankaa_frames_total",
            "Raw frames received",
            "counter",
            g(&self.frames),
        );
        m(
            "ankaa_frames_bad_total",
            "Frames rejected by the decoder",
            "counter",
            g(&self.frames_bad),
        );
        m(
            "ankaa_frames_ignored_total",
            "Frames ignored",
            "counter",
            g(&self.frames_ignored),
        );
        m(
            "ankaa_events_total",
            "Events appended",
            "counter",
            g(&self.events),
        );
        m(
            "ankaa_dropped_rows_total",
            "Rows dropped by appenders",
            "counter",
            g(&self.dropped),
        );
        m(
            "ankaa_late_rows_total",
            "Rows accepted out of time order (within the table's allowed lateness) and inserted in place",
            "counter",
            g(&self.late_rows),
        );
        m(
            "ankaa_trades_total",
            "Trades decoded",
            "counter",
            g(&self.trades),
        );
        m(
            "ankaa_book_deltas_total",
            "Book deltas decoded",
            "counter",
            g(&self.deltas),
        );
        m(
            "ankaa_feed_gaps_total",
            "Depth sequence gaps",
            "counter",
            g(&self.gaps),
        );
        m(
            "ankaa_source_connects_total",
            "WebSocket connects",
            "counter",
            g(&self.connects),
        );
        m(
            "ankaa_source_dropped_full_total",
            "Frames dropped at the source channel",
            "counter",
            g(&self.source_dropped_full).max(ankaa_feeds::counters::dropped_full_total()),
        );
        m(
            "ankaa_source_oversized_total",
            "Generic sources: messages larger than max_message_bytes, dropped",
            "counter",
            ankaa_feeds::counters::oversized_total(),
        );
        m(
            "ankaa_kafka_offset_resets_total",
            "Kafka partitions whose saved position was no longer retained (data lost at the broker)",
            "counter",
            ankaa_feeds::counters::kafka_offset_resets_total(),
        );
        m(
            "ankaa_source_positions_saved_total",
            "Source read positions made durable (file and Kafka sources)",
            "counter",
            g(&self.source_positions_saved),
        );
        m(
            "ankaa_dictionary_entries",
            "Distinct values across the dict columns of user tables",
            "gauge",
            g(&self.dict_entries),
        );
        m(
            "ankaa_wal_bytes",
            "Size of the write-ahead log on disk (refreshed every 5 s)",
            "gauge",
            g(&self.wal_bytes),
        );
        m(
            "ankaa_ingest_workers",
            "Threads mapping messages to rows (storage.ingest_workers; 0: the ingest loop maps)",
            "gauge",
            g(&self.ingest_workers),
        );
        m(
            "ankaa_ingest_map_jobs",
            "Runs of frames out on the mapping workers, not yet applied",
            "gauge",
            g(&self.map_jobs),
        );
        m(
            "ankaa_ingest_map_jobs_total",
            "Runs of frames handed to the mapping workers",
            "counter",
            g(&self.map_jobs_total),
        );
        m(
            "ankaa_wal_oversized_total",
            "Frames too large for a WAL record, dropped before ingest",
            "counter",
            g(&self.wal_oversized),
        );
        m(
            "ankaa_snapshots_total",
            "Query snapshots registered",
            "counter",
            g(&self.snapshots),
        );
        m(
            "ankaa_sealed_chunks",
            "Sealed chunks",
            "gauge",
            g(&self.sealed_chunks),
        );
        m("ankaa_rows", "Rows in tables", "gauge", g(&self.rows));
        m(
            "ankaa_last_ingest_latency_ns",
            "Last frame receive-to-watermark latency (software clock)",
            "gauge",
            g(&self.last_ingest_latency_ns),
        );
        // WAL
        m(
            "ankaa_wal_durable_lsn",
            "Highest LSN known durable (0 = no WAL)",
            "gauge",
            g(&self.wal_durable_lsn),
        );
        m(
            "ankaa_wal_pending_bytes",
            "Bytes appended, not yet committed",
            "gauge",
            g(&self.wal_pending_bytes),
        );
        m(
            "ankaa_wal_commits_total",
            "Group commits",
            "counter",
            g(&self.wal_commits),
        );
        m(
            "ankaa_wal_dropped_total",
            "Frames the WAL did not accept within its wait (stalled disk); dropped, not ingested",
            "counter",
            g(&self.wal_dropped),
        );
        m(
            "ankaa_wal_errors_total",
            "WAL append or commit errors",
            "counter",
            g(&self.wal_errors),
        );
        m(
            "ankaa_wal_recovered_frames",
            "Frames replayed from the WAL at start",
            "gauge",
            g(&self.wal_recovered_frames),
        );
        m(
            "ankaa_wal_truncated_segments_total",
            "WAL segments removed after checkpoints",
            "counter",
            g(&self.tier.wal_truncated_segments),
        );
        m(
            "ankaa_tier_sealed_for_age_total",
            "Open chunks sealed because their first row was older than storage.max_open_age_s",
            "counter",
            g(&self.tier.sealed_for_age),
        );
        m(
            "ankaa_tier_sealed_for_memory_total",
            "Open chunks sealed to keep uncompressed rows within storage.hot_budget_mib",
            "counter",
            g(&self.tier.sealed_for_memory),
        );
        // books
        m(
            "ankaa_book_snapshots_fetched_total",
            "REST depth snapshots fetched",
            "counter",
            g(&self.snapshots_fetched),
        );
        m(
            "ankaa_book_resyncs_total",
            "Order-book resynchronisations requested",
            "counter",
            g(&self.book_resyncs),
        );
        m(
            "ankaa_book_crossed_messages_total",
            "Depth messages after which a book was crossed",
            "counter",
            g(&self.book_crossed),
        );
        m(
            "ankaa_book_stale_messages_total",
            "Depth messages ignored as stale",
            "counter",
            g(&self.book_stale),
        );
        // queries
        let q = &self.query;
        m(
            "ankaa_queries_total",
            "SQL queries planned over Flight",
            "counter",
            g(&q.queries),
        );
        m(
            "ankaa_query_errors_total",
            "Queries rejected or failed",
            "counter",
            g(&q.errors),
        );
        m(
            "ankaa_query_unauthenticated_total",
            "Flight calls refused for a bad token",
            "counter",
            g(&q.unauthenticated),
        );
        m(
            "ankaa_query_rows_total",
            "Rows streamed to clients",
            "counter",
            g(&q.rows),
        );
        m(
            "ankaa_query_do_get_total",
            "DoGet streams completed",
            "counter",
            g(&q.do_get_total),
        );
        m(
            "ankaa_query_do_get_ns_total",
            "Wall nanoseconds of completed DoGet streams",
            "counter",
            g(&q.do_get_ns_total),
        );
        m(
            "ankaa_query_last_do_get_ns",
            "Wall time of the last DoGet",
            "gauge",
            g(&q.last_do_get_ns),
        );
        m(
            "ankaa_replication_received_lsn",
            "Standby: highest primary WAL LSN received (0 = not a standby)",
            "gauge",
            g(&self.replication_lsn),
        );
        m(
            "ankaa_replication_connects_total",
            "Standby: connections to the primary",
            "counter",
            g(&self.replication_connects),
        );
        m(
            "ankaa_replication_connected",
            "Standby: 1 while the primary stream is open",
            "gauge",
            g(&self.replication_connected),
        );
        m(
            "ankaa_replication_applied_lsn",
            "Standby: highest primary WAL LSN applied to the tables",
            "gauge",
            g(&self.replication_applied_lsn),
        );
        let last_rx = g(&self.replication_last_receive_ms);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        m(
            "ankaa_replication_last_receive_age_seconds",
            "Standby: seconds since the last batch arrived from the primary (0 = not a standby or nothing received yet)",
            "gauge",
            if last_rx == 0 {
                0
            } else {
                now_ms.saturating_sub(last_rx) / 1000
            },
        );
        m(
            "ankaa_replication_wal_streams",
            "WAL streams currently served to standbys",
            "gauge",
            g(&q.wal_streams),
        );
        m(
            "ankaa_replication_wal_refused_total",
            "WAL stream requests refused (token, stream limit)",
            "counter",
            g(&q.wal_refused),
        );
        m(
            "ankaa_user_rows_total",
            "Generic sources: messages mapped into rows",
            "counter",
            g(&self.user_rows),
        );
        m(
            "ankaa_user_rejected_total",
            "Generic sources: messages the mapper rejected",
            "counter",
            g(&self.user_rejected),
        );
        m(
            "ankaa_cq_late_rows_total",
            "Continuous queries: rows older than the open window, dropped",
            "counter",
            g(&self.cq_late_rows),
        );
        m(
            "ankaa_cq_rejected_rows_total",
            "Continuous queries: result rows refused by the output table (a defect; expected 0)",
            "counter",
            g(&self.cq_rejected_rows),
        );
        drop(m);
        self.render_tiers(&mut s);
        self.ingest_latency.render(
            &mut s,
            "ankaa_ingest_latency_ns",
            "Receive-to-watermark latency per exchange message (software clock, 10 us quanta)",
        );
        s
    }
}

/// Concurrent `/metrics` connections served; more are closed at once (the listener shares the
/// process's file descriptors with the query endpoints).
const MAX_METRICS_CONNECTIONS: usize = 32;

/// Read an HTTP request head: until the blank line, 4,096 bytes or `deadline`.
async fn read_head(sock: &mut tokio::net::TcpStream, deadline: tokio::time::Instant) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let mut n = 0;
    while n < buf.len() {
        match tokio::time::timeout_at(deadline, sock.read(&mut buf[n..])).await {
            Ok(Ok(0) | Err(_)) | Err(_) => break,
            Ok(Ok(k)) => {
                n += k;
                if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    buf.truncate(n);
    buf
}

/// Serve `GET /metrics` until the listener task is dropped. Minimal HTTP/1.1, one request per
/// connection, bearer token when configured.
pub async fn serve_metrics(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    token: Option<String>,
    started: Instant,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_METRICS_CONNECTIONS));
    loop {
        let (mut sock, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                // EMFILE and friends: back off instead of spinning
                tracing::warn!(error = %e, "metrics accept failed");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            continue; // too many scrapes in flight: drop this connection
        };
        let _ = sock.set_nodelay(true);
        let metrics = metrics.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // one deadline for the whole exchange: a slow client cannot hold a slot
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            let head = read_head(&mut sock, deadline).await;
            if head.is_empty() {
                return;
            }
            let req = String::from_utf8_lossy(&head);
            let first = req.lines().next().unwrap_or("");
            // exact `Authorization: Bearer <token>` header, constant-time compare (as Flight)
            let authorized = token.as_deref().is_none_or(|t| {
                req.lines().any(|l| {
                    l.split_once(':').is_some_and(|(name, value)| {
                        name.trim().eq_ignore_ascii_case("authorization")
                            && ankaa_core::bearer_token(value.trim()).is_some_and(|got| {
                                ankaa_core::constant_time_eq(got.as_bytes(), t.as_bytes())
                            })
                    })
                })
            });
            let path_ok = first.starts_with("GET /metrics ") || first == "GET /metrics";
            let (status, body) = if !path_ok {
                ("404 Not Found", String::from("not found\n"))
            } else if !authorized {
                ("401 Unauthorized", String::from("unauthorized\n"))
            } else {
                ("200 OK", metrics.render(started))
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = tokio::time::timeout_at(deadline, async {
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            })
            .await;
        });
    }
}
