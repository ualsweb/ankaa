# Metrics semantics

`ankaad serve` exposes Prometheus text metrics on `server.metrics_addr` (default `127.0.0.1:9100`,
path `/metrics`, bearer token when one is configured). Every metric name and HELP string lives in
`bins/ankaad/src/metrics.rs`; this page explains what each one means, how it is updated, what a healthy
process looks like, and which alert rules we recommend. The rules are shipped ready to load in
`deploy/prometheus/alerts.yml`, and the dashboard in `deploy/grafana/ankaa-overview.json` plots the same series.

Two caveats apply to everything on this page:

- **Software timestamps.** All latency figures are taken with the host clock (`SystemTime::now()`
  on receive, `Instant`/`SystemTime` on the measuring side). They are informative, not a benchmark.
  The hardware-timestamped protocol (NIC PHC to CNTVCT calibration) is described in
  `docs/measurement-protocol.md` and is not implemented yet.
- **No published numbers.** Nothing in this page states what a "normal" absolute value is. Thresholds in the
  alert rules are starting points you must tune on your own feed and hardware.

## Vocabulary

| Term | Meaning in `ankaad` |
|---|---|
| **frame** | One WebSocket message (text or binary) or one internal control/snapshot frame. The unit of the source channel and of the WAL. |
| **exchange message** | One decoded frame carrying market data; a trade frame yields one trade, a depth frame yields N book deltas. |
| **event** | One row-producing item inside a batch: a trade, a book delta or a snapshot. `events` counts `batch.len()` after decoding. |
| **row** | One row appended to a table (`trades`, `book_deltas`, `quotes`). Rows per event vary: a depth message writes one `book_deltas` row per level and at most one `quotes` row. |
| **snapshot (query)** | The periodic swap of the query view (`storage.snapshot_ms`, default 250 ms). |
| **snapshot (book)** | A REST depth snapshot used to align the L2 book. Different thing; the metric names say `book_`. |

## Update cadence

Metrics are relaxed atomics read at scrape time; nothing on the ingest critical section waits for a scrape.
Three update patterns exist, and they matter when you read a graph:

1. **Per frame / per event, immediately**: `ankaa_frames_total`, `ankaa_frames_bad_total`,
   `ankaa_frames_ignored_total`, `ankaa_events_total`, `ankaa_dropped_rows_total`,
   `ankaa_source_connects_total`, `ankaa_book_resyncs_total`, `ankaa_book_snapshots_fetched_total`,
   `ankaa_last_ingest_latency_ns`, the `ankaa_ingest_latency_ns` summary, all WAL metrics, all query metrics.
2. **Copied from decoder/table statistics on every query-snapshot tick** (every `storage.snapshot_ms`):
   `ankaa_trades_total`, `ankaa_book_deltas_total`, `ankaa_feed_gaps_total`, `ankaa_rows`,
   `ankaa_sealed_chunks`, `ankaa_snapshots_total`. They are monotonic but move in 250 ms steps.
3. **Process-wide, live**: `ankaa_source_dropped_full_total` is incremented by the source tasks at the moment
   a frame is dropped (WebSocket and NATS sources). File and Kafka sources wait for channel space instead of
   dropping, so they never move this counter.

## Ingest

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `ankaa_uptime_seconds` | gauge | s | Seconds since `serve` started (after WAL recovery, before listeners bind). |
| `ankaa_frames_total` | counter | frames | Every frame handed to the ingest loop: live frames from the source channel, control frames (`connect`/`disconnect`), book-snapshot frames, and every frame replayed from the WAL at start. Incremented once per frame before decoding. |
| `ankaa_frames_bad_total` | counter | frames | Frames the decoder rejected with an error other than `Ignored` (malformed JSON/SBE, number overflow, precision loss, bad snapshot body). Each is also logged at `warn`. |
| `ankaa_frames_ignored_total` | counter | frames | Frames the decoder recognised but produced nothing from (subscription acks, unknown stream types). Not an error. |
| `ankaa_events_total` | counter | events | Decoded items handed to the appenders (`trades + deltas + snapshots` per batch), incremented once per exchange message by the batch size. Includes events whose rows were later dropped. |
| `ankaa_dropped_rows_total` | counter | rows | Rows an appender refused: timestamp below the partition watermark (out of order) or a type/arity mismatch. Incremented per batch by the number of rows rejected across all three tables. Expected 0 on an in-order feed; see `docs/temporal-architecture.md`. |
| `ankaa_trades_total` | counter | trades | Trades decoded (decoder statistic, copied on the snapshot tick). |
| `ankaa_book_deltas_total` | counter | deltas | Book delta levels decoded (decoder statistic, copied on the snapshot tick). One depth message with 10 levels adds 10. |

Good looks like: `rate(ankaa_frames_total[1m])` steady and matching the subscribed streams,
`ankaa_frames_bad_total` flat, `ankaa_dropped_rows_total` flat at its post-recovery value.
`ankaa_events_total` is naturally larger than `ankaa_frames_total` because a depth frame carries several levels.

Note on `ankaa_frames_total` after a restart with a WAL: recovery replays every durable frame through the same
path, so the counter starts at the recovered count (also exposed as `ankaa_wal_recovered_frames`) rather than at 0.
Rates are unaffected after the first scrape interval.

## Feed health

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `ankaa_feed_gaps_total` | counter | gaps | Depth sequence gaps detected by the decoders (`U != previous u + 1` on the Binance diff stream; the SBE decoder's gaps are added). Reset of sequence state on reconnect is not counted as a gap. Copied on the snapshot tick. |
| `ankaa_source_connects_total` | counter | connections | `connect` control frames seen by ingest: the first connection plus every reconnect. Per-connection, not per attempt; failed attempts are logged (`connect failed`, with the backoff) but not counted. |
| `ankaa_source_dropped_full_total` | counter | frames | Frames a source task dropped because the 65,536-frame ingest channel was full (ingest slower than the feed). WebSocket and NATS sources drop and count; file and Kafka sources wait instead. Live. |
| `ankaa_book_snapshots_fetched_total` | counter | snapshots | REST depth snapshots fetched successfully (on connect for every depth symbol, and on each resync request; at most one fetch per symbol per 2 s). Failed fetches are logged, not counted. |
| `ankaa_book_resyncs_total` | counter | requests | Symbols for which the book synchroniser asked for a new snapshot after being aligned: a gap in the diff stream, or a snapshot too old for the buffered diffs. Incremented per batch by the number of symbols that need one. The initial snapshot after a (re)connect is not a resync. |
| `ankaa_book_crossed_messages_total` | counter | messages | Depth messages after which the reconstructed book had best bid >= best ask. Expected 0 on a synchronised book; non-zero means a missed update or a venue-side anomaly. |
| `ankaa_book_stale_messages_total` | counter | messages | Depth messages ignored because their last update id was at or before the book's (raced a snapshot, or replayed). Small numbers after each snapshot are normal. |

Good looks like: `ankaa_feed_gaps_total` and `ankaa_book_resyncs_total` flat; `ankaa_source_connects_total`
equal to 1 plus the number of exchange-side disconnects you can explain. A gap is normally followed by exactly one
resync and one snapshot fetch for the affected symbol.

What each one tells you:

- Gaps without resyncs: the gap happened while the book was already waiting for a snapshot, or on a symbol with
  trades only. Gaps are a feed or network problem, not an engine problem.
- Resyncs without gaps: the snapshot came back older than the buffered diffs (slow REST, high diff rate). Check
  `ankaa_book_snapshots_fetched_total` and the `depth snapshot fetch failed` log line.
- A rising `ankaa_source_connects_total` is a reconnect storm: the exchange is closing the socket or the network is
  flapping. Each reconnect resets sequence tracking and forces fresh snapshots for every depth symbol.

## Latency

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `ankaa_last_ingest_latency_ns` | gauge | ns | For the most recent exchange message applied: host time when the batch had been appended (watermark advanced) minus the frame's receive timestamp. Overwritten per message. |
| `ankaa_ingest_latency_ns{quantile="0.5|0.9|0.99|0.999"}` | summary | ns | Quantiles of the same quantity over **every message since process start**. |
| `ankaa_ingest_latency_ns_sum` | summary sum | ns | Sum of all samples. |
| `ankaa_ingest_latency_ns_count` | summary count | samples | Number of samples (one per exchange message applied, including WAL-recovered ones). |
| `ankaa_query_last_do_get_ns` | gauge | ns | Wall time of the last `DoGet` stream from request to the stream being dropped (client finished or went away). |
| `ankaa_query_do_get_ns_total` / `ankaa_query_do_get_total` | counters | ns / streams | Sum of `DoGet` wall times and the number of completed streams; the ratio over a window is the recent mean. |

### How the summary is built

The summary is not a Prometheus client-library summary with a sliding window. It is a fixed-point histogram
(`ankaa_ts::LatencyHistogram`): one `u64` bucket per **10 µs** quantum from 0 to **1 s** (100,001 buckets), plus
one overflow bucket. `record` costs one division and one increment and is called once per exchange message,
after the batch has been appended, behind a mutex that is never contended with the decode path. At scrape time the
quantile is found by walking the buckets (`rank = floor(q·(count−1)) + 1`) and reporting the **lower edge** of the
bucket, so a reported value is exact to within 10 µs and never above the true quantile. A quantile that falls in the
overflow bucket is rendered as `18446744073709551615` (`u64::MAX`); the alert rules below guard against that.

Consequences:

- **Lifetime, not windowed.** A bad minute after a day of good minutes barely moves p99. Use
  `rate(ankaa_ingest_latency_ns_sum[5m]) / rate(ankaa_ingest_latency_ns_count[5m])` for a recent mean and
  `ankaa_last_ingest_latency_ns` for a live spot check; use the summary quantiles for "how has this process behaved
  overall". Restarting the process resets the histogram.
- **What is measured.** The interval starts at `recv_ns`, which the WebSocket task stamps with the host clock
  when it reads the message, and ends with another host-clock read after `Tables::ingest` returns. It therefore
  includes queueing in the source channel, JSON/SBE decoding, book alignment and appends. It excludes the network,
  the kernel receive path and the time until the next query-snapshot swap makes the rows visible (up to
  `storage.snapshot_ms`). "Receive-to-queryable" is thus roughly this number plus up to one snapshot interval.
- **WAL recovery pollutes it.** Frames replayed at start carry their original `recv_ns`, so their "latency" is the
  age of the WAL, which lands in the overflow bucket. On a process with a WAL, p99.9 (and p99 for short runs)
  may read `u64::MAX` until enough live samples dilute them. The deployment guide suggests ignoring the summary for
  `ankaa_wal_recovered_frames / ankaa_ingest_latency_ns_count` worth of samples.
- **Software clock.** `SystemTime` can step (NTP). A negative interval is clamped to 0.

The `*_do_get_*` figures measure the server side of a Flight stream including the client's consumption speed,
since the stream is dropped only when the client is done. A slow client inflates them.

## Durability (WAL)

All WAL metrics are 0 when `storage.wal_dir` is unset.

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `ankaa_wal_durable_lsn` | gauge | LSN | Highest log sequence number known durable (`fdatasync` returned). Set to the recovered LSN at open and after every successful group commit. The HELP says `0 = no WAL`; it is also 0 for a freshly created, still-empty WAL. |
| `ankaa_wal_pending_bytes` | gauge | bytes | Bytes appended to the user-space buffer and not yet committed; reset to 0 after each commit. Bounded by the 256 KiB group-commit threshold plus one frame under normal operation. |
| `ankaa_wal_commits_total` | counter | commits | Group commits performed (write + `fdatasync`). The owner commits every 10 ms when there is pending data, or sooner when the buffer crosses 256 KiB. |
| `ankaa_wal_dropped_total` | counter | frames | Frames the WAL did not accept. Ingest waits for the log when its queue is full (backpressure), so this only moves when the log does not take a frame within 2 s (stalled or failing disk) or the writer has stopped. Such a frame is dropped entirely, **not ingested**: tables and log always agree. Expected 0. |
| `ankaa_wal_bytes` | gauge | bytes | Size of the WAL segments on disk, refreshed every 5 s. Above `storage.wal_budget_mib` the partitions holding the oldest unpersisted rows are sealed and persisted so the log can be truncated. With order-book synchronisation on, the log is kept back to the last feed (re)connection (see `docs/deployment.md`, disk layout). |
| `ankaa_last_tick_ns` | gauge | ns | Time the last snapshot tick took in the ingest loop: publishing new rows to queries, handing chunks to the encode threads, checkpoints. Nothing is ingested while it runs. |
| `ankaa_tick_ns_total` | counter | ns | Sum of the above. `rate(ankaa_tick_ns_total) / 1e9` is the share of the ingest loop spent in ticks (kept at or below one part in nine by spacing ticks out under load); divided by `ankaa_snapshots_total` it is the mean tick. |
| `ankaa_ingest_workers` | gauge | threads | Threads mapping messages to rows (`storage.ingest_workers`); 0 when the ingest loop maps itself. |
| `ankaa_ingest_map_jobs` | gauge | jobs | Runs of logged frames out on the mapping workers and not yet applied (at most two per worker). At its maximum all the time: mapping is the bottleneck, more workers help; near zero under load: the ingest loop or the sources are. |
| `ankaa_ingest_map_jobs_total` | counter | jobs | Runs handed to the mapping workers. |
| `ankaa_wal_oversized_total` | counter | frames | Frames larger than a WAL record may be (64 MiB), dropped before ingest so that memory and log agree. Expected 0. |
| `ankaa_wal_errors_total` | counter | errors | Failed `append` (payload over 64 MiB) or failed `commit` (I/O error, disk full). Each is logged at `warn`. Expected 0. |
| `ankaa_wal_recovered_frames` | gauge | frames | Frames replayed from the WAL at start. Constant for the life of the process. |
| `ankaa_tier_sealed_for_age_total` | counter | chunks | Open chunks sealed (and then encoded) because their first row was older than `storage.max_open_age_s`. |
| `ankaa_tier_sealed_for_memory_total` | counter | chunks | Open chunks sealed to keep uncompressed rows within `storage.hot_budget_mib`. A steady climb under load means the budget is smaller than the open rows of the busy partitions. |
| `ankaa_wal_truncated_segments_total` | counter | segments | WAL segments deleted after a checkpoint (every sealed chunk in a segment, LSNs below the oldest open chunk). Only whole 256 MiB segments are removed, so short runs show 0. |

Good looks like: `rate(ankaa_wal_durable_lsn[1m]) > 0` whenever `rate(ankaa_frames_total[1m]) > 0`
(the LSN advances one per frame), `ankaa_wal_pending_bytes` sawtoothing below ~300 KiB (up to 8 MiB while the disk is the bottleneck: commits batch what queued up during the previous `fdatasync`),
`ankaa_wal_dropped_total` and `ankaa_wal_errors_total` flat at 0. A stuck LSN with rising pending bytes means
`fdatasync` is not returning (storage stall); a stuck LSN with flat pending bytes and rising errors means writes fail.

## Storage

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `ankaa_rows` | gauge | rows | Rows accepted across `trades`, `book_deltas` and `quotes` (sum of partition statistics). Copied on the snapshot tick. |
| `ankaa_sealed_chunks` | gauge | chunks | Sealed (immutable) chunks across the three tables. A partition seals its open chunk at 65,536 rows and at shutdown. |
| `ankaa_snapshots_total` | counter | swaps | Query-view swaps performed (one per `storage.snapshot_ms` tick). Its rate should be `1000 / snapshot_ms` per second; a lower rate means the ingest loop is starved. |
| `ankaa_tier_chunks{tier="hot|warm|cold"}` | gauge | chunks | Sealed chunks per storage tier, set on every snapshot tick by the tiering step. |
| `ankaa_tier_bytes{tier="hot|warm|cold"}` | gauge | bytes | Bytes per tier (plain for hot, encoded size for warm, file size for cold). |
| `ankaa_tier_moves_total{to="warm|cold"}` | counter | chunks | Chunk moves hot→warm and warm→cold since start. |
| `ankaa_tier_encode_jobs` | gauge | chunks | Chunks out on the encode threads (`storage.tier_workers`): being compressed, written as a segment, or merged. Stuck at twice the number of threads while hot bytes grow: the threads do not keep up. |
| `ankaa_tier_errors_total` | counter | failures | Encodes, segment writes and merges that failed; the chunk stays as it was and the log says why (a full disk, usually). Alert on any increase. |
| `ankaa_tier_compactions_total` | counter | merges | Runs of small chunks of a partition merged into one chunk (`storage.compact_small_chunks`). |
| `ankaa_tier_compacted_chunks_total` | counter | chunks | Small chunks replaced by those merges. Divided by the merges: the average run length. |
| `ankaa_segment_cache_bytes` | gauge | bytes | Cold segment files held in memory by the segment cache (`storage.segment_cache_mib`), compressed as on disk. Updated every snapshot tick. |
| `ankaa_segment_cache_reads_total{from="cache|file"}` | counter | reads | Reads of a cold chunk answered from the segment cache, and reads that opened the segment file. A low share of `cache` under steady queries means the queried data is larger than the cache. |
| `ankaa_checkpoints_total` | counter | checkpoints | Checkpoints that produced a WAL truncation request. |

Until tiering lands, `ankaa_sealed_chunks` is the only occupancy figure. Memory per sealed chunk is roughly
rows × row width (`trades`: 8+4+8+8+1+8 = 37 B/row before alignment; `book_deltas`: 45 B; `quotes`: 52 B), and the
open chunk of every partition is copied on each query-snapshot swap.

## Queries (Arrow Flight)

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `ankaa_replication_received_lsn` | gauge | LSN | Standby only: highest primary WAL LSN received. 0 on a primary. |
| `ankaa_replication_connected` | gauge | 0/1 | Standby only: 1 while the WAL stream from the primary is open. Alert when 0 for more than a minute. |
| `ankaa_replication_connects_total` | counter | connections | Standby only: connections to the primary; a rising rate means an unstable link. |
| `ankaa_replication_applied_lsn` | gauge | LSN | Standby only: highest primary WAL LSN applied to the tables and appended to the local WAL (same LSN on both sides). Lag in records = primary `ankaa_wal_durable_lsn` − this. |
| `ankaa_replication_last_receive_age_seconds` | gauge | seconds | Standby only: time since the last batch arrived from the primary (0 before the first batch and on a primary). Grows on an idle primary too: alert on it only together with a non-zero lag. |
| `ankaa_replication_wal_streams` | gauge | streams | WAL streams this server is serving to standbys right now (bounded by `replication.max_standbys`). |
| `ankaa_replication_wal_refused_total` | counter | requests | WAL stream requests refused: wrong token, stream limit reached. Should stay flat; a rise is a misconfigured standby or a probe. |
| `ankaa_user_rows_total` | counter | rows | Generic sources (`[[sources]]`): messages mapped into rows. |
| `ankaa_user_rejected_total` | counter | messages | Generic sources: messages that produced no row: the mapper rejected them (bad JSON, CSV or line protocol, missing field, bad number, dictionary full, timestamp more than a day ahead) or the table did (older than the newest row of the partition). Should stay flat. |
| `ankaa_source_oversized_total` | counter | messages | Generic sources: messages larger than the source's `max_message_bytes`, dropped before parsing. Process-wide, live. |
| `ankaa_source_positions_saved_total` | counter | positions | File and Kafka read positions made permanent (written to `sources.json` after the data before them became durable). Flat while a Kafka source is delivering means positions are not being saved: check `ankaa_wal_durable_lsn`. |
| `ankaa_retention_expired_rows_total` | counter | rows | Rows dropped because they fell out of their table's retention window. |
| `ankaa_retention_expired_chunks_total` | counter | chunks | Sealed chunks dropped by retention. |
| `ankaa_retention_segments_deleted_total` | counter | files | Segment files deleted by retention, once no query held them. Lags `..._expired_chunks_total` while long queries run. |
| `ankaa_kafka_offset_resets_total` | counter | partitions | Kafka partitions whose saved offset was no longer retained by the broker, so the source jumped to the earliest retained offset. Every increment is a gap in the ingested data (broker retention shorter than the outage). |
| `ankaa_dictionary_entries` | gauge | values | Distinct values across the `dict` columns of user tables (each value of a `partition_by` column is a partition). Compare with `max_dict_entries`. |
| `ankaa_cq_late_rows_total` | counter | rows | Continuous queries: rows older than the open bucket/window of their key, dropped (no retraction). Non-zero means the width is too small for the feed's disorder. |
| `ankaa_cq_rejected_rows_total` | counter | rows | Continuous queries: result rows their output table refused. A defect if non-zero (the first one is logged with the query name). |
| `ankaa_queries_total` | counter | plans | SQL statements planned: one per `GetFlightInfo`, `GetSchema` and `DoGet`. A generic Flight client's round trip (`GetFlightInfo` then `DoGet`) counts 2; ANKAA's own clients (`ankaad query`, the Python package) send one `DoGet` with the SQL text as ticket and count 1. |
| `ankaa_query_errors_total` | counter | errors | Statements rejected before planning (syntax error, unknown table, not read-only: DDL/DML/`SET`), failures at `execute_stream`, and batch-level errors during streaming. A single failing query may count once at planning and once more during execution. |
| `ankaa_query_unauthenticated_total` | counter | calls | Flight calls refused for a missing or wrong bearer token (constant-time compare). Only incremented when a token is configured. Not incremented for `/metrics` 401s. |
| `ankaa_query_rows_total` | counter | rows | Rows in record batches streamed to clients, counted as batches leave the executor. |
| `ankaa_query_do_get_total` | counter | streams | `DoGet` streams that reached completion or were dropped. |
| `ankaa_query_do_get_ns_total` | counter | ns | Summed wall time of those streams. |
| `ankaa_query_last_do_get_ns` | gauge | ns | Wall time of the most recent one. |

Good looks like: `ankaa_query_unauthenticated_total` flat at 0 in a closed network; a rise means a misconfigured
client or a probe. `ankaa_query_errors_total` grows with user typos; alert on rate, not on existence.

## Recommended alert rules

The rules below are the content of `deploy/prometheus/alerts.yml`. Thresholds are placeholders to tune; durations
are chosen to ride out one reconnect or one slow REST call. Labels assume `job="ankaad"`.

```yaml
groups:
  - name: ankaa-feed
    rules:
      - alert: AnkaaFeedStalled
        expr: rate(ankaa_frames_total{job="ankaad"}[2m]) == 0 and ankaa_uptime_seconds{job="ankaad"} > 120
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "ankaad {{ $labels.instance }} receives no frames"
          description: "No frames for 2 minutes after warm-up. Check the source log lines (connecting / connect failed) and the exchange status."

      - alert: AnkaaFeedGaps
        expr: increase(ankaa_feed_gaps_total{job="ankaad"}[10m]) > 0
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "Depth sequence gaps on {{ $labels.instance }}"
          description: "{{ $value }} sequence gaps in 10 minutes. Each gap triggers a book resync; persistent gaps mean the host or network drops WebSocket frames."

      - alert: AnkaaBookResyncs
        expr: increase(ankaa_book_resyncs_total{job="ankaad"}[10m]) > 3
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "Order books resynchronising repeatedly on {{ $labels.instance }}"
          description: "{{ $value }} resync requests in 10 minutes. If gaps are flat, the REST snapshot is arriving too old for the buffered diffs (slow REST or rate limiting)."

      - alert: AnkaaReconnectStorm
        expr: increase(ankaa_source_connects_total{job="ankaad"}[15m]) > 3
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "WebSocket reconnecting repeatedly on {{ $labels.instance }}"
          description: "{{ $value }} connections in 15 minutes. Every reconnect resets sequence tracking and refetches every depth snapshot."

      - alert: AnkaaSourceChannelDrops
        expr: increase(ankaa_source_dropped_full_total{job="ankaad"}[5m]) > 0
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "Frames dropped at the source channel on {{ $labels.instance }}"
          description: "The ingest loop fell behind the feed and frames were discarded (data loss)."

      - alert: AnkaaBadFrames
        expr: rate(ankaa_frames_bad_total{job="ankaad"}[5m]) > 0.1
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Decoder rejecting frames on {{ $labels.instance }}"
          description: "More than one rejected frame per 10 s for 5 minutes: a feed format change or a decoder bug. See the 'frame rejected' warnings."

      - alert: AnkaaOutOfOrderDrops
        expr: increase(ankaa_dropped_rows_total{job="ankaad"}[10m]) > 0
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "Appenders dropping rows on {{ $labels.instance }}"
          description: "Rows arrived with a timestamp below the partition watermark (or failed type checks). The feed delivered out of order within a symbol; the rows are not stored."

  - name: ankaa-latency
    rules:
      - alert: AnkaaIngestLatencyP99High
        expr: |
          ankaa_ingest_latency_ns{job="ankaad",quantile="0.99"} > 50e6
          and ankaa_ingest_latency_ns{job="ankaad",quantile="0.99"} < 1e9
          and ankaa_ingest_latency_ns_count{job="ankaad"} > 10 * ankaa_wal_recovered_frames{job="ankaad"} + 1000
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "Ingest p99 above 50 ms on {{ $labels.instance }} (lifetime quantile, software clock)"
          description: "Receive-to-watermark p99 is {{ $value | humanize }} ns. The summary is cumulative since start; the guards skip the overflow sentinel and the WAL-recovery warm-up. Tune the threshold on your hardware."

      - alert: AnkaaIngestLatencyRecentMeanHigh
        expr: |
          rate(ankaa_ingest_latency_ns_sum{job="ankaad"}[5m])
            / rate(ankaa_ingest_latency_ns_count{job="ankaad"}[5m]) > 20e6
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Recent mean ingest latency above 20 ms on {{ $labels.instance }}"
          description: "5-minute mean of receive-to-watermark latency is {{ $value | humanize }} ns. This is the windowed signal; the summary quantiles are lifetime."

  - name: ankaa-wal
    rules:
      - alert: AnkaaWalErrors
        expr: increase(ankaa_wal_errors_total{job="ankaad"}[5m]) > 0
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "WAL append/commit errors on {{ $labels.instance }}"
          description: "{{ $value }} WAL errors in 5 minutes. Check free space and the 'wal commit failed' / 'wal append failed' warnings. Frames since the last good commit are not durable."

      - alert: AnkaaWalQueueDrops
        expr: increase(ankaa_wal_dropped_total{job="ankaad"}[5m]) > 0
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "Frames refused by the WAL on {{ $labels.instance }}"
          description: "The log did not accept frames within its 2 s wait (fdatasync stalls, full or failing disk). Those frames were dropped, not ingested: the data is lost for real-time feeds, and file/Kafka sources re-read it after the next restart (their positions are frozen until then)."

      - alert: AnkaaWalStalled
        expr: |
          rate(ankaa_wal_durable_lsn{job="ankaad"}[2m]) == 0
          and rate(ankaa_frames_total{job="ankaad"}[2m]) > 0
          and ankaa_wal_durable_lsn{job="ankaad"} > 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "WAL durable LSN not advancing on {{ $labels.instance }}"
          description: "Frames arrive but nothing becomes durable. Storage stall or I/O errors."

      - alert: AnkaaWalPendingBytesHigh
        expr: ankaa_wal_pending_bytes{job="ankaad"} > 4 * 1024 * 1024
        for: 1m
        labels: {severity: warning}
        annotations:
          summary: "WAL pending bytes above 4 MiB on {{ $labels.instance }}"
          description: "Normal operation commits at 256 KiB or every 10 ms; a buffer this large means commits are failing or very slow."

  - name: ankaa-query
    rules:
      - alert: AnkaaQueryErrorRate
        expr: |
          rate(ankaa_query_errors_total{job="ankaad"}[5m])
            / clamp_min(rate(ankaa_queries_total{job="ankaad"}[5m]), 1e-9) > 0.5
          and rate(ankaa_queries_total{job="ankaad"}[5m]) > 0.05
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "More than half of SQL statements fail on {{ $labels.instance }}"
          description: "Either a client is sending invalid or non-read-only SQL, or a table/function changed."

      - alert: AnkaaUnauthenticatedCalls
        expr: increase(ankaa_query_unauthenticated_total{job="ankaad"}[5m]) > 10
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "Flight calls with a bad bearer token on {{ $labels.instance }}"
          description: "{{ $value }} refused calls in 5 minutes: a misconfigured client or an unauthorised probe. Check who can reach the Flight port."

  - name: ankaa-process
    rules:
      - alert: AnkaaDown
        expr: up{job="ankaad"} == 0
        for: 1m
        labels: {severity: critical}
        annotations:
          summary: "ankaad metrics endpoint unreachable on {{ $labels.instance }}"
          description: "Scrape failed for 1 minute. With the systemd unit, journalctl -u ankaad shows the exit reason (a panic aborts the process and systemd restarts it)."

      - alert: AnkaaRestarted
        expr: ankaa_uptime_seconds{job="ankaad"} < 300 and changes(ankaa_wal_recovered_frames{job="ankaad"}[10m]) > 0
        for: 0m
        labels: {severity: info}
        annotations:
          summary: "ankaad restarted on {{ $labels.instance }}"
          description: "Uptime under 5 minutes and the recovered-frame count changed: a restart with WAL recovery happened."

      - alert: AnkaaSnapshotTickStarved
        expr: rate(ankaa_snapshots_total{job="ankaad"}[2m]) < 2
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Query snapshots swapping slower than configured on {{ $labels.instance }}"
          description: "Fewer than 2 swaps/s (default is 4/s at snapshot_ms = 250). The ingest loop is saturated or blocked; queries see stale data. Adjust the threshold if you changed storage.snapshot_ms."
```

## Scraping `/metrics`

The endpoint is a minimal HTTP/1.1 responder: one request per connection, a 2 s read timeout, `GET /metrics` only,
`Connection: close`. It accepts the same bearer token as Flight when one is configured (`Authorization: Bearer
<token>`). Give Prometheus the token through `authorization.credentials_file` (see `deploy/prometheus/prometheus.yml`)
and keep the scrape interval at 5-15 s; the render is a few kilobytes.

The token check on `/metrics` is a plain suffix match on the header line, not the constant-time comparison used by
Flight. Keep the endpoint on loopback or a management interface regardless of the token.
