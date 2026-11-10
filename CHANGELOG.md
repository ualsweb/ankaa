# Changelog

All notable changes are recorded here. The project is pre-alpha; there are no releases yet.

## 0.1.0 — first public release
- A general telemetry base, not only a market-data engine:
  - **Missing values.** Columns may be declared `nullable`; a missing JSON field, a JSON `null` or an empty CSV
    field is stored as SQL `NULL` (a validity bitmap next to the column) and kept through the WAL, the warm and cold
    tiers and recovery. Aggregates, statistics shortcuts, table functions and continuous queries skip it.
  - **Late data.** `max_lateness_ms` per table inserts readings that arrive out of order at their place in time
    instead of dropping them; continuous queries run in event time with the same window and rebuild to identical
    results after a crash.
  - **Retention.** `retention = "90d"` per table (`storage.market_retention` for the market tables), measured from
    the newest reading; whole chunks are dropped from every tier and their segment files deleted once no query holds
    them. New metrics `ankaa_retention_*`.
  - **MQTT source** (`kind = "mqtt"`): QoS 1 with a persistent session, acknowledged after hand-off to the engine,
    `$topic` / `$topic.N` selectors to take plant, line or machine from the topic. Tested against Mosquitto in CI.
  - **Many devices.** The schema is shared rather than copied per partition and chunk; a scale test with 100,000
    devices in one table is in `crates/ankaa-store/tests/scale.rs`.
- Project website under `site/`, published by GitHub Pages (`.github/workflows/pages.yml`).
- Launch-readiness pass: restarts no longer lose, repeat or rename data. User-table dictionaries are persisted with
  the segments; file and Kafka sources resume from durable positions (`sources.json`, at least once); source ids are
  stable across config edits; continuous-query tables are rebuilt at start instead of being persisted (same results
  after a restart or crash).
- Replication hardened: standbys keep the primary's LSNs and resume at the end of their own log; truncated or
  diverged logs stop a standby with a clear error; the primary serves only durable records, incrementally.
  **Breaking:** serving the WAL is now opt-in (`replication.serve_wal = true`) and can require its own token
  (`replication.token_env`); `replication.max_standbys` caps streams.
- Queries: chunk pruning on dictionary-key predicates, partitioned lazy scans, and a query deadline that also fires
  when a client stops reading.
- Generic ingest: caps on dictionary size, key length and message size; JSON floats are rounded instead of rejected;
  strict RFC 3339; quoted CSV; a timestamp more than a day ahead is refused; every column must be mapped
  (**breaking**: configurations with unmapped columns or a Kafka `group` are refused at start); `feed.enabled = false`
  for servers fed only by `[[sources]]`; Kafka `start = "earliest"`; lossless backpressure for file and Kafka.
- Continuous queries: the key must be a dictionary column, OHLCV needs an integer or fixed-point value, keyed ASOF
  needs tables that share a dictionary (all checked at start); bounded output tables; the live table names its key
  column like the closed table.
- Operations: new metrics (`ankaa_replication_applied_lsn`, `…_last_receive_age_seconds`, `…_wal_streams`,
  `ankaa_source_oversized_total`, `ankaa_kafka_offset_resets_total`, `ankaa_source_positions_saved_total`,
  `ankaa_dictionary_entries`, `ankaa_cq_rejected_rows_total`, `ankaa_book_crossed_messages_total`,
  `ankaa_book_stale_messages_total`, `ankaa_wal_oversized_total`), live channel-drop counter, alert groups for sources
  and replication, hardened `/metrics` and Postgres listeners, `.csv.gz` replay, tolerant capture decoding.
- Query path, after the first runs on Arm server hardware: scans hand hot chunks to Arrow in place (no column copy;
  batches account for their own bytes) and scan partitions keep storage order. The table functions work on those
  batches where they lie instead of copying a table into one buffer: `ohlcv` buckets run by run, `percentiles`
  feeds its sketches batch by batch, and `asof_join` indexes the right side per key and joins each run of the left
  side by binary search, reading and building only the columns a query uses (**changed:** its output is no longer in
  global time order, add `ORDER BY`). Input that is not in stored order, or whose keys alternate row by row, takes a
  second path that groups and sorts per key; both give the same result. Scans report row counts and key ranges
  to the planner, so a join with a per-key aggregate builds its hash table on the small side and a bare
  `count(*)` needs no scan. The quantile sketch checks its bucket budget
  in constant time (it recounted every bucket on every value) and now holds it as a hard bound. Flight, Postgres and
  `/metrics` sockets set `TCP_NODELAY`. NEON `sum_i64` uses four accumulators. `ankaad query` reports the time to
  the last batch, not including printing. `ankaa-capture` refuses a malformed stream list at start instead of
  retrying it forever.
- Native execution, after profiling on Neoverse V2 showed that queries never reached the SIMD kernels:
  - `DenseAggExec` runs aggregates grouped by dictionary-id or byte columns (and aggregates without `GROUP BY`)
    without hashing, with DataFusion's own accumulators; over a chunk table it answers `count`/`min`/`max` from chunk
    statistics and `sum` with one SIMD pass where a chunk's keys are constant. Installed by `ankaa_sql::session`.
    It also groups by `date_bin(interval, ts)`, alone or with other keys, by arithmetic on the timestamp.
    `ANKAA_DENSE_AGGREGATION=off` turns the operator off.
  - Pre-aggregates: sealed chunks keep min, max and sum per 1,024-row block of their integer columns, and
    segments carry them per column (older segments are read as before). Range aggregates read only the partial
    blocks at the ends of the range, and whole encoded chunks are not decoded.
  - `rolling(table, window_ns, step_ns [, key])`: bars over a trailing window at every step, built from per-step
    bars with a sliding queue (`ankaa_ts::Sliding`), in time independent of the window length.
  - Continuous query kind `rolling` (`step_ns`): the same windows maintained as rows arrive
    (`ankaa_ts::IncrementalRolling`), with the live window in `cq_<name>_live`.
  - Scans: a comparison of the timestamp with a literal is an exact filter (not evaluated again per row); scan
    partitions are in time order and say so, `ORDER BY ts DESC` is a reverse scan, and `... ORDER BY ts [DESC]
    LIMIT n` stops early, also below a filter.
  - `vwap(price, size)` aggregate with exact integer sums.
  - Aggregating before a join (`EagerAggregation`, a logical rule), a merge of sorted partitions on integer keys
    (`OrderedMergeExec`), chunks that record (time, sequence) order, keys constant per chunk skipped row by row,
    one division per bucket in `ohlcv`/`rolling`, faster quantile-sketch increments and order checks (from
    profiles taken with `perf` on Neoverse V2). `ankaad replay --quiet` prints timings only.
  - Plan cache (`ankaa_sql::PlanCache`) in the Flight server and `ankaad replay`: a repeated statement is not
    parsed and optimised again; plans with clock functions are never kept and catalog changes empty it.
  - SIMD: the choice between NEON and SVE2 is made per kernel. The SVE2 64-bit `sum` and `min/max` are unrolled with
    four accumulators; `min_max_i64` now runs on SVE2 wherever the CPU has it (NEON has no 64-bit integer min/max).
    The NEON count kernels use four accumulators. The server build includes the SVE2 kernels.
  - Live snapshots copy only the rows added since the previous snapshot (geometrically merged pieces) instead of
    every open chunk.
  - `percentiles` feeds its sketches run by run through `UddSketch::add_slice`, which computes a value's bucket once
    per recently seen value; the result is bit-identical to adding values one by one.
  - `ankaad` uses mimalloc by default (`--no-default-features` for the system allocator).
  - `ankaad query` and the Python client send one `DoGet` per query (the SQL text is the ticket) instead of
    `GetFlightInfo` + `DoGet`; `ankaad replay` reports planning and execution time separately.
- Late data: `[[tables]] max_lateness_ms` accepts readings up to that much older than the newest of their
  partition and inserts them at their place in time (`ankaa_late_rows_total`); continuous queries close
  buckets in event time with the same bound, so their results do not depend on arrival order and survive
  restarts unchanged (end-to-end test with out-of-order and too-late readings, a crash and a lost WAL).
- Supply chain: GitHub Actions pinned to commit SHAs with read-only permissions; `cargo deny` on every lockfile;
  fuzz and bench crates built in CI; fuzz target `generic_map`.
- Durability: ingest waits for the WAL (no frame is ingested without being logged); adaptive group commit; LSNs
  are never reused after truncation or a wiped log; rows of a new partition carry their LSN; order-book rows stay
  replayable (the log is kept back to the feed connection they depend on); `storage.wal_budget_mib` bounds the log
  on servers with quiet partitions; unusable segments stop the start. **Breaking:** `ankaa_wal_dropped_total` now
  counts frames dropped (not ingested) after a 2 s wait, and a full WAL volume stalls ingest instead of continuing
  in memory.
- Kafka: time-ordered merge across partitions, new partitions picked up, fetch timeouts; positions reported on
  shutdown; source ids recorded in the log.
- Security: lazy, memory-accounted table functions; no query spill to disk; `server.max_concurrent_queries`;
  Postgres `SET` isolation and fail-closed gate; **breaking:** a Flight listener beyond loopback needs TLS or
  `server.allow_plaintext = true`, and tokens of at least 16 characters; stream names restricted to
  `[A-Za-z0-9@_]`; `max_message_bytes` at most 16 MiB; default `max_dict_entries` 100,000; continuous-query names
  must not end in `_live`, ASOF needs a key.
- Verification round: standbys ignore the LSN floor; owed checkpoints are not lost; Postgres accepts transaction
  control and housekeeping statements; bounded shutdown drain; order books are re-based when the WAL outgrows its
  budget; a source message the WAL cannot take stops the server; table functions are lazy at physical planning;
  marked WAL numbering jumps.
- `docs/launch-checklist.md`, `scripts/first-light.sh`, `scripts/soak.sh`.
- Workspace of 15 crates (incl. `ankaa-tier`, `ankaa-pg`, `ankaa-wire`) and the `ankaad` CLI/server.
- SIMD: scalar, NEON and SVE2 (`global_asm!`) backends with differential tests at VL 128/256/512.
- Store: chunks, zone maps, watermarks, tables/partitions with snapshots.
- SQL: DataFusion provider with time-range and projection pushdown.
- Ingest: decoder trait, fixed-point parsing, Binance JSON decoder, `.ankc` capture, live WebSocket source.
- WAL with CRC32C, group commit, torn-tail repair; dm-flakey power-loss rig (1000/1000).
- Operators: streaming ASOF join, OHLCV buckets, L2 order book.
- Arrow Flight server with bearer auth; Prometheus `/metrics`; `ankaad replay|serve|query`.
- Depth snapshot synchronisation (REST snapshots + aligned diffs, straddle rule, resync), capture snapshot frames.
- WAL wired into `serve` (owner thread, group commit, recovery through the decode path, LSN-aware replay skip).
- SQL table functions `asof_join`, `ohlcv`, `percentiles` (UDDSketch); `count(*)` over zero projected columns fixed.
- Binance SBE binary decoder; Databento DBN replay (trades, mbp-1/10 as snapshots); CSV/Parquet trade replay;
  generic length-prefixed framing with an example decoder; `replay --out-dir` CSV export.
- Storage tiers: `ankaa-tier` (Vortex warm encoding, cold segment files with metadata), hot→warm→cold moves with a
  time budget, checkpoints and WAL truncation, persisted symbol dictionary, restart restore verified live.
- TOML configuration (`ankaad serve --config`, `config print|check`) with validation and fail-fast resource checks;
  secrets via environment only; SIGTERM handling.
- TLS and mutual TLS on Flight (`ankaad query --tls-ca/--tls-cert/--tls-key`); constant-time `/metrics` token check.
- Experimental read-only PostgreSQL wire protocol (`ankaa-pg`, SCRAM-SHA-256).
- Full SVE2 backend: all ten kernels (filters, gathers, sums, min/max, counts) in VL-agnostic assembly, differential
  tests at VL 128/256/512, branchless gate on 22 symbols.
- `ankaa-wire` (experimental): Arm generic timer, PHC↔CPU least-squares calibration with residuals, `SO_TIMESTAMPING`
  UDP receiver; AF_XDP placeholder.
- Python package `ankaa` (PyO3 + Arrow C Data Interface, GIL released), notebook; metrics for queries, WAL, books,
  tiers and a fixed-point ingest-latency summary; Grafana dashboard, Prometheus rules, systemd unit, example config.
- Benchmark harness vs DuckDB/ClickHouse/QuestDB and a 31-check differential oracle (DuckDB, Polars).
- Beyond market data: user-defined tables (`[[tables]]`) fed by NATS, Kafka or files in JSON/CSV (`[[sources]]`,
  `ankaa-ingest::generic`, `ankaa-feeds::sources`); continuous queries (`[[continuous]]`: incremental OHLCV, quantiles,
  ASOF → `cq_*` tables); standby replication over Arrow Flight (`__wal__:<lsn>` ticket, `replication.standby_of`,
  `--standby-of`), with end-to-end tests of both; NATS and Kafka sources verified against real brokers (opt-in test +
  `brokers` CI job with service containers).
- README repositioned: real-time time-series engine for Arm, market data as the flagship, any feed via Kafka/NATS.
- Fuzz targets `config_toml`, `framing`, `binance_sbe`, `dbn_replay`; QA/security review fixes (empty-table table functions, Postgres read-only gate at plan level and per-connection SCRAM, release-mode SIMD asserts, bounded query memory and timeouts, record-aligned chunk sealing for exact recovery, WAL truncation after full checkpoints); docs: metrics semantics, deployment, temporal architecture, comparison method.
