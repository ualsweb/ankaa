# Changelog

All notable changes are recorded here. The project is pre-alpha: interfaces may change between releases.

## Unreleased

- SIMD: the SVE2 kernels keep predicates out of the loop body: four full vectors per iteration with
  unpredicated loads, compares under an all-true predicate, one accumulator per vector, a single
  `whilelo` per block and a predicated tail. Counts merge the compare result with one predicated add
  into vector accumulators; filters govern the second compare by the first (no `and`) and count with
  independent `cntp`; the i64 range filter and count use one unsigned compare, `(x - lo) <= (hi - lo)`.
  On Neoverse V2 (Google Axion, 1,024 values) every SVE2 kernel measured before and after is 1.4–2.6× faster (of the count kernels, only `gt` was benchmarked; the other comparisons share its code), and the
  per-kernel choice at 128-bit vectors (`dispatch::SVE2_AT_128`) now takes the filters (215 against
  240 ns f32, 348 against 510 ns i64) and the gathers (91 against 159 ns f32, 211 against 238 ns i64)
  from SVE2, besides `min_max_i64`. Sums, float min/max and counts stay on NEON. Measurements and
  limits: `docs/benchmarks/2026-11-13-axion-sve2-loop-shape.md`. New differential tests: every length
  from 0 to 300, unsorted range bounds, i64 range edges.
- Fixed: a `.dbn` file whose header declares more metadata than the file holds is refused when it is opened
  (`dbn: the header declares … bytes of metadata`). Before, the reader reserved the declared length first, so
  an 11-byte damaged file made `ankaad replay` ask for 4 GiB. Found by the nightly `dbn_replay` fuzz run;
  a compressed file is held to 1 GiB of metadata. The `fuzz-smoke` workflow can now be started by hand.

## 0.2.0 — 2026-11-12

The telemetry release: more ways in (MQTT over TLS, Sparkplug B, line protocol, several readings per
message, Kafka with TLS and SASL), names instead of ids in SQL, Flight SQL, ingest on several cores, and
the storage and query changes that came out of profiling the TSBS workload against other engines
(`docs/benchmarks/2026-11-11-axion-telemetry-comparison.md`).

**Upgrading from 0.1.0.** The segment and log formats did not change (the new chunk metadata is optional). One change breaks
queries: `dict` columns are text in SQL (first entry below).

- **Breaking: `dict` columns (and `symbol`) are text in SQL** (RFC-0002,
  `docs/rfc/0002-dictionary-columns-as-names.md`). `WHERE machine = 'press03'`, `GROUP BY line` and results
  with names, without joining `dictionaries` or `symbols`. Clients receive ordinary strings (`Utf8` over Flight
  and Flight SQL, `text` over the Postgres protocol); inside the engine the column is `Dictionary(UInt32, Utf8)`. The engine still works on the ids: `=`, `<>`, `IN`, `NOT IN` against string
  literals are translated to ids and decided per chunk from its id range, grouping hashes ids, and every batch
  references one shared array of names per dictionary. Table functions and continuous-query tables return
  their key as names. A query that joined `dictionaries`/`symbols` on the id now stops with a cast
  error: drop the join, or set `server.dict_columns = "ids"` (`ankaad serve --dict-ids`) to keep numeric
  columns. `ankaad replay` and the Python capture session still show ids. Storage, log and segment formats are
  unchanged.
- MQTT over TLS: `mqtts://` sources with TLS 1.3/1.2 (rustls, aws-lc-rs), the broker checked against the
  public roots or only a private CA (`tls_ca`), and client certificates (`tls_cert`, `tls_key`); files
  checked at start. Tested against Mosquitto with TLS and mutual TLS in CI
  (`scripts/mqtt-tls-broker.sh`).
- Kafka over TLS, with SASL, for managed Kafka: `tls = true` on a Kafka source (same rustls stack and
  `tls_ca` / `tls_cert` / `tls_key` as MQTT, mutual TLS included) and `sasl_mechanism` (`plain`,
  `scram-sha-256`, `scram-sha-512`) with `sasl_username` and `sasl_password_env`: the password comes from
  the environment and is never logged or printed. SASL without TLS is refused beyond loopback unless the
  source sets `allow_plaintext = true`. `ankaad config check` and the server start now check the TLS
  files of every source (MQTT too) and name the source. A Kafka source that cannot connect says so every
  30 s and stops cleanly on shutdown. Tested against Apache Kafka 3.9 in Docker with TLS, mutual TLS and
  the three mechanisms (`scripts/kafka-tls-broker.sh`; the test is part of the brokers workflow).
  `OAUTHBEARER`, Kerberos and MSK IAM are not supported.
- Several readings per message: `rows = "<array path>"` stores one row per element (`$msg.<path>` reads
  the message around it); a bad element refuses only its row; at most 65,536 rows per message, new
  dictionary values of one message counted together against `max_dict_entries`.
- JSON is read by a single-pass, path-directed scanner instead of a document tree: mapped values are
  recorded as byte ranges, everything else skipped (vectorised quote search, bracket counting), nothing
  allocated per message. Checked against `serde_json` by a differential property test. Mapping cost
  per message 489 → 212 ns (laptop, criterion).
- Sparkplug B sources (`format = "sparkplug"`): one row per metric of every BIRTH and DATA message,
  `$metric`, `$value`, `$string`, `$timestamp`, `$datatype`, `$historical`, `$seq`, `$group`, `$node`,
  `$device`, `$type` selectors; datatypes, transient flags and aliases from the BIRTHs (DATA messages
  without a datatype are read with them), kept in the write-ahead log so restarts, crashes and standbys
  resolve aliases without a new BIRTH; opt-in rebirth requests (`sparkplug_rebirth`). Decoder checked
  against a reference protobuf codec and fuzzed (`sparkplug`). End to end on Google Axion: 505 k rows/s,
  1.1 µs of server CPU per row over MQTT.
- InfluxDB line protocol sources (`format = "line_protocol"`, on MQTT, Kafka, NATS and files): what
  Telegraf, the InfluxDB client libraries and most collectors write feeds a table, one row per line.
  Columns are filled by tag and field name (`map` only to read another key, `$measurement` or `$topic`),
  the first column takes the line's timestamp in `ts_unit` (nanoseconds by default), keys without a column
  are ignored, `measurement = "name"` keeps the lines of one measurement, a malformed line refuses only its
  row. Values are converted by the JSON rules (`12i`/`12u` are integers, tags and strings are text). The
  parser reads in place without allocating, is independent of the transport, is checked line by line
  against the parser of InfluxDB 3 (`influxdb-line-protocol`, a dev-dependency) on generated and damaged
  input, and fuzzed (`line_protocol`); the mapping workers take these sources. About 0.45 µs per Telegraf
  line of 3 tags and 10 fields (laptop, criterion). The value conversion shared by every format is now
  inlined into each format's loop: JSON, CSV and `rows` mapping measure 5–7% faster on the same laptop.
- Tables can gain nullable columns at the end, and a column can become nullable, without losing their
  history: segments written before are read with the added columns `NULL`, nothing is rewritten. Other
  column changes still stop the start with the files and the reason.
- `gapfill(table, column, width_ns [, key [, agg [, method [, start_ns, end_ns]]]])`: one row per bucket
  and key with empty buckets filled (`null`, `locf`, `linear`), seven aggregates; verified against
  DataFusion's own grouping.
- Ingest engine, from end-to-end profiles on Google Axion (`scripts/e2e-profile.sh`,
  `docs/benchmarks/2026-11-11-axion-e2e-ingest.md`): the ingest loop hands frames to the log thread in
  batches; one table lock per message instead of three per row; counters published once per batch;
  frames encoded straight into the log buffer; interrupt moderation of the ingest loop
  (`storage.ingest_coalesce_us`, default 200 µs, invisible to queries) and of the log thread (polls while
  busy, sleeps when idle); dictionaries hashed with foldhash. File ingest 0.68 → 3.2 M rows/s
  (3,350 → 850 ns CPU per row); MQTT 24.6 → 9.9 µs CPU per message.
- `loadgen --connections`, `--format sparkplug` and `e2e-profile.sh --publishers`, `$ANKAAD` for A/B runs.
- Parallel ingest, stage 1 (RFC-0001, `docs/rfc/0001-parallel-ingest.md`): JSON, CSV and line protocol messages are mapped
  to rows on `storage.ingest_workers` threads (default: cores − 2) that share nothing, and applied in log
  order by the ingest loop, so tables, dictionaries (values and ids), the dictionary cap, continuous queries
  and counters are exactly those of one thread (property test over random streams with 1–7 workers).
- Uncompressed rows are bounded: `storage.hot_budget_mib` (default 1 GiB) seals and encodes the partitions with
  the oldest open rows first, and `storage.max_open_age_s` (default 10 min) seals open chunks that wait too
  long. Before, a table with one partition per device kept every reading uncompressed (twice: the open chunk
  and its query view) until 65,536 had arrived for that device. Metrics `ankaa_tier_sealed_for_age_total`,
  `ankaa_tier_sealed_for_memory_total`.
- Mapping workers learn the dictionary ids the ingest loop gives to the values they leave pending and
  resolve those values themselves afterwards: the serial loop hashes only values it has never seen.
- Compression off the ingest loop: `storage.tier_workers` threads (default cores ÷ 4) encode sealed chunks
  and write their segments; the tick hands over every chunk that is due and swaps in the finished ones.
  Before, tiering had 5 % of one core (about 60 MB/s of rows), far below what ingest produces under load.
  A segment finished for a chunk that retention dropped meanwhile is deleted, never attached.
- Flight SQL on the Flight port, read-only, so ADBC drivers and the Flight SQL JDBC driver can connect. The
  existing service recognises Flight SQL commands (a protobuf `Any` in the descriptor, ticket or action body)
  and answers them; plain Flight (`ankaad query`, the Python package) is unchanged. Supported: statements,
  prepared statements without parameters, `GetSchema`, and the metadata calls `GetSqlInfo`, `GetCatalogs`,
  `GetDbSchemas`, `GetTables` (with or without table schemas) and `GetTableTypes`. Statements run through the
  same planning and execution as plain Flight, with the same read-only check, query timeout, cap on concurrent
  queries and memory pool; tickets and prepared-statement handles are the SQL text, so the server keeps
  nothing per client. Refused with a status: updates, bulk ingest, statement parameters, transactions,
  Substrait plans, `CancelQuery`, and the metadata calls for keys and type info.
- `Handshake` is implemented (it answered "unimplemented" before): it takes the token as a bearer header or
  as the password of Basic authentication with any user name, and answers with `authorization: Bearer
  <token>`. This is what ADBC and the JDBC driver use when given a user name and a password. `DoPut`,
  `DoAction` and `ListActions` now check the token before they answer.
- `latest(table [, key])`: the newest row of every key value (the last reading of every device). The cost
  follows the number of key values, not the rows of the table: a chunk that holds one key value is judged
  from its statistics, and the newest row of every sealed chunk is kept in its metadata (`last_row`, also in
  segment files), so on a table partitioned by the key no chunk is decoded at all.
- Compressed chunks decode row ranges (`EncodedChunk::decode_rows`): a query for an hour of a device no
  longer decodes the days its chunk holds, in the warm tier (constant-time slice of the compressed arrays)
  and in the cold tier (row range through the segment layout). `LIMIT` without a time range decodes the
  first rows only.
- Segment cache: `storage.segment_cache_mib` (default 256) keeps recently read cold segment files in
  memory, compressed as on disk and with their footer parsed, and their columns as they are used. A cold
  chunk read again is read like a warm one (a slice of the compressed column is decoded): measured on a
  26,000-row chunk, 4,320 rows of 2 columns went from 61 µs to 4.6 µs. Metrics `ankaa_segment_cache_bytes`,
  `ankaa_segment_cache_reads_total{from}`.
- Predicates on one `dict` column that are not a comparison with a literal (`LIKE`, `<`, `upper(host) = …`,
  regular expressions) are evaluated once over the names of the dictionary and answered by id, per chunk,
  like `=` and `IN`. Before, they ran row by row and a pattern was compiled for every batch.
- Scans prune on flat arrays of chunk statistics (a quarter of a small query went into asking 17,000
  chunks one by one); the tiering tick sums tier sizes once instead of three times.
- Compaction: runs of small sealed chunks of a partition are merged into full ones in the background
  (`storage.compact_small_chunks`, on by default). With many devices the memory bound seals chunks long
  before they are full: a TSBS load of 1,000 hosts left 16,937 chunks of about 1,500 rows where 1,000 would
  do, and every per-chunk cost showed in the profiles (planning, a decode call per column, a file each). A
  merged segment lists the segments it replaces; a start-up that finds both keeps the merged one, so a crash
  in between neither loses nor repeats a row.
- A table's view for queries is rebuilt only when the table changed (it was rebuilt every snapshot tick,
  a walk over every chunk), and `latest()` finds the newest rows on the pruning index.
- The part of `warm_budget_mib` the warm tier does not use is lent to the segment cache.
- Metrics for the threads added lately: `ankaa_ingest_workers`, `ankaa_ingest_map_jobs`,
  `ankaa_ingest_map_jobs_total` (mapping workers), `ankaa_tier_encode_jobs` and `ankaa_tier_errors_total`
  (encode threads; failed encodes and segment writes were only in the log).
- Ingest no longer pauses for the snapshot tick. Measured with 1,000 partitions under load, a tick took
  15 ms of the ingest loop every 50 ms: two `fsync`s to save source positions and the copy of new rows for
  queries. Positions are now written by a thread of their own, a table's view is built on several threads,
  the statistics of a chunk are computed by whoever first needs them (the encode thread or a query) instead
  of at every seal and snapshot, and under load the next tick waits eight times what the last one took.
  The tick is 3.5 ms; a load of 21-column rows went from 1.7 to 2.2 M rows/s on a laptop (development
  signal). New metrics `ankaa_last_tick_ns`, `ankaa_tick_ns_total`.
- Parallel ingest, stage 2 (RFC-0001): `storage.ingest_writers` threads append the rows of mapped runs to
  their partitions, each owning the partitions `key mod N`, while the ingest loop only gives dictionary ids
  and splits the rows. Runs are handed over without waiting (waiting after each one was measured and was a
  loss); the loop waits for the writers before a tick and before anything it applies row by row. A property
  test checks that 2, 3 and 7 writers store exactly what one thread stores. Partitions of a table are behind
  locks of their own (`Table::append_shared`). New metrics say where the ingest loop waits
  (`ankaa_ingest_wait_frames_ns_total`, `…_workers_…`, `…_coalesce_…`).
- Fixed: `GROUP BY <time bucket>, key … ORDER BY <time bucket>` could return rows out of order when the scan
  had several time-ordered partitions (a table partitioned by device). The planner drops the sort there
  because the standard aggregate keeps time order; the dense aggregation that replaces it did not. It now
  sorts its groups when the plan above relies on the order. Found by the factory demo's dashboard; the
  differential tests compared results without regard to row order, and one that checks order is added.
- Fixed: an MQTT source now flushes its acknowledgements when the server stops; before, the broker
  could hand the last messages to the next start again.

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
- Ways to run it without a Rust toolchain: a container image (`Dockerfile`, multi-arch release image on
  `ghcr.io` built from the release binaries), release binaries for Linux arm64/x86-64 and macOS arm64
  (`.github/workflows/release.yml`), and `examples/factory/`: one `docker compose up` for an MQTT plant,
  ANKAA and a Grafana dashboard over the Postgres protocol. Tutorial `docs/tutorials/uns-historian.md`;
  `docs/design-partners.md`.
- A server with no feed and no `[[sources]]` now starts (with a warning) and serves the data it has
  stored, instead of refusing the configuration.
- Fixed: the eager-aggregation rewrite returned `NULL` instead of `0` for a `count` over no rows and
  declared the column nullable (found by the Python tests on the first CI run).
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
