# Architecture

ANKAA is one process: a server (`ankaad`) that takes messages from sources, stores them as columns in
partitioned tables behind a write-ahead log, compresses what is no longer being written, and answers SQL.
This page is the map; the detail of each part lives in the document named next to it.

```
 sources (one task each)           MQTT · Kafka · NATS · files · exchange WebSocket · a primary's log (standby)
        │  frames: raw message + receive time
        ▼
 ingest loop (sequencer) ──batch──▶ WAL thread: CRC32C records, group commit            docs/deployment.md
        │ runs of logged frames, numbered
        ▼
 mapping workers 1..N: message → rows (JSON, CSV, line protocol), no shared state       docs/rfc/0001
        │ mapped runs, back in log order
        ▼
 ingest loop: dictionary ids, counters ──▶ writers 1..S (writer = partition key mod S): stale check, append
        │                                        │
        │ snapshot tick (≥ every snapshot_ms)    ▼
        │                                tables: partitions, each an open chunk + sealed chunks
        ▼                                        │ sealed chunks
 query views (immutable chunk lists)             ▼
        ▲                                encode threads: Vortex (ALP, FastLanes, dictionary) → warm (memory)
        │                                        → segment files (cold) → compaction of small chunks
        │                                        → checkpoints cut the WAL                 docs/deployment.md
 Apache DataFusion session: scans with time and key pruning, dense aggregation,
 table functions, continuous-query tables                                               docs/temporal-architecture.md
        ▲
 Arrow Flight + Flight SQL (one port, TLS, bearer token) · PostgreSQL wire (read-only) · Python · /metrics
```

## The path of a reading

1. **Source.** Each `[[sources]]` entry (and the built-in market feed) is a task that turns what arrives into
   frames and puts them on one bounded channel. Sources acknowledge to their broker after hand-off and keep
   a read position that is saved once the frames are durable (`docs/generic-ingest.md`).
2. **Log before table.** The ingest loop takes up to 1,024 frames per wake-up and hands them to the WAL
   thread in one push; a frame the log refused is never applied (`ankaa-wal`, `bins/ankaad/src/wal_writer.rs`).
3. **Mapping** (RFC-0001 stage 1). Runs of frames go to mapping workers that parse and convert without
   touching shared state; values of `dict` columns they have not learned yet stay as names. Results are
   applied strictly in log order.
4. **Ids, then appends** (stage 2). The loop gives ids to new names (the only step whose result depends on the
   order across partitions) and hands the run to the writers; writer `key mod S` owns a partition, checks
   lateness against its watermark and appends. A run with a name never seen, a table with continuous queries,
   Sparkplug sources and recovery are applied by the loop row by row. Either way the tables, dictionaries
   and counters are what one thread would produce (property-tested).
5. **Tick.** No more often than `snapshot_ms`, and spaced by its own duration under load, the loop waits for
   the writers and publishes: names of dictionaries first, then a new immutable view of every table that
   changed (sealed chunks by reference, the newest rows of open chunks as small sealed copies), then tiering
   and checkpoints. Queries never see a message half applied.

## Storage

- **Table → partitions → chunks.** A table is partitioned by one `dict` column (or not at all). A partition
  has one open chunk and a list of sealed ones, in time order; a chunk holds at most 65,536 rows in typed
  columns (`i64` timestamps and fixed-point decimals, `u32` dictionary ids, `u8`, `f64`) with validity
  bitmaps for `NULL` (`docs/data-model.md`, `ankaa-store`).
- **Statistics.** Per chunk: time range, id range per `dict` column, and per 1,024-row block min, max and sum
  of integer columns. They are computed by the first reader that needs them. Scans prune on them through a
  flat index per chunk list; `count`, `min`, `max`, `sum` over whole blocks are answered from them.
- **Tiers.** Sealed chunks are compressed on encode threads (Vortex: ALP for floats, frame of reference and
  bit-packing, dictionary) into the warm tier (compressed, in memory) and written as segment files.
  Past `warm_budget_mib` the oldest are served from their files (cold) through a cache of recently read
  segments. Compressed chunks decode row ranges, so a time predicate costs the rows it selects.
- **Memory bound and compaction.** Uncompressed rows are bounded (`hot_budget_mib`, `max_open_age_s`), which
  seals chunks early when there are many partitions; a background step merges runs of small chunks of a
  partition into full ones, crash-safe (a merged segment names the segments it replaces).
- **Durability.** WAL records carry a CRC32C; commits are grouped; a checkpoint is the lowest LSN any
  partition still needs, and the log is cut below it. A start-up re-attaches segments and replays the rest
  of the log through the row-by-row path. Schema changes that only add nullable columns read old segments
  as the new schema.

## Queries

SQL is Apache DataFusion over ANKAA's own `TableProvider` (`ankaa-sql`):

- `dict` columns are dictionary-encoded strings whose keys are the stored ids (RFC-0002): predicates on names
  are translated to ids once and decided per chunk; grouping hashes ids.
- A dense aggregation operator replaces hash aggregation when the keys are small integers or time buckets;
  aggregates are pushed below joins on dictionary keys.
- Table functions: `asof_join`, `ohlcv`, `rolling`, `percentiles` (UDDSketch), `gapfill`, `latest`
  (`docs/temporal-architecture.md`). Continuous queries maintain the same results incrementally and appear
  as tables (`docs/continuous-queries.md`).
- SIMD kernels (`ankaa-simd`): scalar, NEON and vector-length-agnostic SVE2, chosen per kernel at start.

## Crates

Engine crates carry no connector or query-framework dependency: Arrow, DataFusion and Vortex appear only in
`ankaa-sql`, `ankaa-tier`, `ankaa-flight`, `ankaa-pg` and `ankaa-py`; MQTT, Kafka and NATS only in
`ankaa-feeds`.

| Crate | Role | `unsafe` |
|---|---|---|
| `ankaa-core` | configuration (TOML, validation), shared types | none |
| `ankaa-simd` | scalar / NEON / SVE2 kernels, selection vectors, runtime dispatch | audited (`docs/unsafe-policy.md`), differential tests at three vector lengths |
| `ankaa-store` | chunks, statistics, partitions, tables, the tier interface | forbidden |
| `ankaa-wal` | write-ahead log: CRC32C records, group commit, replay, truncation, tailing | forbidden |
| `ankaa-ingest` | message → rows: JSON scanner, CSV, line protocol, Sparkplug B, dictionaries, fixed-point parsing | forbidden |
| `ankaa-ts` | operators: ASOF join, time buckets, rolling windows, gap filling, order book, quantile sketches | forbidden |
| `ankaa-tier` | Vortex encoding of sealed chunks, segment files, segment cache | forbidden |
| `ankaa-sql` | DataFusion integration: table provider, scans, dense aggregation, table functions, names | forbidden |
| `ankaa-feeds` | sources (MQTT, Kafka, NATS, file), market-data decoders, `.ankc` captures | forbidden |
| `ankaa-flight` | Arrow Flight and Flight SQL server, log streaming for standbys | forbidden |
| `ankaa-pg` | read-only PostgreSQL wire protocol | forbidden |
| `ankaa-py` | Python package (PyO3, Arrow C Data Interface) | forbidden |
| `ankaa-time`, `ankaa-wire`, `ankaa-ring` | clock calibration, hardware receive timestamps, SPSC ring (bare-metal path, experimental) | audited where present |
| `ankaad` | the server and CLI: ingest loop, mapping and writer pools, tiering, continuous queries, replication, metrics | forbidden |

## Design rules

1. **Vector unit = 1,024 rows** (FastLanes); kernels take slices and produce selection vectors.
2. **Decimals are fixed-point `i64`** with a per-column scale: equality is exact, compression is integer.
3. **One writer per partition at a time; readers never block writers.** Query views are immutable.
4. **Log before table, log order is apply order.** Parallelism (mapping workers, writers) never changes what
   is stored: every parallel path is tested against the one-thread path.
5. **Nothing per row on the ingest path allocates, locks a shared structure or makes a system call.** What
   has a fixed cost per chunk is kept small by keeping chunks full (compaction).
6. **Every parser is a decoder behind a trait**, tested with fixtures, differential tests and fuzzing;
   malformed input is an error, never a panic.
7. **Vector-length-agnostic SVE2**, budgeted at 128-bit vectors: one binary for Graviton, Axion, Cobalt,
   Ampere and the Arm AGI CPU.
8. **Numbers only from real hardware**, with the script and the raw results (`docs/benchmarks/`,
   `docs/measurement-protocol.md`). Laptop numbers are labelled development signal.
