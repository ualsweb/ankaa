# Comparison method: ANKAA vs open-source tick engines

How the comparative numbers of this project are produced (ROADMAP gate G2, PRD N-05, measurement
protocol §6). The harness is `bench/compare/`; the correctness suite it depends on is `bench/oracle/`.

## Scope

- Engines: ANKAA (`ankaad`), QuestDB, ClickHouse, DuckDB. **Open source only.** No kdb+, no commercial
  systems, no numbers taken from vendor publications.
- Workload: the 20-query *tick set* in `bench/compare/queries/manifest.json` — time-range aggregates,
  GROUP BY symbol, latest per symbol, VWAP, OHLCV at 1 s and 5 s, ASOF trades → quotes (backward, with
  tolerance, forward), crossed quotes, spread statistics, percentiles, trades per second, L2-delta
  statistics, symbol join, filters. Row-level ASOF outputs are verified but not timed; their aggregated
  twins are.
- Data: real Binance spot market data recorded with `ankaa-capture` (`.ankc`), replayed by `ankaad` into
  `trades`, `quotes` (top of book reconstructed from L2), `book_deltas` and `symbols`, exported once and
  loaded identically everywhere. The bundled 20 s capture is a smoke test; a report uses a capture of hours.

Telemetry counterpart: `bench/telemetry/` runs the TSBS cpu-only use case (generated DevOps CPU metrics,
the 15 TSBS query types with fixed parameters) on ANKAA, InfluxDB 3 Core, TimescaleDB and QuestDB, with DuckDB as
the reference, under the same rules plus its own (load timing, one engine at a time, at-rest state):
see `bench/telemetry/README.md`.

## Fairness rules

1. **Same box.** All engines run on the same instance, one at a time, nothing else running. Numbers are
   published only from real Arm server hardware (Graviton4 class), never from laptops, laptop VMs or
   emulators (`docs/measurement-protocol.md` §7). `run.py` refuses to write outside `results/smoke/`
   on anything that is not aarch64 Linux.
2. **Same rows.** One export (`ankaad replay --out-dir`, CSV + Parquet with an explicit schema) feeds
   every engine; `prepare.py` checks row counts per table. Fixed-point `Int64` prices/sizes everywhere.
   Timestamps are nanoseconds except in QuestDB (microseconds, documented; no loss for millisecond stamps).
3. **Same semantics.** Each query is written once per dialect with the same ORDER BY, the same bucket
   alignment (left-closed, calendar-aligned), the same ASOF definition (backward: last `right.ts <=
   left.ts`; forward: first `right.ts >= left.ts`; tolerance drops the match), the same NULL handling for
   unmatched rows. Where an engine cannot express something it is reported `n/a` (QuestDB: forward ASOF),
   never replaced by a cheaper query. Where an engine lacks a type (QuestDB: 128-bit integer for
   `notional`) the manifest documents the approximation.
4. **Verified before timed.** Every engine's result is diffed against the DuckDB reference (exact for
   integers/timestamps, tolerance for floats, 1 % band for sketch percentiles) before it is timed; a
   mismatch is shown in the table and the time is declared not comparable. ANKAA itself must pass the
   oracle (`bench/oracle/run.sh`: DuckDB for SQL/ASOF/percentiles, Polars for buckets) at the same commit.
5. **Each engine tuned per its own documentation, never down.** Stock pinned images, schemas and the few
   settings used are committed (`docker-compose.yml`, `harness/engines.py`) with links in
   `bench/compare/README.md`. If a vendor-recommended setting is added for a run it is committed and
   named in the report. Query formulations that help another engine are welcome as pull requests.
6. **Cache policy: warm only.** All datasets fit in memory and every engine is given the data before
   timing; W warm-up executions are discarded, then N runs; median and p95 are reported, all raw times
   are in the JSON. Cold-cache (disk) behaviour is a different benchmark and is not claimed here.
7. **Native clients, stated scope.** QuestDB via PostgreSQL wire (or REST), ClickHouse via HTTP with Arrow
   output, DuckDB in-process, ANKAA via `ankaad replay` (engine-side time) or Arrow Flight
   (client-observed). The scopes are printed next to the table; the Flight path is the like-for-like
   comparison and becomes the default once `ankaad serve` can serve a capture.
8. **Everything needed to reproduce is committed.** Pinned image tags, Python requirements, Rust
   toolchain, exact commands (`bench/compare/README.md`), the dataset manifest (file sizes, sha256, row
   counts, time window), hardware and OS facts collected by `harness/sysinfo.py`, raw per-run times.

## Reporting

`bench/compare/report-template.md` is the skeleton: hardware, OS, versions, dataset, protocol, table,
observations, caveats. Reports live in `docs/benchmarks/`, raw results in `bench/compare/results/`.
A report states where ANKAA is slower as plainly as where it is faster.

## Known caveats

- ANKAA replay-path times exclude the client/transport; the other engines' times include their wire
  protocol and the Python client (Arrow IPC for ClickHouse, DB-API rows for QuestDB). For sub-millisecond
  queries this overhead is a large share of the number: compare orders of magnitude, not decimals, until
  the Flight path is used for ANKAA too.
- Python-side timing (`time.perf_counter`) has microsecond resolution and includes result materialisation;
  this is the same for every engine but favours small result sets.
- The bundled capture is one symbol and 20 s: GROUP BY and ASOF have a single partition. Reports must use
  a long capture and say how many symbols it holds.
- QuestDB's `approx_percentile` and ANKAA's UDDSketch are both approximate with different rank conventions;
  the harness accepts both within the documented band, so percentile timings compare an exact order
  statistic (DuckDB, ClickHouse) with two sketches.
- `OPTIMIZE TABLE ... FINAL` (ClickHouse) and the in-memory DuckDB tables represent a fully compacted,
  idle state; ingest-concurrent query performance is not measured here.
- Container engines run under Docker on the host kernel (no VM on Linux); the harness does not pin CPUs
  or NUMA nodes. State in the report if you did.
