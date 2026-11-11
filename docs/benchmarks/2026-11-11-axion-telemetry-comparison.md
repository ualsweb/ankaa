# Telemetry comparison and function-level profile on Google Axion — 2026-11-11

ANKAA against InfluxDB 3 Core, TimescaleDB and QuestDB on the TSBS `cpu-only` workload (the benchmark the
time-series engines publish themselves), on one Arm server VM, with every result checked against a
reference before it is timed. Then the profile that explains the numbers, function by function, and what
it changed in the engine during the day.

- **Host:** Google Cloud `c4a-standard-4` (Axion, Arm Neoverse V2, 4 vCPU, 16 GB), Ubuntu 24.04,
  `europe-southwest1-a`. Engines run one at a time on the same VM, client on the same VM.
- **Data:** TSBS `cpu-only`, 1,000 hosts, 3 days at 10 s: 25,920,000 rows, 10 tags, 10 float fields
  (generator pinned to `tsbs 8323e59c7402`, seed 123).
- **Engines:** `ankaad` at `5e5c758`,
  InfluxDB 3 Core 3.12.2, TimescaleDB 2.31.0 on PostgreSQL 17.11, QuestDB 10.1.0, each configured as its
  vendor documents for this workload (`bench/telemetry/README.md` lists every setting and why).
- **Method:** `bench/telemetry/run.py`: load (timed), the engine's own post-load step, disk and memory at
  rest, then each of the 15 TSBS query types: answer compared with DuckDB's on the same rows, 3 warm-up
  runs, 20 timed runs, client-observed time until the last row. Raw results:
  [`bench/telemetry/results/2026-11-11-gcp-c4a-standard-4.json`](../../bench/telemetry/results/2026-11-11-gcp-c4a-standard-4.json).

**Read this before quoting anything.** One VM size, four cores, one run, one workload with 1,000 series.
Client paths differ (Arrow Flight for ANKAA, the PostgreSQL protocol for TimescaleDB and QuestDB), and at
these sub-millisecond times the client is a large part of what is measured: an empty `SELECT 1` round trip
to ANKAA over Flight takes 0.32 ms. Nothing here says how the engines compare at 100,000 series, at 64
cores or under concurrent load; those runs are not done.

## Load and storage

| | ANKAA | TimescaleDB | QuestDB | InfluxDB 3 Core |
|---|---:|---:|---:|---:|
| Load, rows/s | **1,106,552** | 383,254 | 801,304 | did not complete, see below |
| Load, seconds | 23.4 | 67.6 | 32.3 | |
| Post-load step | compress, persist, cut the log: 262 s | columnstore conversion: 23.5 s | none | |
| Disk at rest, MiB | **426** | 1,311 | 5,841 | |
| Peak memory during the load, MiB | 5,125 (resident set) | 37 (anonymous; page cache apart) | 491 (anonymous) | |

- ANKAA's 426 MiB are 247 MiB of segments and 179 MiB of write-ahead log that the next checkpoint cuts
  further. The raw CSV is 4.6 GiB.
- **Memory is ANKAA's weak column.** It peaked at 5.1 GiB and stayed at 5.5 GiB resident at rest, for
  247 MiB of compressed data: the load keeps up to 1 GiB of uncompressed rows (`hot_budget_mib`), each
  open chunk has a second copy for queries, and the allocator does not give the peak back. This is not yet
  understood in detail and is the next thing to measure.
- ANKAA reaches its at-rest state (every row compressed and in a segment, one chunk per host, log cut)
  262 s after the load; it answers queries from the first row on, and those 262 s run in the background.
- **InfluxDB 3 Core did not complete**, in two separate runs: a query failed with `Resources exhausted` in
  its deduplication sort on this 16 GB VM before the harness could record its load. It is left out of
  the tables rather than tuned by us; the settings it ran with are in `bench/telemetry/README.md` and the
  full error is in the raw results.

## Queries

Median of 20 runs in milliseconds (p95 in brackets), every answer verified. TimescaleDB twice: as loaded
(rowstore) and after its columnstore conversion, which is how its documentation says to run it.

| TSBS query | ANKAA | TimescaleDB (columnstore) | TimescaleDB (rowstore) | QuestDB |
|---|---:|---:|---:|---:|
| single-groupby-1-1-1 (1 host, 1 h, 1 metric) | 0.74 (0.79) | **0.22** (0.66) | 0.29 (0.56) | 1.18 (2.50) |
| single-groupby-1-1-12 (12 h) | **0.75** (0.77) | 1.15 (1.68) | 2.33 (3.26) | 3.98 (6.94) |
| single-groupby-1-8-1 (8 hosts) | **0.74** (0.82) | 0.74 (1.26) | 1.52 (2.08) | 1.40 (2.43) |
| single-groupby-5-1-1 (5 metrics) | 0.78 (0.84) | **0.32** (0.69) | 0.41 (0.68) | 1.30 (1.81) |
| single-groupby-5-1-12 | **0.88** (0.90) | 1.87 (2.48) | 2.92 (3.94) | 3.44 (4.39) |
| single-groupby-5-8-1 | **0.88** (0.90) | 1.71 (2.28) | 1.80 (2.09) | 1.10 (1.40) |
| cpu-max-all-1 (10 metrics, 8 h) | **0.88** (0.94) | 1.08 (1.71) | 2.31 (2.36) | 1.63 (1.91) |
| cpu-max-all-8 | **1.01** (1.08) | 10.4 (11.2) | 15.7 (16.9) | 4.59 (5.34) |
| double-groupby-1 (all hosts, 12 h, by hour) | **14.6** (14.9) | 671 (678) | 1,052 (1,055) | 71.4 (90.8) |
| double-groupby-5 | **34.1** (34.5) | 1,081 (1,086) | 1,349 (1,353) | 93.2 (100) |
| double-groupby-all | **59.2** (59.6) | 1,568 (1,575) | 1,707 (1,718) | 138 (142) |
| high-cpu-1 (rows over a threshold, 1 host) | **0.83** (0.90) | 1.61 (2.21) | 4.07 (4.65) | 6.02 (7.03) |
| high-cpu-all (384,179 rows returned) | **58.2** (62.9) | 1,371 (1,390) | 1,823 (1,836) | 631 (634) |
| groupby-orderby-limit | 8.77 (8.94) | 44.1 (44.8) | **4.13** (4.41) | 9.44 (11.4) |
| lastpoint (last reading of every host) | **0.80** (0.85) | 387 (387) | 30.6 (31.1) | 2.28 (2.73) |

ANKAA is fastest on 12 of the 15 and level with TimescaleDB on one more. It loses three:

- **Two single-host, one-hour queries** (0.74 and 0.78 ms against 0.22 and 0.32 ms). TimescaleDB answers
  them from an index in a few hundred microseconds. ANKAA's time is almost all fixed cost: 0.32 ms of
  Flight round trip plus about 0.3 ms of query planning (see the profile); the rows themselves take
  30 µs. This is a floor of the path, not of the storage.
- **groupby-orderby-limit** against TimescaleDB's rowstore (8.8 ms against 4.1 ms): its index gives the
  five newest minutes directly; ANKAA aggregates every minute before the end time and then takes five.
  Against TimescaleDB's columnstore (44 ms) and QuestDB (9.4 ms) ANKAA is ahead.

The same queries after a restart of ANKAA (everything read from segment files, then held in the segment
cache) are measured by `scripts/profile-telemetry.sh`; see the table at the end.

## What the morning looked like, and why

The first run of the day, with the engine as it was, read very differently: single-host queries took
10 to 170 ms and `lastpoint` 476 ms, and ANKAA lost 11 of 15 to TimescaleDB or QuestDB. Three things
were wrong, none of them in the storage format:

1. **`dict` columns were numbers in SQL.** Asking for a host by name needed a join with the dictionary,
   and returning a row with its ten tags needed ten. `dict` columns are now names in SQL (RFC-0002): the
   statements are the ones the other engines run, and the engine works on the ids underneath.
2. **There was no way to ask for the newest row per key** without scanning: `latest(table, key)` now
   answers from chunk metadata.
3. **Bounded memory shredded the data.** With 1,000 hosts and 1 GiB for uncompressed rows, open chunks
   were sealed at about 1,500 rows: 16,937 chunks where 1,000 do. Every chunk has a fixed cost.

| | First run (`e5f8363`) | Names in SQL | Final |
|---|---:|---:|---:|
| Load, rows/s | 664,699 | 983,655 | 1,106,552 |
| Segments on disk, MiB | 617 | 617 | 247 |
| Chunks at rest | 16,937 | 16,937 | 1,000 |
| single-groupby-1-1-1, ms | 9.52 | 0.98 | 0.74 |
| cpu-max-all-8, ms | 66.8 | 1.50 | 1.01 |
| double-groupby-1, ms | 28.6 | 29.0 | 14.6 |
| double-groupby-all, ms | 104 | 107 | 59.2 |
| high-cpu-1, ms | 170 | 1.45 | 0.83 |
| high-cpu-all, ms | 165 | 137 | 58.2 |
| lastpoint, ms | 476 | 449 | 0.80 |

## The profile, function by function

`scripts/profile-telemetry.sh` records a `perf` profile of the server (cpu-clock; GCE exposes no hardware
counters) while it loads, compresses, restarts and answers each statement in a loop, and prints the
hottest symbols of each. `bench/benches/hot_paths.rs` measures 55 functions on one core. What they showed
and what was done:

| Found | Where the time went | Change |
|---|---|---|
| 16,937 chunks for 1,000 hosts | every per-chunk cost, in every profile | compaction: runs of small chunks of a partition are merged in the background (`storage.compact_small_chunks`); a merged segment lists what it replaces, so a crash in between loses and repeats nothing |
| Pruning asked each chunk in turn | `ChunkRef::may_overlap`: 23 % of a single-host query | the statistics of a chunk list as flat arrays, built at the first scan of the list |
| Tier sizes summed three times a tick | `Table::tier_stats`: 11.6 % of the server while compressing | once, reused until a chunk moves |
| `LIKE` on a name column | the regex compiler, once per batch: 489 ms for 26 M rows | any predicate on one name column is evaluated over the dictionary's names once, then by id: 0.7 ms |
| Every table's view rebuilt every 50 ms | reference counting over every chunk, idle or not | rebuilt when the table changed |
| A cold read opened its file every time | file read and footer parse: 38 of 55 to 75 µs per call; after a restart large scans were 2.5 to 5 times slower | segment cache: recently read segments stay in memory as stored and are read like warm chunks (61 µs → 4.6 µs for 4,320 rows of 2 columns) |
| A time range decoded whole chunks | 26,000 rows decoded for 360 | row-range decoding in the warm and cold tiers |
| Time-bucket grouping per row | `dense_agg::rows_with`: 17 to 25 % of bucketed group-bys | per run of rows of one bucket |
| Compaction paced at two merges a second | ten minutes to settle after a load | goes on while there is work |

Measured and left as it is, with the reason:

- **Query planning**: about 190 µs of a 220 µs single-host query on a laptop core is DataFusion's physical
  planning (`EnsureRequirements` alone 92 µs). Caching physical plans needs proof that every operator can
  be executed twice; not done.
- **Reference counting** (`__aarch64_ldadd8_*`): 10 to 20 % of scans over many chunks. It follows the
  number of chunks and batches, which compaction cut by 17.
- **Compression**: 13 % of encode CPU is Vortex counting distinct float values with a hash set to choose
  an encoding. Inside the dependency.
- **`percentiles` over the whole table** (26 M rows) exceeds the default 1 GiB query memory pool: the
  function materialises its input. It should stream per partition.
- **Start-up** parses write-ahead-log frames that are already in segments before it skips them (13 % of a
  2.1 s restart).

### After a restart

`scripts/profile-telemetry.sh` on the build before the last two changes (cached segments read like warm
chunks, full compaction; 5,289 chunks at the time), client-observed medians in ms. The run with the final
build is still to be done and will replace this table.

| Statement | Loaded | Restarted |
|---|---:|---:|
| single-groupby-1-1-1 | 0.74 | 0.83 |
| double-groupby-1 | 36.3 | 89.7 |
| high-cpu-all | 168 | 858 |
| lastpoint / `latest('cpu', 'hostname')` | 1.90 | 0.79 |
| `count(*)` | 0.95 | 0.58 |
| `hostname LIKE 'host_1%'` | 0.74 | 0.57 |
| `gapfill` (1,000 hosts, one hour by minute) | 14.7 | 71.9 |
| `SELECT 1` | 0.33 | 0.32 |

## Reproduce

```sh
scripts/gcp/ankaa-gcp.sh create && scripts/gcp/ankaa-gcp.sh provision && scripts/gcp/ankaa-gcp.sh deploy   # ANKAA_GCP_VM=ankaa-bench
# on the VM, in ~/ankaa (bench/telemetry/README.md has the full list):
bench/telemetry/.venv/bin/python bench/telemetry/generate.py
bench/telemetry/.venv/bin/python bench/telemetry/run.py --data-dir bench/telemetry/data/tsbs-cpu-s1000-3d-seed123 \
  --runs 20 --warmup 3 --timescaledb-rowstore
sudo sysctl -w kernel.perf_event_paranoid=-1 && scripts/profile-telemetry.sh
```
