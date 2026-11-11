# bench/telemetry — TSBS cpu-only: ANKAA vs InfluxDB 3 Core, TimescaleDB, QuestDB

A reproducible telemetry comparison in the shape of the Time Series Benchmark Suite
([TSBS](https://github.com/timescale/tsbs)) DevOps **cpu-only** use case: the same generated rows loaded into every
engine through its fastest documented bulk path (timed), the 15 TSBS cpu-only query types with fixed parameters,
every answer diffed against DuckDB before anything is timed.

Method and fairness rules are those of [`docs/benchmarks/comparison-method.md`](../../docs/benchmarks/comparison-method.md)
(same box, same rows, same semantics, verified before timed, no engine tuned down, numbers only from real Arm
server hardware), with the telemetry-specific points below. The harness code is shared with `bench/compare`
(`bench/compare/harness`: query loading, result diffing, engine adapters, system facts).

**Numbers are published only from real Arm server hardware, never from a laptop, a VM on a laptop or an
emulator.** `run.py` enforces it: on anything that is not aarch64 Linux it writes to `results/smoke/`
(git-ignored) and labels the output as a smoke test.

## Why the TSBS shape

TSBS (Timescale's fork of InfluxData's `influxdb-comparisons`) is the workload time-series vendors already publish
against (Timescale and QuestDB among them), so its data generator and query types are known, documented and not ours. Using its
generator unchanged and its query semantics per database means nobody has to trust a workload we designed. What
we change, and why, is listed under "Deviations from TSBS".

The cpu-only data: one row per host every 10 s; 10 string tags per host (hostname, region, datacenter, rack, os,
arch, team, service, service_version, service_environment), 10 CPU metrics (usage_user … usage_guest_nice).
Default run: **1,000 hosts × 3 days = 25,920,000 rows** (seed 123, 2016-01-01 .. 2016-01-04, TSBS's README dates).
`--scale 4000` is TSBS's README scale (103.7 M rows); it needs about 32 GB of RAM for the in-memory reference and
engines, so the default is sized for a 4 vCPU / 16 GB machine (Axion `c4a-standard-4`).

## What is in here

| path | purpose |
|---|---|
| `generate.py` | runs the pinned `tsbs_generate_data --use-case=cpu-only --format=influx`, checks the generator's module version, the line count (= scale × span / interval) and records its sha256; converts; writes `manifest.json` |
| `convert.py` | streaming, vectorised (pyarrow) line protocol → `cpu.parquet` (DuckDB), `cpu.csv` (ANKAA, TimescaleDB), `cpu.lp` with float fields (InfluxDB, QuestDB); explicit schema; every output's row count checked |
| `queries/manifest.json` | the 15 query types, their **fixed** parameters (hosts, windows), comparison rules (exact / approx), `transfer_heavy` flag |
| `queries/{duckdb,ankaa,influxdb3,timescaledb,questdb}.sql` | the same questions per dialect (`-- @query <id>` blocks, `{HOSTS}` `{START}` `{END}` placeholders) |
| `docker-compose.yml` | InfluxDB 3 Core, TimescaleDB, QuestDB: pinned images, each setting justified with a vendor-doc link |
| `run.py` | reference first, then one engine at a time: start empty → load (timed) → post-load step → idle wait → disk / memory → verify → time → stop; writes `results/<date>-<host>.json` + `.md` |
| `probes.py` | memory (cgroup v2 / `/proc`), CPU (idle detection) and `du` of each engine's data |
| `requirements.txt` | exact Python pins |

Adapters added to `bench/compare/harness/engines.py`: `AnkaaServeEngine`, `InfluxDB3Engine`, `TimescaleDBEngine`,
`QuestDBTelemetryEngine`, `DuckDBTelemetryEngine` (and `queries.render_params` for the placeholders).

## Engines, load paths and clients

| engine | version (pinned) | load path (timed) | post-load step | query client |
|---|---|---|---|---|
| ANKAA | `ankaad` built from this repo | `ankaad serve` with a generated config: table `cpu` (ts `timestamp_ns`, 10 tags `dict`, hostname `partition_by`, 10 fields `float64`), a `[[sources]] kind = "file"`, `format = "csv"` source on `cpu.csv`, `wal_dir` + `checkpoint_dir` set (durable). Clock: process spawn → `ankaa_user_rows_total` = rows (no rejected rows allowed), then `count(*)` checked over Flight | – | Arrow Flight (pyarrow, DoGet, SQL as ticket) |
| InfluxDB 3 Core | `influxdb:3.12.2-core` | `POST /api/v3/write_lp?precision=nanosecond&accept_partial=false`, 10,000 lines per request ([vendor optimum](https://docs.influxdata.com/influxdb3/core/write-data/best-practices/optimize-writes/)), `--writers` concurrent requests (default = vCPUs; [vendor advice](https://docs.influxdata.com/influxdb3/core/write-data/http-api/v3-write-lp/)), acknowledged after the WAL flush (`no_sync=false`, default). Clock: first request → `count(*)` = rows | wait until every row is in Parquet (`system.parquet_files`) | Arrow Flight (DoGet with the v3 JSON ticket, the transport of InfluxData's v3 client libraries); `--influxdb3-transport http` for `/api/v3/query_sql?format=parquet` |
| TimescaleDB | `timescale/timescaledb:2.31.0-pg17` | hypertable on `ts`, 12 h chunks (TSBS default), index `(hostname, ts DESC)` (TSBS's partition index) created first; parallel `COPY … FROM STDIN (FORMAT csv)` over `--writers` connections × 50,000 rows per transaction (the [timescaledb-parallel-copy](https://github.com/timescale/timescaledb-parallel-copy) scheme); `ANALYZE` | columnstore: `enable_columnstore`, `segmentby = 'hostname'`, `orderby = 'ts DESC'`, `convert_to_columnstore` on every chunk (parallel), `VACUUM ANALYZE` — [Timescale's recommendation](https://docs.timescale.com/use-timescale/latest/compression/about-compression/) for analytical queries filtered by device/host. Timed separately. `--timescaledb-rowstore` also times the queries before it | psycopg 3, binary results |
| QuestDB | `questdb/questdb:10.1.0` (same pin as `bench/compare`) | ILP over HTTP (`POST /write?precision=n`, QuestDB's [recommended ingestion](https://questdb.com/docs/reference/api/ilp/overview/)), the same `cpu.lp` as InfluxDB, 75,000 lines per request (QuestDB clients' HTTP auto-flush default), `--writers` concurrent; table created first: tags `SYMBOL` (hostname with capacity ≥ hosts), fields `DOUBLE`, `ts` designated, `PARTITION BY DAY WAL`. Clock: first request → rows visible and WAL applied | – | PostgreSQL wire (psycopg 3), as `bench/compare`; `--questdb-transport rest` |
| DuckDB (reference) | `duckdb==2.0.0` | `CREATE TABLE cpu AS SELECT * FROM read_parquet(...)` in memory, in a child process that exits before any engine starts | – | in-process Arrow; timed only with `--engines …,duckdb` |

After the load and post-load step, `run.py` waits until the engine is idle (< 5 % of one core over 5 s, at most
10 min: WAL apply, snapshots, tiering, autovacuum), then records disk (`du -sk` of the data directories, plus the
engine's own accounting: Timescale `hypertable_size`, QuestDB `table_storage()`, InfluxDB Parquet bytes) and memory.

Memory: `ankaad` = `VmHWM` (true peak RSS); containers = peak of the sampled anonymous memory of the container's
cgroup (every 0.5 s) and `memory.peak` (which also counts page cache the container caused, an upper bound); both
are in the JSON, the table shows the first. On hosts without cgroup v2 the fallback is `docker stats` sampling.

## Pinned versions and why

| what | pin | why |
|---|---|---|
| TSBS generator | `github.com/timescale/tsbs` commit `8323e59c74027b108f4ad5ec5d3e498b0101a02e` (pseudo-version `v0.0.0-20260527045238-8323e59c7402`) | TSBS publishes no release tags on the module proxy; this is `master` as of the harness. `generate.py` refuses another build (`go version -m`) unless `--allow-other-tsbs` |
| Go | 1.27.3 (the version tested here) | builds the generator only; the output is expected not to depend on the Go version (compare the hashes below) |
| InfluxDB 3 Core | `influxdb:3.12.2-core` | newest Core release (2026-10-30), official Docker Hub image, multi-arch |
| TimescaleDB | `timescale/timescaledb:2.31.0-pg17` | newest release (2026-10-20) on PostgreSQL 17 (the instruction's pg17; pg18 images also exist); the full image, because the columnstore is not in `-oss` |
| QuestDB | `questdb/questdb:10.1.0` | same as `bench/compare`; 10.2.0 appeared on 2026-11-10, one day before this harness, and was not tested |
| Python | `requirements.txt` | the versions the smoke test ran with; cp312 aarch64 wheels exist for all |

Reference SHA-256 of the raw TSBS output (seed 123, 2016-01-01 .. 2016-01-04, 10 s), measured with the pinned
generator on macOS arm64; `generate.py` records the hash of what it generated in `manifest.json`, so a run on
another machine can be compared:

| scale | lines | sha256 of `cpu.tsbs.lp` |
|---|---|---|
| 100 | 2,592,000 | `78edc493006827d8fb136e03789c07821e43114bf307bbc518f7f9a0ebf6578a` |
| 1000 | 25,920,000 | `d26b6689543c60e57cd605d272ad1bbea6f88516337465cb0cef14a6a626a0c3` |

## Reproduce (Ubuntu 24.04 aarch64, e.g. Google Axion c4a-standard-4)

Needs Docker with the compose plugin, Rust as pinned in `rust-toolchain.toml`, Python 3.12 (`sudo apt-get install -y python3-venv`), about
60 GB free disk at scale 1000 (raw TSBS 8.9 GB + `cpu.lp` 8.7 GB + `cpu.csv` 3.1 GB during generation; engines'
data volumes are dropped after each engine) and nothing else running.

```sh
cd ankaa                                                     # the repository

# 0. Go + the pinned TSBS generator
curl -fsSLO https://go.dev/dl/go1.27.3.linux-arm64.tar.gz
echo "a06e99309dbcbdca1a8ccd2282c3ee146e55930886e26b4f1193269587165f49  go1.27.3.linux-arm64.tar.gz" | sha256sum -c
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.27.3.linux-arm64.tar.gz
export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH
go install github.com/timescale/tsbs/cmd/tsbs_generate_data@8323e59c74027b108f4ad5ec5d3e498b0101a02e

# 1. ANKAA and the Python side
cargo build --release -p ankaad                              # target/release/ankaad
python3 -m venv bench/telemetry/.venv
bench/telemetry/.venv/bin/pip install -r bench/telemetry/requirements.txt

# 2. images (pull before timing anything)
docker compose -p ankaa-tsbs -f bench/telemetry/docker-compose.yml pull

# 3. data: 1,000 hosts x 3 days, seed 123 -> bench/telemetry/data/tsbs-cpu-s1000-3d-seed123
bench/telemetry/.venv/bin/python bench/telemetry/generate.py
#    compare manifest.json source_lp.sha256 with the table above

# 4. load + verify + time every engine, one at a time (duration at scale 1000 not measured yet; expect about an hour)
ANKAA_BENCH_HOST_LABEL=gcp-c4a-standard-4 bench/telemetry/.venv/bin/python bench/telemetry/run.py \
    --data-dir bench/telemetry/data/tsbs-cpu-s1000-3d-seed123 --runs 20 --warmup 3 \
    --timescaledb-rowstore --label "c4a-standard-4 (4 vCPU Axion, 16 GB), Ubuntu 24.04"

# 5. results/<date>-gcp-c4a-standard-4.{json,md}; write the report in docs/benchmarks/ from them
```

Useful switches: `--engines ankaa,questdb` (subset; add `duckdb` to time the reference too), `--queries
lastpoint,high-cpu-1`, `--writers N`, `--influxdb3-gzip`, `--influxdb3-transport http`, `--questdb-transport rest`,
`--keep-volumes` / `--keep-containers` (inspection), `--allow-other-containers` (recorded in the report),
`--ankaa-warm-budget-mib` / `--ankaa-query-memory-mib`. `--no-verify` is refused outside smoke runs.

Smoke test on any machine (nothing publishable; takes a few minutes):

```sh
bench/telemetry/.venv/bin/python bench/telemetry/generate.py --scale 100 --data-dir bench/telemetry/data/smoke
bench/telemetry/.venv/bin/python bench/telemetry/run.py --data-dir bench/telemetry/data/smoke --runs 3 --warmup 1 --smoke
```

## Fairness rules specific to this harness

1. **One engine at a time.** The DuckDB reference is computed in a child process that exits first. Then each
   engine is started on an empty volume, measured and stopped (its volume removed) before the next one starts;
   `ankaad` is stopped before the containers start. Other running containers make `run.py` refuse (unless
   `--allow-other-containers`, which the report records).
2. **Same rows, one source.** The TSBS line protocol is the only source. Every engine loads the same 25.92 M rows
   in the same order, from the format its fastest path takes (line protocol, CSV or Parquet, converted once by
   `convert.py`); the converted files' hashes and row counts are in the manifest. Fields are float64 everywhere
   (see "Deviations").
3. **Fastest documented bulk path, durable.** Each load path above is the vendor's documented bulk path, with
   the vendor's batch size and concurrent writers; every engine acknowledges only durable data (InfluxDB
   `no_sync=false`, QuestDB WAL, PostgreSQL commit, ANKAA WAL group commit with fdatasync). Load time ends when
   every row is visible to queries.
4. **At rest before timing.** Engines are queried in the state the vendor recommends for analytics on data that
   has stopped changing: TimescaleDB in the columnstore (rowstore optional, as its own column), InfluxDB with all
   rows in Parquet, QuestDB with the WAL applied, every engine idle. The post-load steps are timed and reported.
5. **Same question, verified.** Each query is the same question in every dialect, with the same bucket
   alignment, filters, ORDER BY and LIMIT; results are diffed against DuckDB (exact for max values, timestamps and
   strings; relative 1e-9 for averages) before timing; a mismatch is printed in the table and its time declared
   not comparable.
6. **Nothing tuned down; every non-default setting cited** (below). No CPU or memory limits on containers.
7. **Warm runs only:** W warm-ups discarded, N runs, median and p95 reported, all raw times in the JSON.

## Engine configuration (non-defaults only, with reasons)

- **InfluxDB 3 Core** — `--without-auth` (benchmark only; loopback port; auth does not change the data path).
  `--wal-flush-interval=100ms` ("100ms for local disks" in the [config reference](https://docs.influxdata.com/influxdb3/core/reference/config-options/)).
  `INFLUXDB3_QUERY_FILE_LIMIT=4320`: Core refuses queries reading more Parquet files than this (default 432 =
  72 h of 10-minute files); the 3-day dataset has 432 files on the smoke run and more when chunks straddle
  snapshots, so whole-range queries (lastpoint) would otherwise fail; raising it is the documented option.
  `INFLUXDB3_FILE_CACHE_RECENCY=100000h`: the in-memory Parquet cache only takes files newer than now − 5 h by
  default; TSBS data is stamped 2016, so the cache would never apply as it does to live data.
  `INFLUXDB3_FORCE_SNAPSHOT_MAX_AGE=1m`: bounds the wait until all rows are in Parquet (default 1 h); during the
  load snapshots happen every 600 WAL files (one minute at 100 ms) anyway.
- **TimescaleDB** — the image's `timescaledb-tune` sizes memory and parallelism to the machine on first start (left
  as is). `shm_size: 2g` because Docker's 64 MB `/dev/shm` breaks parallel query plans. Schema and columnstore
  settings as in the table above.
- **QuestDB** — stock; query/connection timeouts raised to 10 min (as `bench/compare`); `nofile` ulimit per the
  [capacity planning guide](https://questdb.com/docs/operations/capacity-planning/).
- **ANKAA** — generated config (`work/ankaa/ankaad.toml`, written next to the WAL): `query_timeout_s = 600` (the
  others' timeouts are 10 min), `storage.warm_budget_mib` = 25 % of RAM (the share `timescaledb-tune` gives
  `shared_buffers`), `resources.query_memory_mib` = 20 % of RAM (InfluxDB 3's default query memory pool); all
  else default. Both are flags.

If you change a setting for a published run, say so in the report and commit the change here.

## Query set and semantics

The 15 TSBS cpu-only query types (`tsbs/cmd/tsbs_generate_queries/uses/devops`): `single-groupby-M-H-T` (max of the
first M metrics per minute over H hosts and T hours; M ∈ {1, 5}, H ∈ {1, 8}, T ∈ {1, 12}), `cpu-max-all-H` (max of
all 10 metrics per hour, H hosts, 8 h), `double-groupby-M` (avg of M metrics per hour and host, all hosts, 12 h;
M ∈ {1, 5, 10}), `high-cpu-1` / `high-cpu-all` (rows with usage_user > 90 in 12 h, one host / all hosts),
`groupby-orderby-limit` (max per minute of the 5 latest minutes before a time), `lastpoint` (newest row per host).

Per-dialect shapes follow TSBS's own per-database generators: TimescaleDB (`time_bucket`, `DISTINCT ON`, the CTE of
double-groupby), QuestDB (`SAMPLE BY`, `LATEST ON`), InfluxDB (TSBS's are InfluxQL for 1.x; here InfluxDB 3 SQL
with `date_bin`). ANKAA and DuckDB use standard SQL (`date_bin`, `date_trunc`).

### Deviations from TSBS (each makes the comparison stricter, none makes a query cheaper)

- **Fixed parameters.** TSBS draws random hosts and windows per query instance; here one instance per type is
  written in `queries/manifest.json` (drawn once with a fixed seed, hosts from `host_0..host_99` so any scale
  ≥ 100 works, windows at whole seconds and not bucket-aligned, as TSBS's are). Every engine answers the same
  question, so answers can be diffed. Variance across parameter choices is not measured.
- **Explicit column lists** instead of `SELECT *` (column order differs per engine) and the column `ts` (aliased
  from InfluxDB's `time`).
- **QuestDB `groupby-orderby-limit`**: TSBS's QuestDB query is `SAMPLE BY 1m LIMIT 5` without ORDER BY, i.e. the 5
  *earliest* minutes; the TimescaleDB and InfluxDB queries take the 5 *latest* before the end time. Here every
  engine answers the latter (`ORDER BY minute DESC LIMIT 5`).
- **ORDER BY** is written out where TSBS's QuestDB queries rely on `SAMPLE BY` output order.
- **Float fields.** The pinned TSBS writes the CPU fields as integers (`usage_user=58i`). Every engine stores them
  as float64 (the values are small integers and convert exactly); InfluxDB and QuestDB receive `cpu.lp`, the
  original lines with the `i` suffix removed.
- **ANKAA runs the same statements.** A `dict` column is a dictionary-encoded string in SQL (RFC-0002), so the
  ANKAA queries filter, group and return host names like every other engine; nothing is resolved by the client.
  The only shape of its own is `lastpoint`: `max(ts)` per host joined back (measured faster on ankaad than
  `DISTINCT ON` and `row_number()`); TSBS points are unique per (host, ts), and DuckDB's `DISTINCT ON` answer would
  expose any duplicate.

### Comparison rules

`queries/manifest.json` per query: `keys` (rows are matched by them), `exact` (max/min of stored values,
timestamps, strings), `approx` (averages, relative 1e-9: summation order differs between engines). The diff is
`bench/compare/harness/compare.py` (a vectorised pre-check accepts only cells that are certainly equal, every other
cell goes through the per-value rule, so the verdict is unchanged; `bench/oracle` passes with it).

## Caveats (read before quoting a number)

- **One small machine.** 4 vCPU, single node, Docker on the host kernel, no CPU pinning; the load generator
  (Python HTTP clients for InfluxDB/QuestDB, the COPY client for TimescaleDB) runs on the same 4 vCPUs as the
  engine, as TSBS's loaders would. ANKAA's `file` source reads inside the server, so it has no client process: its
  load path includes parsing the CSV in-engine, the others' includes the HTTP/PG protocol. That is each engine's
  documented fastest path, not an identical one.
- **InfluxDB 3 Core is the open-source edition.** It has no compaction (InfluxDB 3 Enterprise compacts Parquet
  files) and is designed for recent data; its Last Value Cache (an in-memory answer to "last point") is a separate
  feature, not used: every engine answers `lastpoint` with SQL over its stored data.
- **Client-observed times** include each engine's wire protocol and Python client: Arrow Flight (ANKAA, InfluxDB)
  decodes columnar batches, PostgreSQL wire (TimescaleDB, QuestDB) builds Python tuples. For `high-cpu-all`
  (marked †, ~10 % of all rows in 12 h ≈ 430 k rows at scale 1000) transfer dominates, as it does in TSBS.
- **Data stamped 2016.** TSBS's dates. InfluxDB's cache window is widened for it (above); no other engine has
  a wall-clock-relative behaviour that we know of. ANKAA's ingest rejects rows more than 24 h in the *future* only.
- **ANKAA storage at this scale.** A host has 25,920 rows in 3 days, fewer than one ANKAA chunk (65,536 rows), so
  chunks stay open (hot, in memory) unless `storage.wal_budget_mib` (1 GiB) forces partitions to be sealed and
  persisted early. On-disk size for ANKAA is therefore WAL + whatever segments were written; it is reported as
  measured.
- **QuestDB on-disk size** includes uncompressed column files and WAL segments awaiting purge; QuestDB documents
  compression through the file system (ZFS), not used here.
- **Memory figures differ in kind** (VmHWM for `ankaad`, sampled cgroup anonymous memory for containers, plus
  `memory.peak`); compare them as orders of magnitude.
- **Warm only, single client.** No concurrent queries, no ingest-while-querying; cold-cache behaviour is not
  measured.

## Result files and host names

Same rules as `bench/compare`: result files carry a label instead of the machine's hostname, `<arch>-<os>` by
default or `ANKAA_BENCH_HOST_LABEL`. The JSON holds every SQL text as run, every raw time, the dataset manifest,
all settings, image IDs, versions, load facts and hardware facts.
