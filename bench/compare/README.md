# bench/compare — reproducible comparison: ANKAA vs QuestDB, ClickHouse, DuckDB

Same box, same rows, same query set, every result checked against a reference before it is timed.
Method and fairness rules: [`docs/benchmarks/comparison-method.md`](../../docs/benchmarks/comparison-method.md).
Measurement protocol: [`docs/measurement-protocol.md`](../../docs/measurement-protocol.md).

**Numbers are published only from real Arm server hardware (Graviton4 class), never from a laptop,
a VM on a laptop or an emulator.** `run.py` enforces this: on anything that is not aarch64 Linux it
writes to `results/smoke/` (git-ignored) and labels the output as a smoke test.

## What is in here

| path | purpose |
|---|---|
| `docker-compose.yml` | QuestDB `10.1.0` and ClickHouse `26.8.36.3` (pinned, arm64 images). DuckDB runs in-process, ANKAA from the repo binary. |
| `prepare.py` | `ankaad replay --out-dir` exports `trades`, `quotes`, `book_deltas`, `symbols` as CSV + Parquet, then loads the same rows into every engine and checks the row counts. |
| `queries/manifest.json` | The 20-query tick set: ids, descriptions, comparison rules (exact / approx / quantile), which ones are timed. |
| `queries/{ankaa,duckdb,clickhouse,questdb}.sql` | The same queries in each dialect (`-- @query <id>` blocks, `{T0}`/`{T1}` placeholders). |
| `run.py` | Verifies each engine's result against the DuckDB reference, then times each query (warm-up + N runs, median and p95) with each engine's native client. Writes `results/<date>-<host>.json` and `.md`. |
| `harness/` | Shared Python: query loading, export, result diffing, engine adapters, system facts. Also used by `bench/oracle`. |
| `report-template.md` | The report skeleton: hardware, OS, versions, dataset, protocol, caveats, table. |
| `results/` | Committed JSON + Markdown of real runs only. |

Engines and clients:

| engine | load path | query client | time reported |
|---|---|---|---|
| ANKAA (`ankaa`) | capture replayed in a fresh `ankaad replay` process | the same process, `--sql` repeated warm-up + N times | ankaad's own per-query time (plan + execute + collect); excludes replay and transfer |
| ANKAA (`ankaa-flight`) | a running `ankaad serve` holds the data | `pyarrow.flight` (`get_flight_info` + `do_get`, bearer token) | client-observed over gRPC until the last batch is read |
| DuckDB (`duckdb`) | Parquet -> in-memory tables | Python in-process, Arrow result | client-observed in-process |
| ClickHouse (`clickhouse`) | HTTP `INSERT ... FORMAT Parquet` | HTTP :8123, `default_format=Arrow` | client-observed; the server's `elapsed_ns` (`X-ClickHouse-Summary`) is stored too |
| QuestDB (`questdb`) | REST `/imp` CSV with explicit schema, `ts` designated | PostgreSQL wire :8812 (psycopg) or REST `/exec` (`--questdb-transport rest`, stores `timings.execute`) | client-observed |

The scopes differ (the ANKAA replay path has no network between client and engine). The report must
state them side by side; the Flight path is the like-for-like one. `ankaad serve` can serve a capture
without network: `ANKAA_TOKEN=... ankaad serve --url file://<capture.ankc> --no-depth-snapshots`, then
`run.py --engines ankaa-flight --flight grpc://127.0.0.1:50051 --token $ANKAA_TOKEN`.

## Reproduce (for a stranger)

Hardware for a publishable run: an AWS `c8g.*` (Graviton4) or equivalent Arm server, Ubuntu 24.04
aarch64, Docker 27+ with the compose plugin, Rust as pinned in `rust-toolchain.toml`, Python 3.11+.
Nothing else should be running on the box.

```sh
git clone https://github.com/<org>/ankaa && cd ankaa
cargo build --release -p ankaad                              # target/release/ankaad

python3 -m venv bench/.venv
bench/.venv/bin/pip install -r bench/compare/requirements.txt

# 1. correctness first: ANKAA vs DuckDB and Polars on the bundled capture (must be green)
bench/oracle/run.sh

# 2. engines
docker compose -f bench/compare/docker-compose.yml up -d

# 3. dataset: the bundled 20 s capture is only a smoke test. For a report, record a long capture
#    (hours) with snapshots and use it; any .ankc works, several files in time order are allowed:
#    cargo run -p ankaa-feeds --features capture --bin ankaa-capture -- --out captures --duration-secs 3600 --snapshots
bench/.venv/bin/python bench/compare/prepare.py --capture captures/<file>.ankc --expect-snapshots \
    --data-dir bench/compare/data/<name>

# 4. verify + time (all engines, 20 runs after 3 warm-ups; add --label with the instance type)
bench/.venv/bin/python bench/compare/run.py --data-dir bench/compare/data/<name> \
    --engines ankaa,duckdb,clickhouse,questdb --runs 20 --warmup 3 --label "c8g.8xlarge, Ubuntu 24.04"

# 5. report: copy report-template.md to docs/benchmarks/<date>-graviton4-comparison.md and fill it from
#    results/<date>-<host>.json (hardware block = meta.lscpu etc.). Commit JSON, MD and the report.
```

Useful switches: `--queries q05_ohlcv_1s,q18_asof_backward_agg` (subset), `--no-verify` (never for a
report), `--duckdb-threads N`, `--questdb-transport rest`, `--engines ankaa-flight --flight grpc://127.0.0.1:50051 --token $ANKAA_TOKEN`
(needs a running `ankaad serve`), `--load` (reload containers from the export before timing).
`prepare.py --engines duckdb` prepares without containers.

Smoke test on any machine (nothing is published):

```sh
bench/.venv/bin/python bench/compare/prepare.py            # bundled capture -> bench/compare/data/binance-btcusdt-trade-depth-20s
bench/.venv/bin/python bench/compare/run.py --data-dir bench/compare/data/binance-btcusdt-trade-depth-20s --runs 3 --warmup 1 --smoke
```

## Engine configuration (tuned per vendor docs, never down)

- **QuestDB** — stock `questdb/questdb:10.1.0`; worker pools default to the core count; the only
  settings touched are query/connection timeouts (`docker-compose.yml`). Tables use `ts` as designated
  timestamp, `PARTITION BY DAY`. Docs: <https://questdb.com/docs/configuration/>,
  <https://questdb.com/docs/operations/capacity-planning/>.
- **ClickHouse** — stock `clickhouse/clickhouse-server:26.8.36.3`, `MergeTree ORDER BY (symbol, ts)`,
  `OPTIMIZE TABLE ... FINAL` after load so every table is one part; `join_use_nulls = 1` on ASOF queries so
  unmatched rows are NULL like everywhere else. Docs: <https://clickhouse.com/docs/operations/tips>,
  <https://clickhouse.com/docs/sql-reference/statements/select/join#asof-join-usage>.
- **DuckDB** — in-process, default threads (= cores) and memory limit; tables materialised in memory from
  Parquet. Docs: <https://duckdb.org/docs/stable/configuration/overview>,
  <https://duckdb.org/docs/stable/guides/sql_features/asof_join>.
- **ANKAA** — `ankaad` release build from the pinned toolchain; default settings; tables as replayed.

If you change a setting for a published run, say so in the report and commit the change here.

## Precision and semantics notes

- Prices and sizes are fixed-point `Int64` with 8 decimals everywhere (no floats in the data).
- Timestamps: export, DuckDB (`TIMESTAMP_NS`), ClickHouse (`DateTime64(9)`) and ANKAA are nanoseconds.
  **QuestDB stores microseconds**: nanoseconds are truncated on import. Binance stamps are milliseconds,
  so nothing is lost on this data; with sub-microsecond inputs QuestDB's ASOF/SAMPLE BY results may differ.
- `notional` (Σ price·size / 1e8) is computed in 128-bit integers in ANKAA, DuckDB (`HUGEINT`) and
  ClickHouse (`Int128`); QuestDB has no 128-bit integer, so it computes a double and the manifest compares
  it approximately (`engine_overrides`).
- Percentiles: ANKAA's `percentiles()` is a UDDSketch (1 % relative error) that returns a value within
  1 % of the order statistic `sorted[floor(q·(n−1))]`. The DuckDB and ClickHouse references compute that
  exact statistic; QuestDB's `approx_percentile` (HdrHistogram) is accepted when it falls within 1 % of the
  band between that statistic and the next one.
- ASOF: backward = last right row with `right.ts <= left.ts` (inclusive), forward = first with
  `right.ts >= left.ts`; a tolerance drops the match when the distance exceeds it. QuestDB has backward
  (`ASOF JOIN ... TOLERANCE`) but no forward ASOF: `q09` / `q20` are reported `n/a` for it.
- Row-level ASOF outputs (`q07`–`q09`) are verified but not timed: result transfer would dominate and
  differs by client. Their aggregated twins (`q18`–`q20`) are timed.
- Each query's `ORDER BY` is part of the query (the same in every dialect).

## Result files and host names

Result files are written to be published. They carry a label instead of the machine's real
hostname: `<arch>-<os>` by default, or whatever `ANKAA_BENCH_HOST_LABEL` says (for example
`ANKAA_BENCH_HOST_LABEL=c8g.4xlarge-run1 python run.py ...`).
