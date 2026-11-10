# ANKAA tick-query comparison — <YYYY-MM-DD>, <instance type>

> Template. Copy to `docs/benchmarks/<date>-graviton4-comparison.md`, replace every `<...>`, attach the
> JSON from `bench/compare/results/`. **Only runs on real Arm server hardware are published.** Laptops,
> VMs on laptops, QEMU or any emulator are correctness evidence at most and never appear in a report
> (`docs/measurement-protocol.md` §7). Delete this note when done.

## Summary

<Three sentences: what was compared, on what, the headline per query family. No superlatives;
mention where ANKAA is slower too. No comparison with commercial systems.>

## Hardware

| | |
|---|---|
| Cloud / SKU | <AWS c8g.8xlarge, Graviton4 (Neoverse V2), eu-central-1> |
| vCPUs / cores used | <32 vCPU; all engines unpinned> |
| Memory | <64 GiB> |
| `lscpu` | <paste `meta.lscpu` from the JSON, or link to it> |
| NUMA | <paste `numactl -H`> |
| Storage | <EBS gp3 ... (not on the query path: datasets are in memory)> |

## Software

| | |
|---|---|
| OS / kernel | <Ubuntu 24.04.2, 6.8.0-1024-aws, cmdline: ...> |
| ANKAA | commit `<sha>`, `ankaad <version>`, release profile, Rust `<rustc --version>` |
| QuestDB | `questdb/questdb:10.1.0` (reported: `<SELECT build()>`) |
| ClickHouse | `clickhouse/clickhouse-server:26.8.36.3` (reported: `<SELECT version()>`) |
| DuckDB | Python `duckdb <x.y.z>` |
| Docker | `<docker --version>`, compose `<version>` |
| Python | `<3.x>`; `pyarrow <v>`, `psycopg <v>`, `requests <v>` |
| Configuration changes vs `bench/compare/docker-compose.yml` | <none / list> |

## Dataset

| | |
|---|---|
| Source | <Binance spot `btcusdt@trade`, `btcusdt@depth@100ms`, recorded <date> with `ankaa-capture --snapshots`> |
| Capture file(s) | `<name>.ankc`, <bytes>, sha256 `<...>` (in `manifest.json`) |
| Rows | trades <n>, quotes <n>, book_deltas <n>, symbols <n> |
| Time span | <first ts> .. <last ts> (<hours>) ; query window `[T0, T1)` = <...> |
| Timestamp precision | ns in ANKAA/DuckDB/ClickHouse; µs in QuestDB (truncation: <none for ms source stamps>) |
| Data policy | market data used only to exercise the software (`bench/data/README.md`) |

## Protocol

- `bench/oracle/run.sh` green at commit `<sha>` (ANKAA = DuckDB on SQL/ASOF/percentiles, = Polars on buckets).
- `prepare.py` loaded identical rows into every engine; row counts matched.
- `run.py --runs <N> --warmup <W>`; each result verified against the DuckDB reference before timing
  (`verified: true` for every cell below unless marked).
- Warm cache: every engine holds the dataset in memory; the warm-up runs are discarded. No cold-cache
  numbers in this report (see method doc).
- Timing scope per engine: ANKAA `<replay: server-side per-query | flight: client-observed gRPC>`;
  DuckDB in-process; ClickHouse HTTP client-observed (server `elapsed_ns` also in the JSON); QuestDB PG
  wire client-observed.
- Nothing else ran on the instance; engines ran one at a time; <CPU governor / frequency notes if any>.
- Raw per-run times: `bench/compare/results/<date>-<host>.json`.

## Results (median ms; p95 in parentheses)

<paste the table from results/<date>-<host>.md>

`n/a` = not expressible in that engine (QuestDB has no forward ASOF). `mismatch` = result differed from
the reference; the time is shown but is not comparable.

## Observations

<Per query family: what the numbers say, what explains them (plan, data layout), where a different query
formulation would help another engine and whether it was tried. Be concrete and fair.>

## Caveats

- Replay-path ANKAA times exclude client transfer; the other engines' times include it. <Quantify if a
  Flight run exists.>
- Single symbol / <N> symbols; GROUP BY and ASOF partition counts are small — <state the implication>.
- <Any configuration you could not apply, any engine feature not used, anything that surprised you.>

## Reproduce

`bench/compare/README.md`, steps 1–5, at commit `<sha>`.
