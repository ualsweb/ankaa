# First light on Google Axion — 2026-11-10

The first run of ANKAA on Arm server silicon, with a first comparison against QuestDB, ClickHouse and
DuckDB on the same machine, on one hour of live market data. Everything here was produced by the scripts in
this repository (`scripts/first-light.sh`, `scripts/gcp/`, `bench/compare/`) and can be rerun by anyone with a
Google Cloud account.

**Read this first.** These are measurements on one small virtual machine, on one day. They are real hardware
numbers, but they are **not** a benchmark result in the sense of `docs/measurement-protocol.md`: no general
performance claim should be drawn from them. What they are good for is finding problems, and they found twelve
(below), all fixed the same day.

## Machine

| | |
|---|---|
| Instance | Google Cloud `c4a-standard-4` (Axion), `europe-southwest1-a` |
| CPU | Arm Neoverse V2 (implementer 0x41, part 0xd4f), 4 vCPU, 1 thread per core |
| SVE | SVE2 yes, vector length 128 bits; SME no |
| Caches | L1d 64 KiB and L2 2 MiB per core, L3 80 MiB shared |
| Memory | 16 GB |
| Disk | Hyperdisk Balanced, 150 GB |
| OS | Ubuntu 24.04, kernel 7.0.0-1017-gcp |
| Toolchain | rustc 1.99.1 (`rust-toolchain.toml`) |

A second, identical VM ran the comparison so that the 24-hour soak on the first one was not disturbed.

## Correctness on real silicon

| Check | Result |
|---|---|
| SVE2 differential tests (`ANKAA_EXPECT_SVE2=16`), all ten kernels against scalar and NEON | **pass** — the first time the SVE2 assembly ran outside QEMU |
| Whole workspace test suite, release build, native | pass (39 min on 4 vCPU) |
| Correctness oracle: 31 checks against DuckDB and Polars | pass |
| Crash recovery: 407,000-frame replay ingested with a WAL and segments, `kill -9`, restart without a feed | identical row counts (217,000 trades, 3,476,016 book deltas, 79,047 quotes); serving again 1.14 s after start (20 segments re-attached in 2 ms, WAL replay 1.0 s) |
| The hardened systemd unit (`deploy/systemd/ankaad.service`) on live Binance data | starts, passes its config check, ingests; the 24-hour soak is reported separately |

## Single-machine measurements

Criterion (`bench/benches/hot_paths.rs`), one core, medians:

| Path | Result |
|---|---|
| Binance trade decode, JSON / SBE binary | 337 ns / 36 ns |
| Binance depth (10 levels) decode, JSON / SBE | 1.03 µs / 118 ns |
| Fixed-point price parse | 17 ns |
| Append one row to an open chunk | 22 ns |
| Encode a 64k-row chunk for the warm tier (Vortex) / write it as a segment file | 4.8 ms / 11.3 ms |
| Generic ingest, 1,024 JSON / CSV messages | 887 µs / 147 µs |
| Follower catching up 100k WAL records of 200 B | 12.7 ms |

Replay (`ankaad replay`, decode + order books + store, single thread): 3.7 million events from a 183 MB
synthetic capture in 1.06 s, about 3.5 million events per second; 5.4 million events from the real one-hour,
12-symbol capture used below (298 MB) in 1.65 s, about 3.3 million events per second.

### SIMD backends at 128-bit vectors

| Kernel (1,024 values) | scalar | NEON | SVE2 |
|---|---|---|---|
| filter_range_f32 (half selected) | 966 ns | **242 ns** | 305 ns |
| filter_range_i64 (half selected) | 754 ns | **510 ns** | 581 ns |
| sum_f32 | 665 ns | **89 ns** | 261 ns |
| sum_i64 | 85 ns | 85 ns (first run: 322 ns) | 91 ns (first run: 181 ns) |
| min_max_f32 | 119 ns | **55 ns** | 162 ns |
| min_max_i64 | 183 ns | 181 ns (scalar code) | **114 ns** (first run: not measured) |
| count_f32_gt | 175 ns | **57 ns** (first run: 159 ns) | 196 ns |
| count_i64_gt | 108 ns | 108 ns (first run: 324 ns) | 413 ns |

What this says, kernel by kernel:

- At 128-bit vectors NEON is the faster choice for most kernels, so the engine uses NEON on Axion and Graviton4
  and SVE2 only where it wins (`ankaa_simd::best` chooses per kernel; above 128 bits SVE2 is the default).
- **`min_max_i64` is the first kernel where SVE2 beats NEON at 128 bits** (114 against 181 ns): NEON has no 64-bit
  integer minimum or maximum, SVE has. It runs on SVE2 wherever the CPU has it.
- Three hand-written kernels were slower than the compiler's own loop in the first run (`sum_i64` on NEON and
  SVE2, both count kernels on NEON): each used a single accumulator, so every operation waited for the previous
  one. With four accumulators they match or beat it. The SVE2 count kernels still have that flaw (413 and 196 ns);
  they are not selected at this vector length.

## Comparison on a real capture (preliminary)

`bench/compare` on the second VM: the same rows loaded into every engine, every result checked against DuckDB
before it is timed, 20 timed runs after 3 warm-ups, median milliseconds (p95 in parentheses). All 84 results
matched the reference. Raw output: `bench/compare/results/2026-11-11-gcp-c4a-standard-4.json` (queries, every
sample, hardware block; the commit id in it is from the history before the repository was published, the code
is that of the first public commit). This is the tenth and last run (11 November, 10:00 UTC); the earlier ones
are in the next sections.

- **Dataset:** one hour of live Binance data recorded on the VM itself with `ankaa-capture --snapshots`
  (2026-11-10 16:45–17:45 UTC): trades and 100 ms depth updates for 12 symbols (BTC, ETH, SOL, BNB, XRP, DOGE,
  ADA, AVAX, LINK, LTC, TRX, SUI against USDT). 920,452 frames, no reconnects, no sequence gaps; 584,617 trades,
  214,895 quotes, 4,849,537 book deltas. One hour is small: the trades table fits in the 80 MiB L3.
- **Versions:** QuestDB 10.1.0 and ClickHouse 26.8.36.3 (containers, stock settings, nobody tuned them),
  DuckDB 2.0.0 (in-process), ANKAA 0.1.0.
- **Scopes differ**, read across a row with care: `ankaa` and `duckdb` are in-process, `ankaa-flight`,
  `clickhouse` and `questdb` are client-observed over the network stack on loopback. The like-for-like pairs
  are `ankaa` with `duckdb`, and `ankaa-flight` with `clickhouse` and `questdb`.
- **Not every row compares the same work.** `q12`/`q13`: ANKAA's `percentiles()` and QuestDB's
  `approx_percentile` are approximate (ANKAA: 1 % relative error, checked), while the DuckDB and ClickHouse
  queries sort the values and are exact. `q05`/`q06` and `q18`–`q20`: ANKAA runs its own table functions
  (`ohlcv`, `asof_join`); the others run their general SQL (`GROUP BY`, `ASOF JOIN`, `SAMPLE BY`). `q01`, `q02`:
  ANKAA answers from statistics its chunks keep (minimum, maximum and sum per block), the others read the rows.
  The queries of every engine are in `bench/compare/queries/`.
- **One client call per query.** `ankaa-flight` sends a single `DoGet` with the SQL text as ticket, as ANKAA's own
  clients do; a generic Flight client that asks `GetFlightInfo` first pays a second round trip.

| query | ankaa | ankaa-flight | duckdb | clickhouse | questdb |
|---|---|---|---|---|---|
| q01_range_count | 0.220 (0.270) | 0.466 (0.537) | 1.566 (1.667) | 5.062 (5.713) | 1.227 (1.512) |
| q02_agg_by_symbol | 0.340 (0.350) | 0.665 (0.746) | 3.030 (3.381) | 5.812 (6.282) | 6.741 (9.756) |
| q03_latest_per_symbol | 2.405 (2.910) | 2.506 (2.735) | 4.158 (4.277) | 10.6 (11.7) | 15.7 (26.8) |
| q04_vwap_by_symbol | 0.965 (1.390) | 1.264 (1.337) | 2.734 (2.913) | 6.090 (7.173) | 4.041 (6.645) |
| q05_ohlcv_1s | 3.680 (3.800) | 4.379 (4.642) | 21.1 (21.8) | 75.7 (76.7) | 43.6 (58.6) |
| q06_ohlcv_5s | 2.825 (2.940) | 3.197 (3.705) | 18.4 (21.0) | 51.4 (52.1) | 18.3 (21.7) |
| q10_crossed_quotes | 0.335 (0.470) | 0.697 (0.735) | 1.060 (1.259) | 2.687 (2.999) | 2.005 (2.369) |
| q11_spread_stats | 1.640 (1.810) | 1.777 (1.867) | 2.918 (3.045) | 5.470 (5.947) | 3.896 (4.682) |
| q12_percentiles_price | 3.360 (3.410) | 3.833 (4.018) | 50.6 (56.4) | 13.0 (14.7) | 631.3 (632.8) |
| q13_percentiles_size | 3.385 (3.470) | 3.822 (3.891) | 46.8 (52.7) | 9.518 (9.779) | 196.1 (239.2) |
| q14_trades_per_second | 1.600 (1.710) | 1.909 (2.099) | 4.972 (6.196) | 7.892 (8.202) | 5.822 (7.999) |
| q15_book_delta_stats | 10.2 (11.2) | 12.4 (12.6) | 15.9 (16.4) | 21.9 (22.6) | 43.1 (45.8) |
| q16_range_by_name | 0.885 (0.960) | 1.105 (1.174) | 3.286 (3.464) | 7.519 (8.416) | 18.3 (18.7) |
| q17_large_trades | 4.955 (5.150) | 7.039 (8.056) | 7.989 (8.926) | 47.4 (50.8) | 106.0 (108.2) |
| q18_asof_backward_agg | 6.215 (6.430) | 7.273 (7.535) | 25.6 (26.6) | 16.2 (17.0) | 92.8 (96.8) |
| q19_asof_backward_tol_agg | 7.325 (7.490) | 8.511 (8.814) | 27.6 (28.7) | 16.7 (17.3) | 93.5 (93.8) |
| q20_asof_forward_tol_agg | 6.580 (7.350) | 7.515 (7.860) | 34.9 (39.1) | 16.7 (17.6) | n/a |

What the table suggests, and only suggests at this size:

- Over the network, ANKAA is ahead of the three engines on all seventeen queries; the closest are the wide sorted
  result (`q17`, 7.0 against DuckDB's 8.0 in-process) and the count of crossed quotes (`q10`, 0.70 against 1.06).
- The timed runs repeat one statement, so all but the first use ANKAA's plan cache; a statement seen for the
  first time costs 0.3–0.6 ms more. QuestDB also caches compiled queries; DuckDB and ClickHouse plan every run.
- `q04` uses ANKAA's `vwap()` aggregate (exact integer sums); written with floating-point divisions as in the
  other dialects it takes about 1.6 ms in-process instead of 1.0. Every other query is written the same way in
  every dialect, `q16` included (the planner now aggregates by symbol before joining the names).
- The Arrow Flight path adds 0.1–2.1 ms to a query.

### What the new operators do, on and off (same VM, same binary)

Best of five after two warm-ups, milliseconds for plan plus execution, `ankaad replay` on the capture above.
`ANKAA_DENSE_AGGREGATION=off` leaves the same queries to DataFusion's hash aggregate.

| query | hash aggregate | ANKAA operators |
|---|---|---|
| trades per second (`GROUP BY date_bin`) | 3.02 | 1.64 |
| per symbol and minute: count, min, max, sum | 4.30 | 2.11 |
| per symbol and 10 s over 4.85 M book deltas | 21.0 | 8.57 |
| count, sum, min, max by symbol over 4.85 M book deltas | 12.4 | 0.63 |
| the same inside a 50-minute time range | 12.6 | 0.88 |

| query | SQL window functions over 1 s bars | `rolling()` |
|---|---|---|
| bars over a trailing 5 minutes, every second | 45.1 | 7.04 |
| bars over a trailing hour, every second | 40.7 | 8.27 |

(`rolling()` returns a row for every second whose window is not empty; the window-function query only for seconds
that have trades, so it does less.)

### What the day's fixes did (same VM)

The comparison ran ten times. The first two used a synthetic dataset (the bundled 20-second capture repeated
1,000 times, one symbol) and are superseded; they found problems 2–5 below. The others used the real capture
above (`ankaa-flight`, median ms; run 5 is after problems 6 and 8, run 8 after all twelve, run 9 after the
five scan and planning changes listed under them, run 10 after the profiling round of 11 November):

| query | run 3 | run 5 | run 8 | run 9 | run 10 |
|---|---|---|---|---|---|
| q01_range_count | 1.96 | 1.96 | 1.06 | 0.48 | 0.47 |
| q02_agg_by_symbol | 4.06 | 4.06 | 1.12 | 0.68 | 0.67 |
| q03_latest_per_symbol | 14.3 | 5.1 | 3.3 | 2.4 | 2.5 |
| q05_ohlcv_1s | 16.0 | 8.9 | 6.9 | 6.9 | 4.4 |
| q12_percentiles_price | 11.2 | 10.9 | 4.9 | 4.8 | 3.8 |
| q14_trades_per_second | 4.8 | 6.1 | 2.3 | 1.8 | 1.9 |
| q15_book_delta_stats | 23.1 | 23.6 | 14.5 | 13.9 | 12.4 |
| q18_asof_backward_agg | 17.0 | 12.2 | 10.0 | 9.6 | 7.3 |
| q20_asof_forward_tol_agg | 17.8 | 14.5 | 9.9 | 9.9 | 7.5 |

## Problems these runs found, all fixed the same day

1. **A benchmark used up the VM's memory.** `wal/append_no_commit` appended without ever committing, so
   its buffer grew with every iteration until the 16 GB VM killed it. It now measures in bounded batches.
2. **Every Arrow Flight query paid about 83 ms.** The server never disabled Nagle's algorithm on the sockets
   it accepts (`Server::tcp_nodelay` does not apply to listeners passed in), so each response waited for the
   client's delayed ACK, twice per query. `TCP_NODELAY` is now set on every accepted socket (Flight, Postgres,
   `/metrics`), with a test that checks the socket option.
3. **Our own tools charged ANKAA for printing results.** `ankaad replay` stopped its query clock after
   writing the CSV and pretty-printing the result, so queries returning thousands of rows looked 3–10× slower
   than they were. The clock now stops after collect, which is the scope the harness documents; `ankaad query`
   had the same flaw and the same fix.
4. **Scans copied every column and handed over 65,536-row batches.** A scan now gives Arrow the chunk's own
   memory (no copy), in batches of the session size (8,192 rows) that report their own size to the memory pool.
5. **The quantile sketch recounted its buckets on every value.** `percentiles()` on trade sizes (hundreds of
   buckets) was four times slower than on prices (a few). The count is now kept as it changes, and the bucket
   budget became a hard bound.
6. **Table functions were slower inside the server than alone.** They copied their whole input into one buffer
   per call; under the server, or under an aggregation, the system allocator handed that memory back to the
   kernel and page-faulted it in again every time (about 6 ms per call here; tuning the allocator made it
   disappear, which is how it was found). `ohlcv`, `percentiles` and `asof_join` now work on the scan's batches
   where they lie and no longer sort or copy a table as a whole. Input that is not in stored order takes a
   second path that groups and sorts per key; tests check both against an independent oracle. Some variation
   remains on the server path (previous section); evaluating another allocator for the server is open work.
7. **The harness and the capture tool tripped on real data.** The QuestDB loader refused the nanosecond
   timestamps of order-book snapshot rows (QuestDB's `TIMESTAMP` holds microseconds; they are floored now), and
   `ankaa-capture` retried a malformed stream list forever instead of refusing it at start.
8. **The planner knew nothing about our tables.** A scan reported no statistics, so for "the latest row per
   symbol" (`q03`) DataFusion hash-partitioned the whole trades table to join it with a 12-row aggregate. Scans
   now report row counts and key ranges; the join builds on the small side (11.7 ms to 4.6 ms in-process), and a
   bare `count(*)` needs no scan.

9. **Queries never reached the SIMD kernels.** Profiling showed that every query ran on DataFusion's operators
   alone; the kernels were only called by their own tests. Aggregates grouped by small keys (dictionary ids,
   flag bytes, time buckets) and aggregates without `GROUP BY` now run on `DenseAggExec`: group numbers by
   arithmetic instead of hashing, DataFusion's own accumulators for the functions, and, over a chunk table,
   statistics and kernels instead of rows. A differential test suite compares it with the hash aggregate on every
   table layout; the first version of it failed that suite (groups without rows came back as values instead of
   `NULL`) and was corrected before it ran anywhere else.
10. **Run-to-run variation on the server.** The glibc allocator returned query buffers to the kernel and faulted
    them in again. Four alternating rounds per allocator: with mimalloc every query was 2–17 % faster over Flight
    and the ASOF queries stopped varying (8.33–8.35 ms against 8.8–11.6 ms), for about 19 % more resident memory.
    `ankaad` links mimalloc by default.
11. **A snapshot copied every open chunk.** Four times a second, each changed open chunk was copied whole on the
    ingest loop (measured on a laptop: 42 ms per snapshot for a thousand busy partitions). A snapshot now copies
    only the rows added since the previous one (0.3 ms in the same test).
12. **The server binary did not contain the SVE2 kernels**, and two client paths planned every query twice
    (`GetFlightInfo`, then `DoGet`). The build enables them; `ankaad query` and the Python client send one `DoGet`.

After the twelve, five changes to scans and planning made the last gaps to the other engines close (run 9):

- **Time comparisons are exact filters.** The scan already cuts chunks to the time range by binary search;
  DataFusion no longer evaluates the same predicate again on every row.
- **Scans are in time order and can run backwards.** `ORDER BY ts LIMIT n` and `ORDER BY ts DESC LIMIT n` read
  the start or the end of each partition, also below a filter: the latest 100 trades take 0.32 ms on this VM,
  the latest 100 of one symbol above a size 0.78 ms (they read the whole table before).
- **`vwap(price, size)`**, an aggregate with exact integer sums.
- **Plans are reused** for statements seen before, unless they call a function of the clock; any change of the
  catalog empties the cache.
- **Aggregating before joining** names is documented as the idiom; the planner does not make that rewrite.

A second profiling round on 11 November, with `perf` on this VM and result printing excluded, led to run 10:

- **An own merge for sorted partitions.** `ORDER BY ts, seq` merged 17 sorted partitions through DataFusion's
  row-encoded loser tree (55 % of the query in the tree and `memcmp`). Chunks now record whether their rows are in
  (time, sequence) order, and `OrderedMergeExec` compares the two keys as numbers and copies rows in runs:
  `q17` 9.6 to 7.0 ms over Flight, 8.9 to 5.0 in-process.
- **The planner aggregates before a join** (`count`, `sum`, `min`, `max` over dictionary-id equi-joins, exact
  whatever the other side holds): `q16` as written 3.0 to 1.1 ms.
- **One division per bucket** in `ohlcv` and `rolling` (it was one per row, 50 % of the query), a branch-free
  order check, whole-batch accumulators for aggregates without `GROUP BY`, keys constant in a chunk not read row by
  row, a faster sketch increment: `q05` 6.7 to 4.4 ms, `q12` 4.8 to 3.8, `q18` 9.6 to 7.3.
- Measured and not adopted: inlined LSE atomics (no change) and `-C target-cpu=neoverse-v2` (about 1 % on ingest,
  up to 7 % on one query), not worth a binary that only runs on that CPU generation.

Also changed by these runs: SVE2 is preferred only above 128-bit vectors (see the SIMD table), and a mismatching
result is now asked for again before it is recorded, so a result that is wrong once can be told from one that is
wrong every time.

**One thing left unexplained.** In the second synthetic run QuestDB's `q02` returned a `sum(size)` that was low
by exactly 12 × 2^32 while its row count, minima and maxima were right. It did not happen in the first run, nor in
any run on the real capture, nor in 600 repetitions of the query afterwards. We do not know the cause and draw
no conclusion from it.

## Reproduce

```sh
# a Google Cloud project with billing, gcloud logged in (see the header of scripts/gcp/ankaa-gcp.sh)
scripts/gcp/ankaa-gcp.sh create && scripts/gcp/ankaa-gcp.sh provision && scripts/gcp/ankaa-gcp.sh deploy
scripts/gcp/ankaa-gcp.sh first-light && scripts/gcp/ankaa-gcp.sh wait     # -> target/gcp-results/
# on a SEPARATE VM (nothing else may run there): the first `compare` creates it and runs on the bundled capture
ANKAA_GCP_VM=ankaa-bench scripts/gcp/ankaa-gcp.sh compare
# record an hour of live data on that VM (comma-separated streams), then compare on it:
#   target/release/ankaa-capture --streams btcusdt@trade,btcusdt@depth@100ms,ethusdt@trade,... \
#       --snapshots --duration-secs 3600 --out ~/captures
ANKAA_GCP_VM=ankaa-bench COMPARE_CAPTURE='/home/<user>/captures/<file>.ankc' scripts/gcp/ankaa-gcp.sh compare
ANKAA_GCP_VM=ankaa-bench scripts/gcp/ankaa-gcp.sh compare-wait             # -> target/gcp-results/compare/
```
