# Continuous queries

A continuous query is maintained incrementally as rows arrive instead of being recomputed over
history on every request. Dashboards and alerts read a small table; the operators are the same
streaming-shaped ones behind `ohlcv()`, `rolling()`, `percentiles()` and `asof_join()`.

```toml
[[continuous]]
name = "btc_1s"          # SQL tables: cq_btc_1s (closed bars) and cq_btc_1s_live (open bar per key)
kind = "ohlcv"           # ohlcv | rolling | percentiles | asof
table = "trades"
key = "symbol"
value = "price"
size = "size"
width_ns = 1000000000

[[continuous]]
name = "spread_p"
kind = "percentiles"
table = "quotes"
key = "symbol"
value = "ask"
width_ns = 60000000000
quantiles = [0.5, 0.99]

[[continuous]]
name = "btc_5m_every_1s" # rolling 5-minute high/low/volume, a row every second
kind = "rolling"
table = "trades"
key = "symbol"
value = "price"
size = "size"
width_ns = 300000000000  # the window
step_ns = 1000000000     # how often a window ends (width_ns must be a multiple)

[[continuous]]
name = "trades_q"
kind = "asof"
table = "trades"         # left side
right_table = "quotes"   # the latest quote per symbol in force at each trade
key = "symbol"
```

| kind | State per key | Output table `cq_<name>` | Live table `cq_<name>_live` |
|---|---|---|---|
| `ohlcv` | the open bucket | one row per **closed** bar: `ts, key, open, high, low, close, volume, notional, count` | the open bar per key |
| `rolling` | the open step bar, and the closed step bars of the window in a sliding queue (each combined at most three times in its life) | one row per **step end** whose window holds rows: `ts` = the window's exclusive end, then as `ohlcv`; the same rows as `rolling()` over the same data | the window that ends with the open step, rows so far |
| `percentiles` | a `UddSketch` (1 % relative error) per open window | one row per closed window: `ts, key, count, min, max, p50, p99, …` | the open window per key |
| `asof` | the latest right row per key | one row per left row that had a right row in force: left columns + `<right_table>_<column>` for every other right column (all types) | — |

Semantics:
- Buckets and windows are aligned to the epoch (`floor(ts / width) * width`), the same as `ohlcv()`.
- Buckets and windows close in **event time**: when the newest timestamp of their key, minus the
  input table's `max_lateness_ms`, has passed their end. Until then a late row of the bucket is
  still counted, placed by its timestamp (`open` is the price of the earliest row, `close` of the
  latest). With the input partitioned by the query's key, every row the table accepts is in time
  for the query, so results do not depend on arrival order and a rebuild from the stored rows gives
  what live feeding gave. Closed results appear that much later; the open buckets (several per key
  with lateness) are in `cq_<name>_live`.
- A row whose bucket has already closed (older than the table's lateness window allows) is
  **dropped and counted** (`ankaa_cq_late_rows_total`); closed results are never retracted.
- A row without the query's value (`NULL` in a nullable column) is skipped, like a SQL aggregate
  skips it.
- Only rows the input table accepted are fed to a query: a row the table rejects as out of order
  is not counted in a bar either, so `cq_*` and the input always agree.
- `asof` emits a joined row only when a right row is already in force (right timestamp ≤ left
  timestamp). Only the latest right row per key is kept: a right row that arrives out of order,
  and a left row that arrives after a newer right row of its key, are counted as late and
  produce nothing.
- `max_rows` (default 1,000,000) bounds the output table: the bound is enforced every
  `max(max_rows / 16, 4096)` results, by dropping the chunks that are entirely older than the
  newest `max_rows` rows, so the table holds between `max_rows` and `max_rows` plus that step
  (about 6 % from 65,536 rows up; a very small `max_rows` still keeps a few thousand rows).
- Results are exposed through the normal snapshot swap (default 250 ms).

Rules checked at start (a violation stops the server with a message naming the query):
- `key` must be a `dict` column of the input table, or `key = ""` for a single series
  (`ohlcv` and `percentiles` only; `asof` always needs a key).
- A name must not end in `_live` (`cq_<name>_live` is the live table of `<name>`).
- `ohlcv` needs an integer or fixed-point `value` (and `size`); use `percentiles` for `float64`.
- `asof` needs two different tables that share the key dictionary. Today that means the market
  tables (`trades`, `quotes`, `book_deltas`); user tables keep one dictionary each, so a keyed
  ASOF between them is refused rather than joined on unrelated ids.
- The input of a continuous query cannot be another continuous query.

The live table has the same columns as `cq_<name>` (the key column carries the name given in
`key`, for example `symbol` or `rack`) and is exchanged atomically on every snapshot tick.

## Restarts

Continuous-query tables are **derived state**: they are kept in memory and never written to the
WAL or to segments. At start, after the input tables have been restored from segments and the
WAL has been replayed, every query is rebuilt by reading its input tables again in timestamp
order. The closed results, and the bar that was open when the server stopped, are therefore the
same after a clean restart or a crash, and a query added to the configuration is back-filled
from the retained history.

The rebuild reads every retained row of the input tables once per query (the server logs the row
count and the time); on large histories it is the main part of start-up time. When the query's
key is the column its input is partitioned by (`symbol` for the market tables, `partition_by` for
user tables) it goes one partition at a time and holds one decoded chunk per input in memory;
otherwise all partitions are merged by timestamp and one chunk per partition is held.

A rebuild sees the rows in timestamp order, live processing sees them in arrival order. Where the
two differ the results can differ: a row that was late live lands in its bar, and an ASOF left
row that arrived after a newer right row is joined to the right row that was in force at its
timestamp (rows with equal timestamps join right-before-left).

Inputs can be the market tables or any user-defined table (`docs/generic-ingest.md`).
