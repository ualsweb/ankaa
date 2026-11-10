# Temporal architecture

How ANKAA treats time: which clocks stamp what, how ordering is enforced, what a query can see and when,
how order books are aligned in exchange time, how latency is measured today, and what the time-series table
functions mean exactly. Code references: `crates/ankaa-feeds` (decoders, live source, capture format),
`crates/ankaa-store/src/chunk.rs` and `table.rs` (watermarks), `bins/ankaad/src/serve.rs` and `tables.rs`
(snapshot swaps, book alignment), `crates/ankaa-ts` (operators), `crates/ankaa-sql/src/functions.rs` (SQL surface).

## 1. Three time domains

Every row has exactly one timestamp column, `ts`, in column 0. Every event carries two timestamps internally;
one of them becomes `ts`.

| Domain | Clock | Where it is taken | Stored as |
|---|---|---|---|
| **Exchange event time** | the exchange's clock | the message payload (`T` trade time, else `E` event time, for Binance JSON; `transactTime`, else `eventTime`, for SBE) | `ts` of `trades`, `book_deltas`, `quotes` |
| **Receive time** | host `CLOCK_REALTIME` (`SystemTime::now()`) | the WebSocket task, immediately after reading the frame (`Frame.recv_ns`) | WAL and `.ankc` frame header; `recv_ns` on each decoded event; not a table column today |
| **Ingest time** | host `CLOCK_REALTIME` | after `Tables::ingest` returns for the frame (watermark advanced) | only as `now − recv_ns`, the ingest latency sample |

All three are **`i64` nanoseconds since the Unix epoch, UTC**. Exchange times arrive in milliseconds (JSON) or
microseconds (SBE) and are multiplied up (`ms_to_ns`, `us_to_ns`, checked for overflow); the resolution of `ts` is
therefore 1 ms for JSON feeds even though the column is nanoseconds. Arrow exposes `ts` as
`Timestamp(Nanosecond, "UTC")`; in SQL it behaves as a timestamp and `date_bin`, comparisons and `BETWEEN` all work.

The engine never mixes domains in one column. Ordering, watermarks, bucketing and ASOF joins are all in exchange
time. The one deliberate exception is the placement of REST book snapshots (§5).

There is no leap-second handling beyond what the exchange and the host clock do; both are Unix time.

## 2. Ordering and watermarks

A **partition** is one symbol of one table (`Table::partition_for(symbol_id)`; partitions are created on demand).
Each partition owns one open chunk and a list of sealed chunks, and has a **watermark**: the maximum `ts` it has
accepted (`i64::MIN` when empty). The watermark survives chunk rolls (`Chunk::set_watermark` on the new open chunk).

The append rule: a row with `ts >= watermark` is appended (ties are fine, many trades share a millisecond). A row
older than the watermark is accepted only when the table allows lateness (`max_lateness_ms` in `[[tables]]`, 0 by
default and for the exchange tables) and it is no older than `watermark - max_lateness`: it is then **inserted at its
place in time** in the open chunk (`Chunk::insert_row`, after the rows with an equal or smaller `ts`, so equal
timestamps keep their arrival order) and counted in `ankaa_late_rows_total`. Anything older is rejected with
`AppendError::OutOfOrder`, counted in `PartitionStats::dropped_out_of_order` and `ankaa_dropped_rows_total`, and not
stored. Every chunk is therefore in time order, which is what binary-search pruning and the streaming operators rely
on; with lateness, consecutive chunks of a partition may overlap in time, which scans detect from the chunks' time
ranges (an ordered scan then starts a new partition instead of assuming order across them).

Whether a late row is kept depends only on the rows accepted before it, so a crash recovery that replays the log
makes the same decisions, and the rows each chunk holds are decided by arrival order (the first 65,536 rows of a
chunk, wherever they sort). The recovery machinery, which is based on log positions, is unchanged.

Why per symbol and not per table: exchanges order messages per instrument, not globally; a combined stream
interleaves symbols arbitrarily, so a later frame for ETHUSDT may legitimately carry an earlier `ts` than the last
BTCUSDT frame. One watermark per symbol accepts that (`multi_symbol::symbols_do_not_share_watermarks` test).

Why no lateness for the exchange tables: a feed that delivers out of order within one instrument is a decoder bug or
a transport problem, and the engine prefers to make it visible (counter, log). Telemetry is different: devices buffer
while offline and gateways batch, so readings arrive late as a matter of course, and a user table sets the bound it
needs (up to seven days). Inserting a late row moves the rows after it in the open chunk, so very frequent, very late
rows cost more than appends; rows beyond the bound are dropped, never reordered across sealed chunks.

Chunks hold ≤ 65,536 rows; a full chunk is sealed (immutable, zone map computed: min/max per column, including `ts`)
and a new open chunk starts. Sealing also happens for every partition at shutdown (`seal_all`).

## 3. Visibility: snapshots every `snapshot_ms`

Queries never read the open chunks that the ingest thread writes. Every `storage.snapshot_ms` (default 50 ms,
minimum 5) the serve loop calls `Table::snapshot()` on each table and swaps the result into the long-lived
DataFusion providers (`ChunkTable::swap_snapshot`). A snapshot is:

- the `Arc`s of every sealed chunk (shared, zero copy),
- a view of each non-empty open chunk, made of sealed copies of consecutive row ranges ("pieces"). A snapshot
  copies only the rows appended since the previous one into a new piece and merges it with its predecessors while
  they are not larger: at most `log2(rows)` pieces per open chunk, and a row is copied `O(log rows)` times over the
  life of its chunk. A snapshot therefore costs the rows that arrived since the last one, not the rows that are open
  (measured on a laptop: 0.3 ms for a thousand partitions with 32,000 open rows each, where copying every open
  chunk took 42 ms),
- `min_watermark`: the minimum watermark across non-empty partitions.

Running scans keep the chunk set they started with (the provider holds an `Arc<Vec<Arc<Chunk>>>` and the swap replaces
the pointer), so a query sees one consistent set of chunks for its whole execution. This gives **read-committed
visibility per partition**: a query sees every row accepted by the ingest thread up to the last swap, and no row
that was in flight. Different partitions may be visible to different points in time (their own watermarks); a query
that needs a cross-symbol consistent cut should restrict to `ts <= min_watermark`, which is not yet exposed as a SQL
value. That is a known gap.

Consequences for users:

- Data freshness is bounded by `snapshot_ms` plus ingest latency. A row is queryable at the latest about one swap
  interval after its frame was decoded.
- Two queries issued back to back may see different snapshots; within one query the view is stable.
- `symbols` is re-registered on a swap only when the dictionary grew, so a symbol's name may appear one tick after its
  first rows.
- The counters `ankaa_rows`, `ankaa_sealed_chunks`, `ankaa_trades_total`, `ankaa_book_deltas_total` and
  `ankaa_feed_gaps_total` are copied on the same tick, which is why they move in steps of the interval.

Snapshots are also the read path for `replay`: all chunks are sealed after ingest and registered once.

## 4. Late and out-of-order data

| Situation | Where it is caught | Effect | Counter |
|---|---|---|---|
| Row for symbol S with `ts` below S's watermark | appender | row dropped | `ankaa_dropped_rows_total` |
| Depth message whose `U` is not `previous u + 1` | decoder sequence tracker | counted; the book synchroniser decides (below) | `ankaa_feed_gaps_total` |
| Depth message with `last_id <= book.last_id` (stale: raced the snapshot or was replayed) | `BookSync` (synced state) | message ignored | `ankaa_book_stale_messages_total` |
| Depth message not contiguous after sync | `BookSync` | book goes back to buffering, snapshot requested, the message is kept for after it | `ankaa_book_resyncs_total` |
| Buffered diffs older than the snapshot (`last_id <= snapshot_id`) | `BookSync::on_snapshot` | discarded | `dropped_stale` |
| Snapshot older than the buffered stream, or a gap inside the buffer | `BookSync::on_snapshot` | book reset, new snapshot requested | `ankaa_book_resyncs_total` |
| Frame that cannot be decoded | decoder | frame skipped, logged | `ankaa_frames_bad_total` |
| Frame the engine could not keep up with | source channel (65,536 frames) | frame dropped, never blocks the socket | `ankaa_source_dropped_full_total` (see `metrics-semantics.md`) |

Exchange clocks are not monotonic across reconnects either: on a `connect` control frame the decoders reset their
sequence state (`reset_sequences`), so the first message after a reconnect is never counted as a gap, and every
synced book is told to expect a new snapshot (`Tables::on_reconnect`). Watermarks are **not** reset on reconnect:
if the exchange re-sends messages older than what was already stored, those rows are dropped as out of order, which
is the intended de-duplication by time.

## 5. Order books in the exchange time domain

The L2 book for a symbol is rebuilt from a REST snapshot (`lastUpdateId`, bids, asks) plus the diff stream
(`U..u` per message). `ankaa_ts::BookSync` implements the documented alignment: buffer diffs until the snapshot,
drop those with `u <= lastUpdateId`, the first applied diff must straddle `lastUpdateId + 1`
(`U <= lastUpdateId + 1 <= u`), every later diff must have `U == previous u + 1`, otherwise resync.

A REST snapshot has **no exchange timestamp**, only an id. The decoder stamps the `BookSnapshot` with `recv_ns`
for bookkeeping, but writing its levels to `book_deltas` at receive time would violate partition ordering (the host
clock is ahead of or behind the exchange clock by an unknown offset). `Tables` therefore holds the snapshot rows as
pending and writes them **just before the first diff applied on top of it, at `diff.ts − 1 ns`** (exchange time):
one `book_deltas` row per level with `first_update_id = 0` and `update_id = lastUpdateId`, followed by one `quotes`
row with the top of book after the snapshot. The diff's own rows follow at `diff.ts`. This keeps every partition
ordered in exchange time and makes a snapshot recognisable in SQL (`first_update_id = 0`).

Each exchange message is applied **atomically** to the book before the top of book is sampled: levels of one message
may cross transiently (best bid ≥ best ask) and that intermediate state is never emitted. One `quotes` row is written
per message **that changed the top**, with the message's `ts` and `u`. `Tables::crossed_messages` counts messages
after which the book is still crossed (a data-quality signal expected to be 0; exported as `ankaa_book_crossed_messages_total`).

In `replay`, captures recorded with `ankaa-capture --snapshots` contain snapshot frames and are replayed with
`--expect-snapshots`, giving the same exact books offline. Captures without snapshots start books cold: diffs are
applied from the first message and `quotes` are incomplete until levels churn.

## 6. Replay reuses receive timestamps

`.ankc` captures and WAL records store each frame with the `recv_ns` the live source stamped. Replay (`ankaad
replay`, and WAL recovery inside `serve`) feeds `(payload, recv_ns)` to the same decoders, so:

- `ts` values are identical to the live run (they come from the payload);
- `recv_ns` is the original receive time, not the replay time, so a capture replays "as recorded" at any speed;
- the ingest latency recorded during WAL recovery is `now − original recv_ns`, i.e. the age of the record, which is
  why the latency summary is meaningless until live samples dominate (see `metrics-semantics.md`);
- control frames (`connect`, `disconnect`) are replayed too, so sequence resets and snapshot expectations happen at
  the same points as live.

Captures are read in the file order given on the command line and are expected to be in time order; frames inside
a capture are in arrival order.

## 7. How latency is measured today

**Software clock, informative only.** The one latency the engine measures is receive-to-watermark:

```
recv_ns    = SystemTime::now()  in the WebSocket task, when the frame is read
ingest_ns  = SystemTime::now()  in the ingest loop, after Tables::ingest for that frame returned
sample     = max(ingest_ns − recv_ns, 0)
```

The sample goes to `ankaa_last_ingest_latency_ns` (gauge, last value) and to a fixed-point histogram with 10 µs
buckets up to 1 s (`ankaa_ingest_latency_ns` summary, lifetime quantiles). It includes channel queueing, decoding,
book alignment and appends, and excludes the network, the kernel, and the wait for the next snapshot swap
(up to `snapshot_ms`). Both reads are of the same host clock, so NTP slew cancels and only steps between the two
reads matter.

What this is not: a wire-to-queryable figure, a benchmark, or comparable between hosts. The protocol for numbers the
project will publish is in `docs/measurement-protocol.md`: NIC hardware timestamps in the PTP hardware clock (PHC)
domain, the CPU counter (`CNTVCT_EL0`) calibrated to the PHC with an offset re-verified periodically, the calibration
residual published next to every figure, fixed-point histograms rather than sketches on the measurement path, and
software timestamps reported separately and labelled informative. That path (`ankaa-time`, `ankaa-wire`) is Phase 3
work and is not in this build; until then the repository publishes no latency numbers.

## 8. SQL time semantics

Tables: `trades(ts, symbol, price, size, side, seq)`, `book_deltas(ts, symbol, side, price, size, first_update_id,
update_id)`, `quotes(ts, symbol, bid, bid_size, ask, ask_size, update_id)`, `symbols(symbol, name)`. `ts` is
`Timestamp(ns, UTC)`; prices and sizes are `Int64` fixed-point with 8 decimals (`/1e8`).

**Time-range pushdown.** Filters of the form `ts <op> literal`, `literal <op> ts` and `ts BETWEEN a AND b` are
extracted and used to skip chunks by zone map and to cut chunks by binary search on `ts`. The scan returns exactly the
rows inside those bounds, so such a filter is declared *exact* and is not evaluated again on every row (`EXPLAIN`
shows no `FilterExec` for it). Any other filter only prunes chunks and stays in the plan.

**Time order.** A scan is cut into partitions that are each in time order (the chunks of one partition of the table
follow each other in time), up to 64 of them, and tells the planner so. `ORDER BY ts` then merges the partitions
instead of sorting them; `ORDER BY ts DESC` runs the same scan backwards; with a `LIMIT`, each partition stops as
soon as it has given enough rows, also below a filter (`WHERE symbol = 3 AND price > x ORDER BY ts DESC LIMIT n`
reads the end of the table, not the table). `EXPLAIN` shows `reverse` on the scan and no `SortExec`. Sorting on
anything but the timestamp alone works as before.

**Plans are reused.** The server keeps the optimised logical plan of a statement and, when the same text arrives
again, only builds the physical plan (which binds the scan to the chunks that exist at that moment). A plan is
kept only if it is a query that calls no function of the clock or of chance (`now()`, `random()`…), and the cache
is emptied whenever a table is registered, removed or replaced.

**Aggregation over small keys.** A `GROUP BY` on dictionary-id and byte columns of a chunk table (`symbol`, `side`,
any key column of a user table), on `date_bin(interval, ts)` over the table's timestamp (a fixed-width interval,
default origin), on any combination of them, and any aggregate without `GROUP BY`, is run by `DenseAggExec` instead
of the hash aggregate when the chunks' statistics show at most 1,048,576 possible key combinations. A row's keys
become a number by arithmetic (the bucket of a row is `(ts - first bucket) / width`; no key is hashed or compared,
nothing is exchanged between partitions) and groups are numbered as they first appear; the aggregate functions are
DataFusion's own accumulators, so results are identical (`EXPLAIN` shows the operator; a differential test suite
compares both on every table layout). `ANKAA_DENSE_AGGREGATION=off` in the environment turns the operator off.

When nothing but a time range lies between the aggregate and the table, chunks whose keys are constant are not turned
into rows at all (a table partitioned by symbol stores one symbol per chunk). Every sealed chunk keeps the minimum,
maximum and sum of each 1,024-row block of its integer columns (24 bytes per 8 KiB of data), and segments on disk
carry the same three numbers per column. `count(*)`, `min`, `max` and `sum` of a column then come from those
statistics: for a chunk inside the time range nothing is read (an encoded chunk is not decoded); for a live chunk
cut by the range only the two partial blocks at the ends are read, with SIMD kernels (NEON, or SVE2 for the kernels
where it measured faster). The cost of such an aggregate grows with the number of chunks, not with the number of
rows. Everything else (`GROUP BY` on a wide or computed key, `DISTINCT` on one, ordered aggregates, `date_bin`
with months or an explicit origin) is planned as before.

### Table functions

All four read the named tables from the session (the current snapshot), compute in memory, and
return a materialised table. They are suitable for the hot window and for replays; incremental versions are later work.

**`asof_join(left, right, key [, tolerance_ns [, 'forward']])`**

For each left row, the right row with the same `key` and the greatest `right.ts <= left.ts` (backward, default) or
the smallest `right.ts >= left.ts` (`'forward'`). The join runs key by key: each key's left and right rows are walked
once in time order. A partitioned table already stores a key's rows in that order (equal timestamps in ingest order),
so nothing is sorted as a whole; the order is checked per key and only a key found out of order is sorted (by `ts`,
then by the table's last column, `seq` or `update_id`). Among equal `right.ts` the last one in that order wins for
backward and the first for forward. `tolerance_ns` bounds `|left.ts − right.ts|`; a candidate outside it yields no
match. Without it the match is unbounded in time. Unmatched left rows are kept with `NULL` right columns (left outer
semantics). Output: all left columns, then every right column except the key, named `<right>_<column>` (e.g.
`quotes_bid`); only the columns the query uses are read from the tables and built. Output rows are the left rows in
scan order (key by key for a partitioned table), **not** in global time order: add `ORDER BY` when the order matters.
Complexity is O(n + m).

```sql
SELECT t.ts, s.name, t.price/1e8 px, (quotes_ask - quotes_bid)/1e8 spread
FROM asof_join('trades', 'quotes', 'symbol', 500000000) t          -- within 500 ms
JOIN symbols s ON s.symbol = t.symbol
```

**`rolling(table, window_ns, step_ns [, key])`**

For every `key` (default `symbol`) and every step end `e` on the `step_ns` grid, the bar of the trailing window
`[e - window_ns, e)`: `window_end` (= `e`), `open`, `high`, `low`, `close`, `volume`, `notional`, `count`, as `ohlcv`
defines them. `window_ns` must be a positive multiple of `step_ns`. A row is produced for every step end whose window
holds at least one row, also when the last step itself is empty. Rows are bucketed once into steps and each window is
put together from its steps with a sliding queue (two stacks, amortised constant time, no inverse operation needed,
so `high`/`low` cost the same as sums): a one-hour window that moves every second costs the same per output row as a
one-second one. The continuous query kind `rolling` maintains the same windows as rows arrive (see docs/continuous-queries.md).

```sql
SELECT window_end, s.name, close/1e8 AS last, high/1e8 AS high_5m, volume/1e8 AS volume_5m
FROM rolling('trades', 300000000000, 1000000000) r JOIN symbols s ON s.symbol = r.symbol   -- 5 min, every second
```

**`ohlcv(table, width_ns [, key])`**

Fixed-width buckets of `width_ns` nanoseconds **aligned to the Unix epoch** (`floor(ts / width) * width`, floor
division so negative timestamps align too), per `key` (default `symbol`). A 60 s bucket starts at `:00`, a 1 h bucket
at the top of the hour UTC; there is no offset argument. Buckets with no rows are not emitted. Output per key and
bucket: `bucket` (start, `Timestamp(ns, UTC)`), `open`, `high`, `low`, `close` (fixed-point price ticks), `volume`
(sum of `size` ticks), `notional` (Σ price·size divided by 1e8, so it is in price ticks × size units; divide by 1e8
again for a decimal value; VWAP = `notional / volume`), `count`. `open`/`close` are by `ts` order, ties by ingest
order. The table must have `price` and `size` columns.

```sql
SELECT s.name, bucket, open/1e8 o, high/1e8 h, low/1e8 l, close/1e8 c, volume/1e8 v, notional::double/volume vwap
FROM ohlcv('trades', 60000000000) b JOIN symbols s ON s.symbol = b.symbol ORDER BY bucket
```

This agrees with `GROUP BY date_bin(INTERVAL '1 minute', ts, TIMESTAMP '1970-01-01')` (tested).

**`percentiles(table, column, 'q1,q2,…' [, key])`**

Quantiles of `column` cast to `Float64` (fixed-point columns stay in raw ticks) from a UDDSketch with relative error
`α = 0.01` and a budget of 2,048 buckets per key. Output per key (or one row overall): `count`, `min`, `max`
(exact), one `pNN` column per requested quantile (`0.5 → p50`, `0.999 → p99_9`), and `collapses`. Accuracy:
each reported quantile `x̂` satisfies `|x̂ − x| ≤ α·|x|` for the true quantile `x`, i.e. **1 % relative error**, with
`p0`/`p100` exact and reported values clamped to `[min, max]`. If more than 2,048 distinct buckets are needed the
sketch collapses uniformly and the bound doubles per collapse (`α·2^collapses`); `collapses > 0` tells you this
happened. NaN and infinities are skipped; zeros and negative values are handled exactly (separate zero count,
mirrored store). The sketch is not order-dependent, so the input is not sorted.

```sql
SELECT s.name, count, p50/1e8, p99/1e8, p99_9/1e8, collapses
FROM percentiles('trades', 'size', '0.5,0.99,0.999', 'symbol') p JOIN symbols s ON s.symbol = p.symbol
```

**`vwap(price, size)`** (an aggregate, used like `sum`): `Σ price·size / Σ size` with exact integer sums and one
division per group; the result is in the unit of the price column, `NULL` when the sizes sum to zero.

```sql
SELECT symbol, vwap(price, size) / 1e8 AS vwap FROM trades WHERE ts >= '2026-11-10T17:00:00Z' GROUP BY symbol
```

**Aggregating before a join.** `SELECT s.name, count(*), min(t.price) ... FROM trades t JOIN symbols s ON s.symbol =
t.symbol GROUP BY s.name` is planned as `(trades GROUP BY symbol) JOIN symbols`, grouped again by name: the trades
become one row per symbol (answered from statistics) before the join. The rewrite is made for `count`, `sum`, `min` and
`max` of the columns of one side of an inner equi-join on dictionary ids, and is exact whatever the other side holds
(a key with two names, two keys with one name); `EXPLAIN` shows `__ankaa_partial_` columns when it applies.

**Merging sorted partitions.** `ORDER BY` on one or two integer or timestamp columns over partitions that are already
in that order (`ORDER BY ts, seq`) is merged by `OrderedMergeExec`, which compares the keys as numbers and copies rows
out in runs; with a `LIMIT`, DataFusion's own merge is kept because it stops early.

### Things to know

- Table functions take **string literals** for table and column names and integer literals for `tolerance_ns` and
  `width_ns`; expressions are not accepted.
- They operate on the whole registered table (no pushdown of outer `WHERE ts > …` into the function). Filter the
  result, or register a narrower view in a later version.
- Timestamps compare as UTC. Casting `ts` to a string in DataFusion prints it with a `Z` or `+00:00` suffix;
  `replay --out-dir` writes CSV timestamps as `%Y-%m-%dT%H:%M:%S%.9fZ`.
- Fixed-point arithmetic stays exact in `Int64`; `notional` in `ohlcv` is already divided once by 1e8 to fit `i64`.
