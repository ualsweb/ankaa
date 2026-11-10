-- Percentiles: ANKAA's percentiles() approximates the order statistic sorted[floor(q*(n-1))] (0-based)
-- within 1 % relative error; the reference computes exactly that statistic (and the next one as <col>_hi).
-- DuckDB dialect. Tables loaded from the Parquet export (prepare.py): ts is TIMESTAMP_NS (UTC, naive),
-- symbol UINTEGER, side UTINYINT, price/size/seq/update_id BIGINT.
-- {T0}/{T1} are rendered as TIMESTAMP_NS literals.
-- Buckets are computed on integer nanoseconds (epoch_ns / make_timestamp_ns) so no precision is lost
-- (time_bucket() would round-trip through microsecond TIMESTAMP).

-- @query q01_range_count
SELECT count(*) AS n, sum(size) AS volume, min(price) AS lo, max(price) AS hi
FROM trades
WHERE ts >= {T0} AND ts < {T1}

-- @query q02_agg_by_symbol
SELECT symbol, count(*) AS n, min(price) AS lo, max(price) AS hi, sum(size) AS volume,
       min(ts) AS first_ts, max(ts) AS last_ts
FROM trades
GROUP BY symbol
ORDER BY symbol

-- @query q03_latest_per_symbol
SELECT t.symbol, t.ts, t.price, t.size, t.seq
FROM trades t
JOIN (SELECT symbol, max(seq) AS seq FROM trades GROUP BY symbol) m
  ON m.symbol = t.symbol AND m.seq = t.seq
ORDER BY t.symbol

-- @query q04_vwap_by_symbol
SELECT symbol, count(*) AS n,
       sum((price / 100000000.0) * (size / 100000000.0)) / sum(size / 100000000.0) AS vwap
FROM trades
WHERE ts >= {T0} AND ts < {T1}
GROUP BY symbol
ORDER BY symbol

-- @query q05_ohlcv_1s
SELECT symbol,
       make_timestamp_ns((epoch_ns(ts) // 1000000000) * 1000000000) AS bucket,
       first(price ORDER BY ts, seq) AS open, max(price) AS high, min(price) AS low,
       last(price ORDER BY ts, seq) AS close, sum(size) AS volume,
       (sum(price::HUGEINT * size::HUGEINT) // 100000000)::BIGINT AS notional,
       count(*) AS count
FROM trades
GROUP BY symbol, bucket
ORDER BY symbol, bucket

-- @query q06_ohlcv_5s
SELECT symbol,
       make_timestamp_ns((epoch_ns(ts) // 5000000000) * 5000000000) AS bucket,
       first(price ORDER BY ts, seq) AS open, max(price) AS high, min(price) AS low,
       last(price ORDER BY ts, seq) AS close, sum(size) AS volume,
       (sum(price::HUGEINT * size::HUGEINT) // 100000000)::BIGINT AS notional,
       count(*) AS count
FROM trades
GROUP BY symbol, bucket
ORDER BY symbol, bucket

-- @query q07_asof_backward
SELECT t.ts, t.symbol, t.price, t.size, t.side, t.seq,
       q.ts AS quotes_ts, q.bid AS quotes_bid, q.bid_size AS quotes_bid_size,
       q.ask AS quotes_ask, q.ask_size AS quotes_ask_size, q.update_id AS quotes_update_id
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
ORDER BY t.ts, t.seq

-- @query q08_asof_backward_tol
WITH j AS (
  SELECT t.*, q.ts AS qts, q.bid, q.bid_size, q.ask, q.ask_size, q.update_id,
         (q.ts IS NOT NULL AND epoch_ns(t.ts) - epoch_ns(q.ts) <= 100000000) AS ok
  FROM trades t
  ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
)
SELECT ts, symbol, price, size, side, seq,
       CASE WHEN ok THEN qts END AS quotes_ts, CASE WHEN ok THEN bid END AS quotes_bid,
       CASE WHEN ok THEN bid_size END AS quotes_bid_size, CASE WHEN ok THEN ask END AS quotes_ask,
       CASE WHEN ok THEN ask_size END AS quotes_ask_size, CASE WHEN ok THEN update_id END AS quotes_update_id
FROM j
ORDER BY ts, seq

-- @query q09_asof_forward_tol
WITH j AS (
  SELECT t.*, q.ts AS qts, q.bid, q.bid_size, q.ask, q.ask_size, q.update_id,
         (q.ts IS NOT NULL AND epoch_ns(q.ts) - epoch_ns(t.ts) <= 1000000000) AS ok
  FROM trades t
  ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts <= q.ts
)
SELECT ts, symbol, price, size, side, seq,
       CASE WHEN ok THEN qts END AS quotes_ts, CASE WHEN ok THEN bid END AS quotes_bid,
       CASE WHEN ok THEN bid_size END AS quotes_bid_size, CASE WHEN ok THEN ask END AS quotes_ask,
       CASE WHEN ok THEN ask_size END AS quotes_ask_size, CASE WHEN ok THEN update_id END AS quotes_update_id
FROM j
ORDER BY ts, seq

-- @query q10_crossed_quotes
SELECT count(*) AS n_quotes, sum(CASE WHEN bid >= ask THEN 1 ELSE 0 END) AS crossed
FROM quotes

-- @query q11_spread_stats
SELECT symbol, count(*) AS n, min(ask - bid) AS min_spread, max(ask - bid) AS max_spread,
       sum(ask - bid) AS sum_spread, avg(ask - bid) AS avg_spread, avg(bid_size + ask_size) AS avg_depth
FROM quotes
GROUP BY symbol
ORDER BY symbol

-- @query q12_percentiles_price
WITH s AS (SELECT symbol, list_sort(list(price)) AS l, count(*) AS n FROM trades GROUP BY symbol)
SELECT symbol, n AS count, l[1]::DOUBLE AS min, l[n]::DOUBLE AS max,
       l[floor(0.5 * (n - 1))::BIGINT + 1]::DOUBLE AS p50, l[ceil(0.5 * (n - 1))::BIGINT + 1]::DOUBLE AS p50_hi,
       l[floor(0.9 * (n - 1))::BIGINT + 1]::DOUBLE AS p90, l[ceil(0.9 * (n - 1))::BIGINT + 1]::DOUBLE AS p90_hi,
       l[floor(0.99 * (n - 1))::BIGINT + 1]::DOUBLE AS p99, l[ceil(0.99 * (n - 1))::BIGINT + 1]::DOUBLE AS p99_hi
FROM s
ORDER BY symbol

-- @query q13_percentiles_size
WITH s AS (SELECT list_sort(list(size)) AS l, count(*) AS n FROM trades)
SELECT n AS count, l[1]::DOUBLE AS min, l[n]::DOUBLE AS max,
       l[floor(0.5 * (n - 1))::BIGINT + 1]::DOUBLE AS p50, l[ceil(0.5 * (n - 1))::BIGINT + 1]::DOUBLE AS p50_hi,
       l[floor(0.95 * (n - 1))::BIGINT + 1]::DOUBLE AS p95, l[ceil(0.95 * (n - 1))::BIGINT + 1]::DOUBLE AS p95_hi,
       l[floor(0.99 * (n - 1))::BIGINT + 1]::DOUBLE AS p99, l[ceil(0.99 * (n - 1))::BIGINT + 1]::DOUBLE AS p99_hi
FROM s

-- @query q14_trades_per_second
SELECT make_timestamp_ns((epoch_ns(ts) // 1000000000) * 1000000000) AS bucket, count(*) AS n, sum(size) AS volume
FROM trades
GROUP BY bucket
ORDER BY bucket

-- @query q15_book_delta_stats
SELECT symbol, side, count(*) AS n, sum(CASE WHEN size = 0 THEN 1 ELSE 0 END) AS deletes,
       max(update_id) AS last_update_id
FROM book_deltas
GROUP BY symbol, side
ORDER BY symbol, side

-- @query q16_range_by_name
SELECT s.name, count(*) AS n, min(t.price) / 100000000.0 AS lo, max(t.price) / 100000000.0 AS hi
FROM trades t JOIN symbols s ON s.symbol = t.symbol
WHERE t.ts >= {T0} AND t.ts < {T1}
GROUP BY s.name
ORDER BY s.name

-- @query q17_large_trades
SELECT ts, symbol, price, size, side, seq
FROM trades
WHERE size >= 5000000
ORDER BY ts, seq

-- @query q18_asof_backward_agg
SELECT count(*) AS n, count(q.ts) AS matched, sum(q.ask - q.bid) AS spread_sum
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts

-- @query q19_asof_backward_tol_agg
SELECT count(*) AS n,
       count(CASE WHEN epoch_ns(t.ts) - epoch_ns(q.ts) <= 100000000 THEN q.ts END) AS matched,
       sum(CASE WHEN epoch_ns(t.ts) - epoch_ns(q.ts) <= 100000000 THEN q.ask - q.bid END) AS spread_sum
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts

-- @query q20_asof_forward_tol_agg
SELECT count(*) AS n,
       count(CASE WHEN epoch_ns(q.ts) - epoch_ns(t.ts) <= 1000000000 THEN q.ts END) AS matched,
       sum(CASE WHEN epoch_ns(q.ts) - epoch_ns(t.ts) <= 1000000000 THEN q.ask - q.bid END) AS spread_sum
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts <= q.ts
