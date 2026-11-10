-- QuestDB dialect. Tables (prepare.py): ts TIMESTAMP (designated, MICROSECOND precision), symbol INT,
-- side INT, price/size/seq/update_id LONG. Rows are imported in (ts, seq) order so first()/last() ties
-- follow exchange order.
-- {T0}/{T1} are rendered as ISO-8601 string literals with microseconds.
-- Precision notes: QuestDB TIMESTAMP is microseconds, so nanosecond inputs are truncated (no loss for
-- Binance millisecond stamps). QuestDB has no 128-bit integer, so `notional` is a double
-- (manifest engine_overrides compares it approximately). Forward ASOF JOIN does not exist in QuestDB
-- (q09 / q20 are reported as unsupported).

-- @query q01_range_count
SELECT count() AS n, sum(size) AS volume, min(price) AS lo, max(price) AS hi
FROM trades
WHERE ts >= {T0} AND ts < {T1}

-- @query q02_agg_by_symbol
SELECT symbol, count() AS n, min(price) AS lo, max(price) AS hi, sum(size) AS volume,
       min(ts) AS first_ts, max(ts) AS last_ts
FROM trades
ORDER BY symbol

-- @query q03_latest_per_symbol
SELECT t.symbol, t.ts, t.price, t.size, t.seq
FROM trades t
JOIN (SELECT symbol, max(seq) AS seq FROM trades) m
  ON t.symbol = m.symbol AND t.seq = m.seq
ORDER BY t.symbol

-- @query q04_vwap_by_symbol
SELECT symbol, count() AS n,
       sum((price / 100000000.0) * (size / 100000000.0)) / sum(size / 100000000.0) AS vwap
FROM trades
WHERE ts >= {T0} AND ts < {T1}
ORDER BY symbol

-- @query q05_ohlcv_1s
SELECT * FROM (
  SELECT symbol, ts AS bucket, first(price) AS open, max(price) AS high, min(price) AS low,
         last(price) AS close, sum(size) AS volume,
         sum(price * (size / 100000000.0)) AS notional, count() AS "count"
  FROM trades
  SAMPLE BY 1s ALIGN TO CALENDAR
) ORDER BY symbol, bucket

-- @query q06_ohlcv_5s
SELECT * FROM (
  SELECT symbol, ts AS bucket, first(price) AS open, max(price) AS high, min(price) AS low,
         last(price) AS close, sum(size) AS volume,
         sum(price * (size / 100000000.0)) AS notional, count() AS "count"
  FROM trades
  SAMPLE BY 5s ALIGN TO CALENDAR
) ORDER BY symbol, bucket

-- @query q07_asof_backward
SELECT t.ts, t.symbol, t.price, t.size, t.side, t.seq,
       q.ts AS quotes_ts, q.bid AS quotes_bid, q.bid_size AS quotes_bid_size,
       q.ask AS quotes_ask, q.ask_size AS quotes_ask_size, q.update_id AS quotes_update_id
FROM trades t
ASOF JOIN quotes q ON t.symbol = q.symbol
ORDER BY t.ts, t.seq

-- @query q08_asof_backward_tol
SELECT t.ts, t.symbol, t.price, t.size, t.side, t.seq,
       q.ts AS quotes_ts, q.bid AS quotes_bid, q.bid_size AS quotes_bid_size,
       q.ask AS quotes_ask, q.ask_size AS quotes_ask_size, q.update_id AS quotes_update_id
FROM trades t
ASOF JOIN quotes q ON t.symbol = q.symbol TOLERANCE 100T
ORDER BY t.ts, t.seq

-- @query q10_crossed_quotes
SELECT count() AS n_quotes, sum(CASE WHEN bid >= ask THEN 1 ELSE 0 END) AS crossed
FROM quotes

-- @query q11_spread_stats
SELECT symbol, count() AS n, min(ask - bid) AS min_spread, max(ask - bid) AS max_spread,
       sum(ask - bid) AS sum_spread, avg(ask - bid) AS avg_spread, avg(bid_size + ask_size) AS avg_depth
FROM quotes
ORDER BY symbol

-- @query q12_percentiles_price
SELECT symbol, count() AS "count", min(price) AS "min", max(price) AS "max",
       approx_percentile(price, 0.5, 5) AS p50, approx_percentile(price, 0.9, 5) AS p90,
       approx_percentile(price, 0.99, 5) AS p99
FROM trades
ORDER BY symbol

-- @query q13_percentiles_size
SELECT count() AS "count", min(size) AS "min", max(size) AS "max",
       approx_percentile(size, 0.5, 5) AS p50, approx_percentile(size, 0.95, 5) AS p95,
       approx_percentile(size, 0.99, 5) AS p99
FROM trades

-- @query q14_trades_per_second
SELECT ts AS bucket, count() AS n, sum(size) AS volume
FROM trades
SAMPLE BY 1s ALIGN TO CALENDAR

-- @query q15_book_delta_stats
SELECT symbol, side, count() AS n, sum(CASE WHEN size = 0 THEN 1 ELSE 0 END) AS deletes,
       max(update_id) AS last_update_id
FROM book_deltas
ORDER BY symbol, side

-- @query q16_range_by_name
SELECT s.name, count() AS n, min(t.price) / 100000000.0 AS lo, max(t.price) / 100000000.0 AS hi
FROM trades t JOIN symbols s ON s.symbol = t.symbol
WHERE t.ts >= {T0} AND t.ts < {T1}
ORDER BY s.name

-- @query q17_large_trades
SELECT ts, symbol, price, size, side, seq
FROM trades
WHERE size >= 5000000
ORDER BY ts, seq

-- @query q18_asof_backward_agg
SELECT count() AS n, count(quotes_ts) AS matched, sum(quotes_ask - quotes_bid) AS spread_sum
FROM (
  SELECT t.ts, q.ts AS quotes_ts, q.ask AS quotes_ask, q.bid AS quotes_bid
  FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol
)

-- @query q19_asof_backward_tol_agg
SELECT count() AS n, count(quotes_ts) AS matched, sum(quotes_ask - quotes_bid) AS spread_sum
FROM (
  SELECT t.ts, q.ts AS quotes_ts, q.ask AS quotes_ask, q.bid AS quotes_bid
  FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol TOLERANCE 100T
)
