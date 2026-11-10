-- ClickHouse dialect. Tables (prepare.py): ts DateTime64(9, 'UTC'), symbol UInt32, side UInt8,
-- price/size/seq/update_id Int64; MergeTree ORDER BY (symbol, ts).
-- {T0}/{T1} are rendered as toDateTime64('YYYY-MM-DD hh:mm:ss.nnnnnnnnn', 9, 'UTC').
-- join_use_nulls = 1 makes unmatched ASOF rows NULL (as in ANKAA/DuckDB) instead of type defaults.
-- Buckets are computed on integer nanoseconds so the bucket boundary is exact.
-- Percentiles use the same exact order statistic as the DuckDB reference (sorted[floor(q*(n-1))]).

-- @query q01_range_count
SELECT count() AS n, sum(size) AS volume, min(price) AS lo, max(price) AS hi
FROM trades
WHERE ts >= {T0} AND ts < {T1}

-- @query q02_agg_by_symbol
SELECT symbol, count() AS n, min(price) AS lo, max(price) AS hi, sum(size) AS volume,
       min(ts) AS first_ts, max(ts) AS last_ts
FROM trades
GROUP BY symbol
ORDER BY symbol

-- @query q03_latest_per_symbol
SELECT t.symbol AS symbol, t.ts AS ts, t.price AS price, t.size AS size, t.seq AS seq
FROM trades t
JOIN (SELECT symbol, max(seq) AS seq FROM trades GROUP BY symbol) m
  ON m.symbol = t.symbol AND m.seq = t.seq
ORDER BY symbol

-- @query q04_vwap_by_symbol
SELECT symbol, count() AS n,
       sum((price / 100000000.0) * (size / 100000000.0)) / sum(size / 100000000.0) AS vwap
FROM trades
WHERE ts >= {T0} AND ts < {T1}
GROUP BY symbol
ORDER BY symbol

-- @query q05_ohlcv_1s
SELECT symbol,
       fromUnixTimestamp64Nano(intDiv(toUnixTimestamp64Nano(ts), 1000000000) * 1000000000, 'UTC') AS bucket,
       argMin(price, (ts, seq)) AS open, max(price) AS high, min(price) AS low,
       argMax(price, (ts, seq)) AS close, sum(size) AS volume,
       toInt64(intDiv(sum(toInt128(price) * toInt128(size)), 100000000)) AS notional,
       count() AS `count`
FROM trades
GROUP BY symbol, bucket
ORDER BY symbol, bucket

-- @query q06_ohlcv_5s
SELECT symbol,
       fromUnixTimestamp64Nano(intDiv(toUnixTimestamp64Nano(ts), 5000000000) * 5000000000, 'UTC') AS bucket,
       argMin(price, (ts, seq)) AS open, max(price) AS high, min(price) AS low,
       argMax(price, (ts, seq)) AS close, sum(size) AS volume,
       toInt64(intDiv(sum(toInt128(price) * toInt128(size)), 100000000)) AS notional,
       count() AS `count`
FROM trades
GROUP BY symbol, bucket
ORDER BY symbol, bucket

-- @query q07_asof_backward
SELECT t.ts AS ts, t.symbol AS symbol, t.price AS price, t.size AS size, t.side AS side, t.seq AS seq,
       q.ts AS quotes_ts, q.bid AS quotes_bid, q.bid_size AS quotes_bid_size,
       q.ask AS quotes_ask, q.ask_size AS quotes_ask_size, q.update_id AS quotes_update_id
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
ORDER BY ts, seq
SETTINGS join_use_nulls = 1

-- @query q08_asof_backward_tol
SELECT ts, symbol, price, size, side, seq,
       if(ok, qts, NULL) AS quotes_ts, if(ok, bid, NULL) AS quotes_bid, if(ok, bid_size, NULL) AS quotes_bid_size,
       if(ok, ask, NULL) AS quotes_ask, if(ok, ask_size, NULL) AS quotes_ask_size, if(ok, update_id, NULL) AS quotes_update_id
FROM (
  SELECT t.ts AS ts, t.symbol AS symbol, t.price AS price, t.size AS size, t.side AS side, t.seq AS seq,
         q.ts AS qts, q.bid AS bid, q.bid_size AS bid_size, q.ask AS ask, q.ask_size AS ask_size, q.update_id AS update_id,
         (q.ts IS NOT NULL AND toUnixTimestamp64Nano(t.ts) - toUnixTimestamp64Nano(q.ts) <= 100000000) AS ok
  FROM trades t
  ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
)
ORDER BY ts, seq
SETTINGS join_use_nulls = 1

-- @query q09_asof_forward_tol
SELECT ts, symbol, price, size, side, seq,
       if(ok, qts, NULL) AS quotes_ts, if(ok, bid, NULL) AS quotes_bid, if(ok, bid_size, NULL) AS quotes_bid_size,
       if(ok, ask, NULL) AS quotes_ask, if(ok, ask_size, NULL) AS quotes_ask_size, if(ok, update_id, NULL) AS quotes_update_id
FROM (
  SELECT t.ts AS ts, t.symbol AS symbol, t.price AS price, t.size AS size, t.side AS side, t.seq AS seq,
         q.ts AS qts, q.bid AS bid, q.bid_size AS bid_size, q.ask AS ask, q.ask_size AS ask_size, q.update_id AS update_id,
         (q.ts IS NOT NULL AND toUnixTimestamp64Nano(q.ts) - toUnixTimestamp64Nano(t.ts) <= 1000000000) AS ok
  FROM trades t
  ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts <= q.ts
)
ORDER BY ts, seq
SETTINGS join_use_nulls = 1

-- @query q10_crossed_quotes
SELECT count() AS n_quotes, sum(CASE WHEN bid >= ask THEN 1 ELSE 0 END) AS crossed
FROM quotes

-- @query q11_spread_stats
SELECT symbol, count() AS n, min(ask - bid) AS min_spread, max(ask - bid) AS max_spread,
       sum(ask - bid) AS sum_spread, avg(ask - bid) AS avg_spread, avg(bid_size + ask_size) AS avg_depth
FROM quotes
GROUP BY symbol
ORDER BY symbol

-- @query q12_percentiles_price
SELECT symbol, count() AS `count`, toFloat64(min(price)) AS `min`, toFloat64(max(price)) AS `max`,
       toFloat64(arraySort(groupArray(price))[toInt64(floor(0.5 * (count() - 1))) + 1]) AS p50,
       toFloat64(arraySort(groupArray(price))[toInt64(floor(0.9 * (count() - 1))) + 1]) AS p90,
       toFloat64(arraySort(groupArray(price))[toInt64(floor(0.99 * (count() - 1))) + 1]) AS p99
FROM trades
GROUP BY symbol
ORDER BY symbol

-- @query q13_percentiles_size
SELECT count() AS `count`, toFloat64(min(size)) AS `min`, toFloat64(max(size)) AS `max`,
       toFloat64(arraySort(groupArray(size))[toInt64(floor(0.5 * (count() - 1))) + 1]) AS p50,
       toFloat64(arraySort(groupArray(size))[toInt64(floor(0.95 * (count() - 1))) + 1]) AS p95,
       toFloat64(arraySort(groupArray(size))[toInt64(floor(0.99 * (count() - 1))) + 1]) AS p99
FROM trades

-- @query q14_trades_per_second
SELECT fromUnixTimestamp64Nano(intDiv(toUnixTimestamp64Nano(ts), 1000000000) * 1000000000, 'UTC') AS bucket,
       count() AS n, sum(size) AS volume
FROM trades
GROUP BY bucket
ORDER BY bucket

-- @query q15_book_delta_stats
SELECT symbol, side, count() AS n, sum(CASE WHEN size = 0 THEN 1 ELSE 0 END) AS deletes,
       max(update_id) AS last_update_id
FROM book_deltas
GROUP BY symbol, side
ORDER BY symbol, side

-- @query q16_range_by_name
SELECT s.name AS name, count() AS n, min(t.price) / 100000000.0 AS lo, max(t.price) / 100000000.0 AS hi
FROM trades t JOIN symbols s ON s.symbol = t.symbol
WHERE t.ts >= {T0} AND t.ts < {T1}
GROUP BY name
ORDER BY name

-- @query q17_large_trades
SELECT ts, symbol, price, size, side, seq
FROM trades
WHERE size >= 5000000
ORDER BY ts, seq

-- @query q18_asof_backward_agg
SELECT count() AS n, count(q.ts) AS matched, sum(q.ask - q.bid) AS spread_sum
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
SETTINGS join_use_nulls = 1

-- @query q19_asof_backward_tol_agg
SELECT count() AS n,
       count(CASE WHEN toUnixTimestamp64Nano(t.ts) - toUnixTimestamp64Nano(q.ts) <= 100000000 THEN q.ts END) AS matched,
       sum(CASE WHEN toUnixTimestamp64Nano(t.ts) - toUnixTimestamp64Nano(q.ts) <= 100000000 THEN q.ask - q.bid END) AS spread_sum
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
SETTINGS join_use_nulls = 1

-- @query q20_asof_forward_tol_agg
SELECT count() AS n,
       count(CASE WHEN toUnixTimestamp64Nano(q.ts) - toUnixTimestamp64Nano(t.ts) <= 1000000000 THEN q.ts END) AS matched,
       sum(CASE WHEN toUnixTimestamp64Nano(q.ts) - toUnixTimestamp64Nano(t.ts) <= 1000000000 THEN q.ask - q.bid END) AS spread_sum
FROM trades t
ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts <= q.ts
SETTINGS join_use_nulls = 1
