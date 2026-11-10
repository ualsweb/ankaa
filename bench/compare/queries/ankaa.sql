-- ANKAA dialect (Apache DataFusion SQL + ANKAA table functions).
-- Tables: trades(ts, symbol, price, size, side, seq), quotes(ts, symbol, bid, bid_size, ask, ask_size, update_id),
-- book_deltas(ts, symbol, side, price, size, first_update_id, update_id), symbols(symbol, name).
-- ts is Timestamp(ns, UTC); price/size are fixed-point Int64 with scale 8.
-- {T0}/{T1} are rendered as RFC3339 string literals (DataFusion coerces them to the ts type).

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
-- vwap() is ANKAA's aggregate: exact integer sums, one division per group
SELECT symbol, count(*) AS n,
       vwap(price, size) / 100000000.0 AS vwap
FROM trades
WHERE ts >= {T0} AND ts < {T1}
GROUP BY symbol
ORDER BY symbol

-- @query q05_ohlcv_1s
SELECT symbol, bucket, open, high, low, close, volume, notional, count
FROM ohlcv('trades', 1000000000)
ORDER BY symbol, bucket

-- @query q06_ohlcv_5s
SELECT symbol, bucket, open, high, low, close, volume, notional, count
FROM ohlcv('trades', 5000000000)
ORDER BY symbol, bucket

-- @query q07_asof_backward
SELECT ts, symbol, price, size, side, seq,
       quotes_ts, quotes_bid, quotes_bid_size, quotes_ask, quotes_ask_size, quotes_update_id
FROM asof_join('trades', 'quotes', 'symbol')
ORDER BY ts, seq

-- @query q08_asof_backward_tol
SELECT ts, symbol, price, size, side, seq,
       quotes_ts, quotes_bid, quotes_bid_size, quotes_ask, quotes_ask_size, quotes_update_id
FROM asof_join('trades', 'quotes', 'symbol', 100000000)
ORDER BY ts, seq

-- @query q09_asof_forward_tol
SELECT ts, symbol, price, size, side, seq,
       quotes_ts, quotes_bid, quotes_bid_size, quotes_ask, quotes_ask_size, quotes_update_id
FROM asof_join('trades', 'quotes', 'symbol', 1000000000, 'forward')
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
SELECT symbol, count, min, max, p50, p90, p99
FROM percentiles('trades', 'price', '0.5,0.9,0.99', 'symbol')
ORDER BY symbol

-- @query q13_percentiles_size
SELECT count, min, max, p50, p95, p99
FROM percentiles('trades', 'size', '0.5,0.95,0.99')

-- @query q14_trades_per_second
SELECT date_bin(INTERVAL '1 second', ts) AS bucket, count(*) AS n, sum(size) AS volume
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
SELECT count(*) AS n, count(quotes_ts) AS matched, sum(quotes_ask - quotes_bid) AS spread_sum
FROM asof_join('trades', 'quotes', 'symbol')

-- @query q19_asof_backward_tol_agg
SELECT count(*) AS n, count(quotes_ts) AS matched, sum(quotes_ask - quotes_bid) AS spread_sum
FROM asof_join('trades', 'quotes', 'symbol', 100000000)

-- @query q20_asof_forward_tol_agg
SELECT count(*) AS n, count(quotes_ts) AS matched, sum(quotes_ask - quotes_bid) AS spread_sum
FROM asof_join('trades', 'quotes', 'symbol', 1000000000, 'forward')
