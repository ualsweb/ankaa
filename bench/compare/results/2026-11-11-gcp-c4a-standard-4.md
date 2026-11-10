# Tick query set: gcp-c4a-standard-4 (2026-11-11T10:04:44+00:00)

Dataset: trades=584,617, quotes=214,895, book_deltas=4,849,537, symbols=12  
Runs per query: 20 after 3 warm-up; cells are median ms (p95 ms); `mismatch` = result differs from the DuckDB reference (time not comparable); `unstable` = the first answer differed and a repeat matched; `n/a` = not expressible.

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

Timing scope per engine:

- `ankaa` (ankaad 0.1.0): server-side: ankaad per-query time (plan+execute+collect), excludes capture replay and transfer
- `ankaa-flight` (ankaad over Arrow Flight at grpc://127.0.0.1:50051): client-observed: request sent to last row received, result fully materialised
- `duckdb` (duckdb 2.0.0): client-observed in-process: execute + fetch Arrow
- `clickhouse` (clickhouse 26.8.36.3): client-observed: request sent to last row received, result fully materialised
- `questdb` (questdb Build Information: QuestDB 10.1.0): client-observed over PostgreSQL wire (psycopg): execute + fetchall

Environment: hostname=gcp-c4a-standard-4; system=Linux; release=7.0.0-1017-gcp; machine=aarch64; git_commit=c46cfb79b5b9251895e8585b20720c2751755c04; dmi_product_name=Google Compute Engine
