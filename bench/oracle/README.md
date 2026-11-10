# bench/oracle — differential oracle

Checks that ANKAA's SQL, table functions and time buckets return the same answers as independent
implementations (PRD F-06, F-07, F-08, F-10):

- **DuckDB** for every query of the shared tick set (`bench/compare/queries/`): plain SQL, `ASOF JOIN`
  (backward, with tolerance, forward), exact order statistics for `percentiles()`.
- **Polars** (`group_by_dynamic`) for OHLCV at 1 s / 5 s, VWAP, trades per second and the simple aggregates.

Rules: integer, timestamp and string columns must be identical; float aggregates within a relative
tolerance; sketch percentiles within alpha = 0.01 of `sorted[floor(q·(n−1))]` (see `harness/compare.py`).
A failure prints the first differing rows of both sides.

```sh
bench/oracle/run.sh                      # bundled 20 s capture; creates bench/.venv and builds ankaad if needed
bench/oracle/run.sh -k asof -x           # any pytest arguments
ANKAA_ORACLE_CAPTURE=captures/x.ankc ANKAA_ORACLE_EXPECT_SNAPSHOTS=1 bench/oracle/run.sh   # another capture
```

Files: `conftest.py` (export + one `ankaad replay` run over the whole query set, session-scoped),
`test_duckdb_oracle.py`, `test_polars_oracle.py`, `polars_ref.py` (Polars implementations).
The query texts live in `bench/compare/queries/` so the oracle and the benchmark use one source.
