"""Shared code of the ANKAA benchmark harnesses (bench/compare, bench/telemetry) and the differential oracle
(bench/oracle).

Modules:
  queries  -- the tick query set (manifest + per-dialect SQL files) and placeholder rendering
  export   -- run `ankaad replay` to export tables / query results as CSV, read them as Arrow
  compare  -- normalise result tables and diff them, printing the first differing rows
  engines  -- native-client adapters: ANKAA (replay subprocess, Arrow Flight, managed `ankaad serve`), DuckDB,
              ClickHouse, QuestDB, InfluxDB 3 Core, TimescaleDB
  sysinfo  -- hardware / OS / version facts recorded next to every measurement
"""
from pathlib import Path

HARNESS_DIR = Path(__file__).resolve().parent
COMPARE_DIR = HARNESS_DIR.parent
BENCH_DIR = COMPARE_DIR.parent
REPO_DIR = BENCH_DIR.parent
QUERIES_DIR = COMPARE_DIR / "queries"
DEFAULT_CAPTURE = BENCH_DIR / "data" / "binance-btcusdt-trade-depth-20s.ankc"
