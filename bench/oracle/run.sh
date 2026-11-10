#!/usr/bin/env bash
# Differential oracle: ANKAA vs DuckDB (SQL, ASOF JOIN, quantiles) and vs Polars (time buckets).
# One command: bench/oracle/run.sh [pytest args...]   e.g.  bench/oracle/run.sh -k asof -x
# Env: ANKAA_ORACLE_CAPTURE=/path/to/other.ankc  ANKAA_ORACLE_EXPECT_SNAPSHOTS=1  ANKAAD=/path/to/ankaad
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$(dirname "$HERE")"
REPO="$(dirname "$BENCH")"
VENV="$BENCH/.venv"

if [ ! -x "$VENV/bin/python" ]; then
  echo "+ creating venv at $VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q -r "$BENCH/compare/requirements.txt"
fi
"$VENV/bin/python" -c "import duckdb, polars, pyarrow, pandas, pytest" 2>/dev/null || \
  "$VENV/bin/pip" install -q -r "$BENCH/compare/requirements.txt"

if [ -z "${ANKAAD:-}" ] && [ ! -x "$REPO/target/release/ankaad" ]; then
  echo "+ building ankaad (cargo build --release -p ankaad)"
  (cd "$REPO" && cargo build --release -p ankaad)
fi

cd "$HERE"
exec "$VENV/bin/python" -m pytest -q -p no:cacheprovider "$@"
