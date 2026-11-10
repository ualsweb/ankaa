#!/usr/bin/env bash
# Runs ON a dedicated benchmark VM (started by `scripts/gcp/ankaa-gcp.sh compare`): ANKAA vs QuestDB,
# ClickHouse and DuckDB on the same machine with bench/compare. Nothing else may run on the VM.
#
#   COMPARE_CAPTURE  a real capture to compare on (an `.ankc` file recorded by `ankaa-capture
#                    --snapshots`, several symbols, ideally hours). This is the dataset to use.
#   COMPARE_COPIES   without COMPARE_CAPTURE: how many times the bundled 20 s Binance capture is
#                    repeated, time-shifted, to build a synthetic one-symbol dataset (default 1000:
#                    about 3.7 million rows across the three tables). A smoke test, not a result.
#   COMPARE_RUNS     timed runs per query (default 20, after 3 warm-ups)
set -euo pipefail
export LC_ALL=C
cd "$(dirname "$0")/../.."
. "$HOME/.cargo/env"
copies="${COMPARE_COPIES:-1000}"
runs="${COMPARE_RUNS:-20}"
label="gcp-$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type 2>/dev/null | sed 's#.*/##' || echo vm)"
log() { printf '\n== %s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

log "docker"
if ! command -v docker >/dev/null; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io docker-compose-v2 >/dev/null
fi
sudo systemctl enable --now docker >/dev/null

log "build ankaad"
cargo build --release -p ankaad 2>&1 | tail -1

log "python environment"
[ -x bench/.venv/bin/python ] || python3 -m venv bench/.venv
bench/.venv/bin/pip install -q -r bench/compare/requirements.txt pyarrow

log "correctness oracle (ANKAA vs DuckDB and Polars on the bundled capture)"
bench/oracle/run.sh 2>&1 | tail -3

log "engines"
sudo docker compose -f bench/compare/docker-compose.yml up -d 2>&1 | tail -2

mkdir -p bench/compare/data
serve_flags=()
prepare_flags=()
if [ -n "${COMPARE_CAPTURE:-}" ]; then
  cap="$(realpath "$COMPARE_CAPTURE")"
  [ -s "$cap" ] || { echo "COMPARE_CAPTURE=$cap is missing or empty" >&2; exit 2; }
  name="$(basename "$cap" .ankc)"
  log "dataset: real capture $name ($(du -h "$cap" | cut -f1))"
  data="bench/compare/data/capture-$name"
  dataset="real Binance capture $name (recorded with snapshots)"
  prepare_flags+=(--expect-snapshots)
else
  log "dataset: bundled capture x $copies (synthetic repetition)"
  data="bench/compare/data/binance-x$copies"
  cap="$PWD/bench/compare/data/binance-x$copies.ankc"
  dataset="bundled Binance capture x$copies (time-shifted copies, one symbol)"
  [ -s "$cap" ] || python3 bench/scripts/make_profile_capture.py bench/data/binance-btcusdt-trade-depth-20s.ankc "$cap" "$copies"
  serve_flags+=(--no-depth-snapshots)
fi
# prepare.py exports the tables once, loads every engine and waits until each one shows all rows
bench/.venv/bin/python bench/compare/prepare.py --capture "$cap" --data-dir "$data" "${prepare_flags[@]}" 2>&1 | tail -12

log "ankaad serve for the Flight engine"
ANKAA_TOKEN="bench-$(openssl rand -hex 16)"; export ANKAA_TOKEN
pkill -x ankaad 2>/dev/null || true
nohup target/release/ankaad serve --url "file://$cap" "${serve_flags[@]}" \
  --flight-addr 127.0.0.1:50051 --metrics-addr 127.0.0.1:9100 > /tmp/ankaad-bench.log 2>&1 &
served=0
for _ in $(seq 1 600); do
  if grep -q "capture served" /tmp/ankaad-bench.log; then served=1; break; fi
  pgrep -x ankaad >/dev/null || break
  sleep 1
done
if [ "$served" != 1 ]; then
  echo "ankaad did not finish serving the capture:" >&2; tail -20 /tmp/ankaad-bench.log >&2; exit 1
fi
sleep 2

log "verify and time: ankaa, ankaa-flight, duckdb, clickhouse, questdb ($runs runs after 3 warm-ups)"
ANKAA_BENCH_HOST_LABEL="$label" bench/.venv/bin/python bench/compare/run.py --data-dir "$data" \
  --engines ankaa,ankaa-flight,duckdb,clickhouse,questdb --runs "$runs" --warmup 3 \
  --token "$ANKAA_TOKEN" --label "$label, Ubuntu 24.04, $dataset"
pkill -x ankaad 2>/dev/null || true

log "versions"
sudo docker compose -f bench/compare/docker-compose.yml images 2>/dev/null | tail -3
bench/.venv/bin/python -c "import duckdb; print('duckdb', duckdb.__version__)"
ls -1 bench/compare/results/*.json bench/compare/results/*.md 2>/dev/null | tail -2
log "done"
