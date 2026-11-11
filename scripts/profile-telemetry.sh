#!/usr/bin/env bash
# Function-level profile of the engine on the telemetry (TSBS cpu-only) dataset: where the CPU goes
# while it loads, compresses, restarts and answers each kind of query.
#
#   scripts/profile-telemetry.sh [phase ...]
#
# Phases run in the order given (default: load tier queries functions restart queries functions):
#   load       wipe $WORK, load the dataset, profile the whole load
#   tier       profile the running server for $TIER_SECONDS (compression right after a load)
#   queries    every query of bench/telemetry/queries, one profile each
#   functions  latest, gapfill, percentiles and a few plain SQL shapes, one profile each
#   restart    stop the server and profile its start from the segments and the log
# Queries are profiled against the server as it is at that point: "loaded" (rows just arrived, part
# of them still uncompressed) or "restarted" (everything read from segment files).
#
# Needs, on an aarch64 Linux host: `perf`, a release build with symbols (`cargo build --release -p
# ankaad`; the profile keeps debug info), the dataset of bench/telemetry (generate.py) and its
# Python environment (bench/telemetry/.venv). Writes one text report per profile to $OUT
# (default target/profile-telemetry/): the hottest symbols with their share of the samples, and the
# share of every thread. Cloud VMs usually expose no hardware counters, so the samples come from the
# cpu-clock software event (time, not cycles or cache misses).
#
# Settings: DATA (dataset directory), WORK (the server's WAL and segments, wiped by `load`),
# SECONDS_PER_QUERY (default 8), FREQ (samples per second, default 1999), ANKAAD.
set -uo pipefail
cd "$(dirname "$0")/.."

ANKAAD="${ANKAAD:-target/release/ankaad}"
DATA="${DATA:-bench/telemetry/data/tsbs-cpu-s1000-3d-seed123}"
WORK="${WORK:-bench/telemetry/work/profile}"
OUT="${OUT:-target/profile-telemetry}"
PY="${PY:-bench/telemetry/.venv/bin/python}"
SECONDS_PER_QUERY="${SECONDS_PER_QUERY:-8}"
FREQ="${FREQ:-1999}"
FLIGHT_PORT=50081
METRICS_PORT=9181
export ANKAA_TOKEN="${ANKAA_TOKEN:-profile-token}"
PHASES=("$@"); [ ${#PHASES[@]} -eq 0 ] && PHASES=(load tier queries functions restart queries functions)
TIER_SECONDS="${TIER_SECONDS:-45}"

mkdir -p "$OUT" "$WORK"
[ -x "$ANKAAD" ] || { echo "no $ANKAAD: cargo build --release -p ankaad" >&2; exit 1; }
[ -f "$DATA/manifest.json" ] || { echo "no dataset in $DATA: bench/telemetry/generate.py" >&2; exit 1; }
ROWS=$("$PY" -c "import json,sys; print(json.load(open(sys.argv[1]))['rows'])" "$DATA/manifest.json")

metric() { curl -s -H "Authorization: Bearer $ANKAA_TOKEN" "http://127.0.0.1:$METRICS_PORT/metrics" | awk -v m="$1" '$1==m {print int($2)}'; }

# hottest symbols of a profile, then the share of every thread
report() { # <perf.data> <title>
  local data="$1" title="$2" txt="$OUT/$(basename "${1%.data}").txt"
  {
    echo "== $title"
    echo "-- symbols (self time)"
    perf report -i "$data" --stdio --no-children --sort symbol 2>/dev/null \
      | grep -E '^ +[0-9]' | head -40 | sed 's/\[\.\] //; s/\[k\] /kernel: /; s/  */ /g' | cut -c1-190
    echo "-- threads"
    perf report -i "$data" --stdio --no-children --sort comm 2>/dev/null | grep -E '^ +[0-9]' | head -12 | sed 's/  */ /g'
  } > "$txt"
  sed -n '1,26p' "$txt"; echo
}

config() {
  "$PY" - "$DATA" "$WORK" "$FLIGHT_PORT" "$METRICS_PORT" <<'PYEOF' > "$WORK/ankaad.toml"
import json, sys
from pathlib import Path
sys.path.insert(0, "bench/compare")
from harness.engines import AnkaaServeEngine
data, work, flight, metrics = Path(sys.argv[1]), Path(sys.argv[2]).resolve(), int(sys.argv[3]), int(sys.argv[4])
e = AnkaaServeEngine(work, flight_port=flight, metrics_port=metrics, token="unused")
print(e.config_text(data, json.loads((data / "manifest.json").read_text())))
PYEOF
}

start() { # [perf record prefix...]
  "$@" "$ANKAAD" serve --config "$WORK/ankaad.toml" > "$WORK/ankaad.log" 2>&1 &
  SERVER=$!
}
wait_rows() { until [ "$(metric ankaa_user_rows_total)" = "$ROWS" ] 2>/dev/null; do sleep 0.2; kill -0 "$SERVER" 2>/dev/null || { tail -5 "$WORK/ankaad.log"; exit 1; }; done; }
wait_ready() { until curl -s -o /dev/null -H "Authorization: Bearer $ANKAA_TOKEN" "http://127.0.0.1:$METRICS_PORT/metrics"; do sleep 0.05; kill -0 "$SERVER" 2>/dev/null || { tail -5 "$WORK/ankaad.log"; exit 1; }; done; }
pid_of_server() { pgrep -n -x ankaad; }
stop() { kill -TERM "$(pid_of_server)" 2>/dev/null; wait "$SERVER" 2>/dev/null; SERVER=""; }

SERVER=""
STATE="restarted"   # label of the reports: "loaded" right after a load, "restarted" from segments

ensure_server() { # start the server on what $WORK holds, when none is running
  [ -n "$SERVER" ] && kill -0 "$SERVER" 2>/dev/null && return
  [ -f "$WORK/ankaad.toml" ] || config
  start; wait_ready; wait_rows 2>/dev/null
  sleep "${SETTLE:-10}"
}

one() { # <name> <loop_query args...>: profile the server while one statement runs in a loop
  local name="$1-$STATE"; shift
  "$PY" bench/telemetry/loop_query.py "$@" --seconds 2 --flight "grpc://127.0.0.1:$FLIGHT_PORT" --token "$ANKAA_TOKEN" > /dev/null
  perf record -q -e cpu-clock -F "$FREQ" -p "$(pid_of_server)" -o "$OUT/q-$name.data" -- \
    "$PY" bench/telemetry/loop_query.py "$@" --seconds "$SECONDS_PER_QUERY" --flight "grpc://127.0.0.1:$FLIGHT_PORT" --token "$ANKAA_TOKEN" \
    > "$OUT/q-$name.client" 2>&1
  report "$OUT/q-$name.data" "$name: $(cat "$OUT/q-$name.client")"
}

for phase in "${PHASES[@]}"; do
  case "$phase" in
    load)
      [ -n "$SERVER" ] && stop
      rm -rf "$WORK/wal" "$WORK/segments"; config
      t0=$(date +%s.%N)
      start perf record -q -e cpu-clock -F "$FREQ" -o "$OUT/load.data" --
      wait_rows
      echo "load: $ROWS rows in $(echo "$(date +%s.%N) - $t0" | bc) s"
      # the profile of the load ends with the process; report it after the server stops
      STATE="loaded"; LOAD_PENDING=1 ;;
    tier)
      ensure_server
      # sealing, compression and segment writes (right after a load: the bulk of it)
      perf record -q -e cpu-clock -F "$FREQ" -p "$(pid_of_server)" -o "$OUT/tier-$STATE.data" -- sleep "${TIER_SECONDS:-45}"
      report "$OUT/tier-$STATE.data" "$TIER_SECONDS s of sealing, compression and segment writes ($STATE)" ;;
    queries)
      ensure_server
      for q in $("$PY" bench/telemetry/loop_query.py --list); do one "$q" --query "$q"; done ;;
    functions)
      ensure_server
      one latest --sql "SELECT * FROM latest('cpu', 'hostname')"
      one gapfill --sql "SELECT hostname, bucket, value, filled FROM gapfill('cpu', 'usage_user', 60000000000, 'hostname', 'avg', 'linear', 1451692800000000000, 1451696400000000000)"
      one percentiles --sql "SELECT hostname, count, p50, p99 FROM percentiles('cpu', 'usage_user', '0.5,0.99', 'hostname')"
      one count-all --sql "SELECT count(*) FROM cpu"
      one group-by-tag --sql "SELECT region, datacenter, count(*), avg(usage_user) FROM cpu GROUP BY region, datacenter"
      one filter-tag --sql "SELECT count(*), max(usage_system) FROM cpu WHERE region = 'eu-west-1' AND usage_user > 50"
      one like-tag --sql "SELECT count(*) FROM cpu WHERE hostname LIKE 'host_1%'"
      one order-limit --sql "SELECT ts, hostname, usage_user FROM cpu WHERE hostname = 'host_7' ORDER BY ts DESC LIMIT 100"
      one tiny --sql "SELECT 1" ;;
    restart)
      [ -n "$SERVER" ] && stop
      if [ -n "${LOAD_PENDING:-}" ]; then
        report "$OUT/load.data" "load of $ROWS CSV rows (whole process: load, what followed, shutdown)"; LOAD_PENDING=""
      fi
      [ -f "$WORK/ankaad.toml" ] || config
      t0=$(date +%s.%N)
      start perf record -q -e cpu-clock -F "$FREQ" -o "$OUT/restart.data" --
      wait_ready; wait_rows 2>/dev/null
      echo "restart: serving $ROWS rows again after $(echo "$(date +%s.%N) - $t0" | bc) s"
      stop
      report "$OUT/restart.data" "restart: recovery from segments and the log"
      SERVER=""; STATE="restarted" ;;
    *) echo "unknown phase $phase (load tier queries functions restart)" >&2; exit 2 ;;
  esac
done
[ -n "$SERVER" ] && stop
if [ -n "${LOAD_PENDING:-}" ]; then
  report "$OUT/load.data" "load of $ROWS CSV rows (whole process: load, what followed, shutdown)"
fi
echo "reports in $OUT/"
