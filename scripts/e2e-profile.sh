#!/usr/bin/env bash
# End-to-end ingest profile on Linux (the Axion bench VM): broker → ankaad (WAL + segments) → rows.
#
#   scripts/e2e-profile.sh <scenario> [--transport mqtt|file] [--format json|json-array]
#                          [--batch B] [--messages N] [--devices D] [--tls]
#
# Needs: target/release/ankaad, bench/target/release/loadgen, mosquitto (for mqtt), perf.
# Writes target/e2e/<scenario>/: summary.txt, perf-stat.txt, perf-report.txt, ankaad.log.
# Measures the server only: CPU time of ankaad per reading (utime+stime from /proc), wall time
# from the first to the last stored reading, peak resident memory (VmHWM), hardware counters
# and a call-graph profile while it ingests.
set -euo pipefail
export LC_ALL=C
scenario="${1:?scenario name}"; shift
transport=mqtt; format=json; batch=10; messages=1000000; devices=1000; tls=0
while [ $# -gt 0 ]; do
  case "$1" in
    --transport) transport="$2"; shift 2;;
    --format) format="$2"; shift 2;;
    --batch) batch="$2"; shift 2;;
    --messages) messages="$2"; shift 2;;
    --devices) devices="$2"; shift 2;;
    --tls) tls=1; shift;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/target/e2e/$scenario"; work="$(mktemp -d)"
mkdir -p "$out"
ankaad="$root/target/release/ankaad"; loadgen="$root/bench/target/release/loadgen"
token="e2e-profile-token-0123456789"
per_msg=1; [ "$format" = json-array ] && per_msg="$batch"
expected=$((messages * per_msg))
cleanup() { [ -n "${apid:-}" ] && kill "$apid" 2>/dev/null; [ -n "${mpid:-}" ] && kill "$mpid" 2>/dev/null; rm -rf "$work"; }
trap cleanup EXIT

# --- broker ---------------------------------------------------------------------------------
broker_url="mqtt://127.0.0.1:18883"
if [ "$transport" = mqtt ]; then
  cat > "$work/mosquitto.conf" <<EOF
listener 18883 127.0.0.1
allow_anonymous true
persistence false
max_queued_messages 0
max_inflight_messages 0
EOF
  mosquitto -c "$work/mosquitto.conf" > "$work/mosquitto.log" 2>&1 & mpid=$!
  sleep 0.5
fi

# --- source mapping per format --------------------------------------------------------------
case "$format" in
  json) map='map = { ts = "t", dev = "dev", temp = "temp", kw = "kw", ok = "ok" }'; rows='';;
  json-array) map='map = { ts = "t", dev = "$msg.dev", temp = "temp", kw = "kw", ok = "ok" }'; rows='rows = "samples"';;
  *) echo "unknown format $format" >&2; exit 2;;
esac
if [ "$transport" = file ]; then
  "$loadgen" --messages "$messages" --devices "$devices" --format "$format" --batch "$batch" --file "$work/input.ndjson"
  source="kind = \"file\"
url = \"$work/input.ndjson\""
else
  source="kind = \"mqtt\"
url = \"$broker_url\"
subject = \"bench/#\""
fi
cat > "$work/ankaad.toml" <<EOF
[feed]
enabled = false
streams = []
[server]
flight_addr = "127.0.0.1:15051"
metrics_addr = "127.0.0.1:19101"
token_env = "ANKAA_TOKEN"
[storage]
wal_dir = "$work/wal"
checkpoint_dir = "$work/segments"
[[tables]]
name = "telemetry"
partition_by = "dev"
max_dict_entries = 1000000
columns = [
  { name = "ts",   type = "timestamp_ns" },
  { name = "dev",  type = "dict" },
  { name = "temp", type = "float64", nullable = true },
  { name = "kw",   type = "fixed8",  nullable = true },
  { name = "ok",   type = "u8" },
]
[[sources]]
name = "load"
$source
format = "json"
table = "telemetry"
ts_unit = "millis"
max_message_bytes = 1048576
$rows
$map
EOF

metric() { curl -s -H "Authorization: Bearer $token" http://127.0.0.1:19101/metrics | awk -v m="$1" '$1 == m { print $2 }'; }
cpu_ticks() { awk '{ print $14 + $15 }' "/proc/$1/stat"; }

ANKAA_TOKEN="$token" "$ankaad" serve --config "$work/ankaad.toml" > "$out/ankaad.log" 2>&1 & apid=$!
for _ in $(seq 1 100); do [ -n "$(metric ankaa_uptime_seconds)" ] && break; sleep 0.1; done
[ "$transport" = mqtt ] && for _ in $(seq 1 100); do grep -q "mqtt connected" "$out/ankaad.log" && break; sleep 0.1; done

hz=$(getconf CLK_TCK)
# a file source starts reading at once: count its CPU from process start
c0=$(cpu_ticks "$apid"); [ "$transport" = file ] && c0=0
sudo perf stat -e cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses,L1-dcache-load-misses -p "$apid" -o "$out/perf-stat.txt" & pstat=$!
sudo perf record -F 499 -g -p "$apid" -o "$work/perf.data" > /dev/null 2>&1 & prec=$!
t_start=$(date +%s.%N)
if [ "$transport" = mqtt ]; then
  "$loadgen" --messages "$messages" --devices "$devices" --format "$format" --batch "$batch" --mqtt "$broker_url" --qos 1 2> "$out/loadgen.log" &
  lpid=$!
fi
first=""
while :; do
  n=$(metric ankaa_user_rows_total); n=${n:-0}
  [ -z "$first" ] && [ "$n" -gt 0 ] && first=$(date +%s.%N)
  [ "$n" -ge "$expected" ] && break
  sleep 0.05
  if (( $(echo "$(date +%s.%N) - $t_start > 900" | bc) )); then echo "timeout at $n of $expected rows" >&2; break; fi
done
t_end=$(date +%s.%N)
c1=$(cpu_ticks "$apid")
sudo kill -INT "$prec" "$pstat" 2>/dev/null || true
wait "$prec" "$pstat" 2>/dev/null || true
[ -n "${lpid:-}" ] && wait "$lpid" 2>/dev/null || true
hwm_kb=$(awk '/VmHWM/ { print $2 }' "/proc/$apid/status")
rows=$(metric ankaa_user_rows_total); rejected=$(metric ankaa_user_rejected_total)
sudo perf report -i "$work/perf.data" --no-children --stdio --percent-limit 0.8 --sort symbol 2>/dev/null \
  | grep -vE '^#|^$' | head -60 > "$out/perf-report.txt" || true

wall=$(echo "$t_end - ${first:-$t_start}" | bc -l)
cpu_s=$(echo "($c1 - $c0) / $hz" | bc -l)
{
  echo "scenario:        $scenario ($transport, $format, batch $batch, $devices devices, tls $tls)"
  echo "host:            $(uname -m) $(lscpu 2>/dev/null | awk -F: '/Model name/ {gsub(/^ +/,"",$2); print $2; exit}') $(nproc) vCPU"
  echo "commit:          $(git -C "$root" rev-parse --short HEAD)"
  echo "rows stored:     $rows of $expected (rejected ${rejected:-0})"
  printf "wall (first→last row): %.3f s\n" "$wall"
  printf "rows per second: %.0f\n" "$(echo "$rows / $wall" | bc -l)"
  printf "ankaad CPU:      %.3f s = %.0f ns per row\n" "$cpu_s" "$(echo "$cpu_s * 1000000000 / $rows" | bc -l)"
  echo "peak RSS:        $((hwm_kb / 1024)) MiB"
} | tee "$out/summary.txt"
echo "--- perf stat"; grep -E 'cycles|instructions|misses|references' "$out/perf-stat.txt" | head -10
echo "--- top functions (self time)"; head -25 "$out/perf-report.txt"
