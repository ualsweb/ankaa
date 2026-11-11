#!/usr/bin/env bash
# End-to-end ingest profile on Linux (the Axion bench VM): broker → ankaad (WAL + segments) → rows.
#
#   scripts/e2e-profile.sh <scenario> [--transport mqtt|file] [--format json|json-array|sparkplug]
#                          [--batch B] [--messages N] [--devices D] [--tls] [--publishers P]
#
# Needs: target/release/ankaad (or $ANKAAD), bench/target/release/loadgen, mosquitto (for mqtt), perf.
# Writes target/e2e/<scenario>/: summary.txt, perf-stat.txt, perf-report.txt, ankaad.log.
# Measures the server only: CPU time of ankaad per reading (utime+stime from /proc), wall time
# from the first to the last stored reading, peak resident memory (VmHWM), hardware counters
# and a call-graph profile while it ingests.
set -euo pipefail
export LC_ALL=C
scenario="${1:?scenario name}"; shift
transport=mqtt; format=json; batch=10; messages=1000000; devices=1000; tls=0; publishers=1
while [ $# -gt 0 ]; do
  case "$1" in
    --transport) transport="$2"; shift 2;;
    --format) format="$2"; shift 2;;
    --batch) batch="$2"; shift 2;;
    --messages) messages="$2"; shift 2;;
    --devices) devices="$2"; shift 2;;
    --tls) tls=1; shift;;
    --publishers) publishers="$2"; shift 2;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/target/e2e/$scenario"; work="$(mktemp -d)"
mkdir -p "$out"
ankaad="${ANKAAD:-$root/target/release/ankaad}"; loadgen="$root/bench/target/release/loadgen"
token="e2e-profile-token-0123456789"
per_msg=1; [ "$format" != json ] && per_msg="$batch"
expected=$((messages * per_msg))
# Sparkplug: every device's NBIRTH carries its metrics too
[ "$format" = sparkplug ] && expected=$((expected + devices * batch))
# ports of their own per run: a previous ankaad may still be flushing on its way out
base_port=$(( 20000 + ($$ % 20000) ))
flight_port=$base_port; metrics_port=$((base_port + 1)); mqtt_port=$((base_port + 2)); tls_port=$((base_port + 3))
cleanup() {
  if [ -n "${apid:-}" ]; then kill "$apid" 2>/dev/null; wait "$apid" 2>/dev/null || true; fi
  [ -n "${mpid:-}" ] && kill "$mpid" 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT

# --- broker ---------------------------------------------------------------------------------
broker_url="mqtt://127.0.0.1:$mqtt_port"
if [ "$transport" = mqtt ]; then
  cat > "$work/mosquitto.conf" <<EOF
listener $mqtt_port 127.0.0.1
allow_anonymous true
persistence false
max_queued_messages 0
max_inflight_messages 0
EOF
  if [ "$tls" = 1 ]; then
    # ankaad subscribes over TLS (the cost measured); the generator publishes on the plain port
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
      -subj "/CN=e2e CA" -keyout "$work/ca.key" -out "$work/ca.pem" 2>/dev/null
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=e2e broker" \
      -keyout "$work/server.key" -out "$work/server.csr" 2>/dev/null
    printf 'subjectAltName=IP:127.0.0.1\n' > "$work/server.ext"
    openssl x509 -req -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$work/ca.key" \
      -CAcreateserial -days 1 -extfile "$work/server.ext" -out "$work/server.pem" 2>/dev/null
    printf 'listener %s 127.0.0.1\ncafile %s\ncertfile %s\nkeyfile %s\n' \
      "$tls_port" "$work/ca.pem" "$work/server.pem" "$work/server.key" >> "$work/mosquitto.conf"
  fi
  mosquitto -c "$work/mosquitto.conf" > "$work/mosquitto.log" 2>&1 & mpid=$!
  sleep 0.5
fi

# --- source mapping per format --------------------------------------------------------------
subject="bench/#"; [ "$format" = sparkplug ] && subject='spBv1.0/bench/#'
case "$format" in
  json) map='map = { ts = "t", dev = "dev", temp = "temp", kw = "kw", ok = "ok" }'; rows='';;
  json-array) map='map = { ts = "t", dev = "$msg.dev", temp = "temp", kw = "kw", ok = "ok" }'; rows='rows = "samples"';;
  sparkplug) map='map = { ts = "$timestamp", dev = "$node", temp = "$value", kw = "$value", ok = "$historical" }'; rows='';;
  *) echo "unknown format $format" >&2; exit 2;;
esac
if [ "$transport" = file ]; then
  "$loadgen" --messages "$messages" --devices "$devices" --format "$format" --batch "$batch" --file "$work/input.ndjson"
  source="kind = \"file\"
url = \"$work/input.ndjson\""
elif [ "$tls" = 1 ]; then
  source="kind = \"mqtt\"
url = \"mqtts://127.0.0.1:$tls_port\"
tls_ca = \"$work/ca.pem\"
subject = \"$subject\""
else
  source="kind = \"mqtt\"
url = \"$broker_url\"
subject = \"$subject\""
fi
cat > "$work/ankaad.toml" <<EOF
[feed]
enabled = false
streams = []
[server]
flight_addr = "127.0.0.1:$flight_port"
metrics_addr = "127.0.0.1:$metrics_port"
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
format = "$( [ "$format" = sparkplug ] && echo sparkplug || echo json )"
table = "telemetry"
ts_unit = "millis"
max_message_bytes = 1048576
$rows
$map
EOF

metric() { curl -s -H "Authorization: Bearer $token" http://127.0.0.1:$metrics_port/metrics | awk -v m="$1" '$1 == m { print $2 }'; }
cpu_ticks() { awk '{ print $14 + $15 }' "/proc/$1/stat"; }

ANKAA_TOKEN="$token" "$ankaad" serve --config "$work/ankaad.toml" > "$out/ankaad.log" 2>&1 & apid=$!
for _ in $(seq 1 100); do [ -n "$(metric ankaa_uptime_seconds)" ] && break; sleep 0.1; done
[ "$transport" = mqtt ] && for _ in $(seq 1 100); do grep -q "mqtt connected" "$out/ankaad.log" && break; sleep 0.1; done

hz=$(getconf CLK_TCK)
# a file source starts reading at once: count its CPU from process start
c0=$(cpu_ticks "$apid"); [ "$transport" = file ] && c0=0
sudo perf stat -e cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses,L1-dcache-load-misses -p "$apid" -o "$out/perf-stat.txt" &
sudo perf record -F 499 -g -p "$apid" -o "$work/perf.data" > /dev/null 2>&1 &
t_start=$(date +%s.%N)
if [ "$transport" = mqtt ]; then
  "$loadgen" --messages "$messages" --devices "$devices" --format "$format" --batch "$batch" --mqtt "$broker_url" --qos 1 --connections "$publishers" 2> "$out/loadgen.log" &
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
# stop the perf processes themselves (a signal to the sudo wrapper does not reach them)
sudo pkill -INT -f "perf (stat|record) .*-p $apid" 2>/dev/null || true
for _ in $(seq 1 50); do pgrep -f "perf (stat|record) .*-p $apid" >/dev/null || break; sleep 0.1; done
[ -n "${lpid:-}" ] && wait "$lpid" 2>/dev/null || true
hwm_kb=$(awk '/VmHWM/ { print $2 }' "/proc/$apid/status")
rows=$(metric ankaa_user_rows_total); rejected=$(metric ankaa_user_rejected_total)
# self time per function (flat), then the same by thread, to see where each thread spends
sudo perf report -i "$work/perf.data" --no-children --stdio -g none --percent-limit 0.5 --sort dso,symbol 2>/dev/null \
  | grep -vE '^#|^$' | head -60 > "$out/perf-report.txt" || true
sudo perf report -i "$work/perf.data" --no-children --stdio -g none --sort comm 2>/dev/null \
  | grep -vE '^#|^$' | head -15 > "$out/perf-threads.txt" || true

wall=$(echo "$t_end - ${first:-$t_start}" | bc -l)
cpu_s=$(echo "($c1 - $c0) / $hz" | bc -l)
{
  echo "scenario:        $scenario ($transport, $format, batch $batch, $devices devices, tls $tls, publishers $publishers)"
  echo "host:            $(uname -m) $(lscpu 2>/dev/null | awk -F: '/Model name/ {gsub(/^ +/,"",$2); print $2; exit}') $(nproc) vCPU"
  echo "commit:          $(cat "$root/.source-commit" 2>/dev/null | cut -c1-12 || git -C "$root" rev-parse --short HEAD)"
  echo "rows stored:     $rows of $expected (rejected ${rejected:-0})"
  printf "wall (first→last row): %.3f s\n" "$wall"
  printf "rows per second: %.0f\n" "$(echo "$rows / $wall" | bc -l)"
  printf "ankaad CPU:      %.3f s = %.0f ns per row\n" "$cpu_s" "$(echo "$cpu_s * 1000000000 / $rows" | bc -l)"
  echo "peak RSS:        $((hwm_kb / 1024)) MiB"
  [ -f "$out/loadgen.log" ] && tail -1 "$out/loadgen.log"
} | tee "$out/summary.txt"
echo "--- perf stat"; grep -E 'cycles|instructions|misses|references' "$out/perf-stat.txt" | head -10
echo "--- CPU by thread"; cat "$out/perf-threads.txt"
echo "--- top functions (self time)"; head -30 "$out/perf-report.txt"
