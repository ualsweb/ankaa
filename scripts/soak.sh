#!/usr/bin/env bash
# Soak check for a running `ankaad serve`: scrape /metrics at an interval, keep the samples as
# CSV, and fail when a counter that must stay flat moves or the process grows without bound.
#
#   ANKAA_TOKEN=... scripts/soak.sh [--url http://127.0.0.1:9100/metrics] [--hours 24]
#                                   [--interval 30] [--out soak.csv] [--pid <ankaad pid>]
#                                   [--max-rss-growth-pct 50] [--allow-gaps N]
#
# Exit status: 0 = clean for the whole period, 1 = a check failed (the reason is printed and the
# CSV shows when), 2 = the endpoint could not be scraped three times in a row.
#
# What is checked on every sample (deltas since the first sample):
#   ankaa_feed_gaps_total                 <= --allow-gaps (default 0)
#   ankaa_source_dropped_full_total       == 0     frames dropped at the ingest channel
#   ankaa_wal_dropped_total               == 0     frames that never reached the WAL
#   ankaa_wal_errors_total                == 0
#   ankaa_frames_bad_total                == 0     frames a decoder rejected
#   ankaa_cq_rejected_rows_total          == 0
#   ankaa_kafka_offset_resets_total       == 0
#   ankaa_uptime_seconds                  never decreases (a restart fails the soak)
#   memory of --pid (when given) outside the data tiers (resident minus hot and warm tier bytes) grows
#   less than --max-rss-growth-pct (and 64 MiB) after the first hour; resident and data are in the CSV
# The token is read from ANKAA_METRICS_TOKEN or ANKAA_TOKEN and is never printed.
# Needs bash 4 or newer, curl and awk.
set -euo pipefail
export LC_ALL=C
url="http://127.0.0.1:9100/metrics"; hours=24; interval=30; out="soak.csv"; pid=""; max_growth=50; allow_gaps=0
while [ $# -gt 0 ]; do
  case "$1" in
    --url) url="$2"; shift 2;;
    --hours) hours="$2"; shift 2;;
    --interval) interval="$2"; shift 2;;
    --out) out="$2"; shift 2;;
    --pid) pid="$2"; shift 2;;
    --max-rss-growth-pct) max_growth="$2"; shift 2;;
    --allow-gaps) allow_gaps="$2"; shift 2;;
    -h|--help) sed -n '2,24p' "$0"; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
token="${ANKAA_METRICS_TOKEN:-${ANKAA_TOKEN:-}}"
scrape() {
  if [ -n "$token" ]; then
    # the header goes through stdin, not the command line (visible to other local users)
    printf 'header = "Authorization: Bearer %s"\n' "$token" | curl -fsS --max-time 10 -K - "$url"
  else
    curl -fsS --max-time 10 "$url"
  fi
}
# value of an unlabelled metric in a scrape (0 when absent)
val() { awk -v m="$1" '$1 == m { print $2; found = 1; exit } END { if (!found) print 0 }' <<<"$2"; }
rss_kb() { [ -n "$pid" ] && ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo 0; }

flat="ankaa_source_dropped_full_total ankaa_wal_dropped_total ankaa_wal_errors_total ankaa_frames_bad_total ankaa_cq_rejected_rows_total ankaa_kafka_offset_resets_total"
watch="ankaa_frames_total ankaa_events_total ankaa_rows ankaa_wal_durable_lsn ankaa_feed_gaps_total ankaa_book_resyncs_total ankaa_user_rows_total ankaa_user_rejected_total ankaa_query_errors_total ankaa_uptime_seconds $flat"
# bytes the server itself accounts for as data held in memory (hot chunks and the encoded warm tier)
data_kb() { awk '/^ankaa_tier_bytes\{tier="(hot|warm)"\}/ { s += $2 } END { printf "%d", s / 1024 }' <<<"$1"; }
echo "epoch,rss_kb,data_kb,$(echo "$watch" | tr ' ' ',')" >"$out"
end=$(awk -v n="$(date +%s)" -v h="$hours" 'BEGIN { printf "%d", n + h * 3600 }')   # fractions of an hour are fine
declare -A first
first_set=0; misses=0; base_rss=0; last_uptime=0; samples=0; body=""
echo "soak: $url every ${interval}s for ${hours}h -> $out"
while [ "$(date +%s)" -lt "$end" ]; do
  if ! body="$(scrape 2>/dev/null)"; then
    misses=$((misses + 1))
    echo "$(date -u +%FT%TZ) scrape failed ($misses)" >&2
    [ "$misses" -ge 3 ] && { echo "soak: FAILED — metrics endpoint unreachable three times in a row" >&2; exit 2; }
    sleep "$interval"; continue
  fi
  misses=0; samples=$((samples + 1))
  now=$(date +%s); rss="$(rss_kb)"; data="$(data_kb "$body")"; row="$now,$rss,$data"
  for m in $watch; do
    v="$(val "$m" "$body")"; row="$row,$v"
    [ "$first_set" = 0 ] && first[$m]="$v"
  done
  first_set=1
  echo "$row" >>"$out"
  problem=""
  for m in $flat; do
    v="$(val "$m" "$body")"
    [ "$(awk -v a="$v" -v b="${first[$m]}" 'BEGIN { print (a - b > 0) ? 1 : 0 }')" = 1 ] && problem="$problem $m moved from ${first[$m]} to $v;"
  done
  gaps="$(val ankaa_feed_gaps_total "$body")"
  [ "$(awk -v a="$gaps" -v b="${first[ankaa_feed_gaps_total]}" -v n="$allow_gaps" 'BEGIN { print (a - b > n) ? 1 : 0 }')" = 1 ] && problem="$problem feed gaps: $gaps;"
  up="$(val ankaa_uptime_seconds "$body")"
  [ "$(awk -v a="$up" -v b="$last_uptime" 'BEGIN { print (a + 0 < b + 0) ? 1 : 0 }')" = 1 ] && problem="$problem the server restarted (uptime $last_uptime -> $up);"
  last_uptime="$up"
  if [ -n "$pid" ]; then
    kill -0 "$pid" 2>/dev/null || problem="$problem process $pid is gone;"
    # Memory. The server keeps recent data in memory on purpose (hot chunks, then the encoded warm
    # tier up to storage.warm_budget_mib), so resident memory rises until those budgets are full
    # and that rise is not a fault. What must stay flat is the rest: resident memory minus the
    # data the server accounts for. Its baseline is taken after one hour; it may not grow by more
    # than --max-rss-growth-pct and 64 MiB (a small process doubles on noise alone).
    other=$((rss - data))
    if [ "$base_rss" = 0 ] && [ "$(awk -v u="$up" 'BEGIN { print (u >= 3600) ? 1 : 0 }')" = 1 ]; then base_rss="$other"; fi
    if [ "$base_rss" != 0 ] && [ "$(awk -v r="$other" -v b="$base_rss" -v g="$max_growth" 'BEGIN { print (r > b * (1 + g / 100) && r - b > 65536) ? 1 : 0 }')" = 1 ]; then
      problem="$problem memory outside the data tiers grew from ${base_rss} kB to ${other} kB (> ${max_growth} %); resident ${rss} kB, data ${data} kB;"
    fi
  fi
  if [ -n "$problem" ]; then
    echo "soak: FAILED at $(date -u +%FT%TZ) after $samples samples:$problem" >&2
    exit 1
  fi
  sleep "$interval"
done
[ "$samples" -gt 0 ] || { echo "soak: FAILED — no sample was taken" >&2; exit 2; }
echo "soak: clean — $samples samples over ${hours}h; frames $(val ankaa_frames_total "$body"), rows $(val ankaa_rows "$body"), durable LSN $(val ankaa_wal_durable_lsn "$body")"
