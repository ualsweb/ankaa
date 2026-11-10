#!/usr/bin/env bash
# First run on a real Arm server (AWS Graviton4, Google Axion, Ampere, Grace): records what the
# machine is, builds, runs the test suite natively (SVE2 kernels run for real when the CPU has
# SVE2), the microbenchmarks, a replay and a crash-recovery check, and writes everything under
# one result directory that can be attached to a benchmark note.
#
#   scripts/first-light.sh [result-dir]
#
# Environment:
#   FIRST_LIGHT_LABEL   label for the results (default: "<arch>-<kernel>"); the real hostname is
#                       never written, so the directory can be published as it is
#   FIRST_LIGHT_SKIP    space-separated steps to skip: tests bench replay recovery
#   REPLAY_FACTOR       how many times the bundled 20 s sample is repeated for the replay and
#                       recovery input (default 1000: about 180 MB, 3.7 million events)
#
# Needs: Linux on aarch64, a Rust toolchain (rust-toolchain.toml pins it), python3. Nothing is
# installed and nothing outside the repository and the result directory is written.
set -euo pipefail
export LC_ALL=C   # decimal points and stable sort order whatever the host locale is
cd "$(dirname "$0")/.."
repo="$PWD"
label="${FIRST_LIGHT_LABEL:-$(uname -m)-$(uname -r | cut -d- -f1)}"
label="$(printf '%s' "$label" | tr -c 'A-Za-z0-9._-' '-')"
out="${1:-$repo/target/first-light/$(date -u +%Y%m%dT%H%M%SZ)-$label}"
mkdir -p "$out"
skip=" ${FIRST_LIGHT_SKIP:-} "
want() { case "$skip" in *" $1 "*) return 1;; *) return 0;; esac; }
say() { printf '\n== %s\n' "$*" | tee -a "$out/summary.txt"; }
fail=0
now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.3f", b - a }'; }
step() { # step <name> <command...>: run, log, keep going on failure
  local name="$1"; shift
  say "$name"
  local t0=$SECONDS
  if "$@" >"$out/$name.log" 2>&1; then
    echo "ok ($((SECONDS - t0)) s)" | tee -a "$out/summary.txt"
  else
    echo "FAILED ($((SECONDS - t0)) s) — see $name.log" | tee -a "$out/summary.txt"
    fail=1
  fi
}

say "system"
{
  echo "label: $label"
  echo "date_utc: $(date -u +%FT%TZ)"
  echo "kernel: $(uname -sr) $(uname -m)"
  echo "git_commit: $(git rev-parse --short=12 HEAD 2>/dev/null || cut -c1-12 .source-commit 2>/dev/null || echo unknown)"
  [ -d .git ] && echo "git_dirty: $(git status --porcelain 2>/dev/null | wc -l | tr -d ' ') files"
  echo "rustc: $(rustc --version 2>/dev/null || echo missing)"
  if [ -r /proc/cpuinfo ]; then
    echo "cpus: $(grep -c ^processor /proc/cpuinfo)"
    echo "cpu_part: $(grep -m1 -i 'cpu part' /proc/cpuinfo | cut -d: -f2 | tr -d ' ')"
    echo "cpu_implementer: $(grep -m1 -i 'cpu implementer' /proc/cpuinfo | cut -d: -f2 | tr -d ' ')"
    feats="$(grep -m1 -i '^features' /proc/cpuinfo | cut -d: -f2)"
    # Linux names: `atomics` is LSE, `asimd` is NEON
    for f in asimd sve sve2 sve2p1 sme sme2 crc32 aes sha3 atomics rng bf16 i8mm; do
      case " $feats " in *" $f "*) echo "feature_$f: yes";; *) echo "feature_$f: no";; esac
    done
  fi
  [ -r /proc/sys/abi/sve_default_vector_length ] && echo "sve_default_vector_length_bytes: $(cat /proc/sys/abi/sve_default_vector_length)"
  [ -r /proc/meminfo ] && grep -E '^(MemTotal|Hugepagesize|HugePages_Total):' /proc/meminfo | sed 's/  */ /g'
  [ -r /sys/devices/virtual/dmi/id/product_name ] && echo "dmi_product: $(cat /sys/devices/virtual/dmi/id/product_name)"
  command -v lscpu >/dev/null && lscpu | grep -E '^(Model name|Thread|Core|Socket|L1d|L1i|L2|L3|NUMA node\(s\)|BogoMIPS)' | sed 's/  */ /g'
  echo "cpu_governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo n/a)"
  df -h "$repo" | tail -1 | awk '{print "disk_free: " $4}'
} | tee "$out/system.txt" | tee -a "$out/summary.txt"

if [ "$(uname -s)" != Linux ] || [ "$(uname -m)" != aarch64 ]; then
  say "note"
  echo "not aarch64 Linux: results are development signals only (docs/measurement-protocol.md)" | tee -a "$out/summary.txt"
fi

step build cargo build --release --workspace --all-features
ankaad="$repo/target/release/ankaad"

if want tests; then
  # ANKAA_EXPECT_SVE2 makes the suite fail instead of silently skipping when SVE2 is not used
  vl="$(cat /proc/sys/abi/sve_default_vector_length 2>/dev/null || true)"
  if grep -qw sve2 /proc/cpuinfo 2>/dev/null && [ -n "$vl" ]; then
    step tests-simd-sve2 env ANKAA_EXPECT_SVE2="$vl" cargo test --release -p ankaa-simd --features sve2 -- --test-threads=1
  else
    say "tests-simd-sve2"; echo "skipped: this CPU has no SVE2" | tee -a "$out/summary.txt"
  fi
  step tests-workspace cargo test --release --workspace --all-features
fi

if want bench; then
  # the SVE2 backend is benchmarked when the CPU has it (the feature only builds on aarch64 Linux)
  bench_features=""
  grep -qw sve2 /proc/cpuinfo 2>/dev/null && bench_features="--features sve2"
  step bench bash -c "cd bench && cargo bench $bench_features --bench hot_paths -- --noplot"
  grep -E '^[a-z_0-9]+/|time:|thrpt:' "$out/bench.log" >"$out/bench-summary.txt" 2>/dev/null || true
fi

sample="$repo/bench/data/binance-btcusdt-trade-depth-20s.ankc"
big="$out/replay-input.ankc"
if want replay || want recovery; then
  step make-replay-input python3 bench/scripts/make_profile_capture.py "$sample" "$big" "${REPLAY_FACTOR:-1000}"
fi

if want replay && [ -s "$big" ]; then
  say "replay (5 runs, wall seconds)"
  for i in 1 2 3 4 5; do
    s=$(now)
    "$ankaad" replay "$big" --sql "SELECT count(*) FROM trades" >"$out/replay-$i.log" 2>&1 || fail=1
    echo "run $i: $(elapsed "$s" "$(now)") s" | tee -a "$out/summary.txt"
  done
  ls -l "$big" | awk '{print "input bytes: " $5}' | tee -a "$out/summary.txt"
fi

if want recovery && [ -s "$big" ]; then
  # ingest the capture with a WAL and segments, kill -9, restart without a feed, compare row counts
  say "crash recovery"
  work="$out/recovery"; mkdir -p "$work"
  port_f=$((20000 + RANDOM % 20000)); port_m=$((port_f + 1))
  config() { # config <feed url>
    cat <<TOML
[feed]
url = "$1"
depth_snapshots = false
[server]
flight_addr = "127.0.0.1:$port_f"
metrics_addr = "127.0.0.1:$port_m"
[storage]
wal_dir = "$work/wal"
checkpoint_dir = "$work/segments"
checkpoint_interval_s = 1
TOML
  }
  config "file://$big" >"$work/run1.toml"
  config "file://" >"$work/run2.toml"      # no capture: the second run only recovers
  # first data cell of a one-value query result
  cell() { "$ankaad" query --addr "127.0.0.1:$port_f" --sql "$1" 2>/dev/null | awk -F'|' '/^\|/ { n++; if (n == 2) { gsub(/ /, "", $2); print $2 } }'; }
  rows() { echo "trades=$(cell 'SELECT count(*) FROM trades') book_deltas=$(cell 'SELECT count(*) FROM book_deltas') quotes=$(cell 'SELECT count(*) FROM quotes')"; }
  "$ankaad" serve --config "$work/run1.toml" >"$work/run1.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 600); do grep -q "capture served" "$work/run1.log" && break; sleep 1; done
  sleep 3   # several group commits and at least one snapshot tick
  before="$(rows)"
  kill -9 "$pid"; wait "$pid" 2>/dev/null || true
  s=$(now)
  "$ankaad" serve --config "$work/run2.toml" >"$work/run2.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 6000); do grep -q "flight listening" "$work/run2.log" && break; sleep 0.05; done
  e=$(now)
  sleep 1
  after="$(rows)"
  grep -E "segments restored|wal recovery done|continuous queries rebuilt" "$work/run2.log" | sed 's/\x1b\[[0-9;]*m//g' | tee -a "$out/summary.txt"
  kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  echo "rows before the crash: $before" | tee -a "$out/summary.txt"
  echo "rows after recovery:   $after" | tee -a "$out/summary.txt"
  echo "start to serving (segments + WAL replay): $(elapsed "$s" "$e") s" | tee -a "$out/summary.txt"
  for d in wal segments; do echo "$d on disk: $(du -sh "$work/$d" 2>/dev/null | cut -f1)"; done | tee -a "$out/summary.txt"
  if [ -n "$before" ] && [ "$before" = "$after" ] && [ "$before" != "trades= book_deltas= quotes=" ]; then
    echo "recovery: row counts match" | tee -a "$out/summary.txt"
  else
    echo "recovery: ROW COUNTS DIFFER" | tee -a "$out/summary.txt"; fail=1
  fi
  rm -rf "$work/wal" "$work/segments"
fi
rm -f "$big"

say "done"
echo "results: ${out#"$repo"/}" | tee -a "$out/summary.txt"
[ "$fail" = 0 ] || { echo "at least one step failed" | tee -a "$out/summary.txt"; exit 1; }
