#!/usr/bin/env bash
# Test ANKAA on Google Cloud Axion (C4A: Arm Neoverse V2 with SVE2) from a workstation.
#
# Prerequisites (once):
#   - a Google Cloud project with billing (free-trial credit works) and the gcloud CLI
#     logged in: `gcloud auth login` and `gcloud config set project <project-id>`
#   - nothing else: no service-account keys are created or needed
#
# Commands (run from the repository root):
#   scripts/gcp/ankaa-gcp.sh create        create the VM (picks a European zone that offers C4A)
#   scripts/gcp/ankaa-gcp.sh provision     install build tools and Rust on the VM
#   scripts/gcp/ankaa-gcp.sh deploy        copy the committed tree (git archive HEAD) to the VM
#   scripts/gcp/ankaa-gcp.sh first-light   start scripts/first-light.sh on the VM (background)
#   scripts/gcp/ankaa-gcp.sh wait          wait for first-light to finish, then fetch its results
#   scripts/gcp/ankaa-gcp.sh serve         install ankaad as a systemd service fed by live Binance data
#   scripts/gcp/ankaa-gcp.sh soak [hours]  start scripts/soak.sh against that service (default 24)
#   scripts/gcp/ankaa-gcp.sh status        service, soak and disk state
#   scripts/gcp/ankaa-gcp.sh fetch         copy results (first-light, soak CSV, logs) to target/gcp-results/
#   scripts/gcp/ankaa-gcp.sh crash-test    kill -9 the service, check it recovers with the same rows
#   scripts/gcp/ankaa-gcp.sh tunnel        forward Flight (50051) and /metrics (9100) to this machine
#   scripts/gcp/ankaa-gcp.sh ssh           interactive shell on the VM
#   scripts/gcp/ankaa-gcp.sh stop | start  stop the VM (disk kept, compute billing stops) / start it
#   scripts/gcp/ankaa-gcp.sh delete        delete the VM and its disk
#   scripts/gcp/ankaa-gcp.sh all           create + provision + deploy + first-light + wait + serve + soak
#   ANKAA_GCP_VM=ankaa-bench scripts/gcp/ankaa-gcp.sh compare
#                                          on a SEPARATE VM (nothing else may run there): create,
#                                          provision, deploy, then ANKAA vs QuestDB, ClickHouse and
#                                          DuckDB with bench/compare (background; `compare-wait`)
#
# Settings (environment variables):
#   ANKAA_GCP_VM       VM name (default ankaa-axion)
#   ANKAA_GCP_MACHINE  machine type (default c4a-standard-4: 4 vCPU, 16 GB)
#   ANKAA_GCP_ZONE     zone (default: first of a European list that offers the machine type;
#                      Binance refuses connections from US IP addresses)
#   ANKAA_GCP_DISK_GB  boot disk size (default 150: release and test builds of the workspace)
#   FIRST_LIGHT_SKIP   steps of first-light to skip (e.g. "tests replay recovery" to re-run benchmarks)
#
# Security: the VM has no service account and no API scopes; the server binds to loopback only
# (reach it with `tunnel`); the token is generated on the VM and never leaves it; SSH goes
# through OS Login with a key gcloud creates for you.
set -euo pipefail
export LC_ALL=C
cd "$(dirname "$0")/../.."
VM="${ANKAA_GCP_VM:-ankaa-axion}"
MACHINE="${ANKAA_GCP_MACHINE:-c4a-standard-4}"
DISK_GB="${ANKAA_GCP_DISK_GB:-150}"
ZONES_PREFERRED="europe-southwest1-a europe-southwest1-b europe-southwest1-c europe-west1-b europe-west1-c europe-west1-d europe-west4-a europe-west4-b europe-west4-c europe-west3-a europe-west3-b europe-west3-c europe-west2-a europe-west2-b europe-north1-a europe-north1-b"
RESULTS="target/gcp-results"

die() { echo "error: $*" >&2; exit 1; }
say() { printf '\n== %s\n' "$*"; }
need_gcloud() {
  command -v gcloud >/dev/null || die "gcloud is not installed (macOS: brew install --cask google-cloud-sdk)"
  local acct proj
  acct="$(gcloud config get-value account 2>/dev/null || true)"
  proj="$(gcloud config get-value project 2>/dev/null || true)"
  [ -n "$acct" ] || die "not logged in: run gcloud auth login"
  [ -n "$proj" ] || die "no project selected: run gcloud config set project <project-id>"
  PROJECT="$proj"
}

zone() {
  if [ -n "${ANKAA_GCP_ZONE:-}" ]; then echo "$ANKAA_GCP_ZONE"; return; fi
  # the zone of an existing VM wins
  local z
  z="$(gcloud compute instances list --filter="name=($VM)" --format='value(zone.basename())' 2>/dev/null | head -1)"
  if [ -n "$z" ]; then echo "$z"; return; fi
  local offered
  offered="$(gcloud compute machine-types list --filter="name=$MACHINE" --format='value(zone)' 2>/dev/null)"
  for z in $ZONES_PREFERRED; do
    if grep -qx "$z" <<<"$offered"; then echo "$z"; return; fi
  done
  die "no zone in the European list offers $MACHINE; set ANKAA_GCP_ZONE (see: gcloud compute machine-types list --filter=name=$MACHINE)"
}

remote() { # remote <command string>: run on the VM
  gcloud compute ssh "$VM" --zone "$ZONE" --quiet --command "$1"
}

cmd_create() {
  say "project $PROJECT, zone $ZONE, $MACHINE, ${DISK_GB} GB"
  if gcloud compute instances describe "$VM" --zone "$ZONE" >/dev/null 2>&1; then
    echo "VM $VM already exists"; return
  fi
  gcloud compute instances create "$VM" \
    --project "$PROJECT" --zone "$ZONE" \
    --machine-type "$MACHINE" \
    --image-family ubuntu-2404-lts-arm64 --image-project ubuntu-os-cloud \
    --boot-disk-size "${DISK_GB}GB" --boot-disk-type hyperdisk-balanced \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --metadata enable-oslogin=TRUE \
    --no-service-account --no-scopes \
    --labels app=ankaa,purpose=test
  echo "waiting for SSH..."
  for _ in $(seq 1 30); do
    if remote "true" >/dev/null 2>&1; then echo "VM ready"; return; fi
    sleep 10
  done
  die "the VM did not accept SSH within 5 minutes (check the firewall rule default-allow-ssh)"
}

cmd_provision() {
  say "packages and Rust on $VM"
  remote 'set -e
    sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential clang lld pkg-config cmake git curl jq python3 python3-venv tmux >/dev/null
    if ! command -v rustup >/dev/null && [ ! -x "$HOME/.cargo/bin/rustup" ]; then
      curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none >/dev/null
    fi
    echo "cpu: $(lscpu | sed -n "s/^Model name: *//p" | head -1), $(nproc) vCPU"
    grep -qw sve2 /proc/cpuinfo && echo "SVE2: yes, vector length $(( $(cat /proc/sys/abi/sve_default_vector_length) * 8 )) bits" || echo "SVE2: NO"
    code=$(curl -s -o /dev/null -w "%{http_code}" https://api.binance.com/api/v3/ping || true)
    echo "Binance REST from this region: HTTP $code (200 = reachable; 451 = region refused)"'
}

cmd_deploy() {
  say "deploy $(git rev-parse --short=12 HEAD) to $VM"
  [ -z "$(git status --porcelain)" ] || echo "note: uncommitted changes are NOT deployed (git archive HEAD)"
  git archive --format=tar HEAD | gzip -1 \
    | gcloud compute ssh "$VM" --zone "$ZONE" --quiet --command \
      "rm -rf ~/ankaa.new && mkdir ~/ankaa.new && tar -xz -C ~/ankaa.new && echo $(git rev-parse HEAD) > ~/ankaa.new/.source-commit && if [ -d ~/ankaa/target ]; then mv ~/ankaa/target ~/ankaa.new/; fi; rm -rf ~/ankaa && mv ~/ankaa.new ~/ankaa && echo deployed"
  remote 'cd ~/ankaa && . "$HOME/.cargo/env" && rustup show active-toolchain >/dev/null 2>&1 || rustup toolchain install "$(sed -n "s/^channel *= *\"\(.*\)\"/\1/p" rust-toolchain.toml)" --profile minimal --component rustfmt,clippy >/dev/null; rustc --version'
}

cmd_first_light() {
  say "first-light on $VM (runs in the background; ~30-60 min)"
  remote "cd ~/ankaa && rm -f ~/first-light.log ~/first-light.done && nohup setsid bash -c '. \"\$HOME/.cargo/env\"; FIRST_LIGHT_SKIP=\"${FIRST_LIGHT_SKIP:-}\" FIRST_LIGHT_LABEL=gcp-$MACHINE scripts/first-light.sh > ~/first-light.log 2>&1; echo \$? > ~/first-light.done' >/dev/null 2>&1 < /dev/null & echo started"
}

cmd_wait() {
  say "waiting for first-light on $VM"
  local t0=$SECONDS
  while :; do
    local st
    st="$(remote 'cat ~/first-light.done 2>/dev/null || echo running; tail -1 ~/first-light.log 2>/dev/null' 2>/dev/null || echo 'ssh failed')"
    if ! grep -q '^running' <<<"$st" && ! grep -q 'ssh failed' <<<"$st"; then
      echo "first-light finished with exit code $(head -1 <<<"$st")"
      break
    fi
    printf '  %s min: %s\n' "$(( (SECONDS - t0) / 60 ))" "$(tail -1 <<<"$st")"
    sleep 60
  done
  cmd_fetch
  local summary
  # newest run by name (directories start with a UTC timestamp), not by copy time
  summary="$(ls -d "$RESULTS"/first-light/*/ 2>/dev/null | sort | tail -1)"
  [ -n "$summary" ] && cat "$summary/summary.txt"
}

cmd_serve() {
  say "ankaad as a systemd service on $VM (live Binance, WAL, segments, loopback only)"
  remote 'set -e
    cd ~/ankaa && . "$HOME/.cargo/env"
    cargo build --release -p ankaad 2>&1 | tail -1   # always: a binary left by first-light may be older
    sudo install -m 0755 target/release/ankaad /usr/local/bin/ankaad
    id ankaa >/dev/null 2>&1 || sudo useradd --system --home /var/lib/ankaa --shell /usr/sbin/nologin ankaa
    sudo install -d -m 0750 -o root -g ankaa /etc/ankaa
    if [ ! -s /etc/ankaa/ankaad.env ]; then
      sudo install -m 0640 -o root -g ankaa /dev/null /etc/ankaa/ankaad.env
      printf "ANKAA_TOKEN=%s\n" "$(openssl rand -hex 32)" | sudo tee -a /etc/ankaa/ankaad.env >/dev/null
    fi
    sudo tee /etc/ankaa/ankaad.toml >/dev/null <<TOML
# soak configuration written by scripts/gcp/ankaa-gcp.sh
[feed]
url = "wss://data-stream.binance.vision/stream"
rest_url = "https://api.binance.com"
streams = ["btcusdt@trade", "btcusdt@depth@100ms", "ethusdt@trade", "ethusdt@depth@100ms", "solusdt@trade", "solusdt@depth@100ms"]
depth_snapshots = true

[server]
flight_addr = "127.0.0.1:50051"
metrics_addr = "127.0.0.1:9100"
token_env = "ANKAA_TOKEN"

[storage]
wal_dir = "/var/lib/ankaa/wal"
checkpoint_dir = "/var/lib/ankaa/segments"
checkpoint_interval_s = 60
# a small in-memory tier: it is full within the first hours, so a soak sees memory at its plateau
warm_budget_mib = 64
# retention runs during the soak: chunks older than two hours are dropped and their files deleted
market_retention = "2h"

[resources]
query_memory_mib = 2048

[[continuous]]
name = "bars_1m"
kind = "ohlcv"
table = "trades"
key = "symbol"
value = "price"
size = "size"
width_ns = 60000000000

[[continuous]]
name = "roll_5m"
kind = "rolling"
table = "trades"
key = "symbol"
value = "price"
size = "size"
width_ns = 300000000000
step_ns = 1000000000
TOML
    sudo chown root:ankaa /etc/ankaa/ankaad.toml && sudo chmod 0640 /etc/ankaa/ankaad.toml
    sudo install -m 0644 deploy/systemd/ankaad.service /etc/systemd/system/ankaad.service
    sudo systemctl daemon-reload
    sudo systemctl enable ankaad
    sudo systemctl restart ankaad   # also when it was already running: the binary may be new
    sleep 15
    systemctl is-active ankaad
    sudo journalctl -u ankaad --no-pager -n 15 | sed "s/\x1b\[[0-9;]*m//g"'
}

cmd_soak() {
  local hours="${1:-24}"
  say "soak for ${hours} h on $VM"
  remote "set -e
    pid=\$(systemctl show -p MainPID --value ankaad)
    [ \"\$pid\" != 0 ] || { echo 'ankaad is not running: run serve first'; exit 1; }
    sudo install -d -m 0755 /var/lib/ankaa-soak
    sudo systemctl reset-failed ankaa-soak 2>/dev/null || true
    sudo systemd-run --unit ankaa-soak --property=EnvironmentFile=/etc/ankaa/ankaad.env \
      \"\$HOME/ankaa/scripts/soak.sh\" --hours $hours --interval 30 --pid \"\$pid\" --out /var/lib/ankaa-soak/soak.csv
    sleep 3; systemctl status ankaa-soak --no-pager | head -5"
}

cmd_status() {
  remote 'echo "ankaad: $(systemctl is-active ankaad 2>/dev/null) since $(systemctl show -p ActiveEnterTimestamp --value ankaad 2>/dev/null)"
    echo "restarts: $(systemctl show -p NRestarts --value ankaad 2>/dev/null)"
    echo "soak: $(systemctl is-active ankaa-soak 2>/dev/null) ($(($(wc -l < /var/lib/ankaa-soak/soak.csv 2>/dev/null || echo 1) - 1)) samples)"
    sudo journalctl -u ankaa-soak --no-pager -n 3 2>/dev/null | tail -2
    tok=$(sudo sed -n "s/^ANKAA_TOKEN=//p" /etc/ankaa/ankaad.env 2>/dev/null)
    printf "header = \"Authorization: Bearer %s\"\n" "$tok" | curl -s -K - http://127.0.0.1:9100/metrics 2>/dev/null \
      | grep -E "^ankaa_(frames_total|rows|feed_gaps_total|source_dropped_full_total|wal_dropped_total|wal_errors_total|wal_bytes|wal_durable_lsn|uptime_seconds|book_resyncs_total|book_crossed_messages_total) " || echo "metrics not reachable"
    echo "rss: $(ps -o rss= -p $(systemctl show -p MainPID --value ankaad) 2>/dev/null | tr -d " ") kB"
    df -h / /var/lib/ankaa 2>/dev/null | tail -1'
}

cmd_fetch() {
  say "fetching results into $RESULTS/"
  mkdir -p "$RESULTS/first-light" "$RESULTS/soak"
  remote 'mkdir -p ~/export && rm -rf ~/export/* && cp -r ~/ankaa/target/first-light ~/export/ 2>/dev/null || true
    cp ~/first-light.log ~/export/ 2>/dev/null || true
    sudo cp /var/lib/ankaa-soak/soak.csv ~/export/ 2>/dev/null || true
    sudo journalctl -u ankaa-soak --no-pager > ~/export/soak.log 2>/dev/null || true
    sudo journalctl -u ankaad --no-pager | sed "s/\x1b\[[0-9;]*m//g" | tail -2000 > ~/export/ankaad.log 2>/dev/null || true
    sudo chown -R "$USER" ~/export' >/dev/null
  gcloud compute scp --zone "$ZONE" --quiet --recurse "$VM:export/*" "$RESULTS/" >/dev/null
  # the VM user name is derived from the Google account (e-mail): keep it out of everything fetched
  local home
  home="$(remote 'echo $HOME' 2>/dev/null | tail -1)"
  if [ -n "$home" ]; then
    grep -rlF "$home" "$RESULTS" 2>/dev/null | while read -r f; do
      sed -i.bak "s#${home}#~#g" "$f" && rm -f "$f.bak"
    done
  fi
  [ -f "$RESULTS/soak.csv" ] && mv "$RESULTS/soak.csv" "$RESULTS/soak/soak.csv"
  ls -1 "$RESULTS" "$RESULTS/first-light" 2>/dev/null | sed 's/^/  /'
}

cmd_crash_test() {
  say "crash test: kill -9 ankaad, check recovery"
  remote 'tok=$(sudo sed -n "s/^ANKAA_TOKEN=//p" /etc/ankaa/ankaad.env)
    # the last 30 minutes before the cut-off: retention (2 h) never touches them between the two counts
    q() { ANKAA_TOKEN="$tok" ankaad query --addr 127.0.0.1:50051 --sql "SELECT count(*) FROM trades WHERE ts < to_timestamp_nanos($1) AND ts >= to_timestamp_nanos($(( $1 - 1800000000000 )))" 2>/dev/null | awk -F"|" "/^\|/ {n++; if (n==2) {gsub(/ /,\"\",\$2); print \$2}}"; }
    cut=$(( $(date +%s) * 1000000000 - 5000000000 ))
    before=$(q $cut)
    sudo systemctl kill -s KILL ankaad
    for i in $(seq 1 60); do sleep 1; systemctl is-active -q ankaad && break; done
    for i in $(seq 1 60); do sleep 1; after=$(q $cut); [ -n "$after" ] && break; done
    echo "trades older than the cut-off: before $before, after recovery $after"
    sudo journalctl -u ankaad --no-pager -n 40 | sed "s/\x1b\[[0-9;]*m//g" | grep -E "segments restored|wal recovery done|continuous queries rebuilt" | tail -3
    [ "$before" = "$after" ] && echo "crash test: OK" || { echo "crash test: ROW COUNTS DIFFER"; exit 1; }'
  echo "note: a kill -9 restarts ankaad, so a soak running at that moment fails by design; start a new soak afterwards"
}

cmd_compare() {
  say "comparison on $VM (background; about 1-2 h)"
  remote "rm -f ~/compare.log ~/compare.done && nohup setsid bash -c 'COMPARE_COPIES=\"${COMPARE_COPIES:-1000}\" COMPARE_CAPTURE=\"${COMPARE_CAPTURE:-}\" COMPARE_RUNS=\"${COMPARE_RUNS:-20}\" ~/ankaa/scripts/gcp/compare-on-vm.sh > ~/compare.log 2>&1; echo \$? > ~/compare.done' >/dev/null 2>&1 < /dev/null & echo started"
}

cmd_compare_wait() {
  local t0=$SECONDS
  while :; do
    local st
    st="$(remote 'cat ~/compare.done 2>/dev/null || echo running; grep "^== " ~/compare.log 2>/dev/null | tail -1' 2>/dev/null || echo 'ssh failed')"
    if ! grep -q '^running' <<<"$st" && ! grep -q 'ssh failed' <<<"$st"; then
      echo "comparison finished with exit code $(head -1 <<<"$st")"; break
    fi
    printf '  %s min: %s\n' "$(( (SECONDS - t0) / 60 ))" "$(tail -1 <<<"$st")"
    sleep 60
  done
  mkdir -p "$RESULTS/compare"
  remote 'mkdir -p ~/export-compare && rm -rf ~/export-compare/* && cp ~/compare.log ~/export-compare/ && cp ~/ankaa/bench/compare/results/*.json ~/ankaa/bench/compare/results/*.md ~/export-compare/ 2>/dev/null; true' >/dev/null
  gcloud compute scp --zone "$ZONE" --quiet --recurse "$VM:export-compare/*" "$RESULTS/compare/" >/dev/null
  local home; home="$(remote 'echo $HOME' 2>/dev/null | tail -1)"
  [ -n "$home" ] && grep -rlF "$home" "$RESULTS/compare" 2>/dev/null | while read -r f; do sed -i.bak "s#${home}#~#g" "$f" && rm -f "$f.bak"; done
  ls -1 "$RESULTS/compare"
}

cmd_tunnel() {
  say "Flight on 127.0.0.1:50051 and /metrics on 127.0.0.1:9100 (Ctrl-C to stop)"
  echo "token: scripts/gcp/ankaa-gcp.sh ssh, then: sudo cat /etc/ankaa/ankaad.env"
  gcloud compute ssh "$VM" --zone "$ZONE" --quiet -- -N -L 50051:127.0.0.1:50051 -L 9100:127.0.0.1:9100
}

main() {
  local c="${1:-}"; shift || true
  case "$c" in
    ""|-h|--help|help) sed -n '2,37p' "$0"; exit 0;;
  esac
  need_gcloud
  # a new project starts with the Compute API off; zone lookup needs it
  if ! gcloud services list --enabled --project "$PROJECT" --format='value(config.name)' 2>/dev/null | grep -qx compute.googleapis.com; then
    echo "enabling the Compute Engine API (first use of this project; about a minute)..."
    gcloud services enable compute.googleapis.com --project "$PROJECT"
  fi
  ZONE="$(zone)"
  case "$c" in
    create) cmd_create;;
    provision) cmd_provision;;
    deploy) cmd_deploy;;
    first-light) cmd_first_light;;
    wait) cmd_wait;;
    serve) cmd_serve;;
    soak) cmd_soak "${1:-24}";;
    status) cmd_status;;
    fetch) cmd_fetch;;
    crash-test) cmd_crash_test;;
    tunnel) cmd_tunnel;;
    ssh) gcloud compute ssh "$VM" --zone "$ZONE";;
    stop) gcloud compute instances stop "$VM" --zone "$ZONE";;
    start) gcloud compute instances start "$VM" --zone "$ZONE";;
    delete) gcloud compute instances delete "$VM" --zone "$ZONE";;
    compare) cmd_create; cmd_provision; cmd_deploy; cmd_compare;;
    compare-wait) cmd_compare_wait;;
    all) cmd_create; cmd_provision; cmd_deploy; cmd_first_light; cmd_wait; cmd_serve; cmd_soak 24;;
    *) die "unknown command: $c (try --help)";;
  esac
}
main "$@"
