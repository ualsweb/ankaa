# deploy/

Deployment assets for a single-node `ankaad serve`. The full guide is `docs/deployment.md`; metric meanings and
alert rationale are in `docs/metrics-semantics.md`.

| File | What it is |
|---|---|
| `ankaad.example.toml` | Commented configuration. As written it is the loopback development variant (no token, no TLS, no WAL); the production settings (bind addresses, WAL, TLS, memlock) are in the commented lines. Schema: `crates/ankaa-core/src/config.rs`; `ankaad config print` shows defaults, `ankaad config check <file>` validates. |
| `systemd/ankaad.service` | Hardened unit: dedicated user, token from `/etc/ankaa/ankaad.env`, `config check` before start, `Restart=always` as the supervisor for `panic = "abort"`, SIGINT for orderly shutdown and the final WAL commit, `LimitMEMLOCK`, `StateDirectory=ankaa`, sandboxing directives. |
| `prometheus/prometheus.yml` | Scrape job for `/metrics` (10 s, bearer token from `/etc/prometheus/ankaa.token`) that loads `alerts.yml`. |
| `prometheus/alerts.yml` | The recommended alert rules: feed stalled, gaps, resyncs, reconnect storms, channel drops, bad frames, out-of-order drops, ingest latency (lifetime p99 and windowed mean), WAL errors/drops/stall/pending bytes, query error rate, unauthenticated calls, process down/restart, snapshot tick starvation. Thresholds are starting points. |
| `grafana/ankaa-overview.json` | Grafana 10/11 dashboard with a `${DS_PROMETHEUS}` input and an `instance` variable. Rows: Ingest, Feed health, Latency, Durability, Storage, Queries. Counters are plotted with `rate()`/`increase()`; the tier and checkpoint panels show zeros until tiering lands. |

## Quick start (five commands)

Loopback, no token, no TLS, with a WAL. Build first with `cargo build --release -p ankaad`.

```sh
# 1. Start from the example config and enable the WAL (uncomment wal_dir, point it at ./wal)
sed 's|^# wal_dir = .*|wal_dir = "./wal"|' deploy/ankaad.example.toml > ankaad.toml

# 2. Validate it (parses, validates, runs the resource checks)
target/release/ankaad config check ankaad.toml

# 3. Serve live Binance streams over Arrow Flight with /metrics (Ctrl-C to stop; the WAL is recovered on restart)
target/release/ankaad serve --config ankaad.toml

# 4. Query it from another shell
target/release/ankaad query --sql "SELECT s.name, count(*) FROM trades t JOIN symbols s ON s.symbol = t.symbol GROUP BY s.name"

# 5. Scrape the metrics
curl -s http://127.0.0.1:9100/metrics | grep -E '^ankaa_(frames_total|events_total|wal_durable_lsn|ingest_latency_ns)'
```

For a non-loopback deployment set `ANKAA_TOKEN` (required by validation), add TLS files to `[server]`, install the
systemd unit, and load `prometheus/prometheus.yml` + `alerts.yml` and the Grafana dashboard. Steps and the
troubleshooting table are in `docs/deployment.md`.

Pre-alpha: no performance numbers are published, formats are not yet frozen, and items marked "planned" in the
docs (tiering, checkpoints, WAL truncation, memory locking) are not active in this build.
