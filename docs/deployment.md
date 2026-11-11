# Deployment guide (single node)

This page covers running `ankaad serve` on one machine: build, configuration, secrets, TLS, systemd,
Linux tuning, disk layout, backup and restore, upgrades, sizing and troubleshooting. Ready-made files live under
`deploy/` (example config, systemd unit, Prometheus scrape and alert files, Grafana dashboard).

Status reminder: the project is pre-alpha. Nothing below has been soaked for 24 h on production hardware yet
(that is gate G1 in `docs/ROADMAP.md`), and no performance numbers are published. Items marked **planned** describe
what the configuration already names but the engine does not yet do.

## 1. Build

Stable Rust only; the toolchain is pinned in `rust-toolchain.toml` (1.99.1 with `rustfmt`, `clippy`, `llvm-tools`).
Release builds use `panic = "abort"`, thin LTO and `codegen-units = 4` (`Cargo.toml`).

**aarch64 Linux (first-class target)** — Graviton, Ampere, Grace, Axion, Cobalt, GitHub Arm runners:

```sh
rustup toolchain install 1.99.1 --component rustfmt clippy llvm-tools
sudo apt-get install -y build-essential pkg-config        # Debian/Ubuntu; rustls is used, no OpenSSL dev package needed
cargo build --release -p ankaad
ls -l target/release/ankaad
```

The binary detects NEON/SVE2 at run time (HWCAP2 plus a self-test) and picks per kernel between them; one build
runs on every Armv8/v9 server. A C compiler is needed: the default build links mimalloc as its allocator (measured
on Neoverse V2 against glibc's: 2-17 % faster queries over Flight and no run-to-run variation, for about a fifth
more resident memory). `cargo build --release -p ankaad --no-default-features` keeps the system allocator.
Cross-compiling from x86_64 is possible with the `aarch64-unknown-linux-gnu` target but not exercised in CI.

**macOS on Apple Silicon (development)** — the workspace builds and `serve`, `replay` and `query` run natively.
SVE2 kernels are skipped (Apple Silicon has no SVE outside streaming mode). `RLIMIT_MEMLOCK` is checked the same way;
`ulimit -l` applies. Use a separate `CARGO_TARGET_DIR` if you also build inside a Linux VM.

**x86_64 Linux** builds and runs with the scalar backend. It is supported for correctness work only.

Verify the build before deploying:

```sh
target/release/ankaad --version
target/release/ankaad config print           # default TOML
target/release/ankaad replay bench/data/binance-btcusdt-trade-depth-20s.ankc --sql "SELECT count(*) FROM trades"
```

## 2. Configuration

`ankaad serve` reads one TOML file (`--config <file>` / `-c`). Every setting has a CLI flag; **the flag wins** when
both are present. Unknown keys are rejected (`deny_unknown_fields`), so typos fail fast. `ankaad config print`
prints the defaults; `ankaad config check <file>` parses, validates and runs the start-up resource checks (exit code
non-zero on failure), which is what you want in a CI step or a systemd `ExecStartPre`.

The schema is `crates/ankaa-core/src/config.rs`. All fields, with defaults:

### `[feed]`

| Key | Default | CLI | Meaning |
|---|---|---|---|
| `url` | `"wss://data-stream.binance.vision/stream"` | `--url` | WebSocket base URL. Must start with `ws://` or `wss://`. The stream list is appended as `?streams=a/b/c`. The Binance SBE endpoint is `wss://stream-sbe.binance.com/stream` (needs an API key). |
| `rest_url` | `"https://api.binance.com"` | `--rest-url` | REST base for depth snapshots (`/api/v3/depth?symbol=…&limit=1000`). Must start with `http://` or `https://`. |
| `enabled` | `true` | — | `false` runs without the exchange feed: only `[[sources]]` feed the server and `streams` may be empty. |
| `streams` | `["btcusdt@trade", "btcusdt@depth@100ms"]` | `--streams a,b,c` | Binance combined-stream names. Non-empty; each must contain `@` and no `/` or space. |
| `depth_snapshots` | `true` | `--no-depth-snapshots` | Fetch a REST snapshot per depth symbol on every connect and on resync, and align diffs to it (exact books). `false` starts books cold from diffs: cheaper, but `quotes` are incomplete until levels churn. |
| `api_key_env` | unset | `--api-key` / env `BINANCE_API_KEY` | Name of the environment variable holding the exchange API key; `--api-key` and `BINANCE_API_KEY` take precedence. |

### `[storage]`

| Key | Default | CLI | Meaning |
|---|---|---|---|
| `wal_dir` | unset (no WAL) | `--wal-dir` | Directory for write-ahead-log segments. Setting it enables group-committed durability and recovery on start. Created if missing. |
| `hot_sealed_chunks` | `4` | — | Sealed chunks per partition kept uncompressed (hot) before encoding to the warm tier. Must be ≥ 1. |
| `hot_budget_mib` | `1024` | — | Memory for uncompressed rows: open chunks (counted twice: the chunk and its query view) plus hot sealed chunks. Over it, the partitions whose open rows are oldest are sealed and encoded first (open chunks of at least 1,024 rows), then every partition's hot window is given up. Matters with many partitions that fill slowly (one per device): without it their rows would wait uncompressed until 65,536 per device arrived. 0 = no budget. |
| `max_open_age_s` | `600` | — | An open chunk whose first row arrived longer ago than this is sealed and encoded, whatever its size: a device that reports once a minute does not keep its readings uncompressed for 45 days. 0 = never by age. |
| `checkpoint_dir` | unset | — | Directory for segment files (Vortex-encoded sealed chunks, one file per chunk under `<table>/p<partition>-c<id>.vortex`, plus the dictionaries `symbols.json` and `dictionaries.json`). Created if missing. A sealed chunk is written here before it leaves the hot tier; at start the segments are re-attached without decoding and the WAL replay skips the rows they already hold; WAL segments below the checkpoint LSN are deleted. Unset = everything stays in memory and the WAL is never truncated. |
| `warm_budget_mib` | `512` | — | Encoded bytes kept in memory as warm chunks; above it the oldest warm chunks become cold (decoded from their segment file on demand). Needs `checkpoint_dir`. |
| `segment_cache_mib` | `256` | — | Recently read cold segment files kept in memory, compressed as on disk, least recently used out first. A cold chunk read again is decoded without reading its file or parsing its footer (most of the cost of a small cold read). `0` turns it off. Metrics: `ankaa_segment_cache_bytes`, `ankaa_segment_cache_reads_total{from="cache"|"file"}`. |
| `checkpoint_interval_s` | `60` | — | Minimum seconds between WAL truncations. |
| `tier_workers` | cores ÷ 4 (1–16) | — | Threads that encode sealed chunks (Vortex) and write their segments, off the ingest loop: compression keeps up with ingest instead of getting a slice of one core. A tick hands over every chunk that is due and swaps in the finished ones. 0 = encode in the tick within `tier_budget_pct`. At most 256. |
| `tier_budget_pct` | `5` | — | With `tier_workers = 0`: share of one core (1–50 %) the tiering step may spend per snapshot interval; what does not fit waits for the next tick. |
| `wal_budget_mib` | `1024` | — | WAL size above which the partitions holding the oldest unpersisted rows are sealed and persisted early, so the log can be truncated (needs `checkpoint_dir`; at least 512). See "How large the WAL gets" in §8. |
| `pg_addr` | unset | `--pg-addr` | Optional read-only PostgreSQL wire listener (SCRAM-SHA-256, password = token). Plaintext: loopback or a TLS terminator. Requires a token. |
| `metrics_token_env` | unset | — | Separate token for `/metrics`; default = the Flight token. |
| `query_timeout_s` | `30` | — | Wall-clock budget per query (planning + execution + transfer). |
| `resources.query_memory_mib` | `1024` | — | DataFusion memory pool for queries (≥ 64); a query beyond it fails instead of aborting the process. |
| `[[tables]]` | — | — | User-defined tables (`name`, `columns`, `partition_by`, `max_dict_entries`); see `docs/generic-ingest.md`. |
| `[[sources]]` | — | — | MQTT / NATS / Kafka / file sources mapping messages onto a user table (`kind`, `url`, `subject`, `format` = json/csv/line_protocol/sparkplug, `table`, `map`, `ts_unit`, `ts_from_receive`, `max_message_bytes`, `start`, `rows`, `measurement`, `tls_ca`/`tls_cert`/`tls_key`, `sparkplug_rebirth`, and for Kafka `tls`, `sasl_mechanism`/`sasl_username`/`sasl_password_env`, `allow_plaintext`; see `docs/generic-ingest.md`). A Kafka SASL password comes from the environment variable named by `sasl_password_env`. File and Kafka positions are kept in `sources.json` in `wal_dir` (or `checkpoint_dir`). |
| `[[continuous]]` | — | — | Continuous queries (`name`, `kind` = ohlcv/percentiles/asof, `table`, `key`, `value`, `size`, `width_ns`, `quantiles`, `right_table`, `max_rows`); see `docs/continuous-queries.md`. |
| `replication.standby_of` | unset | `--standby-of` | Run as a read-only standby of this primary's Flight address; needs `storage.wal_dir`. `primary_token_env`, `primary_tls_ca`, `primary_tls_domain` refine it; see `docs/replication.md`. |
| `replication.serve_wal` | `false` | — | Serve this node's WAL to standbys (Flight ticket `__wal__:<lsn>`); needs `storage.wal_dir`. `replication.token_env` names the variable holding the token standbys must present (recommended), `replication.max_standbys` (default 4) caps concurrent streams. |
| `snapshot_ms` | `50` | `--snapshot-ms` | Interval of the query-view swap: a row is queryable at the latest this long after it was ingested. Must be ≥ 5. A swap copies only the rows that arrived since the previous one, so a short interval is cheap. |
| `ingest_workers` | cores − 2 (≤ 32) | — | Threads that map JSON, CSV and line protocol messages to rows in parallel while the ingest loop applies the results in log order (RFC-0001): tables, dictionaries and counters are the same as with one thread. They are used under load (runs of 8 frames or more); a trickle is mapped in the loop itself. Sparkplug sources and the exchange feed are always mapped in the loop. 0 = off; at most 256. |
| `ingest_coalesce_us` | `200` | — | Interrupt moderation of the ingest loop (as a network card's `rx-usecs`): while frames arrive more often than once per millisecond and the queue is drained, the loop waits this long before the next batch, so one wake-up takes many frames instead of one. Saves the per-message thread wake-ups (on a VM, an inter-processor interrupt each); a backlog (a full batch of 1,024) or a quiet source never waits. Not visible to queries, which see rows at the next snapshot. 0 = off; at most 10,000. |

### `[server]`

| Key | Default | CLI | Meaning |
|---|---|---|---|
| `flight_addr` | `"127.0.0.1:50051"` | `--flight-addr` | Arrow Flight (gRPC) bind address. The same port answers Flight SQL (ADBC, JDBC; see §4). |
| `metrics_addr` | `"127.0.0.1:9100"` | `--metrics-addr` | Prometheus `/metrics` bind address. |
| `token_env` | `"ANKAA_TOKEN"` | `--token` / env `ANKAA_TOKEN` | Name of the environment variable that holds the bearer token. The token itself is never written in the file (see §3). |
| `tls_cert` | unset | `--tls-cert` | PEM certificate chain for Flight. Must be set together with `tls_key`. |
| `tls_key` | unset | `--tls-key` | PEM private key (PKCS#8 or SEC1). |
| `tls_client_ca` | unset | `--tls-client-ca` | PEM CA bundle; when set, Flight requires a client certificate signed by it (mutual TLS). Requires `tls_cert`/`tls_key`. |
| `allow_plaintext` | `false` | — | Accept a Flight listener beyond loopback **without** TLS. Refused by default: the token and all data would cross the network in clear text. Only for a private network segment or behind a TLS-terminating proxy. |
| `max_concurrent_queries` | `32` | — | Queries executing at once over Flight (plain Flight and Flight SQL together); further ones wait inside their own timeout and are then refused. |
| `dict_columns` | `"names"` | `--dict-ids` (sets `"ids"`) | How `dict` columns and `symbol` appear in SQL. `"names"`: dictionary-encoded text (`WHERE machine = 'press03'`, results carry names; RFC-0002). `"ids"`: the stored `UInt32` ids, names in `dictionaries` / `symbols` (the behaviour before RFC-0002, for dashboards written against it). Storage is the same in both: the setting can be changed at any restart. |

### `[resources]`

| Key | Default | CLI | Meaning |
|---|---|---|---|
| `memlock_mib` | `0` (skip) | — | Locked memory the engine intends to use, in MiB. At start the soft `RLIMIT_MEMLOCK` must be at least this, otherwise `serve` refuses to start. **Note**: today nothing is actually `mlock`ed; this is the fail-fast gate for the arena work that follows. |
| `memory_budget_mib` | `0` (skip) | — | Resident memory budget in MiB. `serve` refuses to start when it exceeds physical memory. No limit is enforced at run time yet. |

### Validation rules (`Config::validate`)

- `feed.url` scheme, `feed.rest_url` scheme, non-empty well-formed `feed.streams`.
- `storage.snapshot_ms >= 5`, `storage.hot_sealed_chunks >= 1`.
- **A Flight listener beyond loopback requires TLS** (`tls_cert`/`tls_key`) unless `server.allow_plaintext = true`,
  and every token must then be at least 16 characters (`openssl rand -hex 32` gives 64).
- **Binding beyond loopback requires a bearer token.** If either `flight_addr` or `metrics_addr` is not a loopback
  address and no token is resolved, start-up fails with
  `binding beyond loopback requires a bearer token (--token, or server.token_env naming a set variable)`.
- `tls_cert` and `tls_key` together; `tls_client_ca` only with them.
- `feed.enabled = false` needs at least one `[[sources]]` entry (or `replication.standby_of`).
- `replication.serve_wal` needs `storage.wal_dir`; a `replication.token_env` that names an unset variable is refused
  (the WAL stream would silently fall back to the query token).
- `[[tables]]`: valid, unique, non-reserved names; first column `timestamp_ns`; `partition_by` names a `dict` column.
- `[[sources]]`: unique names; every table column mapped (the timestamp may come from `ts_from_receive`); CSV maps are
  column indexes; `group` is refused; `max_message_bytes` between 16 and 64 MiB.
- `[[continuous]]` is also checked against the table schemas when the server starts (key and value column types,
  ASOF table pairs); see `docs/continuous-queries.md`.

### Resource checks (`Config::check_resources`, also run by `config check`)

`RLIMIT_MEMLOCK` versus `memlock_mib`, physical memory versus `memory_budget_mib`, every TLS file exists,
`wal_dir` and `checkpoint_dir` can be created.

### Run-time only flags

`--duration-secs N` stops after N seconds (0 = until SIGINT); `--api-key` for the SBE endpoint. Neither is in the file.

## 3. Secrets: tokens via environment only

The bearer token protects Flight and `/metrics`. It is read in this order: `--token` (which clap also fills from
`ANKAA_TOKEN`), then the variable named by `server.token_env`. Because clap reads `ANKAA_TOKEN` first, a custom
`token_env` only takes effect when `ANKAA_TOKEN` is **unset** in the service environment.

The value never appears in the TOML (`token` is `#[serde(skip)]`), in `config print`, or in logs (`Secret`
prints `***`). Generate a long random token and hand it to the process through systemd:

```sh
sudo install -d -m 0750 -o root -g ankaa /etc/ankaa
umask 077
sudo install -m 0640 -o root -g ankaa /dev/null /etc/ankaa/ankaad.env   # created private, then filled
printf 'ANKAA_TOKEN=%s\n' "$(openssl rand -hex 32)" | sudo tee -a /etc/ankaa/ankaad.env >/dev/null
```

The unit in `deploy/systemd/ankaad.service` loads it with `EnvironmentFile=`. Put `BINANCE_API_KEY` in the same file
if you use the SBE endpoint. Rotate by editing the file and restarting; clients send the token as
`authorization: Bearer <token>` on every Flight call and on `/metrics`. Flight SQL clients that only take a user name
and a password send the token as the password (§4, "Flight SQL clients").

Without a token (loopback only) Flight and `/metrics` are open to any local process. That is the development mode.

## 4. TLS

Flight uses rustls through tonic. Without `tls_cert`/`tls_key` the transport is plain HTTP/2 and the token travels in
cleartext; keep that on loopback. `/metrics` has **no TLS** in the current build: bind it to loopback or a management
interface and scrape over a private network or an SSH tunnel.

### Server-only TLS (clients verify the server)

With a private CA and `openssl`:

```sh
cd /etc/ankaa/tls
# CA
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 \
  -subj "/CN=ankaa-ca" -keyout ca.key -out ca.pem
# server key + CSR + cert with the names clients will dial
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -subj "/CN=ankaad" -keyout server.key -out server.csr
printf 'subjectAltName=DNS:ankaad.internal,IP:10.0.0.12\nextendedKeyUsage=serverAuth\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -extfile server.ext -out server.pem
chown root:ankaa server.key && chmod 0640 server.key
```

Or with `step-cli`:

```sh
step certificate create ankaa-ca ca.pem ca.key --profile root-ca --no-password --insecure
step certificate create ankaad server.pem server.key --profile leaf \
  --ca ca.pem --ca-key ca.key --san ankaad.internal --san 10.0.0.12 --not-after 8760h --no-password --insecure
```

Configure:

```toml
[server]
flight_addr = "10.0.0.12:50051"
tls_cert = "/etc/ankaa/tls/server.pem"
tls_key  = "/etc/ankaa/tls/server.key"
```

Query with the CA:

```sh
ANKAA_TOKEN=… ankaad query --addr ankaad.internal:50051 --tls-ca /etc/ankaa/tls/ca.pem --sql "SELECT count(*) FROM trades"
# dialing by IP but the cert only has a DNS SAN:
ANKAA_TOKEN=… ankaad query --addr 10.0.0.12:50051 --tls-ca ca.pem --tls-domain ankaad.internal --sql "..."
```

`--tls-ca` switches the client to `https://`; the server certificate must chain to that CA and match the host part of
`--addr` (or `--tls-domain`). pyarrow clients use `flight.connect("grpc+tls://host:50051", tls_root_certs=open("ca.pem","rb").read())`
with `FlightCallOptions(headers=[(b"authorization", b"Bearer …")])`.

### Flight SQL clients (ADBC, JDBC)

The Flight port also answers Flight SQL, read-only, with the same token, TLS settings, query timeout, cap on concurrent
queries and memory limit. Nothing has to be configured for it.

ADBC (Python shown; `pip install adbc-driver-flightsql pyarrow`):

```python
import adbc_driver_flightsql.dbapi as flight_sql
from adbc_driver_flightsql import DatabaseOptions

conn = flight_sql.connect(
    "grpc+tls://ankaad.internal:50051",                      # grpc:// without TLS (loopback)
    db_kwargs={
        DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer <token>",
        DatabaseOptions.TLS_ROOT_CERTS.value: open("/etc/ankaa/tls/ca.pem").read(),
    },
    autocommit=True,                                         # there are no transactions
)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM trades")
    print(cur.fetch_arrow_table())
```

JDBC (Arrow Flight SQL JDBC driver): `jdbc:arrow-flight-sql://ankaad.internal:50051/?token=<token>&tlsRootCerts=/etc/ankaa/tls/ca.pem`
(`useEncryption=false` for a loopback listener without TLS).

Authentication, as these clients do it:

- **Token.** ADBC's `adbc.flight.sql.authorization_header` and JDBC's `token` put `authorization: Bearer <token>` on
  every call; the JDBC driver first makes a `Handshake` call with that header, which the server accepts.
- **User name and password.** ADBC's `username`/`password` and JDBC's `user`/`password` make one `Handshake` call with
  `authorization: Basic base64(user:password)` and use the bearer token from the response headers afterwards. The
  server accepts any user name with the token as the password and answers with `authorization: Bearer <token>`.
  `Basic` is accepted on `Handshake` only.

Supported: queries (`CommandStatementQuery`), prepared statements without parameters, `GetSchema`, and the metadata
calls `GetSqlInfo`, `GetCatalogs`, `GetDbSchemas`, `GetTables` (with or without the table schemas) and `GetTableTypes`.
Refused with an error: updates and bulk ingest (the server is read-only), statement parameters (`$1`, `?`: write the
values in the SQL text), transactions, Substrait plans, `CancelQuery`, and the metadata calls for keys and type info.
`examples/python/flight_sql_adbc.py` checks a running server through ADBC (run by hand with `adbc_driver_flightsql`
1.13; not part of CI). The JDBC driver has not been run against ANKAA yet.

### Mutual TLS (server also verifies clients)

Issue client certificates from the same CA (or a dedicated client CA) and set `tls_client_ca`:

```sh
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=analyst-1" -keyout client.key -out client.csr
printf 'extendedKeyUsage=clientAuth\n' > client.ext
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -extfile client.ext -out client.pem
```

```toml
[server]
tls_client_ca = "/etc/ankaa/tls/ca.pem"
```

```sh
ankaad query --addr ankaad.internal:50051 --tls-ca ca.pem --tls-cert client.pem --tls-key client.key --sql "..."
```

The bearer token is still required with mutual TLS; the two checks are independent. Certificate rotation needs a
restart (files are read once at start).

## 5. Binding to a management interface

Keep the data plane and the scrape plane separate:

```toml
[server]
flight_addr  = "10.0.0.12:50051"     # clients, TLS
metrics_addr = "10.0.1.12:9100"      # management VLAN, Prometheus only
```

Both non-loopback binds require the token (validation rule). Firewall the ports to the networks that need them
(`nft`/`ufw`); the engine has no allow-list of its own and no rate limit on `/metrics`. Do not expose either port to
the Internet.

## 6. systemd

`deploy/systemd/ankaad.service` is the reference unit. The important parts:

- `User=ankaa`, `Group=ankaa`, a dedicated system account:
  `sudo useradd --system --home /var/lib/ankaa --shell /usr/sbin/nologin ankaa`.
- `EnvironmentFile=/etc/ankaa/ankaad.env` for the token (and `BINANCE_API_KEY`).
- `ExecStartPre=/usr/local/bin/ankaad config check /etc/ankaa/ankaad.toml` fails the start on a bad file instead of
  looping on it.
- `LimitMEMLOCK=` at least `resources.memlock_mib` (or `infinity`); the start-up check compares against the soft limit.
- `Restart=always`, `RestartSec=2`, `StartLimitIntervalSec=0`: release builds have `panic = "abort"`, so a panic
  becomes a process exit and **systemd is the supervisor**. The WAL is recovered on the next start; there is no
  in-process restart.
- `KillSignal=SIGINT`, `TimeoutStopSec=30`: `serve` handles SIGINT (Ctrl-C) with an orderly shutdown (source stops
  within 5 s, channel drained into the WAL and tables, WAL flushed and `fdatasync`ed, Flight drained for up to 5 s).
  Note: only SIGINT is handled; SIGTERM would kill the process without the final commit, which the WAL tolerates (torn
  tail) but which loses up to 10 ms of frames.
- `StateDirectory=ankaa` creates `/var/lib/ankaa` owned by the service user; point `wal_dir`/`checkpoint_dir` under it
  (or use `ReadWritePaths=` for a dedicated mount).
- Hardening: `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `PrivateDevices`,
  `ProtectKernelTunables`, `ProtectKernelModules`, `ProtectControlGroups`, `RestrictAddressFamilies=AF_INET AF_INET6`
  (add `AF_UNIX` if you ever front it with a local proxy), `RestrictNamespaces`, `LockPersonality`,
  `MemoryDenyWriteExecute`, `RestrictRealtime`, `SystemCallArchitectures=native`,
  `SystemCallFilter=@system-service`, `CapabilityBoundingSet=` (empty). `AmbientCapabilities` is not needed: both
  ports are unprivileged.
- Logs go to the journal (`tracing` writes to stderr); set `RUST_LOG=info` (default) or `RUST_LOG=ankaad=debug`
  in the environment file for more.

Install:

```sh
sudo install -m 0755 target/release/ankaad /usr/local/bin/ankaad
sudo install -m 0640 -o root -g ankaa deploy/ankaad.example.toml /etc/ankaa/ankaad.toml   # then edit
sudo install -m 0644 deploy/systemd/ankaad.service /etc/systemd/system/ankaad.service
sudo systemctl daemon-reload && sudo systemctl enable --now ankaad
journalctl -u ankaad -f
```

## 7. Linux tuning

Only the first item is required; everything else is optional and should be measured, not assumed.

**memlock / ulimit.** Set `LimitMEMLOCK=` in the unit (or `ankaa soft memlock …` in `/etc/security/limits.d/` for
interactive runs) to at least `resources.memlock_mib`. `serve` fails fast with
`RLIMIT_MEMLOCK is X MiB, config needs Y MiB (raise with ulimit -l or systemd LimitMEMLOCK)` otherwise. Raise
`LimitNOFILE` as well; Flight clients and the WebSocket need few descriptors, but the default 1024 is tight with many clients.

**Transparent hugepages / hugepages.** The hot tier is planned to live in hugepage arenas that are `mlock`ed (PRD §4.7).
That code is not in this build, so THP settings have no measurable effect yet. When it lands, `madvise` mode
(`/sys/kernel/mm/transparent_hugepage/enabled = madvise`) is the recommended setting so the engine opts in without
affecting the rest of the host.

**CPU isolation and IRQ affinity (optional).** On a dedicated host, isolating the ingest thread from the NIC interrupt
handler and from housekeeping reduces latency jitter: `isolcpus=`/`nohz_full=`/`rcu_nocbs=` on the kernel command
line, `irqbalance` disabled and the NIC queues' `smp_affinity_list` pinned to other cores, then `taskset` or
`CPUAffinity=` in the unit for `ankaad`. The engine does not pin threads itself. Measure before and after with the
`ankaa_ingest_latency_ns` summary; without hardware timestamps the difference may be inside the noise.

**Clock source.** The engine uses `CLOCK_REALTIME` (`SystemTime`) for receive and ingest timestamps. On Arm the
architected counter (`arch_sys_counter`) is the default and the right choice; check
`/sys/devices/system/clocksource/clocksource0/current_clocksource`. Run `chrony` with a nearby source so exchange event
time and host receive time are comparable; the hardware PHC path is later work (`docs/measurement-protocol.md`).

**File system for the WAL.** ext4 with the default `data=ordered` is what the power-loss rig validated (1000/1000 cuts
without loss of acknowledged records). `data=writeback` is not yet tested. XFS is expected to behave the same for
`fdatasync` semantics but has not been put through the rig. Mount the WAL volume with `noatime`. Do not place the WAL
on a network file system.

**fsync expectations.** Each group commit is one `write` plus one `fdatasync`: 10 ms after the first buffered record,
or as soon as 256 KiB are buffered and nothing more is queued, or at 8 MiB. Expect up to 100 `fdatasync` calls per
second at a busy feed. On NVMe this is cheap; on a slow volume commits get larger instead of more frequent, and ingest
runs at the speed the volume makes data durable: the ingest loop waits for the log rather than accepting frames it
cannot log (a real-time feed that outruns the disk then drops at the source channel,
`ankaa_source_dropped_full_total`). Segment creation and deletion also `fsync` the directory.

**Network.** Nothing special: one outbound WebSocket (TLS), one REST call per depth symbol per connect/resync,
inbound gRPC. Default socket buffers are fine for JSON streams.

## 8. Disk layout

```
/etc/ankaa/
  ankaad.toml           # config, root:ankaa 0640
  ankaad.env            # ANKAA_TOKEN (and BINANCE_API_KEY), root:ankaa 0640
  tls/                  # ca.pem, server.pem, server.key (0640)
/var/lib/ankaa/
  wal/                  # storage.wal_dir: wal-<first_lsn:020>.log segments, 256 MiB each,
                        #   + sources.json (source ids and read positions of [[sources]])
  segments/             # storage.checkpoint_dir: <table>/p<partition>-c<id>.vortex
                        #   + symbols.json, dictionaries.json (names behind dictionary ids)
/usr/local/bin/ankaad
```

WAL segments are named by the first LSN they contain (`wal-00000000000000000001.log`); the active segment is the
one with the highest number. Each record is `[len u32][crc32c u32][lsn u64][epoch u64][payload]` where the payload
is the raw feed frame in `.ankc` encoding. Files that do not match the pattern are ignored with a start-up warning
(`sources.json` is expected there). `sources.json` (in `wal_dir`, or in `checkpoint_dir` when there is no WAL) keeps the
id of every source name and how far file and Kafka sources have read; deleting it makes those sources start over.
`symbols.json` and `dictionaries.json` map dictionary ids to names: segments are unreadable without them, so they
belong to every backup of the segment directory. Continuous-query tables (`cq_*`) have no files: they are rebuilt
at start.
Without `checkpoint_dir` there is no truncation, so the WAL grows by roughly the
raw feed volume (JSON frames) for as long as the process runs. Put it on its own volume so a full WAL cannot take the
host down. When the volume fills, commits fail (`ankaa_wal_errors_total` rises), they are retried until space is back,
and ingest waits; frames that wait longer than 2 s are dropped and counted (`ankaa_wal_dropped_total`), never ingested
without being logged. If such a frame was a message of a `[[sources]]` entry the server stops with an error (the unit
restarts it) and file and Kafka sources read again from their last saved position.

**How large the WAL gets.** With `checkpoint_dir`, the log is truncated behind the oldest row that is not yet in a
segment. Two things hold that point back:

- A partition whose open chunk fills slowly (a symbol that rarely trades). When the log exceeds
  `storage.wal_budget_mib` (default 1024), such partitions are sealed and written to segments early.
- **Order books.** `book_deltas` and `quotes` rows are computed from a snapshot plus every diff since the feed
  connected, so after a crash they can only be regenerated by replaying from that connection. With
  `feed.depth_snapshots = true` and depth streams subscribed, the log is therefore kept back to the last feed
  (re)connection at or before the checkpoint. When that makes the log exceed `storage.wal_budget_mib`, the server
  re-bases the books on fresh REST snapshots (at most once every 10 minutes): the log gets a new starting point and
  `quotes` pause for the moment the snapshots take to arrive. Size the WAL volume for the budget plus what the feed
  writes in 10 minutes. Trades-only and source-only servers are not affected.

## 9. Backup and restore

On disk: the WAL (rows not yet in a segment) and, with `checkpoint_dir`, the segment files. At start the segments are re-attached and the WAL replayed with per-record skipping of rows a segment already holds.

**Safe to copy while running:** every segment **except the active one** (the highest-numbered file). Sealed segments
are never modified after rotation. The active segment is append-only and `fdatasync`ed at every commit, so a copy of
it is at worst torn at the tail, which the reader tolerates; but a copy taken mid-write may also contain a partially
written record followed by valid ones, which replay would report as corruption. Prefer `rsync` of the closed
segments plus a snapshot of the active one taken after a clean stop, or an LVM/filesystem snapshot of the whole directory.

```sh
# closed segments only (skip the newest)
ls /var/lib/ankaa/wal/wal-*.log | sort | head -n -1 | xargs -I{} rsync -a {} backup-host:/backups/ankaa/wal/
```

**Restore:** stop the service, place the segments in `wal_dir` with contiguous LSNs (a missing segment in the middle
is detected as `lsn out of sequence` and replay stops there), start the service. Recovery replays every frame through
the decoder and appenders; `ankaa_wal_recovered_frames` and the `wal recovery done` log line show the count. Books
are then re-based on fresh REST snapshots regardless of what the WAL held.

**Config and secrets:** back up `/etc/ankaa` separately with the appropriate access controls; `ankaad.env` is the token.

With checkpoints, a backup is "the WAL plus the segment directory", copied **in that order**: the WAL first, then
`checkpoint_dir` (segment files and the dictionary files are written atomically via rename). The WAL copy holds every
row that was not yet in a segment when it was taken, and the later segment copy can only hold more; rows present in
both are recognised by their LSN and applied once. The other order can lose rows: a chunk persisted after the segment
copy may already have been truncated from the WAL by the time the WAL is copied. A filesystem or volume snapshot of
both directories at once is simpler still. 
`ankaa_wal_truncated_segments_total` reports the segments the engine removed itself.

## 10. Upgrades

Single binary, no migration step: **stop, replace, start**. (Adding nullable columns to a user
table needs no migration either: see "Changing a table" in `docs/generic-ingest.md`.)

```sh
sudo systemctl stop ankaad            # SIGINT, orderly shutdown, final WAL commit
sudo install -m 0755 target/release/ankaad /usr/local/bin/ankaad
/usr/local/bin/ankaad config check /etc/ankaa/ankaad.toml
sudo systemctl start ankaad           # WAL recovery, then live
```

Recovery time is proportional to WAL size (every frame is decoded again). The requirement is under 60 s for a 10 GB
WAL (PRD F-19); it is not yet measured, and without truncation the WAL only grows, so plan a periodic
stop/archive/restart until checkpoints land.

**Format compatibility statement.** The project is at version 0.0.1 with no releases. The WAL record format
(`len/crc32c/lsn/epoch/payload`), the `.ankc` frame encoding carried in the payload (version 1) and the TOML schema are
**not yet frozen**; a change will be announced in `CHANGELOG.md` with a migration or a "start a new WAL directory"
note. Downgrades are not supported. Until a 0.1 release, treat a WAL as readable by the same minor version that
wrote it.

## 11. Sizing rules of thumb

No measurements are published; these are arithmetic, not promises.

- **CPU.** One ingest thread, one WAL thread, the tokio runtime for the WebSocket, snapshot fetches, Flight and
  `/metrics`, plus DataFusion query threads (one per core by default). Two cores run a handful of symbols; reserve
  cores for queries separately. Table functions (`asof_join`, `ohlcv`, `percentiles`) are eager and materialise
  their inputs, so a wide query can use a core and a copy of the table for its duration.
- **Memory.** Hot chunks are plain columns; sealed chunks beyond `hot_sealed_chunks` are encoded (typically 4–12× smaller) and demoted to disk past `warm_budget_mib`; uncompressed rows as a whole are kept within `hot_budget_mib` and open chunks younger than `max_open_age_s`. Per hot row before alignment:
  `trades` 37 B, `book_deltas` 45 B, `quotes` 52 B, in 65,536-row chunks. A depth stream at 100 ms with ~10 levels
  per message yields ~100 `book_deltas` rows/s per symbol (~16 MB/h) plus up to 10 `quotes` rows/s. Add the 64k-row
  open-chunk copies taken every `snapshot_ms`, the 65,536-frame source and WAL channels (bounded by frame size), and
  DataFusion's working set. Set `resources.memory_budget_mib` to what you can afford so start-up refuses an
  impossible host; it is a check, not a cap.
- **Disk.** WAL ≈ raw feed bytes + 24 B per frame + the `.ankc` frame header (14 B) and CRC (4 B), truncated after
  each checkpoint. Segments hold the encoded data for the retention window of each table (`retention`,
  `storage.market_retention`; unset = forever), so budget that window.
- **Many devices.** A partition per device costs about 1.4 KB of memory before its data plus about 1.3 KB for the
  query view of its open chunk. A development signal from an Apple M4 Pro laptop, not a server result: 100,000 devices,
  about 20 M rows/s single-threaded ingest, a 3 ms query snapshot (`crates/ankaa-store/tests/scale.rs`).
- **Network.** JSON depth at 100 ms per symbol is tens of KB/s; the SBE stream is smaller. REST snapshots are
  ~100 KB each (limit 1000), only on connect and resync.

## 12. Observability setup

**Prometheus.** `deploy/prometheus/prometheus.yml` scrapes `/metrics` every 10 s with the bearer token read from
`/etc/prometheus/ankaa.token` (`authorization.credentials_file`), and loads `deploy/prometheus/alerts.yml`, which
holds the rules from `docs/metrics-semantics.md`. Copy both, set the target, and check with `promtool check config`.

**Grafana.** Import `deploy/grafana/ankaa-overview.json` (Dashboards → New → Import → Upload JSON). The dashboard
declares a `${DS_PROMETHEUS}` input; pick your Prometheus datasource in the import dialog. Rows: Ingest, Feed health,
Latency, Durability, Storage, Queries. Counters are plotted with `rate()`; the tier panels show zeros until tiering lands.

## 13. Troubleshooting

| Symptom (log or exit message) | Cause | Fix |
|---|---|---|
| `config parse error: unknown field …` | Typo or a key from a newer version | Compare with `ankaad config print`; keys are `snake_case` under `[feed] [storage] [server] [resources]`. |
| `invalid config: binding beyond loopback requires a bearer token …` | `flight_addr` or `metrics_addr` is not `127.0.0.1`/`::1` and no token resolved | Set `ANKAA_TOKEN` in the environment file (check `EnvironmentFile=` path and permissions), or pass `--token`. If you use a custom `token_env`, make sure `ANKAA_TOKEN` is not also set to something else. |
| `invalid config: server.tls_cert and server.tls_key must be set together` | One of the two missing | Set both, or neither. |
| `resource check failed: RLIMIT_MEMLOCK is X MiB, config needs Y MiB` | Soft memlock limit below `resources.memlock_mib` | `LimitMEMLOCK=` in the unit, `ulimit -l` in a shell; or lower `memlock_mib`. |
| `resource check failed: memory budget … exceeds physical memory` | `memory_budget_mib` larger than RAM | Lower it or move to a bigger host. |
| `resource check failed: TLS file … not found` | Path wrong or unreadable by the service user, or `ProtectSystem=strict` hides it | Fix the path; keep certs under `/etc/ankaa/tls` (readable under `ProtectSystem=strict`); check group ownership of the key. |
| `resource check failed: wal_dir …: Permission denied` | Directory not writable by `ankaa` | Use `StateDirectory=` or `ReadWritePaths=`; `chown ankaa:ankaa`. |
| Flight client gets `Unauthenticated: invalid or missing bearer token`; `ankaa_query_unauthenticated_total` rising | Client token differs or header missing | Send `authorization: Bearer <token>`; compare the client and `/etc/ankaa/ankaad.env`. |
| `curl` to `/metrics` returns `401 Unauthorized` | Token configured, header missing | `curl -H "Authorization: Bearer $ANKAA_TOKEN" http://…:9100/metrics`. |
| Client: `invalid peer certificate: UnknownIssuer` / `NotValidForName` | Client CA does not match the server cert, or the SAN does not cover the dialed host | Use the CA that signed `server.pem`; dial the SAN name or pass `--tls-domain`. |
| Client: `tls handshake eof` / `certificate required` | Server has `tls_client_ca` set, client sent no certificate | Pass `--tls-cert`/`--tls-key` (or the pyarrow `cert_chain`/`private_key` options). |
| Client: `transport error` / `connection refused` on `http://` | Server has TLS on, client did not pass `--tls-ca` (or the reverse) | Match the modes; TLS on the server requires `--tls-ca` (or `grpc+tls://`) on the client. |
| Repeated `connecting` / `connect failed … backoff_s=…` lines | Exchange unreachable, DNS, egress firewall, or wrong `feed.url` | Check egress to the exchange host on 443; backoff doubles from 1 s to a 60 s cap. |
| `ankaa_source_connects_total` climbing every few minutes | Exchange closing idle/over-limit connections or a flapping network | Check the exchange's connection limits for the stream count; check NAT timeouts; look at `disconnect:<reason>` control frames in the log. |
| `ankaa_book_resyncs_total` climbing, gaps flat | REST snapshot older than buffered diffs; REST slow or rate-limited (`depth snapshot fetch failed`) | Fewer depth symbols per process, or check REST reachability; fetches are limited to one per symbol per 2 s. |
| `ankaa_feed_gaps_total` climbing | Frames lost between exchange and decoder (network, host overload) | Check host load; each gap costs a snapshot fetch. If `ankaa_frames_bad_total` also rises, it is a format problem, not a gap. |
| `wal commit failed: … No space left on device` | WAL volume full (no `checkpoint_dir`, or checkpoints lagging) | Stop, archive old segments elsewhere, start; set `checkpoint_dir` so the WAL is truncated. |
| `WAL record is not a capture frame; skipped` at start | A foreign writer or a format change wrote into `wal_dir` | Harmless for that record; if widespread, start a fresh `wal_dir`. |
| `corrupt record in … at offset …: crc mismatch` at start | Disk corruption or a damaged copy restored from backup | Recovery stops at that offset; everything before it is loaded. Restore the segment from a good copy, or truncate the file at the offset to accept the loss. |
| `ignoring non-WAL file in WAL directory` | Editor swap files, `.DS_Store`, notes | Remove them; they are skipped. |
| Start-up takes long, `wal recovery done … ms` large | Big WAL, every frame decoded again | Expected until checkpoints land; periodic stop/archive/restart keeps it bounded. |
| Service restarts in a loop (`journalctl -u ankaad`) | Panic (release builds abort) or a config error at `ExecStartPre` | The journal has the panic message or the config error; the unit uses `Restart=always` with a 2 s delay on purpose. |
| Queries return no rows for the last fraction of a second | Expected: queries see data up to the last snapshot swap | Lower `storage.snapshot_ms` (≥ 20) if freshness matters more than CPU. |
