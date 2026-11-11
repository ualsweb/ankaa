# Roadmap and gates

Phases are gated: a phase closes only when every gate condition is demonstrated with a reproducible
script or test in this repository. Task IDs match commit messages and `docs/STATUS.md`.

| Gate | Condition |
|---|---|
| **G0 — validated** | SQL over in-memory chunks through DataFusion; SVE2 kernels pass differential tests under QEMU at VL 128/256/512; capture tool running; CI green on arm64 runners |
| **G1 — core engine** | 24 h live soak with 0 undetected feed gaps; WAL power-loss rig 1000× clean; requirements F-01..F-08, F-11, F-12, F-18..F-20 of the PRD |
| **G2 — differentiated** | N-01, N-04, N-05, N-07 met on Graviton4; public benchmark report reproducible by a stranger (OSS comparisons only) |
| **G3 — partners** | Design partner(s) on live data; `ankaa-wire` report on named bare metal (or explicit "not measured") |
| **G4 — v1.0** | PRD §10 Definition of Done |

## Phase 0 — Validate (done)
P0-01 dev env + CI · P0-02 `ankaa-simd` scalar/NEON · P0-03 SVE2 `global_asm!` + detection + branchless gate · P0-04 chunk store · P0-05 DataFusion provider with pushdown · P0-06 `.ankc` capture + `ankaa-capture` · P0-07 Vortex spike (`docs/spikes/vortex.md`) · P0-08 first Graviton4 measurements (pending hardware time).

Optional, developer experience: **P0-09 streaming-SVE harness on Apple Silicon** — Apple M4 exposes SME2 with a 512-bit streaming vector length; the subset of SVE legal in streaming mode (loads, stores, arithmetic, predication, reductions, FastLanes unpacking; not `COMPACT`, gathers or scatters) can be tested and timed natively on a laptop instead of under QEMU. Not a product target (no current Arm server has SME); see `docs/dev-environment.md`.

## Phase 1 — Core engine (in progress)
| ID | Task | State |
|---|---|---|
| P1-01 | SPSC descriptor ring (loom, herd7) | done |
| P1-02 | Ingest: decoder trait, fixed-point parsing, symbol dictionary, event batches | done |
| P1-03 | Feeds: Binance JSON, depth snapshot sync, SBE decoder (live verification pending an API key), Databento DBN replay, CSV/Parquet replay, generic framing + example decoder, 8 fuzz targets | done (Databento live: later) |
| P1-04 | Partition-owned appenders (one per symbol), watermarks, drop counters | done |
| P1-05 | WAL: CRC32C records, group commit, torn-tail repair; dm-flakey rig 1000/1000; wired into `serve`; checkpoint policy with LSN-exact replay skip and segment truncation (done); io_uring O_DIRECT (later, measure first) |
| P1-06 | Sealing, zone-map pruning, warm tier (Vortex: ALP/FoR/FastLanes/dict), cold segment files, budgeted tiering, persisted dictionary | done |
| P1-07 | Operators: streaming ASOF, time buckets, L2 book, UDDSketch + fixed-point histogram | done (KLL: not needed yet) |
| P1-08 | Operators in SQL: `asof_join`, `ohlcv`, `percentiles`; DuckDB/Polars oracle 31/31 (`bench/oracle`) | done (incremental versions: later) |
| P1-09 | Arrow Flight server with bearer auth, read-only SQL, TLS + mutual TLS | done (Flight SQL extension: later) |
| P1-10 | Python package (`maturin`, Arrow C Data Interface, GIL released), notebook | done |
| P1-11 | `ankaad replay`, `serve`, `query`, `config`; graceful shutdown (SIGINT/SIGTERM), WAL recovery, TOML config with fail-fast checks | done |
| P1-12 | Security: no-unwrap lints, 8 fuzz targets, `docs/security.md`, read-only Flight/Postgres, constant-time tokens, systemd supervision unit | done |
| P1-13 | Benchmark harness (`bench/compare`, criterion, fixed-point histograms in `/metrics`) done; **24 h live soak: open** |

## Performance work (continuous)
Profiling passes are recorded under `docs/benchmarks/`. Pass 1 (2026-11-09): decoder, ingest locks, NEON kernels, SVE2 filter loop. Pass 1b: SBE decoder, simd-json (slower, opt-in), flat book (slower, reverted), dense forward ASOF, streaming capture reads. Next: typed column appends, SBE live verification.

## Phase 2 — Differentiate and launch
| Item | State |
|---|---|
| SVE2 for all kernels (10 kernels, VL 128/256/512, branchless gate) | done |
| ALP/FastLanes warm/cold tier (Vortex) | done |
| Reproducible benchmark harness vs QuestDB, ClickHouse, DuckDB (`bench/compare`, method doc) | harness done; **numbers: open, Graviton4** |
| Public launch (repo push, write-ups, Arm Learning Path) | open; steps in `docs/launch-checklist.md` |

## Phase 3 — Partners and showcase
| Item | State |
|---|---|
| `ankaa-wire` base: generic timer, PHC↔CPU calibration with residuals, `SO_TIMESTAMPING` receiver | done (experimental) |
| AF_XDP zero-copy receive, bare-metal report (N-14) | open (needs named hardware) |
| Design partners on live data; exchange feed modules under licence | open (after launch) |

## Phase 4 — v1.0
| Item | State |
|---|---|
| Postgres wire protocol (read-only, SCRAM) | done (experimental) |
| Upgrade/backup story (`docs/deployment.md`) | documented |
| Security review of `unsafe` and parsers | two review passes done; external review welcome |
| Hardening from the soak and the first external users | open |

## Beyond v1.0 (implemented early, 2026-11-09, as software; hardened with design partners)
| Item | State |
|---|---|
| Generic ingest: user-defined tables, MQTT / NATS / Kafka / file sources, JSON and CSV mapping, MQTT topic selectors (`docs/generic-ingest.md`) | done; verified against real Mosquitto, NATS and Kafka (local containers + CI) |
| Telemetry base: nullable columns, bounded lateness with in-order insertion, retention per table, 100k-device scale test | done (2026-11-11) |
| MQTT over TLS: certificate verification against public roots or a private CA, client certificates; tested against Mosquitto with TLS and mutual TLS in CI | done (2026-11-11) |
| Several readings per message (`rows`) and a single-pass JSON scanner (no document tree, no allocation per message) | done (2026-11-11) |
| Sparkplug B: one row per metric, BIRTH definitions (datatypes, aliases, transient flags) kept in the write-ahead log across restarts and truncation, opt-in rebirth requests; decoder checked against a reference protobuf codec and fuzzed; end to end against Mosquitto | done (2026-11-11) |
| Ingest engine: batched hand-off to the write-ahead log, one table lock per message, interrupt moderation of the ingest loop and the log thread (profiled end to end on Google Axion) | done (2026-11-11) |
| Adding nullable columns to a table (and making a column nullable) without losing its history: old segments read as the new schema, nothing rewritten | done (2026-11-11) |
| Gap filling and interpolation in SQL: `gapfill()` with `null`, `locf` and `linear` fills, seven aggregates, optional range; verified against DataFusion's own grouping | done (2026-11-11) |
| Next for telemetry: downsampled continuous-query outputs that outlive raw retention; a TSBS-shaped comparison with InfluxDB 3, TimescaleDB and QuestDB | planned |
| Continuous queries: incremental OHLCV, quantiles, rolling windows, ASOF as `cq_*` tables (`docs/continuous-queries.md`) | done; event time with the table's lateness window; no retraction of closed results (rows later than that are counted) |
| Replication: WAL shipping over Flight to a read-only standby, manual promotion (`docs/replication.md`) | done; asynchronous, no election, no split-brain protection |
| Distributed queries across nodes | design note only: scale up on one Arm socket first |

## De-scope order (pre-agreed)
1. `ankaa-wire` showcase → next version · 2. Postgres wire · 3. FSST strings (dictionary only) · 4. SVE2 ASOF (NEON only) · 5. order-book snapshots (replay only).
**Never cut:** WAL power-loss gate, fuzzing, hardware-timestamped measurement protocol, differential SIMD tests.
