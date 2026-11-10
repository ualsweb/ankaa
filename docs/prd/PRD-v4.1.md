# ANKAA-ENGINE — Product Requirements v4.1

**Status:** ACCEPTED 2026-11-09 (decisions recorded as `docs/decisions/ADR-0001..0005`). Earlier drafts (v3.x) are superseded; where this document says "as v3.2" it refers to kernel-level design notes that will be published as RFCs when implemented.
**Rule:** changes by RFC (`docs/rfc/`). Effort figures assume a single maintainer.

## 0. Hardware facts this document is built on

Hardware facts corrected versus v3.2 (verified 2026-11-09): Neoverse V2 = 4×128-bit SVE2 (VL 128); Neoverse V3 / Arm AGI CPU = 2×128 SVE per Arm's spec (VL 128), not 2×256; Graviton4 local DRAM 114 ns; Arm AGI CPU ships "later 2026"; Rust SVE `asm!` operands and intrinsics are nightly-only (2026-09 / 2026-04); `HWCAP2_SVE2` is bit 1 of `AT_HWCAP2` (kernel UAPI; `libc` does not export it).

---

## 1. One-paragraph definition

ANKAA is an **open-source, Arm-native real-time tick-data engine** written in Rust. It ingests millions of market events per second per core from TCP/WebSocket, file replay and (later) exchange multicast, keeps the recent window hot in a columnar in-memory store, exposes **SQL (Apache DataFusion), Arrow Flight and Python/Arrow** with first-class time-series operators (ASOF join, windows, order-book reconstruction, percentiles), and ages data into a compressed cold tier (ALP + FastLanes layouts). It is built for Arm Neoverse (Graviton4, Grace, Cobalt, Arm AGI CPU) with vector-length-agnostic SVE2 kernels, and every performance figure it publishes is hardware-timestamped and reproducible.

**What it is not (v1):** not a tick-to-trade engine, not an FPGA replacement, not a general OLAP warehouse, not a managed cloud service.

---

## 2. Problem and users

### 2.1 Problem
Teams that work with market data today choose between: kdb+ (fast, proprietary, q language, x86-first), QuestDB (open, SQL, Java/JVM, growing in capital markets), ClickHouse (open, general OLAP, not tick-native), ArcticDB (BSL, Python-only, pandas-shaped), or in-house C++. To our knowledge at the time of writing, none is Arm-native, none publishes hardware-timestamped ingest latency, none ships research-grade float compression (ALP) or ISA-agnostic SIMD layouts (FastLanes), and none makes order-book analytics a built-in. Meanwhile Arm is becoming the cheapest compute in every cloud (Graviton4, Axion, Cobalt) and in European providers adopting the Arm AGI CPU.

### 2.2 Personas (ordered by reachability)
| Persona | Job to be done | Today | Why ANKAA |
|---|---|---|---|
| **P1 Crypto market maker / quant dev** (2–20 people) | Capture full-depth books from 5–10 venues 24/7, replay, compute features, backtest in Python | Custom Python/Rust + Parquet, ClickHouse, Tardis | Public feeds, no licensing, binary SBE feeds, cheap Arm cloud, Python-first |
| **P2 Systematic fund researcher** (mid-market, 20–200 people) | Store years of ticks, ASOF joins trades↔quotes, intraday analytics | kdb+, QuestDB, ArcticDB | SQL + Arrow, Arm cloud instances, no q lock-in |
| **P3 Exchange / venue analytics & surveillance** | Reconstruct books, compute latency and quality metrics from own feeds | kdb+, in-house | Hardware-timestamped ingest, measurement rigor, EU hosting |
| **P4 Arm infrastructure providers** | Show flagship workloads on Arm | ClickHouse/Redis ports | Native showcase with reproducible numbers |

P1 is the launch persona: they have the data and the pain, and can adopt an open-source tool directly.

### 2.3 Jobs to be done (v1)
1. Ingest a live binary feed and have it queryable within milliseconds.
2. Replay historical captures at ≥ 10× real time into the same tables.
3. Run SQL and Python over the last N hours in memory and over months on disk, transparently.
4. ASOF-join trades to quotes, build L2 books, compute rolling stats and percentiles, without leaving the engine.
5. Export to Arrow/Parquet and read from pandas/Polars/DuckDB zero-copy.
6. Trust the numbers: every published latency and throughput figure comes with the script that reproduces it.

---

## 3. Scope

### 3.1 In scope (v1.0)
- Engine library (`ankaa-core`) and server binary (`ankaad`).
- Ingest: file replay (PCAP, Parquet, CSV, Databento DBN), TCP/WebSocket (Binance SBE and JSON streams, Databento live), generic length-prefixed binary via a decoder trait.
- Storage: hot columnar chunks (append-only, sealed at 64k rows, zone maps, watermark per feed), WAL with CRC32C + group commit, cold segments (Vortex or Parquet, decision Phase 1).
- Query: SQL via DataFusion `TableProvider`/`ExecutionPlan` with pushdown; native operators: ASOF join, time-bucket aggregation, sliding windows, order-book reconstruction, quantile sketches.
- Interfaces: Arrow Flight (SQL + ticket-based streaming), Python package (`ankaa`) with Arrow C Data Interface and GIL released, Prometheus `/metrics`.
- SIMD: Scalar → NEON → SVE2 (VL-agnostic) backends with runtime dispatch and differential tests; selection-vector-based filtering.
- Platforms: aarch64 Linux first-class (Graviton4, Cobalt, Grace, Arm AGI CPU when available); x86-64 Linux builds and passes tests (scalar/AVX2 via portable code) but is not performance-tuned; macOS Apple Silicon for development only.
- Security: fuzzed parsers, `panic=abort`, authenticated `/metrics`, TLS on Flight, no `unsafe` without policy.

### 3.2 Explicitly out of scope (v1.0)
- Tick-to-trade / order routing / TX signalling (v3.2 RF-22).
- Per-core AF_XDP across 132 cores, RSS steering, chiplet affinity (kept as design notes).
- CXL tier (T2). Removed; trait boundary stays.
- Exchange multicast decoders (ITCH, MDP 3.0, EOBI) unless a design partner provides licensed access. Then: module `ankaa-feeds-exchange`.
- Automatic failover, RBAC, SSO.
- Distributed query.

### 3.3 The latency showcase (Phase 3, module `ankaa-wire`)
A bare-metal ingest path (AF_XDP zero-copy as portable default; DPDK or raw verbs evaluated for certification) with NIC hardware timestamps (`bpf_xdp_metadata_rx_timestamp`, kernel ≥ 6.8) and PHC↔CNTVCT calibration as in v3.2 RFC-017. Deliverable: a reproducible report "wire-to-queryable P99 on Arm" on named hardware. It is a showcase and a partner artifact, not a v1.0 requirement.

---

## 4. Architecture

### 4.1 Own vs reuse (the leverage decision)
| Layer | Decision | Why |
|---|---|---|
| Ingest decoders, hot store, WAL, tiering | **Own** | This is the product's differentiation and where µs matter |
| SIMD kernels (filter, aggregate, ASOF, book) | **Own** (SVE2/NEON) | Nobody ships VL-agnostic SVE2 tick kernels |
| Columnar encodings (ALP, FastLanes, FSST, dict, RLE) | **Reuse Vortex crates** (fallback: port ALP/FastLanes from cwida reference) | Production Rust, DataFusion/DuckDB integration, LF AI & Data governance |
| SQL parsing, planning, optimization, generic execution | **Reuse Apache DataFusion** (pin major, ~monthly releases) | InfluxDB 3, GreptimeDB, Arroyo do exactly this |
| Memory format, IPC, Python bridge | **Reuse Arrow / arrow-rs / PyO3 + arrow C Data Interface** | Industry standard |
| Transport | **Arrow Flight (gRPC)**; Postgres wire later via `pgwire` crate | Broadest client reach |
| Sketches | Own `DDSketch`/`UDDSketch` (flat arrays, negative store, midpoint quantile — as v3.2 RFC-014); KLL via `datasketches` port later | Percentiles of latency vs prices need different guarantees |

### 4.2 Workspace (12 crates, down from 11 + new scope because reuse removes work)
```
ankaa/
├── crates/
│   ├── ankaa-core        # config, errors, metrics, ids, time types (no unsafe)
│   ├── ankaa-time        # PHC/CNTVCT calibration, monotonic→wall mapping (small, kept separate for audit)
│   ├── ankaa-ring        # SPSC descriptor ring (as v3.2 RFC-009; loom + herd7)
│   ├── ankaa-simd        # SimdBackend trait; scalar/, neon/, sve2/ (global_asm!), selection vectors
│   ├── ankaa-store       # chunks, zone maps, watermarks, arena tiering, sealing, cold segments
│   ├── ankaa-wal         # CRC32C records, group commit, replay, truncation at checkpoint
│   ├── ankaa-ingest      # decoder trait, replay sources, TCP/WebSocket (tokio + io_uring ZCRX feature)
│   ├── ankaa-feeds       # Binance SBE/JSON, Databento DBN/live, generic framing
│   ├── ankaa-ts          # ASOF join, windows, time buckets, order-book, sketches
│   ├── ankaa-sql         # DataFusion TableProvider/ExecutionPlan/UDFs, pushdown
│   ├── ankaa-flight      # Arrow Flight server, auth, TLS
│   └── ankaa-py          # PyO3 package `ankaa`
├── bins/ankaad           # server
├── bench/                # criterion + latency harness + reproducible reports
├── fuzz/                 # cargo-fuzz: feeds, WAL, segments, config, Flight tickets
├── wire/ (Phase 3)       # ankaa-wire: AF_XDP/DPDK path, XDP program, HW timestamps
└── docs/                 # architecture, data-model, metrics-semantics, unsafe-policy, security, benchmarks
```

### 4.3 Data model (as v3.2 RFC-016, extended)
- **Table** = schema + ordered list of chunks per partition key (feed, symbol-group).
- **Chunk** = ≤ 65,536 rows, typed columns (`i64` timestamps ns, `i64` fixed-point prices with per-column scale, `i32/i64` sizes, `u32` dictionary-coded symbols/venues, `u8` flags), sealed → immutable; **watermark** = max accepted ts; **zone map** = min/max per column.
- **Vector unit** = 1,024 rows (FastLanes virtual 1024-bit register), so a chunk is 64 vectors; all kernels operate on 1,024-row vectors with selection vectors.
- **Prices are fixed-point integers at ingest.** Exchanges publish decimals; storing `i64` ticks makes ALP degenerate to FOR+bitpack and makes equality exact. `f64` columns exist for derived features (ALP-encoded when cold).
- **Order-book state** is a derived, materialized table per instrument with snapshot every N updates for O(1) replay seek.
- **Consistency:** readers see data up to the confirmed watermark per feed (read-committed per feed); chunks immutable + epoch-based reclamation; no locks on the read path.

### 4.4 Ingest path
```
source (socket / file / wire) → framing → decoder (feed-specific, fuzzed) → Event batch (Arrow-shaped, 1,024)
  → per-partition appender (hot chunk, column arenas) → watermark advance → WAL append (group commit, off hot path)
```
- One ingest thread per source; appenders are partition-owned (no shared mutable state); cross-thread handoff via `ankaa-ring` descriptors only.
- Hot path rule (as v3.2 M6): zero syscalls and zero durable writes between frame arrival and watermark advance.
- Backpressure: drop-tail with `dropped_by_reason` metrics and feed gap detection via sequence numbers (as v3.2 RFC-022).

### 4.5 Query path
- DataFusion plans SQL; `AnkaaTableProvider` pushes down time range, symbol predicates and projection to chunk/zone-map pruning; hot chunks are exposed as Arrow `RecordBatch` without copy; cold segments decoded per 1,024-vector on demand.
- Native operators registered as DataFusion `ExecutionPlan`s: `AsofJoinExec` (streaming merge over pre-sorted partitions, per-partition cursors, bounded memory — unlike sort-based ASOF which needs ~48 GB for 10⁹ rows), `TimeBucketExec`, `BookExec`, `QuantileSketchExec`.
- Python: `ankaa.connect().sql(...)` returns Arrow; `ankaa.Table.asof(...)` etc. GIL released in compute; buffers owned by a deleter-capsule holding an `Arc` to the arena (as v3.2 M-07).

### 4.6 SIMD strategy (corrected from v3.2)
- Budget **VL = 128 bit everywhere** (Graviton4, Grace, Cobalt 100, Arm AGI CPU all report 128). Kernels remain VL-agnostic (`whilelt/rdvl/incp`) and are also tested at VL=256 under QEMU.
- Filtering produces **selection vectors** (u16 indices within a 1,024-row vector); compaction (`COMPACT`/TBL) happens only at materialization. Microbenchmark `COMPACT` (single pipe, 3-cycle on V2) vs 16-entry TBL permute before choosing.
- SVE2 via `global_asm!` + `extern "C"` wrappers on **stable**; nightly SVE intrinsics (`stdarch_aarch64_sve`, Apr 2026) as an experimental third backend behind a feature flag.
- Branchless-v2 rule and dual gate (`llvm-objdump` on marked ranges + `BR_MIS_PRED` < 0.1%) as v3.2 RFC-005.
- Differential tests: scalar vs NEON vs SVE2 (and vs DuckDB/Polars results for operators), relative tolerance 1e-6, NaN/±inf/±0/denormal vectors (RFC-012).

### 4.7 Storage tiers and durability
| Tier | Medium | Content | Encoding |
|---|---|---|---|
| Hot | DRAM, hugepage arenas, `mlock` | open + recently sealed chunks (configurable window, default 4 h) | uncompressed typed columns |
| Warm | DRAM (plain pages) | sealed chunks beyond the hot window, still memory-resident | FastLanes bitpacked + dict (fast decode) |
| Cold | NVMe | immutable segments | Vortex (ALP, FastLanes, FSST, RLE) or Parquet (decision gate P1-G3) |
- Tiering by chunk recency with 5 % core budget, hysteresis, rate-limited memcpy (as v3.2 RFC-006). No PMU/page sampling. SPE/DAMON only as observability.
- WAL: `[len][crc32c][lsn][epoch][payload]`, group commit 10 ms or 256 KiB, `O_DIRECT` via io_uring, truncate at checkpoint (every 4 GiB or 60 s). Power-loss rig (dm-flakey ×1000) is a release gate (RFC-015, never cut).

---

## 5. Hardware matrix (corrected)

| Platform | Core | SVE2 VL / pipes | Access today | Role |
|---|---|---|---|---|
| AWS Graviton4 (c8g/m8g/r8g, `.metal-24xl/48xl`) | Neoverse V2 | 128 b / 4×128 | On demand | **Primary dev + CI + published benchmarks** |
| GitHub Arm runners (Cobalt 100) | Neoverse N2 | 128 b | Free (public repo) | CI (correctness, SVE2) |
| NVIDIA GH200 bare metal (Vultr, Oracle) | Neoverse V2 (Grace) | 128 b / 4×128 | On demand / spot | Bare-metal, HW-timestamp experiments **if NIC exposed** |
| Arm AGI CPU (early access) | Neoverse V3, 136 cores, 2 chiplets | 128 b / 2×128 per Arm spec | "later 2026", early-access form | Flagship showcase, Phase 3+ |
| Google Axion / Azure Cobalt | V2 / N2 | 128 b | On demand | Portability CI |
| Apple M-series | — (NEON only) | — | Laptop | Development only |
| x86-64 | — | — | Any | Correctness only |

Measured DRAM latency to budget: Graviton4 114 ns local / >250 ns remote; Grace >200 ns. **RNF for DRAM: ≤ 130 ns local.** Arm's "sub-100 ns" for the AGI CPU is unverified until measured.

Kernel: ≥ 6.1 for the core; ≥ 6.8 for `ankaa-wire` (XDP RX timestamp hints on mlx5/ice); ≥ 6.15 for io_uring ZCRX feature. Boot parameters for latency hosts as v3.2 §1.3 (with `iommu.passthrough=1`, `cgroup_disable=memory`, `rcu_nocb_poll`).

---

## 6. Functional requirements (v1.0)

| ID | Requirement | Pri | Acceptance |
|---|---|---|---|
| F-01 | Replay ingest from Parquet/CSV/DBN/PCAP at ≥ 10× real time | P0 | 1 day of Binance BTCUSDT L2 replayed in < 1/10 of wall time |
| F-02 | Live TCP/WebSocket ingest: Binance SBE + JSON, Databento live | P0 | 24 h soak, 0 undetected gaps, reconnection with sequence resync |
| F-03 | Decoder trait + generic framing for user-defined binary feeds | P0 | Example decoder in docs; fuzzed |
| F-04 | Hot columnar store, 64k-row sealed chunks, zone maps, per-feed watermark | P0 | Read-committed per feed; property tests |
| F-05 | WAL CRC32C + group commit + replay + truncate at checkpoint | P0 | dm-flakey ×1000 without loss/corruption |
| F-06 | SQL via DataFusion with time/symbol pushdown | P0 | TPC-like tick query set passes vs DuckDB reference |
| F-07 | Native ASOF join (streaming merge, bounded memory) | P0 | Results equal DuckDB `ASOF JOIN`; memory O(partitions) |
| F-08 | Time-bucket aggregates (OHLCV, VWAP, counts) | P0 | Differential vs Polars |
| F-09 | Order-book reconstruction (L2) with snapshots | P0 | Checksums match exchange snapshots on replay |
| F-10 | Quantile sketches (DDSketch/UDDSketch with negatives, flat arrays) | P1 | Error ≤ 1 % relative; proptest |
| F-11 | Arrow Flight server (SQL, tickets, TLS, token auth) | P0 | pyarrow / ADBC client works |
| F-12 | Python package `ankaa` (Arrow zero-copy, GIL released) | P0 | `pip install ankaa`; notebook demo |
| F-13 | Cold tier segments (Vortex or Parquet) with pruning | P0 | Transparent query across hot/warm/cold |
| F-14 | SIMD backends scalar/NEON/SVE2, runtime dispatch (getauxval + macro + self-test) | P0 | Differential suite green on Graviton4 + QEMU VL256 |
| F-15 | Selection-vector filtering + materialization kernels | P0 | Branchless-v2 gate |
| F-16 | FastLanes/ALP/FSST encodings for warm/cold | P0 | ≥ 4× compression on real tick data; decode ≥ 2 GB/s/core |
| F-17 | Chunk tiering hot→warm→cold by recency, 5 % budget | P1 | No thrashing test; overhead ≤ 5 % one core |
| F-18 | `/metrics` (Prometheus) with ingest/query/wal/tier/drop/gap metrics, authenticated | P0 | Scrape from Grafana dashboard in repo |
| F-19 | Graceful shutdown, crash recovery < 60 s for 10 GB WAL | P0 | Test |
| F-20 | Config TOML with validation and fail-fast on memory reservation | P0 | Fuzzed |
| F-21 | Postgres wire protocol (read-only) | P2 | psql connects |
| F-22 | `ankaa-wire` AF_XDP ingest with HW timestamps (Phase 3) | P2 | Report on named bare metal |

---

## 7. Non-functional requirements (v1.0, measured on Graviton4 c8g.8xlarge unless stated)

| ID | Metric | Target | Stretch | How measured |
|---|---|---|---|---|
| N-01 | Decode + append throughput, Binance SBE, per ingest core | ≥ 3 M events/s | 5 M | replay from RAM, 60 s |
| N-02 | TCP ingest per core (io_uring ZCRX feature) | ≥ 1 GB/s | 3 GB/s | loopback + 25 GbE |
| N-03 | Event → queryable latency (socket read to watermark visible), VM | P99 ≤ 1 ms | 100 µs | software timestamps, fixed-point histogram |
| N-04 | Hot column scan (filter + sum, i64/f32), per core | ≥ 4 GB/s | 8 GB/s | criterion, L2 and DRAM resident |
| N-05 | ASOF join, sorted inputs, per core | ≥ 50 M rows/s | 100 M | vs DuckDB on same box |
| N-06 | Order-book update apply, per core | ≥ 10 M updates/s | 20 M | replay |
| N-07 | Compression ratio, real L2 data (ALP + FastLanes + dict) | ≥ 4× | 6× | bytes on disk / Arrow bytes |
| N-08 | Cold decode throughput per core | ≥ 2 GB/s | 4 GB/s | criterion |
| N-09 | WAL durability | 0 loss/corruption in 1,000 power cuts | — | dm-flakey rig in CI |
| N-10 | Python call overhead | < 5 µs per 1 M-row batch | 3 µs | criterion via PyO3 |
| N-11 | Branch misses in hot kernels | < 0.1 % | — | PMU `BR_MIS_PRED` |
| N-12 | Test coverage / miri / fuzz | > 80 % lines; 100 % non-asm unsafe under miri; 24 h fuzz without new panic | — | CI |
| N-13 | Memory latency assumption | ≤ 130 ns local DRAM | — | lmbench at bring-up |
| N-14 | (Phase 3) wire → queryable, bare metal, HW-timestamped | P99 ≤ 10 µs | 5 µs | PHC → CNTVCT calibrated, residual published |

Every published number ships with: hardware SKU, kernel, toolchain, boot params, the exact script, raw histogram, and clock-calibration residual (RFC-017 protocol).

---

## 8. Security (as v3.2 §7, unchanged in spirit)
Fuzz targets: every feed decoder, WAL replay, segment reader, TOML config, Flight ticket parser. `panic=abort` + supervised restart. No `unwrap` on ingest/query paths. `/metrics` and Flight behind token auth and TLS, bind to management interface. `RLIMIT_MEMLOCK` validated at startup. `cargo-deny` and `cargo-audit` in CI; libbpf (Phase 3) pinned BSD-2-Clause. `docs/security.md` is a release artifact.

---

## 9. Roadmap and effort

Estimates are person-hours for one maintainer and include integration and debugging.

| Phase | Weeks | ph | Outcome | Gate |
|---|---|---|---|---|
| **P0 Validate & decide** | 1–3 | 80 | Graviton4 + GitHub Arm CI live; `rdvl`, DRAM latency, COMPACT vs TBL microbench; Vortex vs Parquet spike; Binance SBE capture (1 week of data); DataFusion `TableProvider` hello-world over a hand-built chunk | **G0:** SQL query over an in-memory chunk works through DataFusion; SVE2 `global_asm!` kernel passes differential test on Graviton4 |
| **P1 Core engine** | 4–15 | 400 | ingest (replay + Binance live), hot store, WAL, SPSC, NEON kernels, DataFusion integration, Flight, Python, metrics | **G1:** 24 h live soak with 0 undetected gaps; dm-flakey ×1000 green; F-01..F-08, F-11, F-12, F-18..F-20 done |
| **P2 Differentiate & launch** | 16–25 | 350 | SVE2 backends, selection vectors, ALP/FastLanes warm/cold tier, ASOF/book/sketch operators, benchmark report vs QuestDB/ClickHouse/DuckDB on the same Graviton4 box, docs, public repo, blog post, Arm Learning Path | **G2:** N-01, N-04, N-05, N-07 met; benchmark report reproducible by a stranger; outside users running it on their own data |
| **P3 Partners & showcase** | 26–37 | 400 | design partners on live data; `ankaa-wire` on GH200 bare metal or Arm AGI CPU with HW timestamps; exchange feed module if a partner provides licensed access | **G3:** a design partner running live data; N-14 report on named hardware |
| **P4 v1.0** | 38–50 | 300 | hardening, Postgres wire, upgrade/backup story, security review, v1.0 release | **G4:** DoD §10 |
| **Total** | **~50 weeks** | **~1,530 ph** | | |

De-scope order if a gate slips (pre-agreed): (1) `ankaa-wire` showcase → next version; (2) Postgres wire; (3) FSST/strings (dictionary only); (4) SVE2 ASOF (NEON only); (5) order-book snapshots (replay-only). **Never cut:** WAL power-loss, fuzzing, hardware-timestamped measurement protocol, differential SIMD tests.

---

## 10. Definition of Done (v1.0)
- [ ] F-01..F-20 implemented with tests; F-21/F-22 documented as experimental or deferred.
- [ ] N-01..N-13 measured on Graviton4 and published with scripts; N-14 published if hardware was available, else explicitly "not measured".
- [ ] dm-flakey ×1000 green in CI; 24 h fuzz corpus without new panic; miri green on non-asm unsafe; herd7 litmus green.
- [ ] Differential suite (scalar/NEON/SVE2; vs DuckDB/Polars) green on Graviton4 and QEMU VL256.
- [ ] Reproducible benchmark repo comparing ANKAA vs QuestDB, ClickHouse, DuckDB on the same instance type (OSS only; no kdb+ claims without STAC).
- [ ] `pip install ankaa` works on aarch64 Linux and macOS arm64; notebook demo runs.
- [ ] Docs: architecture, data-model, metrics-semantics, unsafe-policy (miri vs asm), security, benchmarks, deployment (boot params, memlock, hugepages), temporal-architecture.
- [ ] Security review of all `unsafe` and all parsers signed off (external review planned).
- [ ] License, trademark, CONTRIBUTING, DCO, governance published.

---

## 11. Risks (rebased)

| ID | Risk | P | I | Mitigation |
|---|---|---|---|---|
| R-01 | Single-maintainer bandwidth; bus factor | H | H | Reuse (DataFusion, Vortex, Arrow); public repo early; design-partner co-development; grow a second maintainer |
| R-02 | DataFusion API churn (monthly releases, breaking majors) | H | M | Pin major; thin adapter crate; upgrade quarterly |
| R-03 | Vortex immaturity or API churn | M | M | Isolate behind `Encoding` trait; Parquet fallback; port ALP/FastLanes if needed |
| R-04 | Rust SVE support shifts (nightly intrinsics stabilize or change) | M | L | `global_asm!` stable path is independent; intrinsics backend optional |
| R-05 | No V3 hardware for a year | H | M | Everything is VL-agnostic; Graviton4/Grace numbers are publishable today |
| R-06 | GH200 host NIC not tenant-visible → no HW timestamps | M | M | Confirm with the provider before renting; fall back to software timestamps + explicit caveat |
| R-07 | Binance/ venue feed format changes | M | L | Versioned decoders; fuzz corpus; replay regression |
| R-08 | Established engines add comparable Arm paths | H | M | Stay Arm-native, open, measurable, Python/Arrow first |
| R-09 | Benchmark credibility attacked | M | H | Publish everything; invite reproduction; no vendor claims without STAC |
| R-10 | Memory model bugs in rings/arenas | M | H | loom + herd7 + miri; descriptor-only rings; no shared mutable state |
| R-11 | Scope creep back toward v3.2 | H | M | RFC gate; de-scope list; this PRD |

---

## 12. Open technical questions
1. Cold-tier format: Vortex file format vs Parquet as the primary on-disk segment (decision at gate P1-G3 with real L2 data; see `docs/spikes/vortex.md`).
2. Materialization strategy on Neoverse V2: `COMPACT` (single pipe) vs `TBL` permutes vs selection vectors only; needs measurement on Graviton4.
3. Exposure of hot chunks to DataFusion without copying (arena-backed buffers, epoch reclamation).
4. Whether the Flight SQL extension is worth implementing before the Python package.
