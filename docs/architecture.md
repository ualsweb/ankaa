# Architecture

ANKAA is a single-process engine (today) with four planes:

```
   sources: MQTT · Kafka · NATS · exchange WebSocket · file replay (later: multicast)
                      │  raw frames + host receive timestamp
                      ▼
   decoders (per feed, allocation-free, never panic)  ──►  EventBatch (trades, book deltas)
                      │
                      ▼
   partition-owned appenders ──► open chunk (≤ 64k rows) ──seal──► sealed chunks (Arc, immutable) + zone maps
                      │                                              │
                      │ watermark per partition                      │ periodic snapshot (sealed Arcs + copy of open chunk)
                      ▼                                              ▼
   WAL owner (group commit, fdatasync)                     DataFusion SessionContext ◄── Arrow Flight (SQL in ticket) ◄── ankaad query / pyarrow.flight
                                                                                     ◄── Flight SQL, same port     ◄── ADBC / JDBC
```

## Crates
| Crate | Role | `unsafe` |
|---|---|---|
| `ankaa-core` | shared types (grows with config/metrics) | forbidden |
| `ankaa-time` | clock domains, PHC↔counter calibration (Phase 3) | forbidden |
| `ankaa-ring` | SPSC descriptor ring, Acquire/Release only | audited, loom + herd7 |
| `ankaa-simd` | scalar / NEON / SVE2 kernels, selection vectors, runtime dispatch | audited, differential tests at 3 vector lengths |
| `ankaa-store` | chunks, zone maps, watermarks, tables/partitions | none yet (arena in P1-06) |
| `ankaa-wal` | write-ahead log with CRC32C and group commit | none |
| `ankaa-ingest` | decoder trait, fixed-point parsing, symbol dictionary, event batches | forbidden |
| `ankaa-feeds` | Binance JSON decoder, `.ankc` capture format, live WebSocket source, `ankaa-capture` | forbidden |
| `ankaa-ts` | streaming ASOF join, time buckets, L2 order book | forbidden |
| `ankaa-sql` | DataFusion `TableProvider` / `ExecutionPlan` with pushdown | forbidden |
| `ankaa-flight` | Arrow Flight and Flight SQL server (one service, one port) with bearer auth, read-only | forbidden |
| `ankaad` | CLI and server: `replay`, `serve`, `query` | forbidden |

## Design rules
1. **Vector unit = 1,024 rows** (FastLanes); kernels take slices and produce selection vectors; compaction only at materialization.
2. **Prices are fixed-point `i64`** with a per-column scale; equality is exact and ALP degenerates to FoR + bit-packing.
3. **One writer per partition**; readers never block writers; snapshots are immutable.
4. **Zero syscalls and zero durable writes** between frame arrival and watermark advance; the WAL owner batches.
5. **Vector-length-agnostic SVE2** (`whilelt`, `incw`, `rdvl`), budgeted at VL 128; the same binary runs on Graviton4, Grace, Cobalt and the Arm AGI CPU.
6. **Every parser is a decoder behind a trait**, tested with fixtures and (P1-12) fuzzed; malformed input returns errors, never panics.
7. **Numbers only from real hardware**, with the script, the raw histogram and the clock-calibration residual (see `measurement-protocol.md`). Emulated runs are correctness only.

## Data model
See `data-model.md`.
