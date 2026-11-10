# Spike P0-07 — Vortex 0.93 on aarch64: ALP + FastLanes round trip

**Date:** 2026-11-09 · **Host:** Apple M4 Pro (macOS, aarch64; functional check only, no timing claims) · **Code:** `bench/spikes/vortex/`

## Question
Does Vortex (`vortex = "=0.93.0"`, pinned to `arrow 59.2` like DataFusion 55.2) build on aarch64, and does its adaptive compressor produce the ALP → FoR → bit-packing cascade the PRD assumes for decimal-origin prices?

## Result
Yes on both counts. 65,536 synthetic prices around 50,000.00 with ±2-cent random steps:

| Column | Raw (Arrow) | Compressed | Ratio | Encoding tree chosen by `BtrBlocksCompressor` |
|---|---|---|---|---|
| `f64` prices (decimal-origin doubles) | 524,384 B | 81,920 B | **6.40×** | `vortex.alp(f64)` (exponents e=14, f=12) → `fastlanes.for.v2(i64)` (reference 4,999,974) → `fastlanes.bitpacked.v2` (bit_width **10**) |
| `i64` ticks (same prices × 100) | 524,384 B | 81,920 B | **6.40×** | `fastlanes.for.v2(i64)` → `fastlanes.bitpacked.v2` (bit_width 10) |

Both decode back exactly (value-by-value equality, 65,536 rows) through `execute::<Canonical>` and `ArrowSession::execute_arrow`.

## What this confirms for the PRD
1. **ALP on decimal prices collapses to FoR + bit-packing** (PRD v4.1 §4.3): the `f64` path and the fixed-point `i64` path produce byte-identical payloads (81,920 B). Storing prices as `i64` ticks at ingest loses nothing and avoids the ALP exponent search on the hot path.
2. **FastLanes bit-packing is the cold/warm layout** (PRD §4.7): 10 bits/value here; real L2 data will vary, but the mechanism is exactly the one the FastLanes paper describes (1,024-value vectors, ISA-agnostic unpack).
3. **Vortex is usable from Rust today** with a small API surface: `VortexSession::default()`, `session.create_execution_ctx()`, `session.arrow().from_arrow_array(..)`, `BtrBlocksCompressor::from_session(&session).compress(&array, &mut ctx)`, `array.execute::<Canonical>(&mut ctx)`, `session.arrow().execute_arrow(array, None, &mut ctx)`.

## Caveats and decision input for P1-G3 (Vortex vs Parquet for the cold tier)
- 0.x crate with breaking releases roughly monthly; API names changed since public docs (e.g. no `ArrayRef::to_canonical`; execution goes through `Executable`). Pin exact versions and isolate behind an `Encoding` trait.
- Vortex pins `arrow 59.2` and `datafusion 55.0` in its workspace; compatible with our pins today. A DataFusion major bump must wait for Vortex.
- The build adds ~35 crates (`vortex-*`) and a few minutes of compile time; acceptable for the cold tier, not for the hot path crates.
- Upstream CI runs on Graviton4 with SVE, so aarch64 is a first-class target for them.

**Recommendation:** proceed with Vortex for warm/cold encodings behind the `Encoding` trait; keep Parquet as the interchange/export format (and fallback) rather than as the primary cold tier. Re-evaluate at P1-G3 with real Binance L2 data from the capture.
