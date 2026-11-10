# ADR-0004: Build vs reuse: own ingest/store/kernels/operators, reuse DataFusion, Arrow, Vortex

- **Status:** Accepted 2026-11-09
- **Context:** Review of the previous product definition.

## Decision
Own: ingest path, hot columnar chunk store (1,024-row vectors, 64k-row sealed chunks), SVE2/NEON kernels, WAL, tiering, ASOF/window/book/sketch operators. Reuse: Apache DataFusion (SQL planning/execution), Arrow + Arrow Flight, Vortex encodings (ALP, FastLanes, FSST) behind an `Encoding` trait with a Parquet fallback.

## Consequences
Roughly halves the from-scratch surface; accepts DataFusion/Vortex API churn, isolated in `ankaa-sql` and the encoding trait, with exact version pins.
