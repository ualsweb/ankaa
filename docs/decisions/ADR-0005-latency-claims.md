# ADR-0005: Latency claims: only measured, hardware-timestamped, reproducible

- **Status:** Accepted 2026-11-09
- **Context:** Review of the previous product definition.

## Decision
Every published number ships with hardware SKU, kernel, toolchain, boot parameters, script, raw histogram and clock-calibration residual. Emulated (QEMU) runs are functional only. No comparative claims against kdb+/OneTick without a STAC audit; OSS comparisons (QuestDB, ClickHouse, DuckDB) on the same instance type only.

## Consequences
Phase 3 target is P99 <= 10 us wire-to-queryable on named bare metal; no 1.2 us P50 marketing.
