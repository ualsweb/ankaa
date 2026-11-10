# ADR-0002: First hardware: Graviton4 and free GitHub Arm runners; GH200/AGI CPU later

- **Status:** Accepted 2026-11-09
- **Context:** Review of the previous product definition.

## Decision
Develop and test on Apple Silicon (NEON) and a local aarch64 Linux VM with QEMU (SVE2 at VL 128/256/512). CI runs on free GitHub `ubuntu-24.04-arm` runners (Cobalt 100, SVE2). Real performance numbers come from Graviton4 rented by the hour after gate G0. GH200 bare metal and the Arm AGI CPU (via Verda/Arm early access) are Phase 3.

## Consequences
No cloud dependency for correctness work; all SVE2 code is vector-length agnostic and budgeted at VL=128 (Neoverse V2/V3 reality; an earlier draft assumed 2x256, which was wrong).
