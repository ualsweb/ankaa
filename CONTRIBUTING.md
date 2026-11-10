# Contributing to ANKAA

Thank you for considering a contribution. ANKAA is Apache-2.0 licensed and uses the
**Developer Certificate of Origin** (DCO) instead of a CLA: sign every commit with
`git commit -s`, which adds `Signed-off-by: Your Name <you@example.com>` and certifies
https://developercertificate.org/.

## Ground rules
- Correctness first: every SIMD kernel ships with differential tests against the scalar
  backend (NaN, ±inf, ±0, denormals included; relative tolerance 1e-6 for float reductions).
- Every parser (feeds, WAL, segments, config) has a `cargo-fuzz` target.
- `unsafe` follows `docs/unsafe-policy.md`: miri-clean for non-asm unsafe; asm kernels are
  verified by differential tests under QEMU at several SVE vector lengths and on hardware.
- Performance claims only from real hardware with the script committed under `bench/`.
- Design changes go through an RFC in `docs/rfc/` (template `0000-template.md`).

## Workflow
1. Open an issue or RFC for anything beyond a bug fix.
2. `cargo fmt --all && cargo lint && cargo nextest run --workspace` must pass.
3. For SVE2 changes also run `cargo qtest128 && cargo qtest256` (needs `qemu-user`).
4. Commits: Conventional Commits (`feat(ankaa-wal): group commit by epoch`), signed off.

## Good first contributions
Feed decoders (`crates/ankaa-feeds`) with a replay fixture and a fuzz target.
