# Unsafe policy

1. `unsafe` is allowed only in `ankaa-simd` (NEON intrinsics, SVE2 assembly wrappers), `ankaa-ring` (SPSC slots), `ankaa-core` (`getrlimit`/`sysconf`) and `ankaa-wire` (counter reads, `recvmsg` control-message parsing). Every other crate declares `#![forbid(unsafe_code)]`.
2. Every `unsafe` block carries a `// SAFETY:` comment stating the invariant it relies on and where that invariant is established.
3. Two verification paths, because miri cannot execute SIMD intrinsics or assembly:
   - **miri path** (`ankaa-ring`, `ankaa-core`, `ankaa-wire` on Linux): `cargo miri test` in CI with strict provenance and symbolic alignment checks; `ankaa-store` and `ankaa-wal` are `forbid(unsafe_code)` and run there as a regression guard.
   - **differential path** (`ankaa-simd`): every kernel is checked against the scalar reference with property tests including NaN, ±inf, ±0 and denormals; SVE2 kernels additionally run under QEMU at vector lengths 128, 256 and 512 and must pass the branchless gate (`scripts/branchless-gate.sh`).
4. Safe wrappers validate lengths with real `assert!`s, not `debug_assert!`, when a violation would be an out-of-bounds access in release builds (`ankaa-simd` length and gather-index checks; `SelVec::set_len`).
5. Concurrency primitives are modelled with `loom` (`cargo loom`, run in the `miri` workflow); the ARMv8 memory-model argument for the SPSC ring is written as a `herd7` litmus test in `docs/herd7/`.
6. New `unsafe` requires an RFC section naming the invariant, the test that exercises it, and the reviewer.
