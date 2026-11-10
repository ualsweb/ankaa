# Development environment

## Any aarch64 Linux (Graviton, Ampere, Grace, GitHub Arm runners)
```sh
rustup toolchain install 1.99.1 --component rustfmt clippy llvm-tools
cargo nextest run --workspace                      # native tests (SVE2 skipped if the CPU lacks it)
sudo apt-get install -y qemu-user                  # SVE2 at chosen vector lengths under emulation
cargo qtest128 && cargo qtest256 && cargo qtest512 # aliases in .cargo/config.toml, with ANKAA_EXPECT_SVE2 guard
# from a Mac with the colima profile `ankaa`: scripts/vm-qtest.sh 16|32|64 runs the suite and the branchless
# gate inside the VM (builds into $HOME/target-ankaa there, so the macOS target directory is untouched)
scripts/branchless-gate.sh <object files>          # no data-dependent branches in SVE2 kernels
cargo loom                                         # loom models of the SPSC ring
```
Power-loss rig (root, Linux): `cargo build --release -p ankaa-wal --bin wal-torture && sudo scripts/power-loss-rig.sh 100 target/release/wal-torture`.

## macOS on Apple Silicon
The workspace builds and the scalar/NEON tests run natively. Apple Silicon has no SVE outside SME streaming mode, so SVE2 kernels are tested in an aarch64 Linux VM (any of Lima, Colima, UTM, OrbStack) with `qemu-user` installed inside it. Use a separate `CARGO_TARGET_DIR` inside the VM so Linux and macOS artifacts do not collide.

### Optional: streaming SVE on Apple M4 (P0-09)
Apple M4 exposes SME2 with a **512-bit streaming vector length**. The subset of SVE that is legal in streaming mode (contiguous loads/stores, arithmetic, predication, reductions, FastLanes unpacking) can run natively for correctness tests and timing. `COMPACT`, gathers and scatters are not available (the M4 lacks `FEAT_SME_FA64`), so the filter kernel stays on QEMU and Graviton. Build flags matter: enable `sme2` **without** base SVE (`-march=armv8.6-a+sme2` in clang terms) or the compiler emits a plain-SVE `addvl` in non-streaming code and the process gets `SIGILL`. This is a developer-experience path; no current Arm server has SME.

## CI
GitHub-hosted `ubuntu-24.04-arm` runners (Neoverse N2, SVE2): format, clippy with `-D warnings`, tests, `cargo-deny`; a QEMU matrix at VL 128/256/512 with the branchless gate; miri on the non-asm `unsafe` crates; fuzz smoke (when targets exist); the power-loss rig on pull requests touching the WAL.

## Profiling (free tools)
- `criterion` microbenchmarks: `cd bench && cargo bench --bench hot_paths -- --noplot`.
- Sampling profile of the real path: `samply record --save-only --unstable-presymbolicate --rate 5000 -o p.json -- target/release/ankaad replay <capture>` then `python3 bench/scripts/profile_top.py p.json`.
- Static scheduling of SVE2 loops for a Neoverse model: `llvm-mca -mtriple=aarch64-linux-gnu -mcpu=neoverse-v2 loop.s` (Homebrew `llvm` on macOS, `llvm-18` on Ubuntu).
- Large replay input from the bundled sample: `python3 bench/scripts/make_profile_capture.py bench/data/binance-btcusdt-trade-depth-20s.ankc /tmp/p100.ankc 100`.
- Wall time: `hyperfine -N 'target/release/ankaad replay /tmp/p100.ankc'`.
- Hardware counters (cycles, cache and branch misses) need real Arm hardware (`perf stat` on Graviton4); VMs expose software clocks only.
See `docs/benchmarks/2026-11-09-profiling-pass-1.md` for the first pass and its findings.
