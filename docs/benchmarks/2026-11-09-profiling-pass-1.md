# Profiling pass 1 — ingest path on a development machine

**Date:** 2026-11-09 · **Machine:** Apple M4 Pro (laptop, macOS; aarch64 NEON only, no SVE outside SME
streaming mode) · **Build:** `--release` with line tables · **Scope:** development signals for finding
and fixing hot spots. **These are not product performance claims** (see `docs/measurement-protocol.md`);
server numbers will come from Graviton4.

## Tools used (all free and open source, or shipped with the platform)

| Tool | What it gave us | How |
|---|---|---|
| `criterion` (Rust) | per-function microbenchmarks with statistics | `bench/benches/hot_paths.rs`, `cd bench && cargo bench --bench hot_paths -- --noplot` |
| `samply` | sampling CPU profile of the real replay path, 5 kHz, with symbol sidecar | `samply record --save-only --unstable-presymbolicate --rate 5000 -o p.json -- ankaad replay …` then `bench/scripts/profile_top.py p.json` |
| `llvm-mca` (LLVM) | static scheduling analysis of the SVE2 loop bodies against the **Neoverse V2 and V3** pipeline models: cycles per iteration, IPC, per-unit pressure | `llvm-mca -mtriple=aarch64-linux-gnu -mcpu=neoverse-v2 loop.s` (loop bodies extracted from `crates/ankaa-simd/src/sve2/kernels.rs`) |
| `hyperfine` | end-to-end wall time with repetitions | `hyperfine -N 'ankaad replay capture.ankc'` |
| `cargo-show-asm` | Rust → assembly review of hot functions | `cargo asm -p ankaa-simd …` |
| in-binary timers | decode vs ingest split per frame/event | `ankaad replay` reports `decode_ns_per_frame`, `ingest_ns_per_event` |
| `perf` / `llvm-mca-18` in the aarch64 Linux VM | available for Linux-side sampling (software clock only in a VM; hardware PMU counters need Graviton4) | `docs/dev-environment.md` |

Input: `bench/scripts/make_profile_capture.py` turns the 20 s real capture into a 100× time-shifted,
gap-free stream (40,700 frames, 370,900 events, 18 MB).

## End-to-end result (replay of 370,900 events, one core)

| Stage | Replay time | Events/s | Decode per frame | Ingest per event |
|---|---|---|---|---|
| Baseline | 105 ms | 3.5 M | n/a | n/a |
| After single-pass decoder | 74 ms | 5.0 M | 748 ns | 87 ns |
| + zero-alloc depth levels, dense sequence state | 63 ms | 5.9 M | ≈ 640 ns | 78 ns |
| + symbol last-hit cache + FNV, zero-copy capture payloads | 58 ms | 6.4 M | 624 ns | 71 ns |
| + one lock per message, reusable row buffers | **51 ms** | **7.2 M** | **≈ 630 ns** | **51 ns** |

Wall time including process start and file read (hyperfine, 5 runs): 98 ms → 60 ms.

## What the profile showed and what changed

1. **Three JSON passes per frame** (envelope with `RawValue`, type tag, body). Replaced by one pass
   through serde `DeserializeSeed`s dispatching on the `"e"` tag as it is read. Trade frame 590 → 256 ns,
   depth frame with 10 levels 1.52 → 0.69 µs.
2. **`Vec<[&str; 2]>` allocations for bids/asks** (≈ 30 % of samples: `non_null`, `push_mut`). Levels are
   now parsed into two scratch vectors owned by the decoder; steady state allocates nothing.
3. **SipHash on every message** for the per-symbol sequence map and for symbol interning (≈ 19 %).
   Sequence state is a dense `Vec<Option<i64>>` indexed by symbol id; `SymbolDict::intern` checks the
   last symbol first and uses FNV-1a for the map.
4. **A write lock and a `Vec` per row** in the ingest tables. Rows of one exchange message are built
   into a reusable buffer and appended under one lock (`Table::append_row_arrays`).
5. **Payload copies when reading captures** (`decode_all` copied each frame). `decode_all_bytes` slices
   a shared `Bytes` buffer instead.

## SIMD kernels (criterion, 1,024 elements)

| Kernel | Scalar | NEON before | NEON after | Note |
|---|---|---|---|---|
| `filter_range_f32` (half selected) | 307 ns | 180 ns | **130 ns** | 8 lanes/iteration, movemask by narrow + multiply, 256-entry index LUT |
| `filter_range_i64` (half selected) | 427 ns | 351 ns | **238 ns** | 4 lanes/iteration, shared 16-entry LUT |
| `sum_f32` (f64 accumulation) | 419 ns | 122 ns | **75 ns** | four f64×2 accumulators |
| `min_max_f32` | 76 ns | 91 ns (slower than scalar) | **35 ns** | four accumulator pairs; the single-chain version lost to LLVM's auto-vectorized scalar |
| `count_f32` | 104 ns | 38 ns | 38 ns | unchanged |
| `sum_i64` | 54 ns | 54 ns | 54 ns | auto-vectorized scalar already optimal |

Selection-vector filtering at 130 ns per 1,024 rows is ≈ 7.9 G rows/s per core on this machine.

## Other hot paths (criterion)

| Function | Before | After | Note |
|---|---|---|---|
| keyed ASOF join, 100k × 200k rows, 8 keys | 1.23 ms (82 M rows/s) | **149 µs (670 M rows/s)** | dense per-key cursors instead of a hash map |
| time buckets, 100k rows | 90 µs (1.1 G rows/s) | 90 µs | unchanged |
| `Chunk::append_row` | 13.6 ns | 13.6 ns | `Value` enum path; typed appends are the next step |
| `L2Book::apply` | 18 ns | 18 ns | `BTreeMap` per side |
| `parse_fixed` (14-char price) | 10 ns | 10 ns | |
| capture frame encode + decode (CRC32C) | 125 ns | 125 ns | hardware CRC |
| WAL append (no commit) | ≈ 90 ns | ≈ 90 ns | CRC + memcpy |

## SVE2 loops under `llvm-mca` (static, Neoverse V2 model)

| Loop | Before | After | Change |
|---|---|---|---|
| `filter_range_f32_sel` | 3.2 cycles/vector, IPC 2.7, M-pipes at 3.8/3.2 (predicate ops, scalar→vector broadcast, `cntp`/`whilelt` for the store predicate) | **2.2 cycles/vector**, IPC 4.1, M-pipes at 2.0 | running index vector (`incw z4.s`) instead of per-iteration broadcast; unconditional store of the compacted register + `incp` instead of `cntp`+`whilelt`+predicated store |
| `sum_f32` | 1.3 cycles/vector | 1.3 | V-pipe bound on `fcvt`/`fcvtlt` widening, as expected |

Neoverse V3 model: 2.0 cycles/vector for the filter. Static numbers; real Graviton4 figures pending.
The unconditional store relies on `SelVec` slack of 64 lanes (the widest SVE register); correctness is
checked by the differential suite under QEMU at VL 128/256/512 (`cargo qtest128|256|512`).

## Pass 1b — follow-ups on the remaining hot spots (same day)

| Item | Result |
|---|---|
| **Binance SBE binary decoder** (`ankaa-feeds::binance::sbe`, schema `stream_1_0.xml`: trades, depth diff, depth snapshot; best bid/ask ignored) | Implemented from the published schema with an in-test encoder; forward-compatible via block lengths; refuses exponents finer than the fixed-point scale; never panics on truncated or hostile input. Microbenchmark below. Live use needs a free Binance API key (`--api-key` / `BINANCE_API_KEY`, `--url wss://stream-sbe.binance.com/stream`); not yet exercised live. |
| **`simd-json` as JSON back end** (optional feature `simd-json`) | **Slower** for these ~300-byte frames: trade 231 → 330 ns, depth×10 638 → 734 ns. The tape build and the in-place copy do not amortize on small messages. Kept as an opt-in feature; default stays `serde_json` with the single-pass seeds. |
| **Flat sorted `Vec` order-book sides** | **Slower**: `L2Book::apply` 16 → 36 ns with 1,000 levels per side (tail memmove on every insert/delete), ingest 51 → 61 ns/event. Reverted to `BTreeMap`; recorded here so it is not retried blindly. |
| **Forward keyed ASOF** | dense per-key lists and cursors (mirrors the backward path). |
| **Capture reading** | `FrameIter`: streaming, zero-copy frames (no `Vec<Frame>` materialization). |

Decoder comparison (criterion, same instrument and levels):

| Frame | JSON (serde_json, single pass) | JSON (simd-json feature) | SBE binary |
|---|---|---|---|
| trade | 231 ns | 330 ns | **27 ns** (8.8× faster than JSON) |
| depth, 10 levels | 638 ns | 734 ns | **82 ns** (8× faster than JSON) |

## Remaining hot spots (next pass)
- JSON scanning is now the floor for JSON feeds (simd-json measured slower on small frames); the SBE path removes it entirely once a key is available.
- Ingest at ≈ 51 ns/event: typed column appends instead of the `Value` enum path (≈ 13 ns/row today). The order book stays a `BTreeMap` (flat arrays measured slower).

## Reproduce
```sh
python3 bench/scripts/make_profile_capture.py bench/data/binance-btcusdt-trade-depth-20s.ankc /tmp/p100.ankc 100
cargo build --release -p ankaad && ./target/release/ankaad replay /tmp/p100.ankc
samply record --save-only --unstable-presymbolicate --rate 5000 -o /tmp/p.json -- ./target/release/ankaad replay /tmp/p100.ankc
python3 bench/scripts/profile_top.py /tmp/p.json 30
cd bench && cargo bench --bench hot_paths -- --noplot
```
