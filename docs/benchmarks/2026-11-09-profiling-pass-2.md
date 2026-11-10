# Profiling pass 2 — 2026-11-09 (late)

Same method and tools as pass 1 (`2026-11-09-profiling-pass-1.md`): criterion, samply (5 kHz, symbol
sidecar, `bench/scripts/profile_top.py`), hyperfine, release build on an Apple M4 Pro laptop.
**Development signals, not product claims** (`docs/measurement-protocol.md`).

## New paths measured (criterion, `bench/benches/hot_paths.rs`)

| Path | Input | Result |
|---|---|---|
| `tier/encode_warm_64k` | 65,536 sealed trade rows (2.4 MB plain, synthetic random-walk prices) | 3.16 ms (731 MiB/s of plain bytes); **12.4× smaller** (2,424,832 → 196,096 B) through Vortex's BtrBlocks cascade |
| `tier/decode_warm_all_columns` | same chunk | 107 µs (21 GiB/s of plain bytes) |
| `tier/decode_warm_ts_price` | 2 of 6 columns | 51 µs |
| `tier/write_cold_64k` | segment file + fsync + dir fsync | 13.6 ms (fsync-bound; runs in the tiering tick budget, 2–4 chunks per 250 ms tick at 5 %) |
| `tier/read_cold_ts_price` | open file, project 2 columns | 90 µs |
| `tier/read_cold_all_columns` | open file, 6 columns | 206 µs |
| `framing/framer_1024x48_single_push` | 51 KB stream, 1,024 frames | 8.5 µs (5.6 GiB/s) |
| `framing/framer_1024x48_1460B_segments` | same, MTU-sized pushes | 8.9 µs |
| `framing/split_all_1024x48` | whole-buffer split | 4.0 µs (12 GiB/s) |

The synthetic chunk compresses better than real L2 data will (pass-1 spike on real-shaped prices: 6.4×);
the N-07 figure (≥ 4× on real L2) is measured on Graviton4 with captured data, not here.

## WAL recovery (F-19)
See `2026-11-09-recovery-speed.md`: 73.2 MiB / 163,600 frames / 1,483,600 rows recovered in 214 ms
(≈ 342 MiB/s, ≈ 6.9 M rows/s) on the laptop.

## Replay and recovery profiles (samply)
Replay of the 100× capture (40,700 frames, 370,900 events, one core, release):

| Metric | Pass 1b | Pass 2 (after tiers, LSN plumbing, config, metrics histogram) |
|---|---|---|
| in-process replay (`replay done … ms=`) | 51 ms | 51 ms (7.26 M events/s; decode 614 ns/frame, ingest 52 ns/event) |
| wall time incl. process start + file read (hyperfine, 7 runs) | 60 ms | 61.1 ms ± 1.0 |

No regression from the per-partition LSN tracking, the `skipping()` check on every append batch or the
latency histogram (recorded once per exchange message, off the decode path).

Top self-time buckets (samply, 5 kHz, 323 samples; attribution across inlined serde frames is approximate):

| Share | Where | Note |
|---|---|---|
| ~37 % | `serde_json` reader + `visit_event_body` + `next_value`/`parse_ident` | JSON field scanning; the single-pass `DeserializeSeed` design from pass 1 is already the floor for `serde_json` |
| ~18 % (incl.) | `capture::decode_frame` / `decode_all_bytes` | per-frame CRC32C (hardware instruction) and slice bookkeeping; kept, integrity beats 10 ms |
| ~6 % | `SymbolDict::intern` | last-symbol compare on every message; one symbol in this capture, so this is the fixed cost of the cache check |
| ~17 % (incl.) | `discard` / `deserialize_ignored_any` | skipping fields ANKAA does not store (`e`, `s`, …) |

Nothing in the new tiering/checkpoint code appears in the replay profile (it runs on the snapshot tick
only). The WAL-recovery path shares this profile (same decoder and appenders) and was timed separately.

## Changes made in this pass
- No hot-path code changes were needed: the new features did not move the replay numbers.
- Verified that the tiering step fits its budget: encoding one 64k-row chunk costs ~3 ms, writing its segment
  ~14 ms (fsync-bound), against a default budget of 12.5 ms per 250 ms tick (5 % of one core); the step
  moves at most what fits and resumes on the next tick.
- Candidates for a later pass, if a measurement on Graviton4 says they matter: a `simd-json`/custom scanner
  for the depth payload (pass 1b measured `simd-json` slower on these small frames, so not by default); batching
  CRC verification per capture block instead of per frame; skipping the symbol compare when a stream carries
  a single symbol.

## Reproduce
```sh
cd bench && cargo bench --bench hot_paths -- "tier|framing"
cargo build --release -p ankaad
python3 bench/scripts/make_profile_capture.py bench/data/binance-btcusdt-trade-depth-20s.ankc /tmp/p100.ankc 100
hyperfine -N --warmup 2 'target/release/ankaad replay /tmp/p100.ankc'
samply record --save-only --unstable-presymbolicate --rate 5000 -o /tmp/p.json -- target/release/ankaad replay /tmp/p100.ankc
python3 bench/scripts/profile_top.py /tmp/p.json 30
```
