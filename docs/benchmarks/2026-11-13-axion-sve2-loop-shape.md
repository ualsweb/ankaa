# SVE2 kernels with fewer predicates, on Google Axion — 2026-11-13

The SVE2 kernels rewritten after advice from an Arm engineer in the Arm developer community: keep
predicates out of the loop body ("minimize the number of predicated instructions in loops … only for
the final result merge"), and expect SVE at 128 bits to beat NEON only where it does something NEON
cannot, because both move the same data per clock. This note measures that before and after, and
records which kernels the engine now takes from SVE2 at 128-bit vectors.

- **Host:** Google Cloud `c4a-standard-4` (Axion, Arm Neoverse V2, 4 vCPU, SVE2 at 128 bits),
  Ubuntu 24.04, `europe-southwest1-a`, nothing else running on the VM.
- **Method:** `bench/benches/hot_paths.rs`, group `simd_1024` (criterion, one core, 1,024 values per
  call, 100 samples). "Before" is commit `d8894bd`, saved as a criterion baseline; "after" is the
  change described here, on the same VM and build settings, about an hour later. Medians in
  nanoseconds; every 95 % interval is within ±0.2 % of its median.
- **Correctness before timing:** the SVE2 differential suite (17 tests, including every length from
  0 to 300 and the i64 range edge cases) passed natively on the VM and under QEMU at VL 128, 256
  and 512; the branchless gate passed on all 22 kernel symbols; the workspace test suite passed.

## What changed in the kernels

Every streaming kernel now runs a body of four full vectors per iteration: unpredicated loads
(`ldr z, [x, #i, mul vl]`), compares governed by an all-true predicate, one accumulator per vector,
and a single `whilelo` per block for loop control. A `whilelt` loop of single predicated vectors
handles the tail. Before, every vector paid a `whilelt`, every load and compare was governed by it,
and the count kernels fed one serial `incp` counter.

- **count:** the compare result is used once, as the merge: a predicated add of ones into one vector
  accumulator per vector (`add z16.s, p1/m, z16.s, z5.s`), summed with `uaddv` at the end.
- **filter:** the second compare is governed by the first (`fcmle p1.s, p1/z, …`) instead of an
  `and`; the four `cntp` counts are independent and only the store offset is a chain of 1-cycle adds.
- **i64 range (filter and count):** `lo <= x <= hi` as one unsigned compare,
  `(x - lo) <= (hi - lo)`. It needs `lo <= hi`; the wrapper returns an empty result otherwise, as
  the scalar reference does.
- **sum_f32, min_max_f32:** eight and four-pair accumulators, each vector with its own conversion
  registers so the merging `fcvt`/`fcvtlt` do not wait on each other.

`llvm-mca` with the Neoverse V2 model predicted the direction before anything ran (cycles per vector,
old → new): count 2.1 → 1.0, sum_f32 3.1 → 1.1, min_max_f32 2.1 → 0.8, filter_f32 2.4 → 2.3,
filter_i64 3.4 → 2.5.

## Results

| Kernel (1,024 values) | scalar | NEON | SVE2 before | SVE2 after | change | faster at 128 bits |
|---|---:|---:|---:|---:|---:|---|
| filter_range_f32 (half selected) | 966 | 240 | 305 | **215** | −30 % | SVE2 |
| filter_range_i64 (half selected) | 754 | 510 | 583 | **348** | −40 % | SVE2 |
| gather_f32 (half selected) | 154 | 159 | — | **91** | new benchmark | SVE2 |
| gather_i64 (half selected) | 242 | 238 | — | **211** | new benchmark | SVE2 |
| min_max_i64 | 182 | 182 | 114 | **114** | unchanged | SVE2 |
| min_max_f32 | 119 | **55** | 162 | 63 | −61 % | NEON |
| sum_f32 | 664 | **89** | 262 | 124 | −53 % | NEON |
| count_f32_gt | 176 | **57** | 196 | 91 | −54 % | NEON |
| count_i64_gt | 108 | **107** | 427 | 216 | −49 % | NEON |
| count_i64_range | **179** | 191 | — | 208 | new benchmark | scalar |
| sum_i64 | 85 | **85** | 90 | 90 | unchanged | NEON |

The NEON backend has no gather and no 64-bit integer min/max; those columns are its scalar fallback.

What this says:

- **Every SVE2 kernel measured before and after got 1.4–2.6× faster**, as the model predicted. Of the
  count kernels only `gt` is benchmarked; the other comparisons and the f32 range share its code but
  were not timed.
- **SVE2 now wins where it has an instruction NEON lacks:** `compact` in the filters (NEON builds a bit
  mask and looks indices up in a table), native gathers, 64-bit `smin`/`smax`. These five kernels now
  run on SVE2 at 128-bit vectors (`dispatch::SVE2_AT_128`).
- **The plain loops reach NEON at best**, which is what the same data path predicts: `min_max_f32`
  is within 13 %, `sum_f32` 40 % behind. The **counts stay 1.6–2× behind** NEON: each SVE compare
  has to produce a predicate, while NEON's compare produces a mask that is subtracted directly from
  the accumulator. They stay on NEON.
- `count_i64_range` is the one kernel where neither vector backend beats the compiler's scalar loop
  on this data; it stays on NEON, the primary, at 191 ns (7 % slower than scalar).

## Limits

- One VM, one run, one input size (1,024 values, the engine's vector length), one selectivity (half
  the rows for filters and gathers). Gathers and filters vary with selectivity; that is not swept here.
- These are kernel times, not query times. How much of a query is spent in these kernels depends on
  the query; no end-to-end effect is claimed here.
- The scalar `filter_range_i64` measured 679 ns in the "before" run and 754 ns after, with its code
  unchanged; 754 ns matches the first-light note (`2026-11-10-axion-first-light.md`). The table uses
  the after value. Treat differences of a few percent between runs as noise of this kind.
- 128-bit SVE2 only. No SVE2 silicon wider than 128 bits can be rented today; the kernels are tested
  at 256 and 512 bits under QEMU for correctness, which says nothing about their speed there.
