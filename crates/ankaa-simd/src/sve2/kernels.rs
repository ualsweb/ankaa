//! SVE2 kernels in `global_asm!`. All loops are vector-length agnostic (`whilelt`/`incw`).
//!
//! Branch policy (branchless-v2): the only branches are loop control (`b.first`/`b.none`
//! on the governing predicate), never on data values.
//!
//! Calling convention: AAPCS64. Only z0–z7 and p0–p3 are used (caller-saved).
//!
//! Scheduling note (llvm-mca, Neoverse V2 model): the first version of the filter loop was bound by
//! the M pipes (predicate ops, scalar→vector broadcast, cntp/whilelt for the store predicate) at
//! ~3.2 cycles per vector. The current loop keeps a running index vector (`incw z4.s`) and stores
//! the compacted register unconditionally with `incp` for the count.

core::arch::global_asm!(
    ".arch_extension sve2",
    ".text",
    ".p2align 2",
    // ---------------------------------------------------------------------------------
    // usize ankaa_sve_vl_bytes(void)
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve_vl_bytes",
    ".type ankaa_sve_vl_bytes, %function",
    "ankaa_sve_vl_bytes:",
    "    rdvl x0, #1",
    "    ret",
    ".size ankaa_sve_vl_bytes, .-ankaa_sve_vl_bytes",
    // ---------------------------------------------------------------------------------
    // usize ankaa_sve2_filter_range_f32_sel(const f32 *data x0, usize n x1,
    //                                        f32 min s0, f32 max s1, u16 *sel x2)
    // Writes indices i with min <= data[i] <= max into sel (u16), returns count.
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_filter_range_f32_sel",
    ".type ankaa_sve2_filter_range_f32_sel, %function",
    "ankaa_sve2_filter_range_f32_sel:",
    "    mov   z2.s, s0",     // broadcast min
    "    mov   z3.s, s1",     // broadcast max
    "    mov   x3, xzr",      // i = 0
    "    mov   x4, xzr",      // written = 0
    "    index z4.s, #0, #1", // running lane indices: 0,1,2,... advanced by VL each step
    "    whilelt p0.s, x3, x1",
    "    b.none 2f",
    "1:",
    "    ld1w  {{z0.s}}, p0/z, [x0, x3, lsl #2]",
    "    fcmge p1.s, p0/z, z0.s, z2.s", // x >= min (false for NaN)
    "    fcmle p2.s, p0/z, z0.s, z3.s", // x <= max
    "    and   p1.b, p0/z, p1.b, p2.b", // in range
    "    compact z6.s, p1, z4.s",       // pack selected indices to the front
    // Unconditional store of the whole register: lanes beyond the popcount hold stale indices
    // and are overwritten by the next store or land in the SelVec slack (never read). This
    // replaces cntp + whilelt + predicated store (three M-pipe ops) with one store + incp.
    "    st1h  {{z6.s}}, p0, [x2, x4, lsl #1]",
    "    incp  x4, p1.s", // written += popcount(selected)
    "    incw  z4.s",     // lane indices += VL lanes
    "    incw  x3",
    "    whilelt p0.s, x3, x1",
    "    b.first 1b",
    "2:",
    "    mov   x0, x4",
    "    ret",
    ".size ankaa_sve2_filter_range_f32_sel, .-ankaa_sve2_filter_range_f32_sel",
    // ---------------------------------------------------------------------------------
    // f64 ankaa_sve2_sum_f32(const f32 *data x0, usize n x1) -> d0
    // Widens to f64 and accumulates in two vector accumulators (even/odd lanes), then
    // reduces with faddv. Ordered differently from scalar: tolerance 1e-6 in tests.
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_sum_f32",
    ".type ankaa_sve2_sum_f32, %function",
    "ankaa_sve2_sum_f32:",
    "    mov   z1.d, #0", // acc even
    "    mov   z2.d, #0", // acc odd
    "    mov   x3, xzr",
    "    ptrue p2.d",
    "    whilelt p0.s, x3, x1",
    "    b.none 2f",
    "1:",
    "    ld1w  {{z0.s}}, p0/z, [x0, x3, lsl #2]", // inactive lanes are zero
    "    fcvt  z3.d, p2/m, z0.s",                 // even .s lanes -> .d (uses bottom halves)
    "    fadd  z1.d, z1.d, z3.d",
    "    fcvtlt z4.d, p2/m, z0.s", // odd .s lanes -> .d (SVE2)
    "    fadd  z2.d, z2.d, z4.d",
    "    incw  x3",
    "    whilelt p0.s, x3, x1",
    "    b.first 1b",
    "2:",
    "    fadd  z1.d, z1.d, z2.d",
    "    faddv d0, p2, z1.d",
    "    ret",
    ".size ankaa_sve2_sum_f32, .-ankaa_sve2_sum_f32",
    // ---------------------------------------------------------------------------------
    // usize ankaa_sve2_filter_range_i64_sel(const i64 *data x0, usize n x1,
    //                                        i64 min x2, i64 max x3, u16 *sel x4)
    // Same structure as the f32 kernel on 64-bit lanes; st1h from .d lanes narrows to u16.
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_filter_range_i64_sel",
    ".type ankaa_sve2_filter_range_i64_sel, %function",
    "ankaa_sve2_filter_range_i64_sel:",
    "    mov   z2.d, x2",
    "    mov   z3.d, x3",
    "    mov   x5, xzr", // i
    "    mov   x6, xzr", // written
    "    index z4.d, #0, #1",
    "    whilelt p0.d, x5, x1",
    "    b.none 2f",
    "1:",
    "    ld1d  {{z0.d}}, p0/z, [x0, x5, lsl #3]",
    "    cmpge p1.d, p0/z, z0.d, z2.d",
    "    cmple p2.d, p0/z, z0.d, z3.d",
    "    and   p1.b, p0/z, p1.b, p2.b",
    "    compact z6.d, p1, z4.d",
    "    st1h  {{z6.d}}, p0, [x4, x6, lsl #1]",
    "    incp  x6, p1.d",
    "    incd  z4.d",
    "    incd  x5",
    "    whilelt p0.d, x5, x1",
    "    b.first 1b",
    "2:",
    "    mov   x0, x6",
    "    ret",
    ".size ankaa_sve2_filter_range_i64_sel, .-ankaa_sve2_filter_range_i64_sel",
    // ---------------------------------------------------------------------------------
    // usize ankaa_sve2_gather_f32(const f32 *data x0, const u16 *sel x1, usize n x2, f32 *out x3)
    // out[i] = data[sel[i]]: 16-bit indices zero-extended into .s lanes, gather load, store.
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_gather_f32",
    ".type ankaa_sve2_gather_f32, %function",
    "ankaa_sve2_gather_f32:",
    "    mov   x4, xzr",
    "    whilelt p0.s, x4, x2",
    "    b.none 2f",
    "1:",
    "    ld1h  {{z1.s}}, p0/z, [x1, x4, lsl #1]",
    "    ld1w  {{z0.s}}, p0/z, [x0, z1.s, uxtw #2]",
    "    st1w  {{z0.s}}, p0, [x3, x4, lsl #2]",
    "    incw  x4",
    "    whilelt p0.s, x4, x2",
    "    b.first 1b",
    "2:",
    "    mov   x0, x2",
    "    ret",
    ".size ankaa_sve2_gather_f32, .-ankaa_sve2_gather_f32",
    // ---------------------------------------------------------------------------------
    // usize ankaa_sve2_gather_i64(const i64 *data x0, const u16 *sel x1, usize n x2, i64 *out x3)
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_gather_i64",
    ".type ankaa_sve2_gather_i64, %function",
    "ankaa_sve2_gather_i64:",
    "    mov   x4, xzr",
    "    whilelt p0.d, x4, x2",
    "    b.none 2f",
    "1:",
    "    ld1h  {{z1.d}}, p0/z, [x1, x4, lsl #1]",
    "    ld1d  {{z0.d}}, p0/z, [x0, z1.d, lsl #3]",
    "    st1d  {{z0.d}}, p0, [x3, x4, lsl #3]",
    "    incd  x4",
    "    whilelt p0.d, x4, x2",
    "    b.first 1b",
    "2:",
    "    mov   x0, x2",
    "    ret",
    ".size ankaa_sve2_gather_i64, .-ankaa_sve2_gather_i64",
    // ---------------------------------------------------------------------------------
    // i64 ankaa_sve2_sum_i64(const i64 *data x0, usize n x1) -> x0 (wrapping)
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_sum_i64",
    ".type ankaa_sve2_sum_i64, %function",
    "ankaa_sve2_sum_i64:",
    // Main loop: four full vectors per iteration into four accumulators, so the adds do not
    // wait for each other and loop control costs one `whilelo` per four vectors (one `whilelt`
    // per vector and a single accumulator made the first version latency-bound: 181 ns per
    // 1,024 values on Neoverse V2 against 85 ns for the compiler's scalar loop). The tail
    // uses one predicated vector at a time.
    "    mov   z1.d, #0",
    "    mov   z2.d, #0",
    "    mov   z3.d, #0",
    "    mov   z4.d, #0",
    "    ptrue p2.d",
    "    mov   x3, xzr",
    "    cntd  x4, all, mul #4", // elements in four vectors
    "    sub   x7, x4, #1",
    "    mov   x8, x0",
    // a full block starts at x3 when its last element, x3 + 4·VL − 1, is below n; asked
    // with `whilelo` so the loop control stays predicate-driven (branchless gate)
    "    add   x9, x3, x7",
    "    whilelo p1.d, x9, x1",
    "    b.none 3f",
    "1:",
    "    ld1d  {{z0.d}}, p2/z, [x8]",
    "    ld1d  {{z5.d}}, p2/z, [x8, #1, mul vl]",
    "    ld1d  {{z6.d}}, p2/z, [x8, #2, mul vl]",
    "    ld1d  {{z7.d}}, p2/z, [x8, #3, mul vl]",
    "    add   z1.d, z1.d, z0.d",
    "    add   z2.d, z2.d, z5.d",
    "    add   z3.d, z3.d, z6.d",
    "    add   z4.d, z4.d, z7.d",
    "    addvl x8, x8, #4",
    "    add   x3, x3, x4",
    "    add   x9, x3, x7",
    "    whilelo p1.d, x9, x1",
    "    b.first 1b",
    "3:",
    "    whilelt p0.d, x3, x1",
    "    b.none 2f",
    "4:",
    "    ld1d  {{z0.d}}, p0/z, [x0, x3, lsl #3]",
    "    add   z1.d, z1.d, z0.d",
    "    incd  x3",
    "    whilelt p0.d, x3, x1",
    "    b.first 4b",
    "2:",
    "    add   z1.d, z1.d, z2.d",
    "    add   z3.d, z3.d, z4.d",
    "    add   z1.d, z1.d, z3.d",
    "    uaddv d0, p2, z1.d",
    "    fmov  x0, d0",
    "    ret",
    ".size ankaa_sve2_sum_i64, .-ankaa_sve2_sum_i64",
    // ---------------------------------------------------------------------------------
    // void ankaa_sve2_min_max_i64(const i64 *data x0, usize n x1, i64 out[2] x2)
    // Accumulators start at MAX/MIN; merging predication leaves inactive lanes untouched.
    // NEON has no 64-bit integer min/max (the NEON backend falls back to scalar code); SVE
    // has `smin`/`smax` on 64-bit lanes. Same shape as the sum: four vectors per iteration
    // into four accumulator pairs, predicated tail.
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_min_max_i64",
    ".type ankaa_sve2_min_max_i64, %function",
    "ankaa_sve2_min_max_i64:",
    "    mov   x3, #0x7fffffffffffffff",
    "    mov   z1.d, x3", // min accumulators = i64::MAX
    "    mov   z16.d, x3",
    "    mov   z17.d, x3",
    "    mov   z18.d, x3",
    "    mov   x4, #0x8000000000000000",
    "    mov   z2.d, x4", // max accumulators = i64::MIN
    "    mov   z19.d, x4",
    "    mov   z20.d, x4",
    "    mov   z21.d, x4",
    "    ptrue p2.d",
    "    mov   x5, xzr",
    "    cntd  x4, all, mul #4",
    "    sub   x7, x4, #1",
    "    mov   x8, x0",
    "    add   x9, x5, x7",
    "    whilelo p1.d, x9, x1",
    "    b.none 3f",
    "1:",
    "    ld1d  {{z0.d}}, p2/z, [x8]",
    "    ld1d  {{z5.d}}, p2/z, [x8, #1, mul vl]",
    "    ld1d  {{z6.d}}, p2/z, [x8, #2, mul vl]",
    "    ld1d  {{z7.d}}, p2/z, [x8, #3, mul vl]",
    "    smin  z1.d, p2/m, z1.d, z0.d",
    "    smax  z2.d, p2/m, z2.d, z0.d",
    "    smin  z16.d, p2/m, z16.d, z5.d",
    "    smax  z19.d, p2/m, z19.d, z5.d",
    "    smin  z17.d, p2/m, z17.d, z6.d",
    "    smax  z20.d, p2/m, z20.d, z6.d",
    "    smin  z18.d, p2/m, z18.d, z7.d",
    "    smax  z21.d, p2/m, z21.d, z7.d",
    "    addvl x8, x8, #4",
    "    add   x5, x5, x4",
    "    add   x9, x5, x7",
    "    whilelo p1.d, x9, x1",
    "    b.first 1b",
    "3:",
    "    whilelt p0.d, x5, x1",
    "    b.none 2f",
    "4:",
    "    ld1d  {{z0.d}}, p0/z, [x0, x5, lsl #3]",
    "    smin  z1.d, p0/m, z1.d, z0.d",
    "    smax  z2.d, p0/m, z2.d, z0.d",
    "    incd  x5",
    "    whilelt p0.d, x5, x1",
    "    b.first 4b",
    "2:",
    "    smin  z1.d, p2/m, z1.d, z16.d",
    "    smin  z17.d, p2/m, z17.d, z18.d",
    "    smin  z1.d, p2/m, z1.d, z17.d",
    "    smax  z2.d, p2/m, z2.d, z19.d",
    "    smax  z20.d, p2/m, z20.d, z21.d",
    "    smax  z2.d, p2/m, z2.d, z20.d",
    "    sminv d1, p2, z1.d",
    "    smaxv d2, p2, z2.d",
    "    str   d1, [x2]",
    "    str   d2, [x2, #8]",
    "    ret",
    ".size ankaa_sve2_min_max_i64, .-ankaa_sve2_min_max_i64",
    // ---------------------------------------------------------------------------------
    // void ankaa_sve2_min_max_f32(const f32 *data x0, usize n x1, f32 out[2] x2)
    // fminnm/fmaxnm: NaN operands are ignored unless every element is NaN (IEEE minNum),
    // matching the scalar reference. Accumulators start as NaN.
    // ---------------------------------------------------------------------------------
    ".globl ankaa_sve2_min_max_f32",
    ".type ankaa_sve2_min_max_f32, %function",
    "ankaa_sve2_min_max_f32:",
    "    mov   w3, #0x7fc00000",
    "    mov   z1.s, w3", // min acc = NaN
    "    mov   z2.s, w3", // max acc = NaN
    "    mov   x5, xzr",
    "    ptrue p2.s",
    "    whilelt p0.s, x5, x1",
    "    b.none 2f",
    "1:",
    "    ld1w  {{z0.s}}, p0/z, [x0, x5, lsl #2]",
    "    fminnm z1.s, p0/m, z1.s, z0.s",
    "    fmaxnm z2.s, p0/m, z2.s, z0.s",
    "    incw  x5",
    "    whilelt p0.s, x5, x1",
    "    b.first 1b",
    "2:",
    "    fminnmv s1, p2, z1.s",
    "    fmaxnmv s2, p2, z2.s",
    "    str   s1, [x2]",
    "    str   s2, [x2, #4]",
    "    ret",
    ".size ankaa_sve2_min_max_f32, .-ankaa_sve2_min_max_f32",
    // ---------------------------------------------------------------------------------
    // Counting kernels, one symbol per comparison (no data- or op-dependent branches).
    // usize ankaa_sve2_count_f32_<op>(const f32 *data x0, usize n x1, f32 a s0, f32 b s1)
    // usize ankaa_sve2_count_i64_<op>(const i64 *data x0, usize n x1, i64 a x2, i64 b x3)
    // Range uses both operands (a <= x <= b); the others use `a`.
    // ---------------------------------------------------------------------------------
    ".macro ANKAA_COUNT_F32 name, cmp",
    ".globl \\name",
    ".type \\name, %function",
    "\\name:",
    "    mov   z2.s, s0",
    "    mov   x3, xzr",
    "    mov   x4, xzr",
    "    whilelt p0.s, x3, x1",
    "    b.none 2f",
    "1:",
    "    ld1w  {{z0.s}}, p0/z, [x0, x3, lsl #2]",
    "    \\cmp p1.s, p0/z, z0.s, z2.s",
    "    incp  x4, p1.s",
    "    incw  x3",
    "    whilelt p0.s, x3, x1",
    "    b.first 1b",
    "2:",
    "    mov   x0, x4",
    "    ret",
    ".size \\name, .-\\name",
    ".endm",
    "ANKAA_COUNT_F32 ankaa_sve2_count_f32_lt, fcmlt",
    "ANKAA_COUNT_F32 ankaa_sve2_count_f32_le, fcmle",
    "ANKAA_COUNT_F32 ankaa_sve2_count_f32_gt, fcmgt",
    "ANKAA_COUNT_F32 ankaa_sve2_count_f32_ge, fcmge",
    "ANKAA_COUNT_F32 ankaa_sve2_count_f32_eq, fcmeq",
    "ANKAA_COUNT_F32 ankaa_sve2_count_f32_ne, fcmne",
    ".globl ankaa_sve2_count_f32_range",
    ".type ankaa_sve2_count_f32_range, %function",
    "ankaa_sve2_count_f32_range:",
    "    mov   z2.s, s0",
    "    mov   z3.s, s1",
    "    mov   x3, xzr",
    "    mov   x4, xzr",
    "    whilelt p0.s, x3, x1",
    "    b.none 2f",
    "1:",
    "    ld1w  {{z0.s}}, p0/z, [x0, x3, lsl #2]",
    "    fcmge p1.s, p0/z, z0.s, z2.s",
    "    fcmle p2.s, p0/z, z0.s, z3.s",
    "    and   p1.b, p0/z, p1.b, p2.b",
    "    incp  x4, p1.s",
    "    incw  x3",
    "    whilelt p0.s, x3, x1",
    "    b.first 1b",
    "2:",
    "    mov   x0, x4",
    "    ret",
    ".size ankaa_sve2_count_f32_range, .-ankaa_sve2_count_f32_range",
    ".macro ANKAA_COUNT_I64 name, cmp",
    ".globl \\name",
    ".type \\name, %function",
    "\\name:",
    "    mov   z2.d, x2",
    "    mov   x4, xzr",
    "    mov   x5, xzr",
    "    whilelt p0.d, x4, x1",
    "    b.none 2f",
    "1:",
    "    ld1d  {{z0.d}}, p0/z, [x0, x4, lsl #3]",
    "    \\cmp p1.d, p0/z, z0.d, z2.d",
    "    incp  x5, p1.d",
    "    incd  x4",
    "    whilelt p0.d, x4, x1",
    "    b.first 1b",
    "2:",
    "    mov   x0, x5",
    "    ret",
    ".size \\name, .-\\name",
    ".endm",
    "ANKAA_COUNT_I64 ankaa_sve2_count_i64_lt, cmplt",
    "ANKAA_COUNT_I64 ankaa_sve2_count_i64_le, cmple",
    "ANKAA_COUNT_I64 ankaa_sve2_count_i64_gt, cmpgt",
    "ANKAA_COUNT_I64 ankaa_sve2_count_i64_ge, cmpge",
    "ANKAA_COUNT_I64 ankaa_sve2_count_i64_eq, cmpeq",
    "ANKAA_COUNT_I64 ankaa_sve2_count_i64_ne, cmpne",
    ".globl ankaa_sve2_count_i64_range",
    ".type ankaa_sve2_count_i64_range, %function",
    "ankaa_sve2_count_i64_range:",
    "    mov   z2.d, x2",
    "    mov   z3.d, x3",
    "    mov   x4, xzr",
    "    mov   x5, xzr",
    "    whilelt p0.d, x4, x1",
    "    b.none 2f",
    "1:",
    "    ld1d  {{z0.d}}, p0/z, [x0, x4, lsl #3]",
    "    cmpge p1.d, p0/z, z0.d, z2.d",
    "    cmple p2.d, p0/z, z0.d, z3.d",
    "    and   p1.b, p0/z, p1.b, p2.b",
    "    incp  x5, p1.d",
    "    incd  x4",
    "    whilelt p0.d, x4, x1",
    "    b.first 1b",
    "2:",
    "    mov   x0, x5",
    "    ret",
    ".size ankaa_sve2_count_i64_range, .-ankaa_sve2_count_i64_range",
    ".arch_extension nosve2",
);

unsafe extern "C" {
    pub fn ankaa_sve_vl_bytes() -> usize;
    pub fn ankaa_sve2_filter_range_f32_sel(
        data: *const f32,
        n: usize,
        min: f32,
        max: f32,
        sel: *mut u16,
    ) -> usize;
    pub fn ankaa_sve2_sum_f32(data: *const f32, n: usize) -> f64;
    pub fn ankaa_sve2_filter_range_i64_sel(
        data: *const i64,
        n: usize,
        min: i64,
        max: i64,
        sel: *mut u16,
    ) -> usize;
    pub fn ankaa_sve2_gather_f32(
        data: *const f32,
        sel: *const u16,
        n: usize,
        out: *mut f32,
    ) -> usize;
    pub fn ankaa_sve2_gather_i64(
        data: *const i64,
        sel: *const u16,
        n: usize,
        out: *mut i64,
    ) -> usize;
    pub fn ankaa_sve2_sum_i64(data: *const i64, n: usize) -> i64;
    pub fn ankaa_sve2_min_max_i64(data: *const i64, n: usize, out: *mut i64);
    pub fn ankaa_sve2_min_max_f32(data: *const f32, n: usize, out: *mut f32);
    pub fn ankaa_sve2_count_f32_lt(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_f32_le(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_f32_gt(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_f32_ge(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_f32_eq(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_f32_ne(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_f32_range(data: *const f32, n: usize, a: f32, b: f32) -> usize;
    pub fn ankaa_sve2_count_i64_lt(data: *const i64, n: usize, a: i64, b: i64) -> usize;
    pub fn ankaa_sve2_count_i64_le(data: *const i64, n: usize, a: i64, b: i64) -> usize;
    pub fn ankaa_sve2_count_i64_gt(data: *const i64, n: usize, a: i64, b: i64) -> usize;
    pub fn ankaa_sve2_count_i64_ge(data: *const i64, n: usize, a: i64, b: i64) -> usize;
    pub fn ankaa_sve2_count_i64_eq(data: *const i64, n: usize, a: i64, b: i64) -> usize;
    pub fn ankaa_sve2_count_i64_ne(data: *const i64, n: usize, a: i64, b: i64) -> usize;
    pub fn ankaa_sve2_count_i64_range(data: *const i64, n: usize, a: i64, b: i64) -> usize;
}
