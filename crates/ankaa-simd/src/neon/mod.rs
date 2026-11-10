//! NEON backend (128-bit, always available on aarch64).
//!
//! Filters compute a 4-lane (f32) or 2-lane (i64) compare mask, turn it into a small
//! integer via a horizontal add over `{1,2,4,8}`, and look up a table of packed lane
//! indices; the table entry is added to the base index and stored **unconditionally**
//! (the selection vector has slack), then the length advances by the popcount. There is
//! no data-dependent branch in the loop body.

#[allow(clippy::wildcard_imports)]
use core::arch::aarch64::*;

use crate::{BackendKind, CmpOp, SelVec, SimdBackend, debug_check_len};

/// NEON backend.
#[derive(Debug, Default, Clone, Copy)]
pub struct NeonBackend;

/// For each 4-bit mask, the lane indices of the set bits, packed low-to-high (rest zero).
const LUT4: [[u16; 4]; 16] = {
    let mut t = [[0u16; 4]; 16];
    let mut m = 0;
    while m < 16 {
        let mut k = 0;
        let mut lane = 0;
        while lane < 4 {
            if (m >> lane) & 1 == 1 {
                t[m][k] = lane as u16;
                k += 1;
            }
            lane += 1;
        }
        m += 1;
    }
    t
};

/// For each 8-bit mask, the lane indices of the set bits, packed low-to-high (rest zero). 4 KiB, L1-resident.
const LUT8: [[u16; 8]; 256] = {
    let mut t = [[0u16; 8]; 256];
    let mut m = 0;
    while m < 256 {
        let mut k = 0;
        let mut lane = 0;
        while lane < 8 {
            if (m >> lane) & 1 == 1 {
                t[m][k] = lane as u16;
                k += 1;
            }
            lane += 1;
        }
        m += 1;
    }
    t
};

/// 8-bit movemask of two 4-lane u32 compare results (all-ones or zero per lane): narrow each to
/// 16-bit lanes, keep bit 0, then gather the eight bits with one multiply (bits sit at positions
/// 0,16,32,48 of each 64-bit half; `* 0x0001_0002_0004_0008` moves lane k to bit 48+k).
#[inline]
unsafe fn movemask8(m0: uint32x4_t, m1: uint32x4_t) -> usize {
    // SAFETY: register-only operations.
    unsafe {
        let n0 = vmovn_u32(m0); // 4 × u16, each 0xFFFF or 0
        let n1 = vmovn_u32(m1);
        let one = vdup_n_u16(1);
        let a = vget_lane_u64(vreinterpret_u64_u16(vand_u16(n0, one)), 0);
        let b = vget_lane_u64(vreinterpret_u64_u16(vand_u16(n1, one)), 0);
        let lo = (a.wrapping_mul(0x0001_0002_0004_0008) >> 48) & 0xF;
        let hi = (b.wrapping_mul(0x0001_0002_0004_0008) >> 48) & 0xF;
        (lo | (hi << 4)) as usize
    }
}

#[inline]
unsafe fn mask4_to_bits(mask: uint32x4_t) -> usize {
    // SAFETY: pure register ops; the constant slice is 4 lanes.
    unsafe {
        let bits = vld1q_u32([1u32, 2, 4, 8].as_ptr());
        vaddvq_u32(vandq_u32(mask, bits)) as usize
    }
}

impl SimdBackend for NeonBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Neon
    }

    fn filter_range_f32(&self, data: &[f32], min: f32, max: f32, sel: &mut SelVec) -> usize {
        debug_check_len(data.len());
        sel.clear();
        let n = data.len();
        let chunks = n / 8;
        let mut written = 0usize;
        // SAFETY: loads are within `data` (i*8+7 < chunks*8 <= n); each store writes 8 u16 at
        // `written`, and written + 8 <= n + 8 <= SEL_CAPACITY.
        unsafe {
            let store = sel.storage_mut();
            let vmin = vdupq_n_f32(min);
            let vmax = vdupq_n_f32(max);
            let p = data.as_ptr();
            for i in 0..chunks {
                let v0 = vld1q_f32(p.add(i * 8));
                let v1 = vld1q_f32(p.add(i * 8 + 4));
                let m0 = vandq_u32(vcgeq_f32(v0, vmin), vcleq_f32(v0, vmax));
                let m1 = vandq_u32(vcgeq_f32(v1, vmin), vcleq_f32(v1, vmax));
                // 8-bit mask: narrow both masks to 16-bit lanes, take bit 0 of each lane, pack.
                let bits = movemask8(m0, m1);
                let idx = vaddq_u16(vld1q_u16(LUT8[bits].as_ptr()), vdupq_n_u16((i * 8) as u16));
                vst1q_u16(store.as_mut_ptr().add(written), idx);
                written += bits.count_ones() as usize;
            }
        }
        sel.set_len(written);
        for (i, &x) in data.iter().enumerate().skip(chunks * 8) {
            if x >= min && x <= max {
                sel.push(i as u16);
            }
        }
        sel.len()
    }

    fn filter_range_i64(&self, data: &[i64], min: i64, max: i64, sel: &mut SelVec) -> usize {
        debug_check_len(data.len());
        sel.clear();
        let n = data.len();
        let chunks = n / 4;
        let mut written = 0usize;
        // SAFETY: loads within `data`; each store writes 4 u16 at `written` (slack covers it).
        unsafe {
            let store = sel.storage_mut();
            let vmin = vdupq_n_s64(min);
            let vmax = vdupq_n_s64(max);
            let p = data.as_ptr();
            for i in 0..chunks {
                let v0 = vld1q_s64(p.add(i * 4));
                let v1 = vld1q_s64(p.add(i * 4 + 2));
                let m0 = vandq_u64(vcgeq_s64(v0, vmin), vcleq_s64(v0, vmax));
                let m1 = vandq_u64(vcgeq_s64(v1, vmin), vcleq_s64(v1, vmax));
                // 4-bit mask from two 2-lane masks: narrow to 32-bit lanes and combine
                let m = vcombine_u32(vmovn_u64(m0), vmovn_u64(m1));
                let bits = mask4_to_bits(m);
                let idx = vadd_u16(vld1_u16(LUT4[bits].as_ptr()), vdup_n_u16((i * 4) as u16));
                vst1_u16(store.as_mut_ptr().add(written), idx);
                written += bits.count_ones() as usize;
            }
        }
        sel.set_len(written);
        for (i, &x) in data.iter().enumerate().skip(chunks * 4) {
            if x >= min && x <= max {
                sel.push(i as u16);
            }
        }
        sel.len()
    }

    fn gather_f32(&self, data: &[f32], sel: &SelVec, out: &mut [f32]) -> usize {
        // NEON has no gather; the scalar loop is the right tool.
        crate::scalar::ScalarBackend.gather_f32(data, sel, out)
    }

    fn gather_i64(&self, data: &[i64], sel: &SelVec, out: &mut [i64]) -> usize {
        crate::scalar::ScalarBackend.gather_i64(data, sel, out)
    }

    fn sum_f32(&self, data: &[f32]) -> f64 {
        debug_check_len(data.len());
        let n = data.len();
        let chunks = n / 8;
        let mut s;
        // SAFETY: loads within bounds. Four f64x2 accumulators (two per 4-lane vector) break the
        // add latency chain; widening to f64 keeps the sum accurate.
        unsafe {
            let z = vdupq_n_f64(0.0);
            let (mut a0, mut a1, mut a2, mut a3) = (z, z, z, z);
            let p = data.as_ptr();
            for i in 0..chunks {
                let v0 = vld1q_f32(p.add(i * 8));
                let v1 = vld1q_f32(p.add(i * 8 + 4));
                a0 = vaddq_f64(a0, vcvt_f64_f32(vget_low_f32(v0)));
                a1 = vaddq_f64(a1, vcvt_high_f64_f32(v0));
                a2 = vaddq_f64(a2, vcvt_f64_f32(vget_low_f32(v1)));
                a3 = vaddq_f64(a3, vcvt_high_f64_f32(v1));
            }
            s = vaddvq_f64(vaddq_f64(vaddq_f64(a0, a1), vaddq_f64(a2, a3)));
        }
        for &x in &data[chunks * 8..] {
            s += f64::from(x);
        }
        s
    }

    fn sum_i64(&self, data: &[i64]) -> i64 {
        debug_check_len(data.len());
        let n = data.len();
        let chunks = n / 8;
        let mut s;
        // SAFETY: loads within bounds. Four independent accumulators: with one, every add waits
        // for the previous one (2 cycles each on Neoverse V2), which measured 3.8x slower than
        // the compiler's own unrolled loop on Axion.
        unsafe {
            let zero = vdupq_n_s64(0);
            let (mut a0, mut a1, mut a2, mut a3) = (zero, zero, zero, zero);
            let p = data.as_ptr();
            for i in 0..chunks {
                a0 = vaddq_s64(a0, vld1q_s64(p.add(i * 8)));
                a1 = vaddq_s64(a1, vld1q_s64(p.add(i * 8 + 2)));
                a2 = vaddq_s64(a2, vld1q_s64(p.add(i * 8 + 4)));
                a3 = vaddq_s64(a3, vld1q_s64(p.add(i * 8 + 6)));
            }
            s = vaddvq_s64(vaddq_s64(vaddq_s64(a0, a1), vaddq_s64(a2, a3)));
        }
        for &x in &data[chunks * 8..] {
            s = s.wrapping_add(x);
        }
        s
    }

    fn min_max_f32(&self, data: &[f32]) -> (f32, f32) {
        debug_check_len(data.len());
        let n = data.len();
        let chunks = n / 16;
        let (mut mn, mut mx);
        // SAFETY: loads within bounds. Four independent accumulator pairs hide the 2–3 cycle
        // latency of vminnm/vmaxnm (IEEE minNum/maxNum: NaN-ignoring).
        unsafe {
            let nan = vdupq_n_f32(f32::NAN);
            let (mut a0, mut a1, mut a2, mut a3) = (nan, nan, nan, nan);
            let (mut b0, mut b1, mut b2, mut b3) = (nan, nan, nan, nan);
            let p = data.as_ptr();
            for i in 0..chunks {
                let v0 = vld1q_f32(p.add(i * 16));
                let v1 = vld1q_f32(p.add(i * 16 + 4));
                let v2 = vld1q_f32(p.add(i * 16 + 8));
                let v3 = vld1q_f32(p.add(i * 16 + 12));
                a0 = vminnmq_f32(a0, v0);
                a1 = vminnmq_f32(a1, v1);
                a2 = vminnmq_f32(a2, v2);
                a3 = vminnmq_f32(a3, v3);
                b0 = vmaxnmq_f32(b0, v0);
                b1 = vmaxnmq_f32(b1, v1);
                b2 = vmaxnmq_f32(b2, v2);
                b3 = vmaxnmq_f32(b3, v3);
            }
            mn = vminnmvq_f32(vminnmq_f32(vminnmq_f32(a0, a1), vminnmq_f32(a2, a3)));
            mx = vmaxnmvq_f32(vmaxnmq_f32(vmaxnmq_f32(b0, b1), vmaxnmq_f32(b2, b3)));
        }
        for &x in &data[chunks * 16..] {
            mn = mn.min(x);
            mx = mx.max(x);
        }
        (mn, mx)
    }

    fn min_max_i64(&self, data: &[i64]) -> (i64, i64) {
        // No 64-bit integer min/max in NEON; two lanes do not justify compare+select.
        crate::scalar::ScalarBackend.min_max_i64(data)
    }

    fn count_f32(&self, data: &[f32], op: CmpOp<f32>) -> usize {
        debug_check_len(data.len());
        let n = data.len();
        let blocks = n / 16;
        // SAFETY: loads within bounds. The `match` is hoisted out of the loop by writing one
        // loop per variant via the macro below. A matching lane of a mask is all ones (-1), so
        // subtracting the mask counts it; four accumulators, because with one every add waits
        // for the previous one (measured on Neoverse V2: 159 ns per 1,024 values, latency-bound).
        macro_rules! run {
            ($mask:expr) => {{
                let p = data.as_ptr();
                let zero = vdupq_n_u32(0);
                let (mut a0, mut a1, mut a2, mut a3) = (zero, zero, zero, zero);
                for i in 0..blocks {
                    let m0: uint32x4_t = $mask(vld1q_f32(p.add(i * 16)));
                    let m1: uint32x4_t = $mask(vld1q_f32(p.add(i * 16 + 4)));
                    let m2: uint32x4_t = $mask(vld1q_f32(p.add(i * 16 + 8)));
                    let m3: uint32x4_t = $mask(vld1q_f32(p.add(i * 16 + 12)));
                    a0 = vsubq_u32(a0, m0);
                    a1 = vsubq_u32(a1, m1);
                    a2 = vsubq_u32(a2, m2);
                    a3 = vsubq_u32(a3, m3);
                }
                vaddvq_u32(vaddq_u32(vaddq_u32(a0, a1), vaddq_u32(a2, a3))) as usize
            }};
        }
        // SAFETY: NEON is always available on aarch64; loads stay within `data`.
        let mut cnt = unsafe {
            match op {
                CmpOp::Lt(v) => {
                    let c = vdupq_n_f32(v);
                    run!(|x| vcltq_f32(x, c))
                }
                CmpOp::Le(v) => {
                    let c = vdupq_n_f32(v);
                    run!(|x| vcleq_f32(x, c))
                }
                CmpOp::Gt(v) => {
                    let c = vdupq_n_f32(v);
                    run!(|x| vcgtq_f32(x, c))
                }
                CmpOp::Ge(v) => {
                    let c = vdupq_n_f32(v);
                    run!(|x| vcgeq_f32(x, c))
                }
                CmpOp::Eq(v) => {
                    let c = vdupq_n_f32(v);
                    run!(|x| vceqq_f32(x, c))
                }
                CmpOp::Ne(v) => {
                    let c = vdupq_n_f32(v);
                    run!(|x| vmvnq_u32(vceqq_f32(x, c)))
                }
                CmpOp::Range(lo, hi) => {
                    let clo = vdupq_n_f32(lo);
                    let chi = vdupq_n_f32(hi);
                    run!(|x| vandq_u32(vcgeq_f32(x, clo), vcleq_f32(x, chi)))
                }
            }
        };
        cnt += crate::scalar::ScalarBackend.count_f32(&data[blocks * 16..], op);
        cnt
    }

    fn count_i64(&self, data: &[i64], op: CmpOp<i64>) -> usize {
        debug_check_len(data.len());
        let n = data.len();
        let blocks = n / 8;
        // as `count_f32`: subtract the masks into four accumulators (one accumulator measured
        // 324 ns per 1,024 values on Neoverse V2, three times the compiler's scalar loop)
        macro_rules! run {
            ($mask:expr) => {{
                let p = data.as_ptr();
                let zero = vdupq_n_u64(0);
                let (mut a0, mut a1, mut a2, mut a3) = (zero, zero, zero, zero);
                for i in 0..blocks {
                    let m0: uint64x2_t = $mask(vld1q_s64(p.add(i * 8)));
                    let m1: uint64x2_t = $mask(vld1q_s64(p.add(i * 8 + 2)));
                    let m2: uint64x2_t = $mask(vld1q_s64(p.add(i * 8 + 4)));
                    let m3: uint64x2_t = $mask(vld1q_s64(p.add(i * 8 + 6)));
                    a0 = vsubq_u64(a0, m0);
                    a1 = vsubq_u64(a1, m1);
                    a2 = vsubq_u64(a2, m2);
                    a3 = vsubq_u64(a3, m3);
                }
                vaddvq_u64(vaddq_u64(vaddq_u64(a0, a1), vaddq_u64(a2, a3))) as usize
            }};
        }
        // SAFETY: NEON is always available on aarch64; loads stay within `data`.
        let mut cnt = unsafe {
            match op {
                CmpOp::Lt(v) => {
                    let c = vdupq_n_s64(v);
                    run!(|x| vcltq_s64(x, c))
                }
                CmpOp::Le(v) => {
                    let c = vdupq_n_s64(v);
                    run!(|x| vcleq_s64(x, c))
                }
                CmpOp::Gt(v) => {
                    let c = vdupq_n_s64(v);
                    run!(|x| vcgtq_s64(x, c))
                }
                CmpOp::Ge(v) => {
                    let c = vdupq_n_s64(v);
                    run!(|x| vcgeq_s64(x, c))
                }
                CmpOp::Eq(v) => {
                    let c = vdupq_n_s64(v);
                    run!(|x| vceqq_s64(x, c))
                }
                CmpOp::Ne(v) => {
                    let c = vdupq_n_s64(v);
                    run!(|x| {
                        let eq = vceqq_s64(x, c);
                        veorq_u64(eq, vdupq_n_u64(u64::MAX))
                    })
                }
                CmpOp::Range(lo, hi) => {
                    let clo = vdupq_n_s64(lo);
                    let chi = vdupq_n_s64(hi);
                    run!(|x| vandq_u64(vcgeq_s64(x, clo), vcleq_s64(x, chi)))
                }
            }
        };
        cnt += crate::scalar::ScalarBackend.count_i64(&data[blocks * 8..], op);
        cnt
    }
}
