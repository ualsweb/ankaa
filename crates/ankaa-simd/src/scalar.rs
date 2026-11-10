//! Reference scalar backend. Every other backend is tested against this one.

use crate::{BackendKind, CmpOp, SelVec, SimdBackend, debug_check_len};

/// Scalar reference implementation.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScalarBackend;

#[inline]
#[allow(clippy::float_cmp)]
fn cmp_f32(x: f32, op: CmpOp<f32>) -> bool {
    match op {
        CmpOp::Lt(v) => x < v,
        CmpOp::Le(v) => x <= v,
        CmpOp::Gt(v) => x > v,
        CmpOp::Ge(v) => x >= v,
        CmpOp::Eq(v) => x == v,
        CmpOp::Ne(v) => x != v,
        CmpOp::Range(lo, hi) => x >= lo && x <= hi,
    }
}

#[inline]
fn cmp_i64(x: i64, op: CmpOp<i64>) -> bool {
    match op {
        CmpOp::Lt(v) => x < v,
        CmpOp::Le(v) => x <= v,
        CmpOp::Gt(v) => x > v,
        CmpOp::Ge(v) => x >= v,
        CmpOp::Eq(v) => x == v,
        CmpOp::Ne(v) => x != v,
        CmpOp::Range(lo, hi) => x >= lo && x <= hi,
    }
}

impl SimdBackend for ScalarBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::Scalar
    }

    fn filter_range_f32(&self, data: &[f32], min: f32, max: f32, sel: &mut SelVec) -> usize {
        debug_check_len(data.len());
        sel.clear();
        for (i, &x) in data.iter().enumerate() {
            // Branch-free in spirit: the predicate is a bool; the push is unconditional into
            // a scratch slot. Kept simple here because this is the reference, not the hot path.
            if x >= min && x <= max {
                sel.push(i as u16);
            }
        }
        sel.len()
    }

    fn filter_range_i64(&self, data: &[i64], min: i64, max: i64, sel: &mut SelVec) -> usize {
        debug_check_len(data.len());
        sel.clear();
        for (i, &x) in data.iter().enumerate() {
            if x >= min && x <= max {
                sel.push(i as u16);
            }
        }
        sel.len()
    }

    fn gather_f32(&self, data: &[f32], sel: &SelVec, out: &mut [f32]) -> usize {
        let idx = sel.as_slice();
        assert!(out.len() >= idx.len(), "gather: output too small");
        for (o, &i) in out.iter_mut().zip(idx) {
            *o = data[i as usize];
        }
        idx.len()
    }

    fn gather_i64(&self, data: &[i64], sel: &SelVec, out: &mut [i64]) -> usize {
        let idx = sel.as_slice();
        assert!(out.len() >= idx.len(), "gather: output too small");
        for (o, &i) in out.iter_mut().zip(idx) {
            *o = data[i as usize];
        }
        idx.len()
    }

    fn sum_f32(&self, data: &[f32]) -> f64 {
        debug_check_len(data.len());
        data.iter().map(|&x| f64::from(x)).sum()
    }

    fn sum_i64(&self, data: &[i64]) -> i64 {
        debug_check_len(data.len());
        data.iter().fold(0i64, |a, &x| a.wrapping_add(x))
    }

    fn min_max_f32(&self, data: &[f32]) -> (f32, f32) {
        debug_check_len(data.len());
        let mut mn = f32::NAN;
        let mut mx = f32::NAN;
        for &x in data {
            // f32::min/max ignore NaN operands (IEEE minNum/maxNum semantics).
            mn = mn.min(x);
            mx = mx.max(x);
        }
        (mn, mx)
    }

    fn min_max_i64(&self, data: &[i64]) -> (i64, i64) {
        debug_check_len(data.len());
        data.iter()
            .fold((i64::MAX, i64::MIN), |(mn, mx), &x| (mn.min(x), mx.max(x)))
    }

    fn count_f32(&self, data: &[f32], op: CmpOp<f32>) -> usize {
        debug_check_len(data.len());
        data.iter().filter(|&&x| cmp_f32(x, op)).count()
    }

    fn count_i64(&self, data: &[i64], op: CmpOp<i64>) -> usize {
        debug_check_len(data.len());
        data.iter().filter(|&&x| cmp_i64(x, op)).count()
    }
}
