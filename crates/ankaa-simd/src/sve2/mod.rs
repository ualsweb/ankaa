//! SVE2 backend: vector-length-agnostic kernels written as `global_asm!` (stable Rust) with
//! `extern "C"` wrappers. Only built with `--features sve2` on aarch64 Linux.
//!
//! Every kernel of the trait is implemented in SVE2 ([`PORTED`]); the NEON backend is kept as a
//! member so a kernel can be switched back to NEON with a one-line change if a measurement on
//! real hardware says so.

pub mod detect;
mod kernels;

use crate::neon::NeonBackend;
use crate::{BackendKind, CmpOp, SelVec, SimdBackend, debug_check_len};

/// Kernels implemented in SVE2 assembly (the rest delegate to NEON).
pub const PORTED: &[&str] = &[
    "filter_range_f32",
    "filter_range_i64",
    "gather_f32",
    "gather_i64",
    "sum_f32",
    "sum_i64",
    "min_max_f32",
    "min_max_i64",
    "count_f32",
    "count_i64",
];

/// SVE2 backend.
#[derive(Debug, Clone, Copy)]
pub struct Sve2Backend {
    /// SVE vector length in bytes, read with `rdvl` at construction.
    pub vl_bytes: usize,
    #[allow(dead_code)]
    neon: NeonBackend,
}

impl Default for Sve2Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl Sve2Backend {
    /// Construct; panics if SVE2 is unavailable (use [`detect::sve2_available`] first).
    #[must_use]
    pub fn new() -> Self {
        assert!(detect::sve2_available(), "SVE2 not available on this host");
        // SAFETY: `rdvl` is a base-SVE instruction; availability was just checked.
        let vl_bytes = unsafe { kernels::ankaa_sve_vl_bytes() };
        Self {
            vl_bytes,
            neon: NeonBackend,
        }
    }
}

impl SimdBackend for Sve2Backend {
    fn kind(&self) -> BackendKind {
        BackendKind::Sve2
    }

    fn filter_range_f32(&self, data: &[f32], min: f32, max: f32, sel: &mut SelVec) -> usize {
        debug_check_len(data.len());
        sel.clear();
        // SAFETY: `data` is a valid slice of at most VECTOR_LEN elements (checked above). The
        // kernel stores one whole register (≤ 64 lanes) at offset `written` per vector; since
        // `written + lanes <= data.len() + 64 <= SEL_CAPACITY`, every store stays inside the
        // selection storage (the slack beyond `len()` is never read).
        let n = unsafe {
            kernels::ankaa_sve2_filter_range_f32_sel(
                data.as_ptr(),
                data.len(),
                min,
                max,
                sel.storage_mut().as_mut_ptr(),
            )
        };
        sel.set_len(n);
        n
    }

    fn filter_range_i64(&self, data: &[i64], min: i64, max: i64, sel: &mut SelVec) -> usize {
        debug_check_len(data.len());
        sel.clear();
        // SAFETY: as for `filter_range_f32`; .d lanes ≤ 32 per store, within the slack.
        let n = unsafe {
            kernels::ankaa_sve2_filter_range_i64_sel(
                data.as_ptr(),
                data.len(),
                min,
                max,
                sel.storage_mut().as_mut_ptr(),
            )
        };
        sel.set_len(n);
        n
    }

    fn gather_f32(&self, data: &[f32], sel: &SelVec, out: &mut [f32]) -> usize {
        let idx = sel.as_slice();
        assert!(out.len() >= idx.len(), "gather: output too small");
        let max = idx.iter().copied().max().map_or(0, usize::from);
        assert!(
            idx.is_empty() || max < data.len(),
            "gather: index out of range"
        );
        // SAFETY: every index is < data.len() (checked above, in release too); `out` holds at
        // least `idx.len()` elements; predicated stores never exceed `idx.len()`.
        unsafe {
            kernels::ankaa_sve2_gather_f32(data.as_ptr(), idx.as_ptr(), idx.len(), out.as_mut_ptr())
        }
    }

    fn gather_i64(&self, data: &[i64], sel: &SelVec, out: &mut [i64]) -> usize {
        let idx = sel.as_slice();
        assert!(out.len() >= idx.len(), "gather: output too small");
        let max = idx.iter().copied().max().map_or(0, usize::from);
        assert!(
            idx.is_empty() || max < data.len(),
            "gather: index out of range"
        );
        // SAFETY: as above.
        unsafe {
            kernels::ankaa_sve2_gather_i64(data.as_ptr(), idx.as_ptr(), idx.len(), out.as_mut_ptr())
        }
    }

    fn sum_f32(&self, data: &[f32]) -> f64 {
        debug_check_len(data.len());
        // SAFETY: valid slice.
        unsafe { kernels::ankaa_sve2_sum_f32(data.as_ptr(), data.len()) }
    }

    fn sum_i64(&self, data: &[i64]) -> i64 {
        debug_check_len(data.len());
        // SAFETY: valid slice.
        unsafe { kernels::ankaa_sve2_sum_i64(data.as_ptr(), data.len()) }
    }

    fn min_max_f32(&self, data: &[f32]) -> (f32, f32) {
        debug_check_len(data.len());
        let mut out = [f32::NAN; 2];
        // SAFETY: valid slice; `out` has two elements as the kernel expects.
        unsafe { kernels::ankaa_sve2_min_max_f32(data.as_ptr(), data.len(), out.as_mut_ptr()) };
        (out[0], out[1])
    }

    fn min_max_i64(&self, data: &[i64]) -> (i64, i64) {
        debug_check_len(data.len());
        let mut out = [0i64; 2];
        // SAFETY: as above.
        unsafe { kernels::ankaa_sve2_min_max_i64(data.as_ptr(), data.len(), out.as_mut_ptr()) };
        (out[0], out[1])
    }

    fn count_f32(&self, data: &[f32], op: CmpOp<f32>) -> usize {
        debug_check_len(data.len());
        let (p, n) = (data.as_ptr(), data.len());
        // SAFETY: valid slice; each kernel reads at most `n` elements.
        unsafe {
            match op {
                CmpOp::Lt(v) => kernels::ankaa_sve2_count_f32_lt(p, n, v, 0.0),
                CmpOp::Le(v) => kernels::ankaa_sve2_count_f32_le(p, n, v, 0.0),
                CmpOp::Gt(v) => kernels::ankaa_sve2_count_f32_gt(p, n, v, 0.0),
                CmpOp::Ge(v) => kernels::ankaa_sve2_count_f32_ge(p, n, v, 0.0),
                CmpOp::Eq(v) => kernels::ankaa_sve2_count_f32_eq(p, n, v, 0.0),
                CmpOp::Ne(v) => kernels::ankaa_sve2_count_f32_ne(p, n, v, 0.0),
                CmpOp::Range(lo, hi) => kernels::ankaa_sve2_count_f32_range(p, n, lo, hi),
            }
        }
    }

    fn count_i64(&self, data: &[i64], op: CmpOp<i64>) -> usize {
        debug_check_len(data.len());
        let (p, n) = (data.as_ptr(), data.len());
        // SAFETY: as above.
        unsafe {
            match op {
                CmpOp::Lt(v) => kernels::ankaa_sve2_count_i64_lt(p, n, v, 0),
                CmpOp::Le(v) => kernels::ankaa_sve2_count_i64_le(p, n, v, 0),
                CmpOp::Gt(v) => kernels::ankaa_sve2_count_i64_gt(p, n, v, 0),
                CmpOp::Ge(v) => kernels::ankaa_sve2_count_i64_ge(p, n, v, 0),
                CmpOp::Eq(v) => kernels::ankaa_sve2_count_i64_eq(p, n, v, 0),
                CmpOp::Ne(v) => kernels::ankaa_sve2_count_i64_ne(p, n, v, 0),
                CmpOp::Range(lo, hi) => kernels::ankaa_sve2_count_i64_range(p, n, lo, hi),
            }
        }
    }
}
