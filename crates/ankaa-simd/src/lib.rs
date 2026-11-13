//! SIMD backends (scalar, NEON, SVE2 via `global_asm!`) with selection vectors and runtime dispatch.
//!
//! Design (PRD v4.1 §4.6):
//! - Kernels operate on **vectors of at most [`VECTOR_LEN`] = 1,024 rows** (the `FastLanes` unit).
//! - Filters produce a **selection vector** ([`SelVec`]) of row indices instead of compacting
//!   values; compaction happens only at materialization ([`SimdBackend::gather_f32`] etc.).
//! - Every backend is checked against [`scalar`] by differential tests, including NaN, ±inf,
//!   ±0 and denormals, with a relative tolerance of 1e-6 for float reductions.
//! - SVE2 kernels are **vector-length agnostic** (`whilelt`/`incw`), budgeted at `VL=128`.

pub mod scalar;
pub mod sel;

#[cfg(target_arch = "aarch64")]
pub mod neon;

#[cfg(all(feature = "sve2", target_arch = "aarch64", target_os = "linux"))]
pub mod sve2;

pub mod dispatch;

pub use dispatch::{BackendKind, Kernel, SimdError, backend, best, detect, init_backend};
pub use sel::{SelVec, VECTOR_LEN};

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Comparison predicate for counting kernels. An enum (monomorphized per variant in the
/// scalar backend) rather than a function pointer, so the comparison inlines.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CmpOp<T> {
    /// `x < v`
    Lt(T),
    /// `x <= v`
    Le(T),
    /// `x > v`
    Gt(T),
    /// `x >= v`
    Ge(T),
    /// `x == v`
    Eq(T),
    /// `x != v`
    Ne(T),
    /// `min <= x <= max`
    Range(T, T),
}

/// A SIMD backend. All slices are at most [`VECTOR_LEN`] long; longer inputs are a
/// programming error and panic in debug builds.
///
/// Float semantics: comparisons follow IEEE 754 (`NaN` never satisfies a range), reductions
/// accumulate in `f64`; `min`/`max` ignore `NaN` unless every element is `NaN`.
pub trait SimdBackend: Send + Sync + 'static {
    /// Which backend this is.
    fn kind(&self) -> BackendKind;

    /// Select indices `i` with `min <= data[i] <= max`. Returns the count written to `sel`.
    fn filter_range_f32(&self, data: &[f32], min: f32, max: f32, sel: &mut SelVec) -> usize;
    /// Select indices `i` with `min <= data[i] <= max`. Returns the count written to `sel`.
    fn filter_range_i64(&self, data: &[i64], min: i64, max: i64, sel: &mut SelVec) -> usize;

    /// Materialize `data[sel[k]]` into `out[k]` for `k < sel.len()`. Returns the count.
    fn gather_f32(&self, data: &[f32], sel: &SelVec, out: &mut [f32]) -> usize;
    /// Materialize `data[sel[k]]` into `out[k]` for `k < sel.len()`. Returns the count.
    fn gather_i64(&self, data: &[i64], sel: &SelVec, out: &mut [i64]) -> usize;

    /// Sum with `f64` accumulation.
    fn sum_f32(&self, data: &[f32]) -> f64;
    /// Wrapping sum.
    fn sum_i64(&self, data: &[i64]) -> i64;
    /// `(min, max)`; `(NaN, NaN)` for an all-NaN or empty slice.
    fn min_max_f32(&self, data: &[f32]) -> (f32, f32);
    /// `(min, max)`; `(i64::MAX, i64::MIN)` for an empty slice.
    fn min_max_i64(&self, data: &[i64]) -> (i64, i64);
    /// Count elements satisfying `op`.
    fn count_f32(&self, data: &[f32], op: CmpOp<f32>) -> usize;
    /// Count elements satisfying `op`.
    fn count_i64(&self, data: &[i64], op: CmpOp<i64>) -> usize;
}

/// Length precondition of every kernel. A real `assert!` (not `debug_assert!`): the NEON and
/// SVE2 filters write up to one register past the selected count into the `SelVec` slack, and
/// that bound only holds for `len <= VECTOR_LEN` (policy: safe APIs stay sound in release).
#[inline]
pub(crate) fn debug_check_len(len: usize) {
    assert!(
        len <= VECTOR_LEN,
        "slice longer than VECTOR_LEN ({len} > {VECTOR_LEN})"
    );
}
