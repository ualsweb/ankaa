//! Runtime backend selection.
//!
//! Detection (PRD v4.1 §4.6, v3.2 C-10): on aarch64 Linux a backend is chosen only when
//! `getauxval(AT_HWCAP2)`, `is_aarch64_feature_detected!` **and** a SIGILL-guarded self-test
//! agree. The chosen backend is process-global; a second `init_backend` with a different
//! `force` fails instead of being silently ignored (v3.2 m-06).

use std::sync::OnceLock;

use crate::SimdBackend;
use crate::scalar::ScalarBackend;

/// Available backends, in increasing order of capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BackendKind {
    /// Portable reference.
    Scalar,
    /// Arm NEON (128-bit, always present on aarch64).
    Neon,
    /// Arm SVE2 (vector-length agnostic; Linux only; `sve2` feature).
    Sve2,
}

impl BackendKind {
    /// Parse an override string (`ANKAA_SIMD=scalar|neon|sve2`).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "scalar" => Some(Self::Scalar),
            "neon" => Some(Self::Neon),
            "sve2" => Some(Self::Sve2),
            _ => None,
        }
    }
}

impl core::fmt::Display for BackendKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Scalar => "scalar",
            Self::Neon => "neon",
            Self::Sve2 => "sve2",
        })
    }
}

/// Errors from backend initialisation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimdError {
    /// `init_backend(Some(k))` was called after the backend was fixed to a different kind.
    AlreadyInitialized {
        /// Backend in use.
        current: BackendKind,
        /// Backend requested.
        requested: BackendKind,
    },
    /// The requested backend is not available on this CPU/OS/build.
    Unavailable(BackendKind),
}

impl core::fmt::Display for SimdError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AlreadyInitialized { current, requested } => write!(
                f,
                "SIMD backend already initialised as {current}, cannot switch to {requested}"
            ),
            Self::Unavailable(k) => write!(f, "SIMD backend {k} is not available on this host"),
        }
    }
}
impl std::error::Error for SimdError {}

/// Detect the best backend available on this host (no side effects).
///
/// SVE2 is the primary backend only when its vectors are wider than NEON's 128 bits. At VL 128
/// (Neoverse V2, Google Axion) both move the same data per clock, NEON is the primary and
/// [`best`] takes from SVE2 only the kernels of [`SVE2_AT_128`], where SVE2 measured faster
/// (`docs/benchmarks/`). `ANKAA_SIMD=sve2` still forces SVE2 for everything (see
/// [`init_backend`]).
#[must_use]
pub fn detect() -> BackendKind {
    #[cfg(all(feature = "sve2", target_arch = "aarch64", target_os = "linux"))]
    {
        if crate::sve2::detect::sve2_available() && crate::sve2::detect::vector_bytes() > 16 {
            return BackendKind::Sve2;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        return BackendKind::Neon;
    }
    #[allow(unreachable_code)]
    BackendKind::Scalar
}

/// True when `kind` can run on this host in this build.
#[must_use]
pub fn is_available(kind: BackendKind) -> bool {
    match kind {
        BackendKind::Scalar => true,
        BackendKind::Neon => cfg!(target_arch = "aarch64"),
        BackendKind::Sve2 => {
            #[cfg(all(feature = "sve2", target_arch = "aarch64", target_os = "linux"))]
            {
                crate::sve2::detect::sve2_available()
            }
            #[cfg(not(all(feature = "sve2", target_arch = "aarch64", target_os = "linux")))]
            {
                false
            }
        }
    }
}

/// Construct a backend of the given kind. Fails if unavailable.
pub fn make(kind: BackendKind) -> Result<Box<dyn SimdBackend>, SimdError> {
    if !is_available(kind) {
        return Err(SimdError::Unavailable(kind));
    }
    Ok(match kind {
        BackendKind::Scalar => Box::new(ScalarBackend),
        #[cfg(target_arch = "aarch64")]
        BackendKind::Neon => Box::new(crate::neon::NeonBackend),
        #[cfg(all(feature = "sve2", target_arch = "aarch64", target_os = "linux"))]
        BackendKind::Sve2 => Box::new(crate::sve2::Sve2Backend::new()),
        #[allow(unreachable_patterns)]
        other => return Err(SimdError::Unavailable(other)),
    })
}

/// A kernel of [`SimdBackend`], for choosing its backend separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Kernel {
    /// [`SimdBackend::filter_range_f32`]
    FilterRangeF32,
    /// [`SimdBackend::filter_range_i64`]
    FilterRangeI64,
    /// [`SimdBackend::gather_f32`]
    GatherF32,
    /// [`SimdBackend::gather_i64`]
    GatherI64,
    /// [`SimdBackend::sum_f32`]
    SumF32,
    /// [`SimdBackend::sum_i64`]
    SumI64,
    /// [`SimdBackend::min_max_f32`]
    MinMaxF32,
    /// [`SimdBackend::min_max_i64`]
    MinMaxI64,
    /// [`SimdBackend::count_f32`]
    CountF32,
    /// [`SimdBackend::count_i64`]
    CountI64,
}

/// Kernels that run on SVE2 whenever the CPU has it, also at 128-bit vectors where NEON is the
/// primary backend. At 128 bits both move the same data per clock, so SVE2 wins only where it
/// does something NEON cannot; each entry is backed by a measurement on Neoverse V2 (Google
/// Axion, 1,024 values, `docs/benchmarks/`):
///
/// - `FilterRangeF32`, `FilterRangeI64`: `compact` packs the selected indices in one
///   instruction where NEON builds a bit mask and looks the indices up in a table; the i64
///   range is also one unsigned compare. 215 ns against 240 ns (f32), 348 ns against 510 ns
///   (i64), half the rows selected.
/// - `GatherF32`, `GatherI64`: native gathers; NEON has none (its backend runs scalar code).
///   91 ns against 159 ns (f32), 211 ns against 238 ns (i64), half the rows selected.
/// - `MinMaxI64`: NEON has no 64-bit integer min/max (its backend runs scalar code); SVE has
///   `smin`/`smax` on 64-bit lanes. 114 ns against 181 ns.
///
/// The plain streaming loops stay on NEON: with the same data path they reach at best its
/// speed, and the counts pay for producing a predicate where NEON subtracts a mask (2026-11-13,
/// SVE2 against NEON: `sum_f32` 124/89 ns, `min_max_f32` 63/55, `count_f32` 91/57, `count_i64`
/// 216/107, `sum_i64` 90/85).
pub const SVE2_AT_128: &[Kernel] = &[
    Kernel::FilterRangeF32,
    Kernel::FilterRangeI64,
    Kernel::GatherF32,
    Kernel::GatherI64,
    Kernel::MinMaxI64,
];

/// The backend [`detect`] names, with the kernels of [`SVE2_AT_128`] taken from SVE2 when the
/// primary is not SVE2 already: the choice is made per kernel, not per CPU.
struct PerKernel {
    primary: Box<dyn SimdBackend>,
    /// The SVE2 backend, when available and not the primary.
    sve2: Option<Box<dyn SimdBackend>>,
}

impl PerKernel {
    fn pick(&self, k: Kernel) -> &dyn SimdBackend {
        match &self.sve2 {
            Some(b) if SVE2_AT_128.contains(&k) => b.as_ref(),
            _ => self.primary.as_ref(),
        }
    }
}

impl SimdBackend for PerKernel {
    fn kind(&self) -> BackendKind {
        self.primary.kind()
    }
    fn filter_range_f32(&self, data: &[f32], min: f32, max: f32, sel: &mut crate::SelVec) -> usize {
        self.pick(Kernel::FilterRangeF32)
            .filter_range_f32(data, min, max, sel)
    }
    fn filter_range_i64(&self, data: &[i64], min: i64, max: i64, sel: &mut crate::SelVec) -> usize {
        self.pick(Kernel::FilterRangeI64)
            .filter_range_i64(data, min, max, sel)
    }
    fn gather_f32(&self, data: &[f32], sel: &crate::SelVec, out: &mut [f32]) -> usize {
        self.pick(Kernel::GatherF32).gather_f32(data, sel, out)
    }
    fn gather_i64(&self, data: &[i64], sel: &crate::SelVec, out: &mut [i64]) -> usize {
        self.pick(Kernel::GatherI64).gather_i64(data, sel, out)
    }
    fn sum_f32(&self, data: &[f32]) -> f64 {
        self.pick(Kernel::SumF32).sum_f32(data)
    }
    fn sum_i64(&self, data: &[i64]) -> i64 {
        self.pick(Kernel::SumI64).sum_i64(data)
    }
    fn min_max_f32(&self, data: &[f32]) -> (f32, f32) {
        self.pick(Kernel::MinMaxF32).min_max_f32(data)
    }
    fn min_max_i64(&self, data: &[i64]) -> (i64, i64) {
        self.pick(Kernel::MinMaxI64).min_max_i64(data)
    }
    fn count_f32(&self, data: &[f32], op: crate::CmpOp<f32>) -> usize {
        self.pick(Kernel::CountF32).count_f32(data, op)
    }
    fn count_i64(&self, data: &[i64], op: crate::CmpOp<i64>) -> usize {
        self.pick(Kernel::CountI64).count_i64(data, op)
    }
}

/// The fastest known kernels of this host: the backend [`detect`] names, plus the kernels of
/// [`SVE2_AT_128`] from SVE2 (see `PerKernel`).
#[must_use]
pub fn best() -> Box<dyn SimdBackend> {
    let primary =
        make(detect()).unwrap_or_else(|_| Box::new(ScalarBackend) as Box<dyn SimdBackend>);
    let sve2 = (primary.kind() != BackendKind::Sve2 && !SVE2_AT_128.is_empty())
        .then(|| make(BackendKind::Sve2).ok())
        .flatten();
    Box::new(PerKernel { primary, sve2 })
}

static BACKEND: OnceLock<Box<dyn SimdBackend>> = OnceLock::new();

/// Initialise the process-global backend. `force = None` uses [`best`] (or, when the
/// `ANKAA_SIMD` environment variable is set, exactly the backend it names). Calling again with a different `force`
/// returns [`SimdError::AlreadyInitialized`].
pub fn init_backend(force: Option<BackendKind>) -> Result<&'static dyn SimdBackend, SimdError> {
    // a forced or overridden backend is that backend alone; left to itself the process gets
    // the per-kernel choice
    let overridden = force.or_else(|| {
        std::env::var("ANKAA_SIMD")
            .ok()
            .and_then(|s| BackendKind::parse(&s))
    });
    let requested = overridden.unwrap_or_else(detect);
    if let Some(existing) = BACKEND.get() {
        if existing.kind() != requested && force.is_some() {
            return Err(SimdError::AlreadyInitialized {
                current: existing.kind(),
                requested,
            });
        }
        return Ok(existing.as_ref());
    }
    let b = match overridden {
        Some(kind) => make(kind)?,
        None => best(),
    };
    // If another thread raced us, keep theirs and report a conflict only when forced.
    let stored = BACKEND.get_or_init(|| b);
    if stored.kind() != requested && force.is_some() {
        return Err(SimdError::AlreadyInitialized {
            current: stored.kind(),
            requested,
        });
    }
    Ok(stored.as_ref())
}

/// The process-global backend (auto-detected on first use).
pub fn backend() -> &'static dyn SimdBackend {
    match init_backend(None) {
        Ok(b) => b,
        // an `ANKAA_SIMD` override naming an unavailable backend falls back to detection
        Err(_) => match BACKEND.get() {
            Some(b) => b.as_ref(),
            None => init_backend(Some(detect())).unwrap_or_else(|_| {
                BACKEND
                    .get_or_init(|| Box::new(crate::scalar::ScalarBackend))
                    .as_ref()
            }),
        },
    }
}
