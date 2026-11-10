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
/// SVE2 is preferred only when its vectors are wider than NEON's 128 bits. Measured on
/// Neoverse V2 (Google Axion, VL 128): every SVE2 kernel was slower than its NEON
/// counterpart (`docs/benchmarks/`), so at VL 128 SVE2 offers nothing NEON does not, and
/// NEON is chosen. `ANKAA_SIMD=sve2` still forces it (see [`init_backend`]).
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

/// The backend [`detect`] names, with single kernels taken from another backend where that
/// one measured faster on real hardware: the choice is made per kernel, not per CPU.
///
/// Today one kernel moves: `min_max_i64` runs on SVE2 whenever the CPU has it, also at
/// 128-bit vectors. NEON has no 64-bit integer minimum or maximum (its backend falls back to
/// scalar code); SVE has `smin`/`smax` on 64-bit lanes. Neoverse V2, 1,024 values: 114 ns
/// against 181 ns.
struct PerKernel {
    primary: Box<dyn SimdBackend>,
    /// `min_max_i64` from here when set.
    min_max_i64: Option<Box<dyn SimdBackend>>,
}

impl SimdBackend for PerKernel {
    fn kind(&self) -> BackendKind {
        self.primary.kind()
    }
    fn filter_range_f32(&self, data: &[f32], min: f32, max: f32, sel: &mut crate::SelVec) -> usize {
        self.primary.filter_range_f32(data, min, max, sel)
    }
    fn filter_range_i64(&self, data: &[i64], min: i64, max: i64, sel: &mut crate::SelVec) -> usize {
        self.primary.filter_range_i64(data, min, max, sel)
    }
    fn gather_f32(&self, data: &[f32], sel: &crate::SelVec, out: &mut [f32]) -> usize {
        self.primary.gather_f32(data, sel, out)
    }
    fn gather_i64(&self, data: &[i64], sel: &crate::SelVec, out: &mut [i64]) -> usize {
        self.primary.gather_i64(data, sel, out)
    }
    fn sum_f32(&self, data: &[f32]) -> f64 {
        self.primary.sum_f32(data)
    }
    fn sum_i64(&self, data: &[i64]) -> i64 {
        self.primary.sum_i64(data)
    }
    fn min_max_f32(&self, data: &[f32]) -> (f32, f32) {
        self.primary.min_max_f32(data)
    }
    fn min_max_i64(&self, data: &[i64]) -> (i64, i64) {
        self.min_max_i64
            .as_ref()
            .unwrap_or(&self.primary)
            .min_max_i64(data)
    }
    fn count_f32(&self, data: &[f32], op: crate::CmpOp<f32>) -> usize {
        self.primary.count_f32(data, op)
    }
    fn count_i64(&self, data: &[i64], op: crate::CmpOp<i64>) -> usize {
        self.primary.count_i64(data, op)
    }
}

/// The fastest known kernels of this host: the backend [`detect`] names, plus single kernels
/// from another backend where measured faster (see `PerKernel`).
#[must_use]
pub fn best() -> Box<dyn SimdBackend> {
    let primary =
        make(detect()).unwrap_or_else(|_| Box::new(ScalarBackend) as Box<dyn SimdBackend>);
    let min_max_i64 = (primary.kind() != BackendKind::Sve2)
        .then(|| make(BackendKind::Sve2).ok())
        .flatten();
    Box::new(PerKernel {
        primary,
        min_max_i64,
    })
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
