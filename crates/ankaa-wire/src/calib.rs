//! Clock calibration (RFC-017 in the v3.2 lineage): map a reference clock (NIC PHC) onto the
//! CPU clock with a linear model `ref ≈ a + b·cpu`, fitted by least squares over paired samples,
//! and publish the residual so every latency figure carries its own uncertainty.

use crate::WireError;

/// Fitted model and its quality. The fit is anchored at the first sample `(x0, y0)` and all
/// arithmetic happens on differences, so nanosecond precision survives epoch-sized values
/// (an `f64` cannot represent 1.7e18 ns to better than ~256 ns).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Calibration {
    /// Anchor CPU timestamp (first sample).
    pub x0: u64,
    /// Anchor reference timestamp (first sample).
    pub y0: u64,
    /// Offset `a` in nanoseconds relative to the anchors: `ref − y0 = a + b·(cpu − x0)`.
    pub offset_ns: f64,
    /// Slope `b` (dimensionless); `(b − 1)·1e9` is the drift in ppb.
    pub slope: f64,
    /// Root-mean-square residual in nanoseconds.
    pub rms_residual_ns: f64,
    /// Largest absolute residual in nanoseconds.
    pub max_residual_ns: f64,
    /// Samples used.
    pub samples: usize,
}

impl Calibration {
    /// Drift in parts per billion.
    #[must_use]
    pub fn drift_ppb(&self) -> f64 {
        (self.slope - 1.0) * 1e9
    }

    /// Map a CPU timestamp onto the reference clock (nanoseconds, fractional).
    #[must_use]
    pub fn to_ref_ns(&self, cpu_ns: u64) -> f64 {
        let dx = (i128::from(cpu_ns) - i128::from(self.x0)) as f64;
        self.y0 as f64 + self.offset_ns + self.slope * dx
    }

    /// Residual of one sample in nanoseconds (`ref − model(cpu)`), exact to the ns.
    #[must_use]
    pub fn residual_ns(&self, cpu_ns: u64, ref_ns: u64) -> f64 {
        let dx = (i128::from(cpu_ns) - i128::from(self.x0)) as f64;
        let dy = (i128::from(ref_ns) - i128::from(self.y0)) as f64;
        dy - (self.offset_ns + self.slope * dx)
    }
}

/// Fit `ref = a + b·cpu` over `(cpu_ns, ref_ns)` pairs. Needs ≥ 2 distinct CPU times.
pub fn fit(samples: &[(u64, u64)]) -> Result<Calibration, WireError> {
    if samples.len() < 2 {
        return Err(WireError::Calibration("need at least two samples"));
    }
    let (x0, y0) = samples[0];
    let n = samples.len() as f64;
    let xs: Vec<f64> = samples
        .iter()
        .map(|&(x, _)| (i128::from(x) - i128::from(x0)) as f64)
        .collect();
    let ys: Vec<f64> = samples
        .iter()
        .map(|&(_, y)| (i128::from(y) - i128::from(y0)) as f64)
        .collect();
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;
    let (mut sxx, mut sxy) = (0.0f64, 0.0f64);
    for (x, y) in xs.iter().zip(&ys) {
        let dx = x - mean_x;
        sxx += dx * dx;
        sxy += dx * (y - mean_y);
    }
    if sxx == 0.0 {
        return Err(WireError::Calibration("all CPU timestamps identical"));
    }
    let slope = sxy / sxx;
    let offset = mean_y - slope * mean_x;
    let (mut ss, mut mx) = (0.0f64, 0.0f64);
    for (x, y) in xs.iter().zip(&ys) {
        let r = y - (offset + slope * x);
        ss += r * r;
        mx = mx.max(r.abs());
    }
    Ok(Calibration {
        x0,
        y0,
        offset_ns: offset,
        slope,
        rms_residual_ns: (ss / n).sqrt(),
        max_residual_ns: mx,
        samples: samples.len(),
    })
}

/// Take one paired sample using the "sandwich" method: read the CPU clock before and after the
/// reference read and keep the midpoint; the half-width is the sample's own uncertainty.
/// Returns `(cpu_mid_ns, ref_ns, half_width_ns)`.
pub fn sandwich(mut read_ref: impl FnMut() -> u64) -> (u64, u64, u64) {
    let t0 = crate::clock::cpu_ns();
    let r = read_ref();
    let t1 = crate::clock::cpu_ns();
    (t0 + (t1 - t0) / 2, r, (t1 - t0) / 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn recovers_offset_and_drift(
            offset in -1.0e12f64..1.0e12,
            drift_ppb in -500.0f64..500.0,
            n in 8usize..200,
            noise in 0.0f64..50.0,
        ) {
            let slope = 1.0 + drift_ppb * 1e-9;
            let base: u64 = 1_700_000_000_000_000_000;
            let mut s = 0x1234_5678_9abc_def0u64;
            // generate in the integer domain so the truth itself is ns-exact at epoch scale
            let samples: Vec<(u64, u64)> = (0..n).map(|i| {
                s ^= s << 13; s ^= s >> 7; s ^= s << 17;
                let dx = i as u64 * 1_000_000; // every 1 ms
                let x = base + dx;
                let jitter = ((s % 2001) as f64 - 1000.0) / 1000.0 * noise;
                let dy = (dx as f64 * (slope - 1.0) + jitter).round() as i128;
                let y = i128::from(x) + offset.round() as i128 + dy;
                (x, y as u64)
            }).collect();
            let c = fit(&samples).unwrap();
            // offset is weakly determined over a short window; check the mapping instead
            // a least-squares fit is not an interpolant: a single residual can exceed the
            // jitter amplitude, but never twice it (plus the 1 ns rounding of the generator)
            for &(x, y) in &samples {
                let err = c.residual_ns(x, y).abs();
                prop_assert!(err <= 2.0 * noise + 1.0, "mapping error {err} > 2*noise {noise}");
            }
            prop_assert!(c.rms_residual_ns <= noise + 1.0);
            prop_assert!(c.max_residual_ns <= 2.0 * noise + 1.0);
            // drift recovered within what the window supports: noise/(span) in ppb. The truth is
            // rounded to whole nanoseconds (as real timestamps are), which is ±0.5 ns of noise
            // even when `noise` is 0: over a short window that alone allows tens of ppb
            let span_ns = (n as f64 - 1.0) * 1e6;
            let tol_ppb = 1e9 * 6.0 * (noise + 0.5) / span_ns + 1.0;
            prop_assert!((c.drift_ppb() - drift_ppb).abs() <= tol_ppb, "drift {} vs {drift_ppb} (tol {tol_ppb})", c.drift_ppb());
        }
    }

    #[test]
    fn rejects_degenerate_input() {
        assert!(fit(&[(1, 1)]).is_err());
        assert!(fit(&[(5, 1), (5, 2)]).is_err());
        let c = fit(&[(0, 10), (10, 20)]).unwrap();
        assert!((c.slope - 1.0).abs() < 1e-12 && c.offset_ns.abs() < 1e-9);
        assert!((c.to_ref_ns(5) - 15.0).abs() < 1e-9);
    }

    #[test]
    fn sandwich_reports_half_width() {
        let (mid, r, hw) = sandwich(crate::clock::monotonic_ns);
        assert!(r > 0 && mid > 0);
        assert!(hw < 1_000_000_000);
    }
}
