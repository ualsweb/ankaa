//! Clock sources.

/// Convert `ticks` of a `freq_hz` counter to nanoseconds without overflow or rounding drift.
#[must_use]
pub fn ticks_to_ns(ticks: u64, freq_hz: u64) -> u64 {
    if freq_hz == 0 {
        return 0;
    }
    let ns = (u128::from(ticks) * 1_000_000_000u128) / u128::from(freq_hz);
    u64::try_from(ns).unwrap_or(u64::MAX)
}

/// Arm generic timer: `(CNTVCT_EL0, CNTFRQ_EL0)`; `None` on other architectures.
#[must_use]
pub fn cntvct() -> Option<(u64, u64)> {
    #[cfg(target_arch = "aarch64")]
    {
        let (cnt, frq): (u64, u64);
        // SAFETY: reading the virtual counter and its frequency has no side effects and is
        // permitted at EL0 on Linux and macOS.
        unsafe {
            core::arch::asm!("isb", "mrs {0}, cntvct_el0", out(reg) cnt, options(nomem, nostack));
            core::arch::asm!("mrs {0}, cntfrq_el0", out(reg) frq, options(nomem, nostack));
        }
        Some((cnt, frq))
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        None
    }
}

/// Generic timer in nanoseconds, or the monotonic clock on other architectures.
#[must_use]
pub fn cpu_ns() -> u64 {
    match cntvct() {
        Some((c, f)) if f > 0 => ticks_to_ns(c, f),
        _ => monotonic_ns(),
    }
}

/// `CLOCK_MONOTONIC_RAW` (Linux) / `CLOCK_MONOTONIC` elsewhere, in nanoseconds.
#[must_use]
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    #[cfg(target_os = "linux")]
    let id = libc::CLOCK_MONOTONIC_RAW;
    #[cfg(not(target_os = "linux"))]
    let id = libc::CLOCK_MONOTONIC;
    // SAFETY: clock_gettime writes into the provided timespec.
    let rc = unsafe { libc::clock_gettime(id, &raw mut ts) };
    if rc != 0 {
        return 0;
    }
    u64::try_from(ts.tv_sec).unwrap_or(0) * 1_000_000_000 + u64::try_from(ts.tv_nsec).unwrap_or(0)
}

/// `CLOCK_REALTIME` in nanoseconds since the Unix epoch.
#[must_use]
pub fn realtime_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: as above.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &raw mut ts) };
    if rc != 0 {
        return 0;
    }
    u64::try_from(ts.tv_sec).unwrap_or(0) * 1_000_000_000 + u64::try_from(ts.tv_nsec).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_conversion_is_exact() {
        assert_eq!(ticks_to_ns(24_000_000, 24_000_000), 1_000_000_000);
        assert_eq!(ticks_to_ns(1, 1_000_000_000), 1);
        assert_eq!(ticks_to_ns(u64::MAX, 1), u64::MAX); // saturates
        assert_eq!(ticks_to_ns(5, 0), 0);
    }

    #[test]
    fn clocks_are_monotonic() {
        let a = cpu_ns();
        let b = cpu_ns();
        assert!(b >= a);
        let m1 = monotonic_ns();
        let m2 = monotonic_ns();
        assert!(m2 >= m1 && m1 > 0);
        assert!(realtime_ns() > 1_600_000_000_000_000_000);
        if let Some((_, f)) = cntvct() {
            assert!(f >= 1_000_000, "generic timer frequency {f}");
        }
    }
}
