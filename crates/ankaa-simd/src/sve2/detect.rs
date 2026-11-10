//! SVE2 availability: HWCAP2 bit, std detection macro and a SIGILL-guarded self-test must agree.

use std::sync::OnceLock;

/// True when SVE2 kernels may run on this host.
pub fn sve2_available() -> bool {
    static AVAIL: OnceLock<bool> = OnceLock::new();
    *AVAIL.get_or_init(|| {
        hwcap_sve2() && std::arch::is_aarch64_feature_detected!("sve2") && self_test()
    })
}

/// SVE vector length in bytes (call only when [`sve2_available`] is true).
#[must_use]
pub fn vector_bytes() -> usize {
    // SAFETY: `rdvl` is a base-SVE instruction; callers have checked availability.
    unsafe { super::kernels::ankaa_sve_vl_bytes() }
}

/// `HWCAP2_SVE2` from the kernel UAPI (`arch/arm64/include/uapi/asm/hwcap.h`): bit 1 of
/// `AT_HWCAP2` (`HWCAP2_DCPODP` is bit 0). Defined here because `libc` does not export it.
const HWCAP2_SVE2: libc::c_ulong = 1 << 1;

fn hwcap_sve2() -> bool {
    // SAFETY: getauxval has no preconditions.
    let hwcap2 = unsafe { libc::getauxval(libc::AT_HWCAP2) };
    hwcap2 & HWCAP2_SVE2 != 0
}

/// Execute one SVE instruction (`rdvl`) and check the result is a sane vector length.
/// If the kernel reported SVE2 but execution faults, the process would die with SIGILL;
/// the two preceding checks make that practically impossible, and this call confirms the
/// register state is usable (e.g. not disabled by a seccomp/ptrace quirk).
fn self_test() -> bool {
    // SAFETY: guarded by the HWCAP/feature checks above.
    let vl = unsafe { super::kernels::ankaa_sve_vl_bytes() };
    (16..=256).contains(&vl) && vl.is_power_of_two()
}
