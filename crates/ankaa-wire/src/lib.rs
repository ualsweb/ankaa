//! `ankaa-wire`: the hardware-timestamped ingest path (PRD §3.3, F-22, N-14). **Experimental.**
//!
//! What is here today, all testable without special hardware:
//! - [`clock`]: read the Arm generic timer (`CNTVCT_EL0` / `CNTFRQ_EL0`) and the portable
//!   monotonic clock; convert ticks to nanoseconds exactly (128-bit intermediate).
//! - [`calib`]: least-squares calibration between two clocks sampled in pairs (NIC PTP hardware
//!   clock ↔ CPU counter), with offset, drift and the residual that the measurement protocol
//!   publishes (`docs/measurement-protocol.md`).
//! - `udp` (Linux only): a UDP receiver that requests `SO_TIMESTAMPING` hardware receive
//!   timestamps and reports, per datagram, the hardware time when the NIC provides one and the
//!   software time otherwise, so a report can never confuse the two.
//!
//! What is not here: the `AF_XDP` zero-copy ring (`afxdp` feature is a placeholder returning
//! [`WireError::Unsupported`]) and any latency number. Numbers come only from named bare
//! metal with the published scripts; until then the status is "not measured".
#![cfg_attr(
    not(any(target_arch = "aarch64", target_os = "linux")),
    allow(dead_code)
)]

pub mod calib;
pub mod clock;
#[cfg(target_os = "linux")]
pub mod udp;

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors of the wire path.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// Feature compiled in but not available (`AF_XDP` placeholder, non-Linux hosts).
    #[error("unsupported on this build/host: {0}")]
    Unsupported(&'static str),
    /// Socket or ioctl failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Not enough or degenerate samples for a calibration.
    #[error("calibration: {0}")]
    Calibration(&'static str),
}

/// `AF_XDP` receive socket (placeholder until the Phase 3 hardware is available).
#[cfg(feature = "afxdp")]
pub mod afxdp {
    use super::WireError;

    /// Configuration of one XSK on one queue.
    #[derive(Debug, Clone)]
    pub struct XskConfig {
        /// Interface name.
        pub ifname: String,
        /// RX queue id.
        pub queue: u32,
        /// UMEM frames.
        pub frames: u32,
    }

    /// Open an `AF_XDP` socket. Always `Unsupported` in this build.
    pub fn open(_cfg: &XskConfig) -> Result<(), WireError> {
        Err(WireError::Unsupported(
            "`AF_XDP` receive is scheduled for Phase 3 (bpf_xdp_metadata_rx_timestamp, kernel >= 6.8)",
        ))
    }
}
