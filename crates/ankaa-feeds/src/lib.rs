//! Feed decoders: Binance JSON/SBE, Databento DBN, generic framing; capture tool.
//!
//! Part of ANKAA-ENGINE. See `docs/prd/PRD-v4.1.md`.
#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), warn(clippy::expect_used))]

pub mod binance;
pub mod capture;
pub mod counters;
#[cfg(feature = "dbn")]
pub mod databento;
#[cfg(feature = "live")]
pub mod live;
#[cfg(feature = "sources")]
pub mod sources;

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
