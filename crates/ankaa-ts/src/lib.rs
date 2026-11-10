//! Time-series operators (PRD v4.1 §4.5): streaming ASOF join over pre-sorted inputs, time
//! buckets (OHLCV/VWAP), L2 order-book reconstruction, quantile sketches.
//!
//! All operators work on plain slices of `i64`/`u32` so they can run on hot chunk columns
//! without conversion, and each one has a naive reference implementation in its tests.
#![forbid(unsafe_code)]

pub mod asof;
pub mod book;
pub mod bucket;
pub mod incremental;
pub mod sketch;
pub mod sliding;
pub mod sync;

pub use asof::{AsofDirection, asof_join, asof_join_keyed};
pub use book::{BookSide, L2Book, Level, TopOfBook};
pub use bucket::{Ohlcv, time_buckets};
pub use incremental::{
    IncrementalAsof, IncrementalOhlcv, IncrementalPercentiles, IncrementalRolling, QuantileRow,
};
pub use sketch::{LatencyHistogram, UddSketch};
pub use sliding::{Sliding, join_bars, rolling_bars};
pub use sync::{BookSync, Decision};

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
