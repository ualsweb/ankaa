//! Ingest pipeline: decoder trait, event batches, fixed-point parsing, symbol dictionary,
//! replay sources.
//!
//! Hot-path rules (PRD v4.1 §4.4): decoders are allocation-free after warm-up, never panic on
//! malformed input (they return [`DecodeError`]), and produce columnar [`EventBatch`]es of at
//! most 1,024 events that appenders push into chunks.
#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), warn(clippy::expect_used))]

pub mod batch;
pub mod fixed;
pub mod framing;
pub mod generic;
pub mod json_scan;
pub mod sparkplug;
pub mod symbols;

pub use batch::{BookDelta, BookSnapshot, EventBatch, Side, Trade};
pub use fixed::{ParseError, parse_fixed};
pub use framing::{FrameError, Framer, LengthPrefixed, Prefix};
pub use generic::{MapError, Mapped, RowMapper, TsUnit};
pub use symbols::{SymbolDict, SymbolId};

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Decoder failure. Decoders must never panic on untrusted input.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    /// The frame is not in the expected format (bad JSON, unknown schema, missing field).
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    /// A numeric field could not be parsed into fixed point.
    #[error("bad number in field {field}: {source}")]
    Number {
        /// Field name.
        field: &'static str,
        /// Parse error.
        #[source]
        source: ParseError,
    },
    /// Message type recognised but intentionally not handled (counted, not an error path).
    #[error("ignored message type")]
    Ignored,
}

/// A feed decoder: turns one raw frame into events appended to `out`.
pub trait Decoder {
    /// Decode `frame` received at `recv_ns` (host clock, ns since epoch) into `out`.
    ///
    /// Returns the number of events appended. `Err(DecodeError::Ignored)` is normal for
    /// heartbeats or unsubscribed message types and is counted by the caller, not logged.
    fn decode(
        &mut self,
        frame: &[u8],
        recv_ns: i64,
        out: &mut EventBatch,
    ) -> Result<usize, DecodeError>;

    /// Human-readable decoder name for metrics labels.
    fn name(&self) -> &'static str;
}
