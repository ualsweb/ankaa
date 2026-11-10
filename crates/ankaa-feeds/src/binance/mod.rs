//! Binance spot market-data decoders.
//!
//! JSON (keyless) combined streams: `{"stream": "...", "data": {...}}` with `trade` and
//! `depthUpdate` payloads; SBE binary streams (API key required) via [`sbe`]. Both share one
//! symbol dictionary so ids are consistent.

pub mod json;
pub mod sbe;

pub use json::{BinanceJsonDecoder, DecoderStats};
pub use sbe::{BinanceSbeDecoder, SbeStats};
