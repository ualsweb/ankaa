//! Hot columnar chunk store.
//!
//! Data model (PRD v4.1 §4.3):
//! - A [`Chunk`] holds at most [`CHUNK_ROWS`] = 65,536 rows = 64 × [`VECTOR_LEN`]-row vectors.
//! - Columns are typed ([`Column`]): `i64` nanosecond timestamps, `i64` fixed-point prices with a
//!   per-column decimal scale, `i64` sizes, `u32` dictionary ids, `u8` flags, `f64` features.
//! - Rows are appended in timestamp order per partition; the chunk **watermark** is the
//!   maximum accepted timestamp. Sealing makes the chunk immutable and freezes its
//!   [`ZoneMap`] (min/max per column) used for pruning.
//!
//! No `unsafe` yet; the arena allocator and tiering (P1-06) will introduce audited unsafe.
#![forbid(unsafe_code)]

pub mod chunk;
pub mod column;
pub mod schema;
pub mod table;
pub mod tier;

pub use ankaa_simd::VECTOR_LEN;
pub use chunk::{BlockStats, Chunk, ChunkId, ChunkState, ZoneMap};
pub use column::{Column, ColumnStats, Value};
pub use schema::{ColumnType, Field, Schema, SchemaError};
pub use table::{Partition, PartitionStats, Table, TableSnapshot, TableWriter};
pub use tier::{
    ChunkMeta, ChunkRef, DecodeError as TierDecodeError, EncodedChunk, Evolved, NullableColumn,
    Tier, TierStats, clamp_rows, schema_evolves,
};

/// Maximum rows per chunk: 64 `FastLanes` vectors.
pub const CHUNK_ROWS: usize = 64 * VECTOR_LEN;

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
