//! Storage tiers (PRD §4.7): a sealed chunk is **hot** (plain typed columns in DRAM), **warm**
//! (encoded in DRAM) or **cold** (an immutable encoded segment on disk, decoded on demand).
//!
//! The store itself knows nothing about encodings: warm and cold chunks are
//! [`EncodedChunk`] trait objects produced by an encoding crate (`ankaa-tier` with Vortex),
//! so the hot path never links a compressor. Partitions keep a list of [`ChunkRef`]s and
//! swap a hot chunk for its encoded form atomically under the partition lock.

use std::fmt::Debug;
use std::sync::Arc;

use crate::chunk::{Chunk, ChunkId};
use crate::column::Column;
use crate::schema::Schema;

/// Which tier a chunk lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Tier {
    /// Uncompressed columns in memory.
    Hot,
    /// Encoded columns in memory.
    Warm,
    /// Encoded segment on disk.
    Cold,
}

/// Immutable facts about a sealed chunk, carried into warm/cold forms and persisted with
/// segments so a restart can rebuild partitions without decoding anything.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkMeta {
    /// Table name.
    pub table: String,
    /// Partition key (symbol id).
    pub partition: u32,
    /// Chunk id within the partition.
    pub id: ChunkId,
    /// Column schema.
    pub schema: Schema,
    /// Row count.
    pub num_rows: usize,
    /// Minimum timestamp.
    pub ts_min: i64,
    /// Maximum timestamp (= watermark of the chunk).
    pub ts_max: i64,
    /// WAL LSN range `(first, last)` of the frames that produced the rows (`(0, 0)` unknown).
    pub lsn_range: (u64, u64),
    /// Rows of the last WAL record held by this chunk (the record may continue in the next chunk).
    #[serde(default)]
    pub last_lsn_rows: u32,
    /// `(column, min, max)` of every dictionary-id column, for pruning on key predicates
    /// without decoding. Empty in segments written before this field existed (no pruning).
    #[serde(default)]
    pub u32_ranges: Vec<(u32, u32, u32)>,
    /// `(column, min, max, wrapping sum)` of every `i64` column: `count`, `min`, `max` and
    /// `sum` over a whole encoded chunk are answered from here, without decoding it. Empty in
    /// segments written before this field existed (those chunks are decoded).
    #[serde(default)]
    pub i64_stats: Vec<(u32, i64, i64, i64)>,
    /// First and last value of the last column when the rows are in `(timestamp, last column)`
    /// order (see `ZoneMap::tie_order`). Absent in older segments (no order is promised).
    #[serde(default)]
    pub tie_order: Option<(i64, i64)>,
}

impl ChunkMeta {
    /// Describe a sealed, non-empty chunk of `table`/`partition`.
    #[must_use]
    pub fn of(table: &str, partition: u32, chunk: &Chunk) -> Self {
        let ts = chunk.timestamps();
        Self {
            table: table.to_owned(),
            partition,
            id: chunk.id(),
            schema: chunk.schema().clone(),
            num_rows: chunk.num_rows(),
            ts_min: ts.first().copied().unwrap_or(i64::MIN),
            ts_max: chunk.watermark(),
            lsn_range: chunk.lsn_range(),
            last_lsn_rows: chunk.last_lsn_rows(),
            u32_ranges: chunk
                .zone_map()
                .map(|z| {
                    z.per_column
                        .iter()
                        .enumerate()
                        .filter_map(|(i, st)| match st {
                            crate::ColumnStats::U32 { min, max } => {
                                Some((u32::try_from(i).ok()?, *min, *max))
                            }
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            i64_stats: chunk
                .zone_map()
                .map(|z| {
                    z.blocks
                        .iter()
                        .enumerate()
                        .filter_map(|(i, b)| {
                            let (lo, hi, sum) = b.as_ref()?.total();
                            Some((u32::try_from(i).ok()?, lo, hi, sum))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            tie_order: chunk.zone_map().and_then(|z| z.tie_order),
        }
    }
}

/// Error while decoding an encoded chunk.
#[derive(Debug, thiserror::Error)]
#[error("decode {tier:?} chunk {id:?}: {msg}")]
pub struct DecodeError {
    /// Tier of the failing chunk.
    pub tier: Tier,
    /// Chunk id.
    pub id: ChunkId,
    /// Encoder-specific message.
    pub msg: String,
}

/// A decoded column with the rows that hold a value (`None`: every row).
pub type NullableColumn = (Column, Option<Vec<bool>>);

/// An encoded (warm or cold) chunk. Implementations are immutable and thread-safe.
pub trait EncodedChunk: Send + Sync + Debug {
    /// Metadata.
    fn meta(&self) -> &ChunkMeta;
    /// Tier.
    fn tier(&self) -> Tier;
    /// Encoded size in bytes (memory for warm, file size for cold).
    fn encoded_bytes(&self) -> usize;
    /// Decode the given columns (by index) into plain columns, all rows.
    fn decode(&self, projection: &[usize]) -> Result<Vec<Column>, DecodeError>;
    /// [`Self::decode`] with, per column, the rows that hold a value (`None` = all of them;
    /// a `NULL` row holds a placeholder in the plain column).
    fn decode_nullable(&self, projection: &[usize]) -> Result<Vec<NullableColumn>, DecodeError> {
        Ok(self
            .decode(projection)?
            .into_iter()
            .map(|c| (c, None))
            .collect())
    }
}

/// A chunk in any tier.
#[derive(Debug, Clone)]
pub enum ChunkRef {
    /// Hot: plain columns.
    Hot(Arc<Chunk>),
    /// Warm or cold: encoded.
    Encoded(Arc<dyn EncodedChunk>),
}

impl ChunkRef {
    /// Chunk id.
    #[must_use]
    pub fn id(&self) -> ChunkId {
        match self {
            Self::Hot(c) => c.id(),
            Self::Encoded(e) => e.meta().id,
        }
    }

    /// Tier.
    #[must_use]
    pub fn tier(&self) -> Tier {
        match self {
            Self::Hot(_) => Tier::Hot,
            Self::Encoded(e) => e.tier(),
        }
    }

    /// Row count.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        match self {
            Self::Hot(c) => c.num_rows(),
            Self::Encoded(e) => e.meta().num_rows,
        }
    }

    /// True when no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Maximum timestamp (`i64::MIN` when empty).
    #[must_use]
    pub fn watermark(&self) -> i64 {
        match self {
            Self::Hot(c) => c.watermark(),
            Self::Encoded(e) => e.meta().ts_max,
        }
    }

    /// `[min, max]` timestamps, `None` when empty.
    #[must_use]
    pub fn time_range(&self) -> Option<(i64, i64)> {
        match self {
            Self::Hot(c) => {
                if c.is_empty() {
                    None
                } else {
                    Some((c.timestamps()[0], c.watermark()))
                }
            }
            Self::Encoded(e) => {
                let m = e.meta();
                (m.num_rows > 0).then_some((m.ts_min, m.ts_max))
            }
        }
    }

    /// True when rows with timestamps in `[lo, hi]` may exist.
    #[must_use]
    pub fn may_overlap(&self, lo: i64, hi: i64) -> bool {
        self.time_range()
            .is_some_and(|(min, max)| !(hi < min || lo > max))
    }

    /// True when rows whose dictionary-id column `col` lies in `[lo, hi]` may exist. Chunks
    /// without statistics for the column always answer `true`.
    #[must_use]
    pub fn may_overlap_u32(&self, col: usize, lo: u32, hi: u32) -> bool {
        match self {
            Self::Hot(c) => match c.zone_map().and_then(|z| z.per_column.get(col)) {
                Some(crate::ColumnStats::U32 { min, max }) => !(hi < *min || lo > *max),
                _ => true,
            },
            Self::Encoded(e) => e
                .meta()
                .u32_ranges
                .iter()
                .find(|(c, _, _)| *c as usize == col)
                .is_none_or(|&(_, min, max)| !(hi < min || lo > max)),
        }
    }

    /// First and last value of the last column when the chunk's rows are in
    /// `(timestamp, last column)` order.
    #[must_use]
    pub fn tie_order(&self) -> Option<(i64, i64)> {
        match self {
            Self::Hot(c) => c.zone_map().and_then(|z| z.tie_order),
            Self::Encoded(e) => e.meta().tie_order,
        }
    }

    /// `(min, max, wrapping sum)` of `i64` column `col` over the whole chunk, when the chunk
    /// has statistics for it (any tier; nothing is decoded).
    #[must_use]
    pub fn i64_stats(&self, col: usize) -> Option<(i64, i64, i64)> {
        match self {
            Self::Hot(c) => c
                .zone_map()
                .and_then(|z| z.block_stats(col))
                .map(crate::BlockStats::total),
            Self::Encoded(e) => e
                .meta()
                .i64_stats
                .iter()
                .find(|(c, ..)| *c as usize == col)
                .map(|&(_, lo, hi, sum)| (lo, hi, sum)),
        }
    }

    /// `(min, max)` of dictionary-id column `col`, when the chunk has statistics for it.
    #[must_use]
    pub fn u32_range(&self, col: usize) -> Option<(u32, u32)> {
        match self {
            Self::Hot(c) => match c.zone_map().and_then(|z| z.per_column.get(col)) {
                Some(crate::ColumnStats::U32 { min, max }) => Some((*min, *max)),
                _ => None,
            },
            Self::Encoded(e) => e
                .meta()
                .u32_ranges
                .iter()
                .find(|(c, _, _)| *c as usize == col)
                .map(|&(_, min, max)| (min, max)),
        }
    }

    /// WAL LSN range.
    #[must_use]
    pub fn lsn_range(&self) -> (u64, u64) {
        match self {
            Self::Hot(c) => c.lsn_range(),
            Self::Encoded(e) => e.meta().lsn_range,
        }
    }

    /// Rows of the last WAL record held by this chunk.
    #[must_use]
    pub fn last_lsn_rows(&self) -> u32 {
        match self {
            Self::Hot(c) => c.last_lsn_rows(),
            Self::Encoded(e) => e.meta().last_lsn_rows,
        }
    }

    /// Schema.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        match self {
            Self::Hot(c) => c.schema(),
            Self::Encoded(e) => &e.meta().schema,
        }
    }

    /// Bytes held in memory by this chunk (0 for cold).
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        match self {
            Self::Hot(c) => c
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| c.column(i).map_or(0, |col| col.len() * col.value_size()))
                .sum(),
            Self::Encoded(e) => {
                if e.tier() == Tier::Warm {
                    e.encoded_bytes()
                } else {
                    0
                }
            }
        }
    }

    /// The hot chunk, if this is one.
    #[must_use]
    pub fn as_hot(&self) -> Option<&Arc<Chunk>> {
        match self {
            Self::Hot(c) => Some(c),
            Self::Encoded(_) => None,
        }
    }
}

/// Per-tier counts and bytes for one partition or table.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TierStats {
    /// Hot sealed chunks.
    pub hot_chunks: usize,
    /// Warm chunks.
    pub warm_chunks: usize,
    /// Cold chunks.
    pub cold_chunks: usize,
    /// Bytes of hot sealed chunks.
    pub hot_bytes: usize,
    /// Encoded bytes of warm chunks.
    pub warm_bytes: usize,
    /// On-disk bytes of cold chunks.
    pub cold_bytes: usize,
}

impl TierStats {
    /// Add one chunk.
    pub fn add(&mut self, c: &ChunkRef) {
        match c {
            ChunkRef::Hot(_) => {
                self.hot_chunks += 1;
                self.hot_bytes += c.memory_bytes();
            }
            ChunkRef::Encoded(e) if e.tier() == Tier::Warm => {
                self.warm_chunks += 1;
                self.warm_bytes += e.encoded_bytes();
            }
            ChunkRef::Encoded(e) => {
                self.cold_chunks += 1;
                self.cold_bytes += e.encoded_bytes();
            }
        }
    }

    /// Sum.
    pub fn merge(&mut self, o: &Self) {
        self.hot_chunks += o.hot_chunks;
        self.warm_chunks += o.warm_chunks;
        self.cold_chunks += o.cold_chunks;
        self.hot_bytes += o.hot_bytes;
        self.warm_bytes += o.warm_bytes;
        self.cold_bytes += o.cold_bytes;
    }
}
