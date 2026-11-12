//! Tables and partitions: the mutable, append-only owner of chunks.
//!
//! A [`Table`] has one [`Partition`] per partition key (e.g. symbol group). Each partition owns
//! exactly one open chunk plus a list of sealed, immutable chunks shared as `Arc`. Appends are
//! partition-local (one writer per partition, PRD v4.1 §4.4); readers take a
//! [`TableSnapshot`]: the sealed `Arc`s plus a copy of the open chunk's current rows. The copy
//! is bounded by `CHUNK_ROWS` and is the P1 shape; zero-copy exposure of arena-backed open
//! chunks is P1-06.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::chunk::{AppendError, Chunk, ChunkId};
use crate::column::Value;
use crate::schema::Schema;
use crate::tier::{ChunkRef, Tier, TierStats};

/// Per-partition counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStats {
    /// Rows accepted.
    pub rows: u64,
    /// Rows rejected as out of order: older than the watermark by more than the table's
    /// allowed lateness.
    pub dropped_out_of_order: u64,
    /// Rows accepted although older than the watermark (within the allowed lateness).
    pub late_rows: u64,
    /// Rows rejected for other reasons (type/arity).
    pub dropped_invalid: u64,
    /// Chunks sealed.
    pub sealed_chunks: u64,
    /// Rows dropped by retention (older than the table's retention window).
    pub expired_rows: u64,
}

/// One partition: open chunk + sealed history.
#[derive(Debug)]
pub struct Partition {
    key: u32,
    schema: Schema,
    open: Chunk,
    sealed: Vec<ChunkRef>,
    next_id: u64,
    stats: PartitionStats,
    /// WAL LSN of the record whose rows are being appended (0 = none).
    current_lsn: u64,
    /// Rows of `current_lsn` seen so far in this replay (for the straddling record).
    current_lsn_seen: u32,
    /// Rows from WAL records with `lsn < persisted_through_lsn` are already in persisted
    /// segments and are skipped during recovery replay; of record `persisted_through_lsn`
    /// itself the first `persisted_lsn_rows` rows are skipped (the rest went to the next chunk).
    persisted_through_lsn: u64,
    persisted_lsn_rows: u32,
    /// The query view of the open chunk (see [`OpenView`]).
    open_view: parking_lot::Mutex<OpenView>,
    /// How much older than the watermark a row may be and still be accepted, ns (0 = rows
    /// must arrive in time order).
    max_lateness: i64,
    /// When the open chunk received its first row (`None` while it is empty): how long its rows
    /// have been waiting, uncompressed, for the chunk to fill.
    open_since: Option<std::time::Instant>,
    /// Changes to the sealed chunks (sealed, moved between tiers, merged, restored, expired).
    /// With the rows accepted it tells whether queries need a new view ([`Self::version`]).
    moves: u64,
}

/// What queries see of a chunk that is still being written: sealed copies of consecutive row
/// ranges ("pieces"), each with a zone map of its own.
///
/// A snapshot copies only the rows appended since the previous one, into a new piece, and
/// merges that piece with its predecessors while they are not larger than it. Piece sizes
/// therefore shrink geometrically towards the newest: at most `log2(rows)` pieces, and a row is
/// copied `O(log rows)` times over the life of its chunk, however often snapshots are taken.
/// (Copying the whole open chunk at every snapshot, as before, cost time proportional to
/// everything that was open: 42 ms per snapshot for a thousand busy partitions.)
#[derive(Debug, Default)]
struct OpenView {
    /// The open chunk the pieces belong to.
    chunk: Option<ChunkId>,
    /// `(first row, piece)`, in row order, covering rows `0..rows`.
    pieces: Vec<(usize, Arc<Chunk>)>,
    rows: usize,
}

impl Partition {
    fn new(key: u32, schema: Schema, max_lateness: i64) -> Self {
        let open = Chunk::new(ChunkId(0), schema.clone());
        Self {
            key,
            schema,
            open,
            sealed: Vec::new(),
            next_id: 1,
            stats: PartitionStats::default(),
            current_lsn: 0,
            current_lsn_seen: 0,
            persisted_through_lsn: 0,
            persisted_lsn_rows: 0,
            open_view: parking_lot::Mutex::new(OpenView::default()),
            max_lateness,
            open_since: None,
            moves: 0,
        }
    }

    /// A number that grows with every change queries can see: a row accepted, a chunk sealed,
    /// moved to another tier, merged, restored or expired. Equal numbers: the same content.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.stats.rows.wrapping_add(self.moves)
    }

    /// Rows in the open chunk.
    #[must_use]
    pub fn open_rows(&self) -> usize {
        self.open.num_rows()
    }

    /// Bytes of the open chunk's columns (its query view holds a copy of about the same size).
    #[must_use]
    pub fn open_bytes(&self) -> usize {
        (0..self.schema.fields().len())
            .map(|i| self.open.column(i).map_or(0, |c| c.len() * c.value_size()))
            .sum()
    }

    /// When the open chunk received its first row (`None` while empty).
    #[must_use]
    pub fn open_since(&self) -> Option<std::time::Instant> {
        self.open_since
    }

    /// Set the WAL LSN for the rows appended next (0 = unknown). Rows from an LSN at or below
    /// the persisted-through LSN are skipped (already durable in a segment).
    pub fn set_current_lsn(&mut self, lsn: u64) {
        if lsn != self.current_lsn {
            self.current_lsn_seen = 0;
        }
        self.current_lsn = lsn;
    }

    /// Highest WAL LSN fully contained in persisted segments.
    #[must_use]
    pub fn persisted_through_lsn(&self) -> u64 {
        self.persisted_through_lsn
    }

    /// Mark LSNs up to `lsn` as persisted, with `rows` rows of record `lsn` in the segment.
    pub fn set_persisted_through_lsn(&mut self, lsn: u64, rows: u32) {
        if lsn > self.persisted_through_lsn {
            self.persisted_through_lsn = lsn;
            self.persisted_lsn_rows = rows;
        } else if lsn == self.persisted_through_lsn && lsn != 0 {
            self.persisted_lsn_rows = self.persisted_lsn_rows.saturating_add(rows);
        }
    }

    /// True when every row of the current LSN must be skipped (recovery replay of a record that is
    /// entirely inside persisted segments).
    #[must_use]
    pub fn skipping(&self) -> bool {
        self.current_lsn != 0 && self.current_lsn < self.persisted_through_lsn
    }

    /// Per-row skip for the record that straddles the persisted boundary: `true` for the rows
    /// that are already in a segment.
    fn skip_row_of_straddling_record(&mut self) -> bool {
        if self.current_lsn != 0
            && self.current_lsn == self.persisted_through_lsn
            && self.current_lsn_seen < self.persisted_lsn_rows
        {
            self.current_lsn_seen += 1;
            return true;
        }
        false
    }

    /// Sealed chunks across tiers, oldest first.
    #[must_use]
    pub fn sealed(&self) -> &[ChunkRef] {
        &self.sealed
    }

    /// Replace the sealed chunk with `id` by `replacement` (hot → warm → cold moves). Returns
    /// `false` when no sealed chunk has that id.
    pub fn replace_sealed(&mut self, id: ChunkId, replacement: ChunkRef) -> bool {
        match self.sealed.iter_mut().find(|c| c.id() == id) {
            Some(slot) => {
                *slot = replacement;
                self.moves += 1;
                true
            }
            None => false,
        }
    }

    /// Replace the sealed chunks `ids` (adjacent, in this order) by `merged`, which holds
    /// their rows: compaction. Returns `false`, changing nothing, when the partition no longer
    /// holds exactly that run (retention dropped part of it meanwhile).
    pub fn replace_sealed_run(&mut self, ids: &[ChunkId], merged: ChunkRef) -> bool {
        let Some(first) = ids.first() else {
            return false;
        };
        let Some(at) = self.sealed.iter().position(|c| c.id() == *first) else {
            return false;
        };
        let run = self.sealed.get(at..at + ids.len());
        if !run.is_some_and(|run| run.iter().zip(ids).all(|(c, id)| c.id() == *id)) {
            return false;
        }
        self.sealed.splice(at..at + ids.len(), [merged]);
        self.moves += 1;
        self.stats.sealed_chunks = self
            .stats
            .sealed_chunks
            .saturating_sub(ids.len() as u64 - 1);
        true
    }

    /// An id for a chunk made outside the partition (a merged chunk): never given before.
    pub fn fresh_chunk_id(&mut self) -> ChunkId {
        let id = ChunkId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Re-attach a sealed chunk from a persisted segment at start-up (before any append).
    /// Keeps chunks ordered by time, advances the id counter and the ordering watermark.
    pub fn restore_sealed(&mut self, chunk: ChunkRef) {
        debug_assert!(self.open.is_empty(), "restore before appending");
        let (_, last_lsn) = chunk.lsn_range();
        self.set_persisted_through_lsn(last_lsn, chunk.last_lsn_rows());
        self.next_id = self.next_id.max(chunk.id().0 + 1);
        self.stats.sealed_chunks += 1;
        self.stats.rows += chunk.num_rows() as u64;
        self.moves += 1;
        let wm = chunk.watermark();
        let pos = self.sealed.partition_point(|c| c.watermark() <= wm);
        self.sealed.insert(pos, chunk);
        if self.open.id().0 < self.next_id {
            self.open = Chunk::new(ChunkId(self.next_id), self.schema.clone());
            self.next_id += 1;
        }
        self.open.set_watermark(self.watermark_sealed());
    }

    fn watermark_sealed(&self) -> i64 {
        self.sealed
            .iter()
            .map(ChunkRef::watermark)
            .max()
            .unwrap_or(i64::MIN)
    }

    /// Per-tier counts and bytes of the sealed chunks.
    #[must_use]
    pub fn tier_stats(&self) -> TierStats {
        let mut t = TierStats::default();
        for c in &self.sealed {
            t.add(c);
        }
        t
    }

    /// Number of sealed chunks still hot.
    #[must_use]
    pub fn hot_sealed(&self) -> usize {
        self.sealed.iter().filter(|c| c.tier() == Tier::Hot).count()
    }

    /// First WAL LSN this partition still depends on: the minimum over the open chunk and every
    /// sealed chunk that is not yet in a segment (`u64::MAX` when everything is persisted). The
    /// WAL may be truncated before the minimum of this over all partitions.
    #[must_use]
    pub fn checkpoint_lsn(&self) -> u64 {
        let mut lsn = u64::MAX;
        let (first, _) = self.open.lsn_range();
        if !self.open.is_empty() && first != 0 {
            lsn = first;
        }
        for c in &self.sealed {
            let (f, l) = c.lsn_range();
            // `>=`: a hot chunk that ends at the persisted LSN holds the rest of a record that
            // straddles the segment boundary; its rows are not in a segment yet
            if c.tier() == Tier::Hot && l >= self.persisted_through_lsn && f != 0 {
                lsn = lsn.min(f);
            }
        }
        lsn
    }

    /// Partition key.
    #[must_use]
    pub fn key(&self) -> u32 {
        self.key
    }

    /// Append one row; seals and rolls the open chunk when it is full. Out-of-order rows are
    /// dropped and counted (feeds deliver in order; a drop here signals a decoder/feed problem).
    pub fn append_row(&mut self, row: &[Value]) -> Result<(), AppendError> {
        if self.skipping() || self.skip_row_of_straddling_record() {
            return Ok(());
        }
        if self.open.is_full() {
            self.seal_open();
        }
        // a row older than the watermark: inserted at its place when within the allowed
        // lateness, dropped and counted beyond it. The decision depends only on the rows
        // accepted before it, so a recovery replay makes the same one.
        let late = match row.first() {
            Some(Value::I64(ts)) => {
                let wm = self.watermark();
                if *ts >= wm {
                    false
                } else if self.max_lateness > 0 && *ts >= wm.saturating_sub(self.max_lateness) {
                    true
                } else {
                    self.stats.dropped_out_of_order += 1;
                    return Err(AppendError::OutOfOrder {
                        ts: *ts,
                        watermark: wm,
                    });
                }
            }
            _ => false,
        };
        let result = if late {
            self.open.insert_row(row).map(|at| {
                self.stats.late_rows += 1;
                self.invalidate_view_from(at);
            })
        } else {
            self.open.append_row(row)
        };
        match result {
            Ok(()) => {
                self.stats.rows += 1;
                self.open.note_lsn(self.current_lsn);
                // one clock read per chunk, not per row
                self.open_since.get_or_insert_with(std::time::Instant::now);
                Ok(())
            }
            Err(e @ AppendError::OutOfOrder { .. }) => {
                self.stats.dropped_out_of_order += 1;
                Err(e)
            }
            Err(e) => {
                self.stats.dropped_invalid += 1;
                Err(e)
            }
        }
    }

    /// A row was inserted at `at` in the open chunk: the pieces of its query view from there on
    /// no longer match the rows and are rebuilt by the next snapshot.
    fn invalidate_view_from(&mut self, at: usize) {
        let view = self.open_view.get_mut();
        if view.chunk != Some(self.open.id()) || at >= view.rows {
            return;
        }
        while let Some((start, piece)) = view.pieces.last() {
            if *start + piece.num_rows() <= at {
                break;
            }
            view.pieces.pop();
        }
        view.rows = view
            .pieces
            .last()
            .map_or(0, |(start, piece)| start + piece.num_rows());
    }

    /// Seal the open chunk now (e.g. on a time boundary or shutdown) and start a new one.
    pub fn seal_open(&mut self) {
        if self.open.is_empty() {
            return;
        }
        let mut done = std::mem::replace(
            &mut self.open,
            Chunk::new(ChunkId(self.next_id), self.schema.clone()),
        );
        self.next_id += 1;
        self.open_since = None;
        // (its statistics are computed by whoever first needs them: the encode thread or a query)
        done.mark_sealed();
        self.open.set_watermark(done.watermark());
        self.sealed.push(ChunkRef::Hot(Arc::new(done)));
        self.stats.sealed_chunks += 1;
        self.moves += 1;
    }

    /// Watermark: max accepted timestamp across the partition (`i64::MIN` when empty).
    #[must_use]
    pub fn watermark(&self) -> i64 {
        if self.open.is_empty() {
            // the empty open chunk carries the watermark of everything before it, also after
            // retention has dropped those chunks
            self.sealed
                .last()
                .map_or(i64::MIN, ChunkRef::watermark)
                .max(self.open.watermark())
        } else {
            self.open.watermark()
        }
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> PartitionStats {
        self.stats
    }

    /// Sealed chunks (shared) plus the view of the open chunk if it has rows: sealed copies of
    /// consecutive row ranges, in row order (see `OpenView`). Costs the rows appended since
    /// the last call, not the rows that are open.
    #[must_use]
    pub fn snapshot_chunks(&self) -> Vec<ChunkRef> {
        let mut v: Vec<ChunkRef> = self.sealed.clone();
        if !self.open.is_empty() {
            let mut view = self.open_view.lock();
            let rows = self.open.num_rows();
            if view.chunk != Some(self.open.id()) || view.rows > rows {
                // the open chunk was sealed and replaced since the last snapshot
                *view = OpenView {
                    chunk: Some(self.open.id()),
                    ..OpenView::default()
                };
            }
            if view.rows < rows {
                let mut start = view.rows;
                while let Some((first, last)) = view.pieces.last()
                    && last.num_rows() <= rows - start
                {
                    start = *first;
                    view.pieces.pop();
                }
                view.pieces
                    .push((start, Arc::new(self.open.copy_rows(start..rows))));
                view.rows = rows;
            }
            v.extend(
                view.pieces
                    .iter()
                    .map(|(_, c)| ChunkRef::Hot(Arc::clone(c))),
            );
        }
        v
    }

    /// Retention: remove the sealed chunks whose newest row is older than `before_ts` and return
    /// them (the caller deletes their segment files once no query holds them). The open chunk
    /// and the ordering watermark are untouched, so a reading that old is still rejected as
    /// late instead of being stored again.
    pub fn expire_before(&mut self, before_ts: i64) -> Vec<ChunkRef> {
        if self
            .sealed
            .first()
            .is_none_or(|c| c.watermark() >= before_ts)
        {
            return Vec::new();
        }
        let wm = self.watermark();
        let (expired, kept): (Vec<ChunkRef>, Vec<ChunkRef>) = std::mem::take(&mut self.sealed)
            .into_iter()
            .partition(|c| c.watermark() < before_ts);
        self.sealed = kept;
        if self.open.is_empty() {
            self.open.set_watermark(wm);
        }
        self.stats.expired_rows += expired.iter().map(|c| c.num_rows() as u64).sum::<u64>();
        self.moves += 1;
        expired
    }

    /// Drop sealed chunks whose watermark is below `before_ts` (after they were tiered/checkpointed).
    pub fn evict_sealed_before(&mut self, before_ts: i64) -> usize {
        let n = self.sealed.len();
        self.sealed.retain(|c| c.watermark() >= before_ts);
        n - self.sealed.len()
    }
}

/// A table: schema + partitions behind one lock. Writers lock per append batch; readers
/// lock briefly to snapshot.
#[derive(Debug)]
pub struct Table {
    name: String,
    schema: Schema,
    /// Every partition behind a lock of its own. The table's lock guards the list: held for
    /// writing by everything that needs the partitions to itself (one message's rows, a tiering
    /// step, growth of the list), where the inner locks cost nothing (`get_mut`); held for
    /// reading by readers and by writers that append to different partitions at the same time
    /// ([`Table::append_shared`]).
    partitions: RwLock<Vec<Slot>>,
    /// WAL LSN of the record being ingested (0 = none). Kept on the table and handed to a
    /// partition when it is touched, so setting it costs the same with one partition or a
    /// million, and a partition created by the record itself is stamped too.
    current_lsn: std::sync::atomic::AtomicU64,
    /// Allowed lateness of rows, ns (see [`Partition`]); given to every partition.
    max_lateness: i64,
}

/// Write guard over a table's partitions (see [`Table::writer`]). The guard is one
/// `parking_lot` lock word: taking it is a single compare-and-swap, releasing it a single
/// store-release, so a message of `n` rows costs two atomic operations instead of `6n`.
pub struct TableWriter<'a> {
    parts: parking_lot::RwLockWriteGuard<'a, Vec<Slot>>,
    lsn: u64,
    schema: &'a Schema,
    max_lateness: i64,
    /// Rows appended since the lock was last offered to readers.
    held: u32,
}

impl std::fmt::Debug for TableWriter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableWriter")
            .field("partitions", &self.parts.len())
            .field("lsn", &self.lsn)
            .finish_non_exhaustive()
    }
}

impl TableWriter<'_> {
    /// Rows appended before the guard lets waiting readers in (a fair unlock + relock, only
    /// when someone is waiting): bounds how long a query waits behind a large message.
    pub const ROWS_PER_HOLD: u32 = 1024;

    /// Watermark of partition `idx` (`None` when it does not exist yet, `i64::MIN` when empty).
    #[must_use]
    pub fn partition_watermark(&self, idx: usize) -> Option<i64> {
        self.parts.get(idx).map(|p| p.lock().watermark())
    }

    /// Append one row to the partition of `key`, creating it (and any below it) when new, and
    /// say whether it was stored: `false` when recovery skips it (its WAL record is already in
    /// a segment). Same semantics as [`Table::partition_for`] + [`Table::append_row_stored`].
    pub fn append_row_stored(&mut self, key: u32, row: &[Value]) -> Result<bool, AppendError> {
        self.held += 1;
        if self.held >= Self::ROWS_PER_HOLD {
            self.held = 0;
            parking_lot::RwLockWriteGuard::bump(&mut self.parts);
        }
        let idx = key as usize;
        if idx >= self.parts.len() {
            self.grow(idx);
        }
        let p = self.parts[idx].get_mut();
        p.set_current_lsn(self.lsn);
        if p.skipping() {
            return Ok(false);
        }
        let before = p.stats().rows;
        p.append_row(row)?;
        Ok(p.stats().rows != before)
    }

    #[cold]
    fn grow(&mut self, idx: usize) {
        while self.parts.len() <= idx {
            let k = u32::try_from(self.parts.len()).unwrap_or(u32::MAX);
            self.parts.push(Slot::new(Partition::new(
                k,
                self.schema.clone(),
                self.max_lateness,
            )));
        }
    }
}

/// Immutable view for queries.
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    /// Table name.
    pub name: String,
    /// Schema.
    pub schema: Schema,
    /// Chunks across all partitions (sealed shared + open copies), in partition order.
    pub chunks: Vec<ChunkRef>,
    /// Minimum watermark across non-empty partitions (`i64::MIN` when the table is empty):
    /// queries that need read-committed-across-partitions semantics read up to this.
    pub min_watermark: i64,
}

/// A partition behind its own lock (see `Table::partitions`).
type Slot = parking_lot::Mutex<Partition>;

/// Fewest partitions for which a snapshot is built on several threads (starting them costs
/// more than the views of a few partitions).
const PARALLEL_SNAPSHOT_PARTITIONS: usize = 256;

/// Threads a snapshot of `partitions` partitions is built on: one per 128 partitions, at most
/// 8 and at most the cores of the machine less one for whoever else is running.
fn snapshot_threads(partitions: usize) -> usize {
    if partitions < PARALLEL_SNAPSHOT_PARTITIONS {
        return 1;
    }
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    (partitions / 128)
        .min(8)
        .min(cores.saturating_sub(1))
        .max(1)
}

/// The chunks queries see of `parts`, in order, and the smallest watermark of the partitions
/// that hold rows (`i64::MAX` when none does).
fn snapshot_of(parts: &[Slot]) -> (Vec<ChunkRef>, i64) {
    let mut chunks = Vec::new();
    let mut min_wm = i64::MAX;
    for p in parts {
        let p = p.lock();
        let wm = p.watermark();
        if wm != i64::MIN {
            min_wm = min_wm.min(wm);
        }
        chunks.extend(p.snapshot_chunks());
    }
    (chunks, min_wm)
}

impl Table {
    /// New table with `partitions` partitions.
    #[must_use]
    pub fn new(name: impl Into<String>, schema: Schema, partitions: usize) -> Self {
        let parts = (0..partitions.max(1))
            .map(|k| Slot::new(Partition::new(k as u32, schema.clone(), 0)))
            .collect();
        Self {
            name: name.into(),
            schema,
            partitions: RwLock::new(parts),
            current_lsn: std::sync::atomic::AtomicU64::new(0),
            max_lateness: 0,
        }
    }

    /// The same table accepting rows up to `ns` older than its partition's watermark: they
    /// are inserted at their place in time instead of being dropped (sensors that buffer while
    /// offline, gateways that batch). Rows later than that are still dropped and counted.
    #[must_use]
    pub fn with_max_lateness(mut self, ns: i64) -> Self {
        self.max_lateness = ns.max(0);
        for p in self.partitions.get_mut() {
            p.get_mut().max_lateness = self.max_lateness;
        }
        self
    }

    /// Allowed lateness, ns.
    #[must_use]
    pub fn max_lateness(&self) -> i64 {
        self.max_lateness
    }

    /// Table name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Schema.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Newest timestamp stored in any partition (`i64::MIN` when empty): "now" for retention.
    #[must_use]
    pub fn newest_ts(&self) -> i64 {
        self.partitions
            .read()
            .iter()
            .map(|p| p.lock().watermark())
            .max()
            .unwrap_or(i64::MIN)
    }

    /// Retention: drop, in every partition, the sealed chunks older than `newest_ts() -
    /// keep_ns`. Returns them with their partition key.
    pub fn expire(&self, keep_ns: i64) -> Vec<(u32, ChunkRef)> {
        let newest = self.newest_ts();
        if newest == i64::MIN || keep_ns <= 0 {
            return Vec::new();
        }
        let cutoff = newest.saturating_sub(keep_ns);
        let mut out = Vec::new();
        self.for_each_partition(|p| {
            let key = p.key();
            out.extend(p.expire_before(cutoff).into_iter().map(|c| (key, c)));
        });
        out
    }

    /// Number of partitions.
    #[must_use]
    pub fn num_partitions(&self) -> usize {
        self.partitions.read().len()
    }

    /// Partition index for a key (symbol id). Partitions are created on demand so that every key
    /// owns its own partition and its own watermark: ordering is per symbol, never across symbols.
    pub fn partition_for(&self, key: u32) -> usize {
        let idx = key as usize;
        {
            let parts = self.partitions.read();
            if idx < parts.len() {
                return idx;
            }
        }
        let mut parts = self.partitions.write();
        while parts.len() <= idx {
            let k = u32::try_from(parts.len()).unwrap_or(u32::MAX);
            parts.push(Slot::new(Partition::new(
                k,
                self.schema.clone(),
                self.max_lateness,
            )));
        }
        idx
    }

    /// Watermark of partition `idx` (`None` when it does not exist yet, `i64::MIN` when empty).
    #[must_use]
    pub fn partition_watermark(&self, idx: usize) -> Option<i64> {
        self.partitions
            .read()
            .get(idx)
            .map(|p| p.lock().watermark())
    }

    /// Run `f` with exclusive access to one partition (the writer's hot path).
    pub fn with_partition<R>(&self, idx: usize, f: impl FnOnce(&mut Partition) -> R) -> R {
        let lsn = self.current_lsn.load(std::sync::atomic::Ordering::Relaxed);
        let mut parts = self.partitions.write();
        let p = parts[idx].get_mut();
        p.set_current_lsn(lsn);
        f(p)
    }

    /// Append one row to `partition` under a single lock and say whether it was stored: `false`
    /// when recovery skips it (its WAL record is already in a segment). The generic ingest path
    /// uses it so that one row costs one lock round trip.
    pub fn append_row_stored(&self, partition: usize, row: &[Value]) -> Result<bool, AppendError> {
        self.with_partition(partition, |p| {
            if p.skipping() {
                return Ok(false);
            }
            let before = p.stats().rows;
            p.append_row(row)?;
            Ok(p.stats().rows != before)
        })
    }

    /// Exclusive write access to every partition for the length of one ingest unit (a message
    /// and its rows): one lock round trip instead of one per check and per row. Readers wait
    /// for at most [`TableWriter::ROWS_PER_HOLD`] rows (see [`TableWriter::append_row_stored`]).
    #[must_use]
    pub fn writer(&self) -> TableWriter<'_> {
        TableWriter {
            parts: self.partitions.write(),
            lsn: self.current_lsn.load(std::sync::atomic::Ordering::Relaxed),
            schema: &self.schema,
            max_lateness: self.max_lateness,
            held: 0,
        }
    }

    /// Append rows that all belong to `partition` (caller groups by key).
    /// Returns `(accepted, first_error)`; processing continues past dropped rows.
    pub fn append_rows(&self, partition: usize, rows: &[&[Value]]) -> (usize, Option<AppendError>) {
        self.with_partition(partition, |p| {
            if p.skipping() {
                return (rows.len(), None);
            }
            let mut ok = 0;
            let mut first_err = None;
            for r in rows {
                match p.append_row(r) {
                    Ok(()) => ok += 1,
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            (ok, first_err)
        })
    }

    /// Append fixed-arity rows (no slice-of-slices indirection) that all belong to `partition`.
    pub fn append_row_arrays<const N: usize>(
        &self,
        partition: usize,
        rows: &[[Value; N]],
    ) -> (usize, Option<AppendError>) {
        self.with_partition(partition, |p| {
            if p.skipping() {
                return (rows.len(), None);
            }
            let mut ok = 0;
            let mut first_err = None;
            for r in rows {
                match p.append_row(r) {
                    Ok(()) => ok += 1,
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            (ok, first_err)
        })
    }

    /// Like [`Self::append_row_arrays`], calling `on_accept` for every row that was appended
    /// (not for rows dropped as out of order or invalid, nor for rows skipped during recovery).
    pub fn append_row_arrays_with<const N: usize>(
        &self,
        partition: usize,
        rows: &[[Value; N]],
        mut on_accept: impl FnMut(&[Value; N]),
    ) -> (usize, Option<AppendError>) {
        self.with_partition(partition, |p| {
            if p.skipping() {
                return (rows.len(), None);
            }
            let mut ok = 0;
            let mut first_err = None;
            for r in rows {
                let before = p.stats.rows;
                match p.append_row(r) {
                    Ok(()) => {
                        ok += 1;
                        if p.stats.rows != before {
                            on_accept(r);
                        }
                    }
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            (ok, first_err)
        })
    }

    /// Set the WAL LSN of the record being ingested (0 = none). Partitions pick it up when
    /// they are next touched through [`Self::with_partition`].
    pub fn set_current_lsn(&self, lsn: u64) {
        self.current_lsn
            .store(lsn, std::sync::atomic::Ordering::Relaxed);
    }

    /// Append one row to the partition of `key` while other threads append to other partitions
    /// of this table: the list is only read, the partition is locked for the append. `lsn` is
    /// the WAL record the row came from (0: none). A row older than the partition's watermark
    /// by more than `stale_after` ns (`None`: no such check, as during recovery) is refused
    /// with `Ok(None)` and nothing changes; otherwise `Ok(Some(stored))`, where `stored` is
    /// `false` when recovery skips the row because a segment already holds it.
    ///
    /// The partition must exist ([`Self::partition_for`], which needs the list to itself).
    /// Rows of one partition must come from one thread at a time, in their order: the lock
    /// makes concurrent calls safe, not ordered.
    pub fn append_shared(
        &self,
        key: u32,
        lsn: u64,
        row: &[Value],
        stale_after: Option<i64>,
    ) -> Result<Option<bool>, AppendError> {
        let parts = self.partitions.read();
        let Some(slot) = parts.get(key as usize) else {
            return Err(AppendError::Type(0));
        };
        let mut p = slot.lock();
        if let (Some(lateness), Some(Value::I64(ts))) = (stale_after, row.first())
            && *ts < p.watermark().saturating_sub(lateness)
        {
            return Ok(None);
        }
        p.set_current_lsn(lsn);
        if p.skipping() {
            return Ok(Some(false));
        }
        let before = p.stats().rows;
        p.append_row(row)?;
        Ok(Some(p.stats().rows != before))
    }

    /// Run `f` on every partition (tiering, checkpointing).
    pub fn for_each_partition(&self, mut f: impl FnMut(&mut Partition)) {
        for p in self.partitions.write().iter_mut() {
            f(p.get_mut());
        }
    }

    /// Per-tier totals across partitions.
    #[must_use]
    pub fn tier_stats(&self) -> TierStats {
        let mut t = TierStats::default();
        for p in self.partitions.read().iter() {
            t.merge(&p.lock().tier_stats());
        }
        t
    }

    /// Highest WAL LSN covered by persisted segments across partitions (0 = none).
    #[must_use]
    pub fn max_persisted_lsn(&self) -> u64 {
        self.partitions
            .read()
            .iter()
            .map(|p| p.lock().persisted_through_lsn())
            .max()
            .unwrap_or(0)
    }

    /// Minimum checkpoint LSN across partitions (`u64::MAX` when nothing depends on the WAL).
    #[must_use]
    pub fn checkpoint_lsn(&self) -> u64 {
        self.partitions
            .read()
            .iter()
            .map(|p| p.lock().checkpoint_lsn())
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Seal every partition's open chunk (shutdown / time boundary).
    pub fn seal_all(&self) {
        for p in self.partitions.write().iter_mut() {
            p.get_mut().seal_open();
        }
    }

    /// A number that changes whenever the content queries see changes (rows, chunks, tiers):
    /// while it stays the same, the last [`Self::snapshot`] is still exact. Costs a pass over
    /// the partitions, not over their chunks.
    #[must_use]
    pub fn version(&self) -> u64 {
        let parts = self.partitions.read();
        parts.iter().fold(parts.len() as u64, |v, p| {
            v.wrapping_add(p.lock().version())
        })
    }

    /// Consistent snapshot for queries.
    #[must_use]
    pub fn snapshot(&self) -> TableSnapshot {
        let parts = self.partitions.read();
        // The view of a partition that received rows copies those rows (see `OpenView`). With
        // many partitions that is most of what a snapshot costs, and the caller is the ingest
        // loop: the partitions are split among a few threads, each building the views of its
        // share, and the lists are put together in partition order.
        let threads = snapshot_threads(parts.len());
        let (chunks, min_wm) = if threads <= 1 {
            snapshot_of(&parts)
        } else {
            let per = parts.len().div_ceil(threads);
            let pieces: Vec<(Vec<ChunkRef>, i64)> = std::thread::scope(|scope| {
                let handles: Vec<_> = parts
                    .chunks(per)
                    .map(|share| scope.spawn(move || snapshot_of(share)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
                    .collect()
            });
            let mut chunks = Vec::with_capacity(pieces.iter().map(|(c, _)| c.len()).sum());
            let mut min_wm = i64::MAX;
            for (c, wm) in pieces {
                chunks.extend(c);
                min_wm = min_wm.min(wm);
            }
            (chunks, min_wm)
        };
        TableSnapshot {
            name: self.name.clone(),
            schema: self.schema.clone(),
            chunks,
            // (`i64::MAX`: no partition holds a row)
            min_watermark: if min_wm == i64::MAX { i64::MIN } else { min_wm },
        }
    }

    /// Aggregate stats across partitions.
    #[must_use]
    pub fn stats(&self) -> PartitionStats {
        self.partitions
            .read()
            .iter()
            .fold(PartitionStats::default(), |a, p| {
                let s = p.lock().stats();
                PartitionStats {
                    rows: a.rows + s.rows,
                    dropped_out_of_order: a.dropped_out_of_order + s.dropped_out_of_order,
                    late_rows: a.late_rows + s.late_rows,
                    dropped_invalid: a.dropped_invalid + s.dropped_invalid,
                    sealed_chunks: a.sealed_chunks + s.sealed_chunks,
                    expired_rows: a.expired_rows + s.expired_rows,
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CHUNK_ROWS, ColumnStats};

    fn row(ts: i64, sym: u32, seq: i64) -> Vec<Value> {
        vec![
            Value::I64(ts),
            Value::U32(sym),
            Value::I64(1),
            Value::I64(1),
            Value::U8(0),
            Value::I64(seq),
        ]
    }

    /// The write guard creates partitions on demand, stamps the current LSN, skips rows that
    /// recovery has already persisted, and gives the same table as the per-row calls, also
    /// across the points where it lets readers in.
    #[test]
    #[cfg_attr(miri, ignore = "thousands of appends")]
    fn writer_matches_per_row_appends() {
        let per_row = Table::new("trades", Schema::trades(), 1);
        let guarded = Table::new("trades", Schema::trades(), 1);
        let rows = i64::from(TableWriter::ROWS_PER_HOLD) * 3 + 5;
        per_row.set_current_lsn(9);
        guarded.set_current_lsn(9);
        let mut w = guarded.writer();
        assert_eq!(w.partition_watermark(4), None);
        for i in 0..rows {
            let key = (i % 5) as u32;
            let r = row(i, key, i);
            let part = per_row.partition_for(key);
            assert_eq!(
                per_row.append_row_stored(part, &r),
                w.append_row_stored(key, &r)
            );
        }
        assert_eq!(
            w.partition_watermark(4),
            (0..rows).filter(|i| i % 5 == 4).max()
        );
        drop(w);
        assert_eq!(per_row.num_partitions(), guarded.num_partitions());
        for k in 0..5 {
            assert_eq!(
                per_row.partition_watermark(k),
                guarded.partition_watermark(k)
            );
        }
        assert_eq!(per_row.stats().rows, rows as u64);
        assert_eq!(guarded.stats().rows, rows as u64);
        assert_eq!(per_row.max_persisted_lsn(), guarded.max_persisted_lsn());
    }

    /// Retention drops whole sealed chunks older than the window, per partition, measured from
    /// the newest row of the table; the ordering watermark survives, so an expired reading that
    /// turns up again is rejected rather than stored.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "bulk appends: no unsafe code beyond the small tests, hours under Miri"
    )]
    fn retention_drops_old_chunks_and_keeps_the_watermark() {
        let t = Table::new("trades", Schema::trades(), 2);
        // partition 0: 5 chunks of 1,000 rows, 1 ns apart; partition 1: one early chunk only
        for i in 0..5_000i64 {
            t.with_partition(0, |p| {
                p.append_row(&row(i, 0, i)).unwrap();
                if i % 1_000 == 999 {
                    p.seal_open();
                }
            });
        }
        for i in 0..500i64 {
            t.with_partition(1, |p| p.append_row(&row(i, 1, i)).unwrap());
        }
        t.with_partition(1, Partition::seal_open);
        assert_eq!(t.newest_ts(), 4_999);
        // keep 2,500 ns: cutoff 2,499 → chunks ending at 999 and 1,999 go, and partition 1's
        let gone = t.expire(2_500);
        let rows: usize = gone.iter().map(|(_, c)| c.num_rows()).sum();
        assert_eq!(rows, 2_500);
        assert_eq!(gone.iter().filter(|(k, _)| *k == 1).count(), 1);
        assert_eq!(t.stats().expired_rows, 2_500);
        let snap = t.snapshot();
        assert_eq!(
            snap.chunks.iter().map(ChunkRef::num_rows).sum::<usize>(),
            3_000
        );
        // nothing more to drop on a second call
        assert!(t.expire(2_500).is_empty());
        // partition 1 is empty now, but its watermark is not forgotten
        t.with_partition(1, |p| {
            assert_eq!(p.watermark(), 499);
            assert!(p.append_row(&row(10, 1, 0)).is_err());
            p.append_row(&row(600, 1, 1)).unwrap();
        });
    }

    /// The view of an open chunk is made of pieces: whatever the rhythm of appends and
    /// snapshots, the pieces are exactly the open rows in order, they stay few, each carries
    /// correct statistics, and the rows copied to build them stay within the logarithmic bound.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "bulk appends: no unsafe code beyond the small tests, hours under Miri"
    )]
    fn open_view_is_incremental_and_exact() {
        let t = Table::new("trades", Schema::trades(), 1);
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let (mut appended, mut copied) = (0i64, 0usize);
        let mut known: std::collections::HashSet<*const Chunk> = std::collections::HashSet::new();
        let mut snapshots = 0usize;
        // across a chunk roll, with bursts of very different sizes (1 row .. thousands)
        while appended < i64::try_from(CHUNK_ROWS).unwrap() + 40_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let burst = match x % 5 {
                0 => 1,
                1 => 7,
                2 => 300,
                3 => 2_500,
                _ => 0, // a snapshot with nothing new
            };
            for _ in 0..burst {
                let r = row(appended * 3, 1, appended);
                assert_eq!(t.append_rows(0, &[&r]), (1, None));
                appended += 1;
            }
            let snap = t.snapshot();
            snapshots += 1;
            // every row, once, in order
            let mut next = 0i64;
            for c in &snap.chunks {
                let ChunkRef::Hot(c) = c else { panic!("hot") };
                let seq = c.column(5).unwrap().as_i64().unwrap();
                let ts = c.timestamps();
                for (i, &s) in seq.iter().enumerate() {
                    assert_eq!(s, next);
                    assert_eq!(ts[i], next * 3);
                    next += 1;
                }
                let Some(ColumnStats::I64 { min, max }) = c.zone_map().map(|z| z.per_column[0])
                else {
                    panic!("statistics")
                };
                assert_eq!((min, max), (ts[0], ts[ts.len() - 1]));
                assert_eq!(c.watermark(), ts[ts.len() - 1]);
                if c.num_rows() < CHUNK_ROWS && known.insert(Arc::as_ptr(c)) {
                    copied += c.num_rows(); // a piece built by this snapshot
                }
            }
            assert_eq!(next, appended);
            let open_rows = usize::try_from(appended).unwrap() % CHUNK_ROWS;
            let pieces = snap
                .chunks
                .iter()
                .filter(|c| c.num_rows() < CHUNK_ROWS)
                .count();
            assert!(pieces <= 18, "{pieces} pieces for {open_rows} open rows");
            // a second snapshot without appends builds nothing
            let again = t.snapshot();
            for (a, b) in snap.chunks.iter().zip(&again.chunks) {
                let (ChunkRef::Hot(a), ChunkRef::Hot(b)) = (a, b) else {
                    panic!("hot")
                };
                assert!(Arc::ptr_eq(a, b));
            }
        }
        assert!(snapshots > 100);
        // each row is copied at most once per doubling of its piece
        let bound = usize::try_from(appended).unwrap() * 18;
        assert!(
            copied <= bound,
            "{copied} rows copied for {appended} appended"
        );
        // far fewer than copying the open chunk at every snapshot would
        assert!(
            copied < snapshots * CHUNK_ROWS / 8,
            "{copied} rows in {snapshots} snapshots"
        );
    }

    /// Every accepted row once, every chunk in time order with matching statistics.
    fn check(t: &Table, accepted: &[(i64, i64)]) {
        let snap = t.snapshot();
        let mut seen: Vec<(i64, i64)> = Vec::new();
        for c in &snap.chunks {
            let ChunkRef::Hot(c) = c else { panic!("hot") };
            let ts = c.timestamps();
            assert!(ts.windows(2).all(|w| w[0] <= w[1]), "chunk in time order");
            let Some(ColumnStats::I64 { min, max }) = c.zone_map().map(|z| z.per_column[0]) else {
                panic!("statistics")
            };
            assert_eq!((min, max), (ts[0], ts[ts.len() - 1]));
            let seq = c.column(5).unwrap().as_i64().unwrap();
            seen.extend(ts.iter().copied().zip(seq.iter().copied()));
        }
        let mut want = accepted.to_vec();
        want.sort_unstable();
        seen.sort_unstable();
        assert_eq!(seen, want);
    }

    /// Rows within the allowed lateness are stored at their place in time, across chunk
    /// rolls and between snapshots; rows beyond it are dropped and counted; every snapshot
    /// shows each accepted row once, every chunk in time order.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "bulk appends: no unsafe code beyond the small tests, hours under Miri"
    )]
    fn late_rows_are_inserted_in_time_order() {
        let lateness = 5_000;
        let t = Table::new("telemetry", Schema::trades(), 1).with_max_lateness(lateness);
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut accepted: Vec<(i64, i64)> = Vec::new(); // (ts, seq)
        let (mut newest, mut dropped) = (i64::MIN, 0u64);
        let n = i64::try_from(CHUNK_ROWS).unwrap() + 30_000;
        for i in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // mostly in order, often a few seconds late, sometimes far too late
            let ts = match x % 50 {
                0 => i * 10 - 1_000_000,
                1..=12 => i * 10 - i64::try_from(x % 6_000).unwrap(),
                _ => i * 10,
            };
            let (ok, err) = t.append_rows(0, &[&row(ts, 1, i)]);
            if ts >= newest.saturating_sub(lateness) {
                assert_eq!((ok, err), (1, None), "row {i} ts {ts} newest {newest}");
                accepted.push((ts, i));
                newest = newest.max(ts);
            } else {
                assert_eq!(ok, 0);
                assert!(matches!(err, Some(AppendError::OutOfOrder { .. })));
                dropped += 1;
            }
            if i % 997 == 0 {
                check(&t, &accepted);
            }
        }
        check(&t, &accepted);
        let st = t.stats();
        assert_eq!(st.dropped_out_of_order, dropped);
        assert!(st.late_rows > 1_000, "{}", st.late_rows);
        assert!(dropped > 100, "{dropped}");
        assert!(st.sealed_chunks >= 1);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "bulk appends: no unsafe code beyond the small tests, hours under Miri"
    )]
    fn rolls_chunks_and_snapshots() {
        let t = Table::new("trades", Schema::trades(), 2);
        let n = CHUNK_ROWS + 10;
        for i in 0..n {
            let i64v = i64::try_from(i).unwrap();
            let r = row(i64v, 1, i64v);
            let (ok, err) = t.append_rows(t.partition_for(1), &[&r]);
            assert_eq!((ok, err), (1, None));
        }
        // ordering is enforced across the chunk roll: a row older than the sealed chunk is dropped
        let stale = row(5, 1, 7);
        let (ok, err) = t.append_rows(t.partition_for(1), &[&stale]);
        assert_eq!(ok, 0);
        assert!(matches!(err, Some(AppendError::OutOfOrder { .. })));
        let s = t.stats();
        assert_eq!(s.rows as usize, n);
        assert_eq!(s.sealed_chunks, 1);
        let snap = t.snapshot();
        assert_eq!(snap.chunks.len(), 2);
        assert_eq!(snap.chunks[0].num_rows(), CHUNK_ROWS);
        assert_eq!(snap.chunks[1].num_rows(), 10);
        assert_eq!(snap.min_watermark, i64::try_from(n - 1).unwrap());
        // out of order is dropped and counted; valid rows after it still land
        let bad = row(5, 1, 99);
        let good = row(i64::try_from(n).unwrap(), 1, 100);
        let (ok, err) = t.append_rows(t.partition_for(1), &[&bad, &good]);
        assert_eq!(ok, 1);
        assert!(matches!(err, Some(AppendError::OutOfOrder { .. })));
        assert_eq!(t.stats().dropped_out_of_order, 2);
        t.seal_all();
        assert_eq!(t.stats().sealed_chunks, 2);
        assert_eq!(t.snapshot().chunks.len(), 2);
    }
}

#[cfg(test)]
mod multi_symbol {
    use super::*;

    /// The record that creates a partition stamps its rows like any other: they hold the
    /// checkpoint back and are recognised on replay.
    #[test]
    fn rows_of_a_new_partition_carry_the_current_lsn() {
        let t = Table::new("trades", Schema::trades(), 1);
        let row = |ts: i64, sym: u32| {
            vec![
                Value::I64(ts),
                Value::U32(sym),
                Value::I64(1),
                Value::I64(1),
                Value::U8(0),
                Value::I64(0),
            ]
        };
        t.set_current_lsn(7);
        // partition 3 does not exist yet when the LSN is set
        let p = t.partition_for(3);
        assert_eq!(t.append_rows(p, &[&row(10, 3)]), (1, None));
        assert_eq!(t.checkpoint_lsn(), 7);
        t.set_current_lsn(9);
        assert_eq!(t.append_rows(t.partition_for(0), &[&row(5, 0)]), (1, None));
        assert_eq!(t.checkpoint_lsn(), 7, "the oldest unpersisted row decides");
        // a small chunk does not reserve a full one
        let snap = t.snapshot();
        assert_eq!(snap.chunks.iter().map(ChunkRef::num_rows).sum::<usize>(), 2);
    }

    #[test]
    fn symbols_do_not_share_watermarks() {
        let t = Table::new("trades", Schema::trades(), 1);
        let r_a = vec![
            Value::I64(100),
            Value::U32(0),
            Value::I64(1),
            Value::I64(1),
            Value::U8(0),
            Value::I64(0),
        ];
        let r_b = vec![
            Value::I64(50),
            Value::U32(1),
            Value::I64(1),
            Value::I64(1),
            Value::U8(0),
            Value::I64(1),
        ];
        assert_eq!(t.append_rows(t.partition_for(0), &[&r_a]), (1, None));
        // symbol 1 arrives later with an earlier exchange timestamp: must be accepted
        assert_eq!(t.append_rows(t.partition_for(1), &[&r_b]), (1, None));
        assert_eq!(t.stats().dropped_out_of_order, 0);
        assert_eq!(t.num_partitions(), 2);
        assert_eq!(t.snapshot().min_watermark, 50);
    }
}
