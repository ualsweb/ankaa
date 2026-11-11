//! Tiering and checkpoints (PRD F-05, F-13, F-17): run from the snapshot tick with a time budget.
//!
//! Every tick:
//! 1. **hot → warm**: sealed chunks beyond the hot window (`hot_sealed_chunks` newest per
//!    partition stay plain) are encoded with Vortex; when a checkpoint directory is configured the
//!    encoded chunk is also written as a segment file first, so a sealed chunk is durable on disk
//!    before its hot copy is released. The partition then points at the warm (in-memory encoded)
//!    chunk and records the WAL LSNs it covers.
//! 2. **warm → cold**: when warm bytes exceed the budget, the oldest persisted warm chunks are
//!    replaced by cold references (decoded from the segment on demand).
//! 3. **checkpoint**: once every sealed chunk is in a segment, the WAL only needs records newer
//!    than the oldest open chunk; segments below that LSN are deleted (rate-limited).
//!
//! Before these, **retention**: tables with a retention window drop the sealed chunks older
//! than it (measured from the table's newest row). Their segment files are deleted once no
//! query snapshot holds the chunk any more (a query that started before the drop finishes
//! on the data it saw).
//!
//! **Encoding off the ingest loop.** With encode threads ([`Tiering::with_encoders`]) a tick only
//! picks the chunks due (all of them, not a time budget's worth) and hands them over; Vortex
//! encoding and the segment write with its `fsync` run on those threads, and the next tick swaps
//! the finished chunks in under the partition lock. Ingest never waits for compression, and
//! compression keeps up with ingest on as many cores as it is given. A finished segment whose
//! chunk retention dropped meanwhile goes to the graveyard (deleted once free), never into the
//! table: expired data cannot come back at the next start.
//!
//! On start, [`Tiering::restore`] lists the segments and re-attaches them as cold chunks, then
//! the WAL replay skips records whose LSN is already covered by a segment.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ankaa_store::{ChunkId, ChunkRef, EncodedChunk as _, Table, Tier, TierStats};
use ankaa_tier::{ColdChunk, Encoder, WarmChunk};
use tracing::{info, warn};

use crate::metrics::Metrics;
use crate::tables::Tables;

/// Key of a sealed chunk across tables.
type Key = (String, u32, ChunkId);

/// Smallest open chunk sealed to stay within the memory budget (an age limit seals any size).
pub const MIN_SEAL_ROWS: usize = 1024;

/// How long tier totals are reused while no chunk changes tier (see `Tiering::tier_totals`).
const STATS_MAX_AGE: Duration = Duration::from_millis(500);

/// Tiering state.
pub struct Tiering {
    enc: Arc<Encoder>,
    checkpoint_dir: Option<PathBuf>,
    hot_sealed: usize,
    warm_budget_bytes: usize,
    budget: Duration,
    checkpoint_interval: Duration,
    last_truncate: Option<Instant>,
    /// Tier totals of all tables and the moves they were taken after: summing them walks every
    /// chunk, so a tick reuses them until a chunk moves or [`STATS_MAX_AGE`] passes.
    totals: Option<(Instant, u64, TierStats)>,
    /// `storage.segment_cache_mib` in bytes (0: no segment cache).
    segment_cache_bytes: usize,
    /// Compaction: on or off, when partitions were last searched, partitions with a merge out.
    compact: bool,
    last_compact: Option<Instant>,
    merging: std::collections::HashSet<(String, u32)>,
    /// Merges done and chunks they replaced.
    pub compactions: u64,
    pub compacted_chunks: u64,
    /// Segment written for a chunk (table, partition, id) → cold reference.
    cold: HashMap<Key, Arc<ColdChunk>>,
    /// Partitions `(table, key)` whose sealed chunks are persisted at once, whatever the hot
    /// window: they hold the oldest rows the WAL is still kept for (see [`Self::relieve_wal`]).
    forced: std::collections::HashSet<(String, u32)>,
    /// Retention window per table name, nanoseconds.
    retention: HashMap<String, i64>,
    /// Expired segments whose files are deleted once nothing else holds them.
    graveyard: Vec<Arc<ColdChunk>>,
    /// Segments were written since the last checkpoint was handed out: one is owed as soon as
    /// the interval allows, even if the tick that finds it due persists nothing itself.
    checkpoint_pending: bool,
    /// Counters.
    pub moves_to_warm: u64,
    /// Counters.
    pub moves_to_cold: u64,
    /// Counters.
    pub checkpoints: u64,
    /// Encode errors (chunk stays hot; retried next tick).
    pub errors: u64,
    /// Encode threads (`None`: encode in the tick, within its time budget).
    pool: Option<EncodePool>,
    /// Chunks handed to the encode threads and not swapped in yet.
    in_flight: std::collections::HashSet<Key>,
}

/// A sealed chunk to encode (and persist).
struct EncodeJob {
    table: String,
    partition: usize,
    key: u32,
    /// The hot chunk to encode; for a compaction an empty chunk that only carries the id the
    /// merged chunk will have.
    chunk: Arc<ankaa_store::Chunk>,
    /// Compaction: the chunks to merge into one.
    merge: Option<Merge>,
}

/// A run of small encoded chunks of one partition to be made one chunk.
struct Merge {
    /// The chunks, adjacent and in the partition's order.
    parts: Vec<ChunkRef>,
    /// What the merged chunk's metadata will list as replaced: the parts and what they replaced.
    replaces: Vec<u64>,
    /// WAL bookkeeping of the merged chunk: first LSN, last LSN, rows of the last record.
    lsn: (u64, u64, u32),
}

/// A sealed chunk below this many rows is small: compaction merges runs of them.
pub const SMALL_CHUNK_ROWS: usize = ankaa_store::CHUNK_ROWS / 4;
/// Fewest small chunks merged when the result is still small (a merge that makes a chunk of at
/// least [`SMALL_CHUNK_ROWS`] takes two).
const MIN_MERGE_PARTS: usize = 4;
/// Most chunks one merge takes, and most ids a merged chunk may list as replaced.
const MAX_MERGE_PARTS: usize = 64;
const MAX_REPLACED_IDS: usize = 4096;
/// How often partitions are searched for runs to merge.
const COMPACT_EVERY: Duration = Duration::from_secs(1);

/// Encode a chunk (and write its segment when `dir` is given); for a compaction, first build
/// the merged chunk from its parts. Runs on an encode thread, or in the tick without them.
fn run_job(
    enc: &Arc<Encoder>,
    dir: Option<&Path>,
    job: &EncodeJob,
) -> Result<(Arc<WarmChunk>, Option<Arc<ColdChunk>>), String> {
    let warm = match &job.merge {
        None => enc
            .encode_warm(&job.table, job.key, &job.chunk)
            .map_err(|e| e.to_string())?,
        Some(m) => {
            let schema = job.chunk.schema().clone();
            let all: Vec<usize> = (0..schema.len()).collect();
            let mut parts = Vec::with_capacity(m.parts.len());
            for part in &m.parts {
                let ChunkRef::Encoded(e) = part else {
                    return Err("compaction of a chunk that is not encoded".into());
                };
                let cols = e.decode_nullable(&all).map_err(|e| e.to_string())?;
                parts.push(cols.into_iter().unzip());
            }
            let merged = ankaa_store::Chunk::merged(job.chunk.id(), schema, &parts, m.lsn)?;
            enc.encode_warm_replacing(&job.table, job.key, &merged, m.replaces.clone())
                .map_err(|e| e.to_string())?
        }
    };
    let cold = match dir {
        Some(d) => Some(enc.write_cold(d, &warm).map_err(|e| e.to_string())?),
        None => None,
    };
    Ok((warm, cold))
}

/// A finished [`EncodeJob`]: the warm chunk and, with a checkpoint directory, its segment.
struct EncodeDone {
    job: EncodeJob,
    result: Result<(Arc<WarmChunk>, Option<Arc<ColdChunk>>), String>,
}

/// Threads that encode sealed chunks and write their segments.
struct EncodePool {
    jobs: std::sync::mpsc::Sender<EncodeJob>,
    done: std::sync::mpsc::Receiver<EncodeDone>,
    threads: Vec<std::thread::JoinHandle<()>>,
    /// Jobs handed out at most: enough to keep every thread busy, bounded memory.
    max_in_flight: usize,
}

impl EncodePool {
    fn start(n: usize, enc: &Arc<Encoder>, dir: Option<&Path>) -> std::io::Result<Self> {
        let (job_tx, job_rx) = std::sync::mpsc::channel::<EncodeJob>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<EncodeDone>();
        // jobs take milliseconds each: one shared queue costs nothing measurable
        let job_rx = Arc::new(parking_lot::Mutex::new(job_rx));
        let mut threads = Vec::with_capacity(n);
        for i in 0..n {
            let (job_rx, done_tx, enc, dir) = (
                job_rx.clone(),
                done_tx.clone(),
                enc.clone(),
                dir.map(Path::to_path_buf),
            );
            threads.push(
                std::thread::Builder::new()
                    .name(format!("ankaa-tier-{i}"))
                    .spawn(move || {
                        loop {
                            let next = job_rx.lock().recv();
                            let Ok(job) = next else { return };
                            let result = run_job(&enc, dir.as_deref(), &job);
                            if done_tx.send(EncodeDone { job, result }).is_err() {
                                return;
                            }
                        }
                    })?,
            );
        }
        Ok(Self {
            jobs: job_tx,
            done: done_rx,
            threads,
            max_in_flight: 2 * n,
        })
    }
}

impl Drop for EncodePool {
    fn drop(&mut self) {
        // close the queue; threads finish the job in hand and stop
        let (tx, _) = std::sync::mpsc::channel();
        drop(std::mem::replace(&mut self.jobs, tx));
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// What a restore found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RestoreStats {
    /// Segments attached.
    pub segments: usize,
    /// Rows they hold.
    pub rows: u64,
    /// Highest LSN covered by any segment.
    pub max_lsn: u64,
    /// Unreadable files skipped.
    pub bad_files: usize,
    /// Segments written before their table gained columns (read with the added ones `NULL`).
    pub evolved: usize,
    /// Segments deleted because a merged segment holds their rows (a crash came between
    /// writing the merged one and deleting them).
    pub replaced: usize,
}

impl Tiering {
    /// `hot_sealed`: sealed chunks per partition kept plain; `warm_budget_bytes`: encoded bytes in
    /// memory before demotion to cold; `budget`: wall time per tick; `checkpoint_dir`: segments.
    pub fn new(
        checkpoint_dir: Option<PathBuf>,
        hot_sealed: usize,
        warm_budget_bytes: usize,
        budget: Duration,
        checkpoint_interval: Duration,
    ) -> Self {
        Self {
            enc: Arc::new(Encoder::new()),
            checkpoint_dir,
            hot_sealed: hot_sealed.max(1),
            warm_budget_bytes,
            budget,
            checkpoint_interval,
            last_truncate: None,
            totals: None,
            segment_cache_bytes: 0,
            compact: true,
            last_compact: None,
            merging: std::collections::HashSet::new(),
            compactions: 0,
            compacted_chunks: 0,
            cold: HashMap::new(),
            forced: std::collections::HashSet::new(),
            retention: HashMap::new(),
            graveyard: Vec::new(),
            checkpoint_pending: false,
            moves_to_warm: 0,
            moves_to_cold: 0,
            checkpoints: 0,
            errors: 0,
            pool: None,
            in_flight: std::collections::HashSet::new(),
        }
    }

    /// Encode and persist on `n` threads instead of in the tick (0 keeps it in the tick).
    /// Keep up to `bytes` of recently read segment files in memory (`storage.segment_cache_mib`).
    /// Call before any segment is restored or written.
    pub fn with_segment_cache(mut self, bytes: usize) -> Self {
        self.enc = Arc::new(Encoder::new().with_segment_cache(bytes));
        self.segment_cache_bytes = bytes;
        self
    }

    /// Chunks and bytes per tier over all tables. Walking every chunk of every partition is
    /// the cost (a table of many small chunks: milliseconds), and a tick asks three times, so
    /// the sum is kept until a chunk changes tier here or it is [`STATS_MAX_AGE`] old (chunks
    /// sealed by ingest in between show up then).
    fn tier_totals(&mut self, tables: &Tables) -> TierStats {
        let moves = self.moves_to_warm + self.moves_to_cold;
        if let Some((at, seen, total)) = &self.totals
            && *seen == moves
            && at.elapsed() < STATS_MAX_AGE
        {
            return *total;
        }
        let mut total = TierStats::default();
        for (_, t) in tables.persistent() {
            total.merge(&t.tier_stats());
        }
        self.totals = Some((Instant::now(), moves, total));
        total
    }

    pub fn with_encoders(mut self, n: usize) -> std::io::Result<Self> {
        if n > 0 {
            self.pool = Some(EncodePool::start(
                n,
                &self.enc,
                self.checkpoint_dir.as_deref(),
            )?);
        }
        Ok(self)
    }

    /// Swap in the chunks the encode threads finished; with `wait`, until none is out.
    fn collect_encodes(&mut self, tables: &Tables, wait: bool) -> bool {
        let mut persisted = false;
        loop {
            let next = match &self.pool {
                None => return persisted,
                Some(_) if self.in_flight.is_empty() => return persisted,
                Some(p) if wait => p.done.recv().ok(),
                Some(p) => p.done.try_recv().ok(),
            };
            let Some(done) = next else { return persisted };
            let EncodeDone { job, result } = done;
            self.in_flight
                .remove(&(job.table.clone(), job.key, job.chunk.id()));
            match result {
                Ok((warm, cold)) => {
                    persisted |= self.install(tables, &job, &warm, cold);
                }
                Err(e) => {
                    self.errors += 1;
                    if job.merge.is_some() {
                        self.merging.remove(&(job.table.clone(), job.key));
                    }
                    warn!(table = %job.table, partition = job.key, chunk = job.chunk.id().0, error = %e, "encode or segment write failed; the chunk stays as it is");
                }
            }
        }
    }

    /// Put an encoded chunk in place of its hot copy (and record its segment). A chunk that is no
    /// longer there (retention dropped it meanwhile) is not put back: its segment file goes to the
    /// graveyard, so expired rows cannot return at the next start. Returns whether a segment now
    /// backs the partition.
    fn install(
        &mut self,
        tables: &Tables,
        job: &EncodeJob,
        warm: &Arc<WarmChunk>,
        cold: Option<Arc<ColdChunk>>,
    ) -> bool {
        if let Some(merge) = &job.merge {
            return self.install_merged(tables, job, merge, warm, cold);
        }
        let Some((_, table)) = tables
            .persistent()
            .into_iter()
            .find(|(n, _)| *n == job.table.as_str())
        else {
            if let Some(c) = cold {
                self.graveyard.push(c);
            }
            return false;
        };
        let (_, last_lsn) = job.chunk.lsn_range();
        let last_rows = job.chunk.last_lsn_rows();
        let id = job.chunk.id();
        let replaced = table.with_partition(job.partition, |p| {
            let ok = p.replace_sealed(id, ChunkRef::Encoded(warm.clone()));
            if ok && cold.is_some() {
                p.set_persisted_through_lsn(last_lsn, last_rows);
            }
            ok
        });
        if !replaced {
            if let Some(c) = cold {
                self.graveyard.push(c);
            }
            return false;
        }
        self.moves_to_warm += 1;
        match cold {
            Some(c) => {
                self.cold.insert((job.table.clone(), job.key, id), c);
                true
            }
            None => false,
        }
    }

    /// Put a merged chunk in place of the run it was made from. The segments of the run are
    /// dead from here on: their files go once no query reads them (and a restart before that
    /// skips them, because the merged segment lists them). When the run is no longer there
    /// (retention dropped part of it meanwhile) the merged chunk is given up instead.
    fn install_merged(
        &mut self,
        tables: &Tables,
        job: &EncodeJob,
        merge: &Merge,
        warm: &Arc<WarmChunk>,
        cold: Option<Arc<ColdChunk>>,
    ) -> bool {
        self.merging.remove(&(job.table.clone(), job.key));
        let ids: Vec<ankaa_store::ChunkId> = merge.parts.iter().map(ChunkRef::id).collect();
        let replaced = tables
            .persistent()
            .into_iter()
            .find(|(n, _)| *n == job.table.as_str())
            .is_some_and(|(_, table)| {
                table.with_partition(job.partition, |p| {
                    p.key() == job.key
                        && p.replace_sealed_run(&ids, ChunkRef::Encoded(warm.clone()))
                })
            });
        if !replaced {
            if let Some(c) = cold {
                self.graveyard.push(c);
            }
            return false;
        }
        for id in &ids {
            if let Some(old) = self.cold.remove(&(job.table.clone(), job.key, *id)) {
                self.graveyard.push(old);
            }
        }
        self.compactions += 1;
        self.compacted_chunks += ids.len() as u64;
        // a move for the tier totals: the chunk counts changed
        self.moves_to_warm += 1;
        if let Some(c) = cold {
            self.cold
                .insert((job.table.clone(), job.key, warm.meta().id), c);
        }
        // nothing new became durable: the rows were in segments before
        false
    }

    /// Find runs of small encoded chunks and hand them out to be merged (one run per partition
    /// at a time; in the tick itself, within its time budget, when there are no encode
    /// threads). A run is adjacent chunks of fewer than [`SMALL_CHUNK_ROWS`] rows each, in time
    /// order, every one of them in a segment when segments are kept, that fit one chunk
    /// together; it is merged when it has [`MIN_MERGE_PARTS`] chunks or makes a chunk that is
    /// no longer small.
    ///
    /// Small chunks come from bounding memory: with many partitions and a budget for
    /// uncompressed rows, open chunks are sealed long before they are full. Every chunk has a
    /// fixed cost (its statistics at planning, a decode call per column, a file), so a
    /// partition in a hundred pieces is read a hundred times slower than its rows need.
    fn compact_small_chunks(&mut self, tables: &Tables, t0: Instant) -> bool {
        let mut persisted = false;
        for (name, table) in tables.persistent() {
            for idx in 0..table.num_partitions() {
                match &self.pool {
                    Some(p) if self.in_flight.len() >= p.max_in_flight => return persisted,
                    None if t0.elapsed() > self.budget => return persisted,
                    _ => {}
                }
                let job = table.with_partition(idx, |p| {
                    if self.merging.contains(&(name.to_owned(), p.key())) {
                        return None;
                    }
                    let run = self.small_run(name, p)?;
                    let parts: Vec<ChunkRef> = run.clone().map(|i| p.sealed()[i].clone()).collect();
                    let mut replaces: Vec<u64> = Vec::new();
                    for part in &parts {
                        replaces.push(part.id().0);
                        if let ChunkRef::Encoded(e) = part {
                            replaces.extend_from_slice(&e.meta().replaces);
                        }
                    }
                    replaces.sort_unstable();
                    replaces.dedup();
                    if replaces.len() > MAX_REPLACED_IDS {
                        return None;
                    }
                    let (first, last) = (parts.first()?, parts.last()?);
                    let lsn = (
                        first.lsn_range().0,
                        last.lsn_range().1,
                        last.last_lsn_rows(),
                    );
                    let key = p.key();
                    let schema = table.schema().clone();
                    let id = p.fresh_chunk_id();
                    Some(EncodeJob {
                        table: name.to_owned(),
                        partition: idx,
                        key,
                        chunk: Arc::new(ankaa_store::Chunk::new(id, schema)),
                        merge: Some(Merge {
                            parts,
                            replaces,
                            lsn,
                        }),
                    })
                });
                let Some(job) = job else { continue };
                self.merging.insert((job.table.clone(), job.key));
                if let Some(pool) = &self.pool {
                    self.in_flight
                        .insert((job.table.clone(), job.key, job.chunk.id()));
                    let _ = pool.jobs.send(job);
                    continue;
                }
                match run_job(&self.enc, self.checkpoint_dir.as_deref(), &job) {
                    Ok((warm, cold)) => persisted |= self.install(tables, &job, &warm, cold),
                    Err(e) => {
                        self.errors += 1;
                        self.merging.remove(&(job.table.clone(), job.key));
                        warn!(table = %job.table, partition = job.key, error = %e, "compaction failed; the chunks stay as they are");
                    }
                }
            }
        }
        persisted
    }

    /// The first run of sealed chunks of `p` worth merging, as positions in `p.sealed()`.
    fn small_run(&self, table: &str, p: &ankaa_store::Partition) -> Option<std::ops::Range<usize>> {
        let sealed = p.sealed();
        let small = |c: &ChunkRef| {
            matches!(c, ChunkRef::Encoded(_))
                && c.num_rows() < SMALL_CHUNK_ROWS
                && (self.checkpoint_dir.is_none()
                    || self.cold.contains_key(&(table.to_owned(), p.key(), c.id())))
        };
        let mut start = 0;
        while start < sealed.len() {
            if !small(&sealed[start]) {
                start += 1;
                continue;
            }
            let (mut end, mut rows) = (start + 1, sealed[start].num_rows());
            while end < sealed.len()
                && end - start < MAX_MERGE_PARTS
                && small(&sealed[end])
                && rows + sealed[end].num_rows() <= ankaa_store::CHUNK_ROWS
                // the merged rows must be in time order
                && sealed[end].time_range().map(|r| r.0) >= sealed[end - 1].time_range().map(|r| r.1)
            {
                rows += sealed[end].num_rows();
                end += 1;
            }
            let parts = end - start;
            if parts >= MIN_MERGE_PARTS || (parts >= 2 && rows >= SMALL_CHUNK_ROWS) {
                return Some(start..end);
            }
            start = end;
        }
        None
    }

    /// Whether small chunks are merged (`storage.compact_small_chunks`; on by default).
    #[must_use]
    pub fn with_compaction(mut self, on: bool) -> Self {
        self.compact = on;
        self
    }

    /// Retention windows (table name → nanoseconds); tables not named keep everything.
    #[must_use]
    pub fn with_retention(mut self, retention: HashMap<String, i64>) -> Self {
        self.retention = retention;
        self
    }

    /// Drop expired chunks and delete the segment files nothing holds any more.
    fn apply_retention(&mut self, tables: &Tables, metrics: &Metrics) {
        let tm = &metrics.tier;
        for (name, table) in tables.persistent() {
            let Some(&keep) = self.retention.get(name) else {
                continue;
            };
            for (key, chunk) in table.expire(keep) {
                tm.expired_chunks.fetch_add(1, Ordering::Relaxed);
                tm.expired_rows
                    .fetch_add(chunk.num_rows() as u64, Ordering::Relaxed);
                self.forced.remove(&(name.to_owned(), key));
                if let Some(cold) = self.cold.remove(&(name.to_owned(), key, chunk.id())) {
                    self.graveyard.push(cold);
                }
            }
        }
        // a segment still referenced by a query snapshot (or a warm copy being read) stays on
        // disk until the next tick that finds it free
        self.graveyard.retain(|cold| {
            if Arc::strong_count(cold) > 1 {
                return true;
            }
            match std::fs::remove_file(cold.path()) {
                Ok(()) => {
                    tm.segments_deleted.fetch_add(1, Ordering::Relaxed);
                    false
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                Err(e) => {
                    warn!(path = %cold.path().display(), error = %e, "expired segment not deleted; retried");
                    true
                }
            }
        });
    }

    /// Segment directory, if persistence is on.
    pub fn checkpoint_dir(&self) -> Option<&Path> {
        self.checkpoint_dir.as_deref()
    }

    /// Persist the symbol dictionary (ids are positions) next to the segments, atomically.
    /// Segments store dictionary ids, so the dictionary must outlive WAL truncation.
    pub fn save_symbols(&self, dict: &ankaa_ingest::SymbolDict) -> std::io::Result<()> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(());
        };
        std::fs::create_dir_all(dir)?;
        let path = dir.join("symbols.json");
        let tmp = dir.join("symbols.json.tmp");
        let json = serde_json::to_vec(dict.names())?;
        std::fs::write(&tmp, json)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(dir)?.sync_all()
    }

    /// Load the persisted dictionary into `dict` (which must be empty). Returns the count.
    pub fn load_symbols(&self, dict: &mut ankaa_ingest::SymbolDict) -> anyhow::Result<usize> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(0);
        };
        let path = dir.join("symbols.json");
        if !path.exists() {
            return Ok(0);
        }
        anyhow::ensure!(
            dict.is_empty(),
            "symbol dictionary must be empty before restore"
        );
        let names: Vec<String> = serde_json::from_slice(&std::fs::read(&path)?)?;
        for n in &names {
            dict.intern(n);
        }
        Ok(names.len())
    }

    /// Persist the dictionaries of user tables (`dict` columns) next to the segments,
    /// atomically. Like the symbol dictionary they must be on disk before any segment that
    /// stores their ids, and they must outlive WAL truncation.
    pub fn save_dictionaries(&self, tables: &Tables) -> std::io::Result<()> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(());
        };
        let mut doc: std::collections::BTreeMap<&str, std::collections::BTreeMap<&str, &[String]>> =
            std::collections::BTreeMap::new();
        for u in &tables.user {
            for (ci, d) in u.dicts.iter().enumerate() {
                if let Some(d) = d {
                    doc.entry(u.name.as_str())
                        .or_default()
                        .insert(u.table.schema().fields()[ci].name.as_str(), d.names());
                }
            }
        }
        std::fs::create_dir_all(dir)?;
        let path = dir.join("dictionaries.json");
        let tmp = dir.join("dictionaries.json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(&doc)?)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(dir)?.sync_all()
    }

    /// Load the persisted user-table dictionaries (which must be empty). Returns the number of
    /// values loaded. Columns that no longer exist are ignored.
    pub fn load_dictionaries(&self, tables: &mut Tables) -> anyhow::Result<usize> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(0);
        };
        let path = dir.join("dictionaries.json");
        if !path.exists() {
            return Ok(0);
        }
        let doc: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, Vec<String>>,
        > = serde_json::from_slice(&std::fs::read(&path)?)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let mut n = 0;
        for u in &mut tables.user {
            let Some(cols) = doc.get(&u.name) else {
                continue;
            };
            for (ci, d) in u.dicts.iter_mut().enumerate() {
                let Some(d) = d else { continue };
                let Some(names) = cols.get(&u.table.schema().fields()[ci].name) else {
                    continue;
                };
                anyhow::ensure!(d.is_empty(), "dictionary must be empty before restore");
                for name in names {
                    d.intern(name);
                }
                anyhow::ensure!(
                    d.len() == names.len(),
                    "{}: dictionary of {}.{} holds duplicate values",
                    path.display(),
                    u.name,
                    u.table.schema().fields()[ci].name
                );
                n += names.len();
            }
        }
        Ok(n)
    }

    /// Re-attach persisted segments to the (empty) tables.
    ///
    /// A segment written before the table gained nullable columns at the end (or before a
    /// column became nullable) is read as the current schema, the added columns `NULL`
    /// ([`ankaa_store::Evolved`]); nothing is rewritten. Fails, without attaching anything that
    /// could be overwritten later, when a segment cannot be read or its columns cannot be read
    /// as the table's (a column removed, renamed, retyped or reordered, or a non-nullable one
    /// added). Starting without those files would hide their rows and then reuse their chunk
    /// ids, replacing the old files with new data. The message names the files and the
    /// difference; the operator decides (restore the previous table definition, or move the
    /// files out of the directory).
    pub fn restore(&mut self, tables: &Tables) -> anyhow::Result<RestoreStats> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(RestoreStats::default());
        };
        let mut st = RestoreStats::default();
        let mut problems: Vec<String> = Vec::new();
        let segments = self.enc.scan_dir(dir, |p, e| {
            st.bad_files += 1;
            problems.push(format!("{}: unreadable ({e})", p.display()));
        })?;
        // segments whose rows live on in a merged segment: a crash came between writing that
        // one and deleting these
        let dead: std::collections::HashSet<(String, u32, u64)> = segments
            .iter()
            .flat_map(|s| {
                let m = s.meta();
                m.replaces
                    .iter()
                    .map(|id| (m.table.clone(), m.partition, *id))
                    .collect::<Vec<_>>()
            })
            .collect();
        let (dead_segments, segments): (Vec<_>, Vec<_>) = segments.into_iter().partition(|s| {
            let m = s.meta();
            dead.contains(&(m.table.clone(), m.partition, m.id.0))
        });
        for seg in dead_segments {
            let path = seg.path().to_path_buf();
            drop(seg);
            match std::fs::remove_file(&path) {
                Ok(()) => st.replaced += 1,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "segment replaced by a merged one could not be deleted; it is ignored");
                }
            }
        }
        let all = tables.persistent();
        let names: HashMap<&str, &Table> = all.iter().copied().collect();
        for seg in &segments {
            let meta = seg.meta();
            if let Some(table) = names.get(meta.table.as_str())
                && let Err(why) = ankaa_store::schema_evolves(&meta.schema, table.schema())
            {
                problems.push(format!(
                    "{}: cannot be read as table {:?} now: {why}",
                    seg.path().display(),
                    meta.table
                ));
            }
        }
        if !problems.is_empty() {
            let shown = problems
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n  ");
            anyhow::bail!(
                "{} segment file(s) under {} cannot be used:\n  {shown}\nA table may gain nullable columns at the end (and a column may become nullable); other changes need the previous table definition back, or these files moved out of the directory to start without their rows.",
                problems.len(),
                dir.display()
            );
        }
        for seg in segments {
            let meta = seg.meta().clone();
            let Some(table) = names.get(meta.table.as_str()) else {
                // a table that is no longer configured: its files are left alone
                warn!(table = %meta.table, path = %seg.path().display(), "segment for a table that is not configured; ignored");
                continue;
            };
            let part = table.partition_for(meta.partition);
            // the same allocation as `seg`: the cold map's reference count still tells whether
            // a query holds the file
            let chunk = ankaa_store::Evolved::wrap(seg.clone(), table.schema())
                .map_err(|why| anyhow::anyhow!("{}: {why}", seg.path().display()))?;
            if meta.schema != *table.schema() {
                st.evolved += 1;
            }
            table.with_partition(part, |p| p.restore_sealed(ChunkRef::Encoded(chunk)));
            st.segments += 1;
            st.rows += meta.num_rows as u64;
            st.max_lsn = st.max_lsn.max(meta.lsn_range.1);
            self.cold
                .insert((meta.table.clone(), meta.partition, meta.id), seg);
        }
        Ok(st)
    }

    /// Keep uncompressed rows within `budget` bytes and open chunks younger than `max_age`: seal
    /// the open chunks that are too old, then, while the hot bytes (open chunks twice, for their
    /// query view, plus hot sealed chunks) exceed the budget, the open chunks with the oldest
    /// first row (only those of at least [`MIN_SEAL_ROWS`] rows, so busy partitions are not
    /// shredded into small chunks); still over it, every partition's hot window is given up.
    /// Sealed partitions are marked so that the next tiering steps encode all their hot chunks.
    /// Returns `(sealed for age, sealed for memory)`.
    pub fn relieve_memory(
        &mut self,
        tables: &Tables,
        budget: usize,
        max_age: Option<Duration>,
    ) -> (usize, usize) {
        let now = Instant::now();
        let mut hot = self.tier_totals(tables).hot_bytes;
        // (first row, bytes, rows, table, partition)
        let mut open: Vec<(Instant, usize, usize, String, u32)> = Vec::new();
        for (name, table) in tables.persistent() {
            table.for_each_partition(|p| {
                if let Some(since) = p.open_since() {
                    let b = p.open_bytes();
                    hot += 2 * b;
                    open.push((since, b, p.open_rows(), name.to_owned(), p.key()));
                }
            });
        }
        open.sort_by_key(|o| o.0);
        let mut seal: Vec<(String, u32)> = Vec::new();
        let mut by_age = 0;
        let mut by_memory = 0;
        for (since, b, rows, name, key) in open {
            let old = max_age.is_some_and(|m| now.duration_since(since) >= m);
            let over = budget > 0 && hot > budget && rows >= MIN_SEAL_ROWS;
            if old || over {
                if old {
                    by_age += 1;
                } else {
                    by_memory += 1;
                }
                // once encoded, its bytes leave the hot tier (the view's copy goes at once)
                hot = hot.saturating_sub(2 * b);
                seal.push((name, key));
            }
        }
        for (name, key) in seal {
            if let Some((_, table)) = tables.persistent().into_iter().find(|(n, _)| *n == name) {
                table.with_partition(key as usize, ankaa_store::Partition::seal_open);
            }
            self.forced.insert((name, key));
        }
        if budget > 0 && hot > budget {
            // sealed hot chunks alone exceed the budget: no partition keeps a hot window
            for (name, table) in tables.persistent() {
                table.for_each_partition(|p| {
                    if p.hot_sealed() > 0 {
                        self.forced.insert((name.to_owned(), p.key()));
                    }
                });
            }
        }
        (by_age, by_memory)
    }

    /// The WAL has outgrown its budget and `target_lsn` is where its second-oldest segment
    /// starts: seal and persist the partitions that still hold rows from before it, so that the
    /// next checkpoint can pass `target_lsn` and the oldest segment can go. Without this, one
    /// quiet partition (a symbol that trades once an hour) whose open chunk never fills keeps
    /// the whole log alive. A partition is only touched again after the log has grown by
    /// another segment, so this does not shred busy tables into small chunks.
    pub fn relieve_wal(&mut self, tables: &Tables, target_lsn: u64) -> usize {
        if self.checkpoint_dir.is_none() {
            return 0;
        }
        let mut forced = 0;
        for (name, table) in tables.persistent() {
            table.for_each_partition(|p| {
                // a partition already marked is waiting for its segments to be written: sealing
                // it again on every call would only pile up small chunks
                if p.checkpoint_lsn() < target_lsn && self.forced.insert((name.to_owned(), p.key()))
                {
                    p.seal_open();
                    forced += 1;
                }
            });
        }
        forced
    }

    /// One tiering step. Returns the WAL checkpoint LSN when segments were written and the WAL
    /// may be truncated (rate-limited by `checkpoint_interval`).
    #[allow(clippy::too_many_lines)]
    pub fn tick(&mut self, tables: &Tables, metrics: &Metrics) -> Option<u64> {
        let t0 = Instant::now();
        let mut persisted_any = false;
        if !self.retention.is_empty() || !self.graveyard.is_empty() {
            self.apply_retention(tables, metrics);
        }
        // 1. hot → warm (+ segment): swap in what the encode threads finished, then hand out
        // what is due (or, without threads, encode here within the time budget)
        persisted_any |= self.collect_encodes(tables, false);
        'outer: for (name, table) in tables.persistent() {
            let mut n_parts = table.num_partitions();
            let mut idx = 0;
            while idx < n_parts {
                let threaded = self.pool.is_some();
                if threaded {
                    if self.in_flight.len() >= self.pool.as_ref().map_or(0, |p| p.max_in_flight) {
                        break 'outer;
                    }
                } else if t0.elapsed() > self.budget {
                    break 'outer;
                }
                // pick the oldest hot sealed chunk beyond the hot window that is not being
                // encoded already, under the lock
                let in_flight = &self.in_flight;
                let candidate = table.with_partition(idx, |p| {
                    let sealed = p.sealed();
                    let hot_total = sealed.iter().filter(|c| c.tier() == Tier::Hot).count();
                    let forced_key = (name.to_owned(), p.key());
                    let forced = self.forced.contains(&forced_key);
                    if forced && hot_total == 0 {
                        self.forced.remove(&forced_key);
                    }
                    let keep_hot = if forced { 0 } else { self.hot_sealed };
                    if hot_total <= keep_hot {
                        return None;
                    }
                    let due = hot_total - keep_hot;
                    sealed
                        .iter()
                        .filter(|c| c.tier() == Tier::Hot)
                        .take(due)
                        .filter(|c| !in_flight.contains(&(name.to_owned(), p.key(), c.id())))
                        .find_map(|c| c.as_hot().cloned())
                        .map(|c| (p.key(), c))
                });
                let Some((key, chunk)) = candidate else {
                    idx += 1;
                    n_parts = table.num_partitions();
                    continue;
                };
                let job = EncodeJob {
                    table: name.to_owned(),
                    partition: idx,
                    key,
                    chunk,
                    merge: None,
                };
                if let Some(pool) = &self.pool {
                    self.in_flight
                        .insert((job.table.clone(), key, job.chunk.id()));
                    let _ = pool.jobs.send(job);
                    continue;
                }
                let encoded = run_job(&self.enc, self.checkpoint_dir.as_deref(), &job);
                match encoded {
                    Ok((warm, cold)) => persisted_any |= self.install(tables, &job, &warm, cold),
                    Err(e) => {
                        self.errors += 1;
                        warn!(table = name, partition = key, chunk = job.chunk.id().0, error = %e, "encode or segment write failed; chunk stays hot");
                        idx += 1;
                    }
                }
            }
        }
        // 1b. compaction: runs of small encoded chunks of a partition become one chunk
        if self.compact
            && self
                .last_compact
                .is_none_or(|t| t.elapsed() >= COMPACT_EVERY)
        {
            self.last_compact = Some(Instant::now());
            persisted_any |= self.compact_small_chunks(tables, t0);
        }
        // 2. warm → cold when over budget (only chunks that have a segment)
        if self.checkpoint_dir.is_some() {
            let mut warm_bytes: usize = self.tier_totals(tables).warm_bytes;
            if warm_bytes > self.warm_budget_bytes {
                // oldest first by max timestamp
                let mut warm: Vec<(i64, Key, Arc<ColdChunk>)> = Vec::new();
                for (name, table) in tables.persistent() {
                    table.for_each_partition(|p| {
                        for c in p.sealed() {
                            if c.tier() == Tier::Warm
                                && let Some(cold) =
                                    self.cold.get(&(name.to_owned(), p.key(), c.id()))
                            {
                                warm.push((
                                    c.watermark(),
                                    (name.to_owned(), p.key(), c.id()),
                                    cold.clone(),
                                ));
                            }
                        }
                    });
                }
                warm.sort_by_key(|(wm, _, _)| *wm);
                for (_, (name, key, id), cold) in warm {
                    if warm_bytes <= self.warm_budget_bytes || t0.elapsed() > self.budget {
                        break;
                    }
                    let Some((_, table)) = tables
                        .persistent()
                        .into_iter()
                        .find(|(n, _)| *n == name.as_str())
                    else {
                        continue;
                    };
                    let part = table.partition_for(key);
                    let freed = table.with_partition(part, |p| {
                        let before = p
                            .sealed()
                            .iter()
                            .find(|c| c.id() == id)
                            .map_or(0, ChunkRef::memory_bytes);
                        if p.replace_sealed(id, ChunkRef::Encoded(cold.clone())) {
                            before
                        } else {
                            0
                        }
                    });
                    warm_bytes = warm_bytes.saturating_sub(freed);
                    self.moves_to_cold += 1;
                }
            }
        }
        // 3. metrics
        let total = self.tier_totals(tables);
        let tm = &metrics.tier;
        tm.hot_chunks
            .store(total.hot_chunks as u64, Ordering::Relaxed);
        tm.warm_chunks
            .store(total.warm_chunks as u64, Ordering::Relaxed);
        tm.cold_segments
            .store(total.cold_chunks as u64, Ordering::Relaxed);
        tm.hot_bytes
            .store(total.hot_bytes as u64, Ordering::Relaxed);
        tm.warm_bytes
            .store(total.warm_bytes as u64, Ordering::Relaxed);
        tm.cold_bytes
            .store(total.cold_bytes as u64, Ordering::Relaxed);
        tm.moves_to_warm
            .store(self.moves_to_warm, Ordering::Relaxed);
        tm.moves_to_cold
            .store(self.moves_to_cold, Ordering::Relaxed);
        tm.compactions.store(self.compactions, Ordering::Relaxed);
        tm.compacted_chunks
            .store(self.compacted_chunks, Ordering::Relaxed);
        // what the warm tier leaves of its budget is lent to the segment cache: after a restart
        // nothing is warm, and the segments queries return to take that place
        if self.segment_cache_bytes > 0 {
            self.enc.set_segment_cache(
                self.segment_cache_bytes
                    .saturating_add(self.warm_budget_bytes.saturating_sub(total.warm_bytes)),
            );
        }
        let cache = self.enc.segment_cache();
        tm.segment_cache_bytes
            .store(cache.bytes as u64, Ordering::Relaxed);
        tm.segment_cache_hits.store(cache.hits, Ordering::Relaxed);
        tm.segment_cache_misses
            .store(cache.misses, Ordering::Relaxed);
        // 4. checkpoint LSN
        self.checkpoint_pending |= persisted_any;
        if self.checkpoint_dir.is_none() || !self.checkpoint_pending {
            return None;
        }
        let due = self
            .last_truncate
            .is_none_or(|t| t.elapsed() >= self.checkpoint_interval);
        if !due {
            return None;
        }
        let mut lsn = tables
            .persistent()
            .iter()
            .map(|(_, t)| t.checkpoint_lsn())
            .min()
            .unwrap_or(u64::MAX);
        if lsn == u64::MAX {
            // nothing depends on the WAL any more: everything sealed is in segments
            let max_persisted = tables
                .persistent()
                .iter()
                .map(|(_, t)| t.max_persisted_lsn())
                .max()
                .unwrap_or(0);
            if max_persisted == 0 {
                return None;
            }
            lsn = max_persisted + 1;
        }
        if lsn == 0 {
            return None;
        }
        self.last_truncate = Some(Instant::now());
        self.checkpoint_pending = false;
        self.checkpoints += 1;
        tm.checkpoints.store(self.checkpoints, Ordering::Relaxed);
        info!(checkpoint_lsn = lsn, "checkpoint");
        Some(lsn)
    }

    /// Persist everything sealed (shutdown): hot window ignored, no time budget; chunks being
    /// encoded on the threads are waited for first, the rest is encoded here.
    pub fn flush_all(&mut self, tables: &Tables, metrics: &Metrics) -> Option<u64> {
        self.checkpoint_pending |= self.collect_encodes(tables, true);
        self.pool = None;
        let (hot, budget, interval) = (self.hot_sealed, self.budget, self.checkpoint_interval);
        self.hot_sealed = 0;
        self.budget = Duration::from_secs(3600);
        self.checkpoint_interval = Duration::ZERO;
        // hot_sealed is max(1) in `new`; emulate 0 by looping until nothing moves
        let mut last = None;
        for _ in 0..4 {
            let before = self.moves_to_warm;
            let r = self.tick(tables, metrics);
            if r.is_some() {
                last = r;
            }
            if self.moves_to_warm == before {
                break;
            }
        }
        self.hot_sealed = hot;
        self.budget = budget;
        self.checkpoint_interval = interval;
        last
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::inconsistent_digit_grouping,
    clippy::large_digit_groups,
    clippy::unreadable_literal
)]
mod tests {
    use super::*;
    use ankaa_ingest::{EventBatch, Side, SymbolId, Trade};
    use ankaa_store::CHUNK_ROWS;

    fn trades(from: usize, n: usize) -> EventBatch {
        let mut b = EventBatch::with_capacity(n);
        for i in from..from + n {
            b.trades.push(Trade {
                ts_ns: 1_700_000_000_000_000_000 + i as i64 * 1_000,
                recv_ns: 0,
                symbol: SymbolId(u32::from(i % 2 == 0)),
                price: 65_000_00000000 + (i % 7) as i64 * 1_000_000,
                size: 1_000_000 + (i % 13) as i64,
                side: if i % 3 == 0 { Side::Bid } else { Side::Ask },
                seq: i as i64,
            });
        }
        b
    }

    fn total_rows(t: &Tables) -> u64 {
        t.trades.stats().rows
    }

    fn segment_count(dir: &Path) -> usize {
        std::fs::read_dir(dir.join("trades")).map_or(0, |r| r.filter_map(Result::ok).count())
    }

    /// Tick until the encode threads have nothing left out and nothing new is due.
    fn settle(t: &mut Tiering, tables: &Tables, metrics: &Metrics) {
        let t0 = Instant::now();
        loop {
            let before = t.moves_to_warm;
            t.tick(tables, metrics);
            if t.in_flight.is_empty() && t.moves_to_warm == before {
                return;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "encode threads did not settle"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Every row of `trades` as text, in partition and stored order, whatever tier it is in.
    fn dump(t: &Tables) -> Vec<String> {
        let mut out = Vec::new();
        for c in &t.trades.snapshot().chunks {
            let n = c.schema().len();
            let all: Vec<usize> = (0..n).collect();
            match c {
                ChunkRef::Hot(h) => {
                    for row in 0..h.num_rows() {
                        let cells: Vec<_> = (0..n).map(|i| h.value(i, row)).collect();
                        out.push(format!("{cells:?}"));
                    }
                }
                ChunkRef::Encoded(e) => {
                    let cols = e.decode_nullable(&all).unwrap();
                    for row in 0..e.meta().num_rows {
                        let cells: Vec<_> = cols.iter().map(|(col, _)| col.get(row)).collect();
                        out.push(format!("{cells:?}"));
                    }
                }
            }
        }
        out
    }

    /// 40 small sealed chunks per symbol, as bounded memory leaves them.
    fn fragmented() -> Tables {
        let mut tables = Tables::new(false);
        for i in 0..40 {
            tables.ingest(&trades(i * 500, 500));
            for idx in 0..tables.trades.num_partitions() {
                tables
                    .trades
                    .with_partition(idx, ankaa_store::Partition::seal_open);
            }
        }
        tables
    }

    /// Tick (searching for runs every time) until nothing is out and nothing more is merged.
    fn compacted(t: &mut Tiering, tables: &Tables, metrics: &Metrics) {
        let t0 = Instant::now();
        loop {
            let before = (t.moves_to_warm, t.compactions);
            t.last_compact = None;
            t.tick(tables, metrics);
            if t.in_flight.is_empty() && (t.moves_to_warm, t.compactions) == before {
                return;
            }
            assert!(t0.elapsed() < Duration::from_secs(30), "did not settle");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Small chunks of a partition become full ones and no row changes: the same rows in the
    /// same order before and after, in the tick and on encode threads, and after a restart
    /// from the segments, of which only the merged ones are left.
    #[test]
    fn small_chunks_are_merged_and_no_row_changes() {
        let metrics = Metrics::default();
        for threads in [0, 2] {
            let dir = tempfile::tempdir().unwrap();
            let tables = fragmented();
            let before = dump(&tables);
            assert_eq!(before.len(), 20_000);
            let sealed_before = tables.trades.tier_stats();
            let mut t = Tiering::new(
                Some(dir.path().to_path_buf()),
                1,
                usize::MAX,
                Duration::from_secs(5),
                Duration::ZERO,
            )
            .with_encoders(threads)
            .unwrap();
            compacted(&mut t, &tables, &metrics);
            let st = tables.trades.tier_stats();
            assert!(
                t.compactions >= 2,
                "threads {threads}: {} merges",
                t.compactions
            );
            assert!(
                st.warm_chunks + st.cold_chunks <= 4,
                "threads {threads}: {st:?} (before: {sealed_before:?})"
            );
            assert_eq!(dump(&tables), before, "threads {threads}");
            assert_eq!(total_rows(&tables), 20_000);
            // the segments of the merged-away chunks are gone
            t.tick(&tables, &metrics);
            assert_eq!(segment_count(dir.path()), t.cold.len(), "threads {threads}");
            assert_eq!(t.cold.len(), st.warm_chunks + st.cold_chunks);
            // a compaction switched off leaves the chunks as they are
            let untouched = fragmented();
            let mut off = Tiering::new(None, 1, usize::MAX, Duration::from_secs(5), Duration::ZERO)
                .with_compaction(false);
            compacted(&mut off, &untouched, &metrics);
            assert_eq!(off.compactions, 0);
            assert!(untouched.trades.tier_stats().warm_chunks > 40);

            // restart from the segments
            drop(t);
            let restarted = Tables::new(false);
            let mut t2 = Tiering::new(
                Some(dir.path().to_path_buf()),
                1,
                usize::MAX,
                Duration::from_secs(5),
                Duration::ZERO,
            );
            let rs = t2.restore(&restarted).unwrap();
            assert_eq!((rs.replaced, rs.bad_files), (0, 0));
            let persisted: Vec<String> = dump(&restarted);
            // (the newest chunk of each symbol stayed hot and was never persisted)
            assert_eq!(persisted.len() as u64, rs.rows);
            assert!(persisted.iter().all(|r| before.contains(r)));
        }
    }

    /// A crash between writing a merged segment and deleting the segments it replaces leaves
    /// both on disk. The next start keeps the merged one and deletes the others: no row twice.
    #[test]
    fn a_crash_during_compaction_neither_loses_nor_repeats_rows() {
        let metrics = Metrics::default();
        let dir = tempfile::tempdir().unwrap();
        let backup = tempfile::tempdir().unwrap();
        let tables = fragmented();
        let mut t = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            usize::MAX,
            Duration::from_secs(5),
            Duration::ZERO,
        )
        .with_compaction(false);
        compacted(&mut t, &tables, &metrics);
        // every small chunk is in a segment; keep copies of those files
        let small = segment_count(dir.path());
        assert!(small > 70);
        for f in std::fs::read_dir(dir.path().join("trades")).unwrap() {
            let f = f.unwrap();
            std::fs::copy(f.path(), backup.path().join(f.file_name())).unwrap();
        }
        let persisted_before: std::collections::BTreeSet<String> = {
            let check = Tables::new(false);
            let mut tr = Tiering::new(
                Some(dir.path().to_path_buf()),
                1,
                usize::MAX,
                Duration::from_secs(5),
                Duration::ZERO,
            );
            tr.restore(&check).unwrap();
            dump(&check).into_iter().collect()
        };
        // merge, let the old files be deleted, then put them back: the state after a crash
        // that came before the deletion
        let mut t = t.with_compaction(true);
        compacted(&mut t, &tables, &metrics);
        t.tick(&tables, &metrics);
        let merged = segment_count(dir.path());
        assert!(merged <= 4, "{merged} segments left");
        drop(t);
        for f in std::fs::read_dir(backup.path()).unwrap() {
            let f = f.unwrap();
            let back = dir.path().join("trades").join(f.file_name());
            if !back.exists() {
                std::fs::copy(f.path(), back).unwrap();
            }
        }
        assert_eq!(segment_count(dir.path()), small + merged);

        let restarted = Tables::new(false);
        let mut t2 = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            usize::MAX,
            Duration::from_secs(5),
            Duration::ZERO,
        );
        let rs = t2.restore(&restarted).unwrap();
        assert_eq!(rs.replaced, small);
        assert_eq!(rs.segments, merged);
        assert_eq!(segment_count(dir.path()), merged);
        let rows = dump(&restarted);
        let distinct: std::collections::BTreeSet<String> = rows.iter().cloned().collect();
        assert_eq!(rows.len(), distinct.len(), "a row came back twice");
        assert_eq!(distinct, persisted_before);
    }

    /// Encoding on threads leaves exactly what encoding in the tick leaves: the same chunks in
    /// each tier, the same rows, the same segment files.
    #[test]
    fn encode_threads_persist_what_the_tick_persists() {
        let metrics = Metrics::default();
        let run = |threads: usize| {
            let dir = tempfile::tempdir().unwrap();
            let mut tables = Tables::new(false);
            for i in (0..7 * CHUNK_ROWS).step_by(4096) {
                tables.ingest(&trades(i, 4096));
            }
            let mut t = Tiering::new(
                Some(dir.path().to_path_buf()),
                1,
                usize::MAX,
                Duration::from_secs(5),
                Duration::ZERO,
            )
            .with_encoders(threads)
            .unwrap();
            settle(&mut t, &tables, &metrics);
            let st = tables.trades.tier_stats();
            (
                (st.hot_chunks, st.warm_chunks, st.cold_chunks),
                total_rows(&tables),
                segment_count(dir.path()),
                t.moves_to_warm,
            )
        };
        let inline = run(0);
        assert!(inline.3 > 0, "something was encoded");
        assert_eq!(run(1), inline);
        assert_eq!(run(3), inline);
    }

    /// A chunk that retention drops while its segment is being written never comes back: the
    /// finished segment is deleted (graveyard), not attached, and not found at the next start.
    #[test]
    fn a_chunk_expired_while_encoding_does_not_come_back() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Metrics::default();
        let mut tables = Tables::new(false);
        for i in (0..6 * CHUNK_ROWS).step_by(4096) {
            tables.ingest(&trades(i, 4096));
        }
        let mut t = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            usize::MAX,
            Duration::from_secs(5),
            Duration::ZERO,
        )
        .with_encoders(2)
        .unwrap();
        // hand the chunks out, then drop every sealed chunk before they are swapped in
        t.tick(&tables, &metrics);
        assert!(!t.in_flight.is_empty());
        let dropped = tables.trades.expire(1);
        assert!(!dropped.is_empty());
        drop(dropped);
        settle(&mut t, &tables, &metrics);
        // no encoded copy came back, and the files are gone once nothing holds them
        let st = tables.trades.tier_stats();
        assert_eq!((st.warm_chunks, st.cold_chunks), (0, 0));
        t.tick(&tables, &metrics);
        assert_eq!(segment_count(dir.path()), 0);
        let restarted = Tables::new(false);
        let mut t2 = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            usize::MAX,
            Duration::from_secs(5),
            Duration::ZERO,
        );
        assert_eq!(t2.restore(&restarted).unwrap().segments, 0);
    }

    /// Uncompressed rows stay bounded: open chunks older than the age limit are sealed whatever
    /// their size; over the byte budget the ones with the oldest first row go first, never one
    /// under `MIN_SEAL_ROWS`; sealed partitions are encoded by the next tiering steps (also
    /// without a checkpoint directory), and no row is lost.
    #[test]
    fn memory_relief_seals_old_and_large_open_chunks_first() {
        let metrics = Metrics::default();
        let mut tables = Tables::new(false);
        // symbol 1 first (oldest open rows), then symbol 0; 3,000 rows each, plus a small one
        let mut b = trades(0, 0);
        for i in 0..3_000 {
            let mut t = trades(2 * i + 1, 1).trades[0];
            t.symbol = SymbolId(1);
            b.trades.push(t);
        }
        tables.ingest(&b);
        std::thread::sleep(Duration::from_millis(20));
        let mut b = trades(0, 0);
        for i in 0..3_000 {
            let mut t = trades(10_000 + 2 * i, 1).trades[0];
            t.symbol = SymbolId(0);
            b.trades.push(t);
        }
        tables.ingest(&b);
        let open = |t: &Tables| {
            let mut v = Vec::new();
            t.trades.for_each_partition(|p| v.push(p.open_rows()));
            v
        };
        assert_eq!(open(&tables), vec![3_000, 3_000]);
        let mut tier = Tiering::new(None, 4, usize::MAX, Duration::from_secs(5), Duration::ZERO);
        // no budget, no age limit: nothing
        assert_eq!(tier.relieve_memory(&tables, 0, None), (0, 0));
        // a budget that one partition's rows exceed: the oldest (symbol 1) only
        let row = tables.trades.schema().fields().len() * 8;
        let budget = 2 * 3_000 * row + 1;
        assert_eq!(tier.relieve_memory(&tables, budget, None), (0, 1));
        assert_eq!(open(&tables), vec![3_000, 0]);
        // the age limit seals what is left, whatever the budget
        assert_eq!(
            tier.relieve_memory(&tables, 0, Some(Duration::ZERO)),
            (1, 0)
        );
        assert_eq!(open(&tables), vec![0, 0]);
        // the sealed chunks leave the hot tier (the hot window of 4 does not keep them)
        for _ in 0..10 {
            tier.tick(&tables, &metrics);
        }
        let st = tables.trades.tier_stats();
        assert_eq!((st.hot_chunks, st.warm_chunks), (0, 2));
        assert_eq!(total_rows(&tables), 6_000);
        // a chunk under MIN_SEAL_ROWS is left to fill, however tight the budget
        tables.ingest(&trades(20_000, 10));
        assert_eq!(tier.relieve_memory(&tables, 1, None), (0, 0));
    }

    /// Retention drops the old chunks of every tier, and a segment file goes only once no query
    /// snapshot holds its chunk; after a restart the expired data does not come back.
    #[test]
    fn retention_expires_chunks_and_deletes_segments_when_free() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Metrics::default();
        let mut tables = Tables::new(false);
        // per symbol: 4 sealed chunks + an open one; rows 1 µs apart
        let n = 8 * CHUNK_ROWS + 1_000;
        let mut i = 0;
        let mut lsn = 1u64;
        while i < n {
            tables.set_lsn(lsn);
            tables.ingest(&trades(i, 1_000.min(n - i)));
            i += 1_000;
            lsn += 1;
        }
        let mut tiering = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        tiering.tick(&tables, &metrics);
        let segments = |d: &Path| {
            std::fs::read_dir(d.join("trades")).map_or(0, |r| r.filter_map(Result::ok).count())
        };
        let written = segments(dir.path());
        assert!(written >= 4, "{written}");
        // a query that started before the retention step holds the old chunks
        let held = tables.trades.snapshot();
        // keep the newest ~1.5 chunks of time per symbol (both symbols interleave: one chunk of a
        // symbol spans 2 × 65,536 µs)
        let keep_ns = 3 * CHUNK_ROWS as i64 * 1_000;
        tiering.retention.insert("trades".into(), keep_ns);
        tiering.tick(&tables, &metrics);
        let expired = metrics.tier.expired_chunks.load(Ordering::Relaxed);
        assert!(expired >= 4, "{expired}");
        let rows_left: usize = tables
            .trades
            .snapshot()
            .chunks
            .iter()
            .map(ChunkRef::num_rows)
            .sum();
        assert_eq!(
            rows_left as u64 + metrics.tier.expired_rows.load(Ordering::Relaxed),
            n as u64
        );
        // the held snapshot still reads every row, from files that are still there
        assert_eq!(segments(dir.path()), written);
        let old_rows: usize = held.chunks.iter().map(ChunkRef::num_rows).sum();
        assert_eq!(old_rows, n);
        for c in &held.chunks {
            if let ChunkRef::Encoded(e) = c {
                e.decode(&[0]).unwrap();
            }
        }
        drop(held);
        tiering.tick(&tables, &metrics);
        let deleted = metrics.tier.segments_deleted.load(Ordering::Relaxed);
        assert!(deleted >= 4, "{deleted}");
        assert_eq!(segments(dir.path()) as u64, written as u64 - deleted);

        // restart: only the kept segments come back
        tables.seal_all();
        tiering.flush_all(&tables, &metrics);
        let kept_rows = tables.trades.stats().rows - tables.trades.stats().expired_rows;
        let tables2 = Tables::new(false);
        let mut tiering2 = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        tiering2.restore(&tables2).unwrap();
        assert_eq!(total_rows(&tables2), kept_rows);
    }

    #[test]
    fn restart_restores_segments_and_skips_replayed_lsns() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Metrics::default();
        // 1. ingest 3 chunks worth of trades per symbol, one "frame" (LSN) per 1,000 trades
        let mut tables = Tables::new(false);
        let n = 6 * CHUNK_ROWS + 500; // per symbol: 3 sealed chunks + a small open one
        let mut lsn = 1u64;
        let mut i = 0;
        while i < n {
            tables.set_lsn(lsn);
            tables.ingest(&trades(i, 1_000.min(n - i)));
            i += 1_000;
            lsn += 1;
        }
        let rows_before = total_rows(&tables);
        let mut tiering = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0, // zero warm budget: everything persisted goes cold immediately
            Duration::from_secs(10),
            Duration::ZERO,
        );
        let ck = tiering.tick(&tables, &metrics);
        assert!(tiering.moves_to_warm >= 1, "{}", tiering.moves_to_warm);
        assert!(tiering.moves_to_cold >= 1);
        assert!(ck.is_some_and(|l| l > 1), "{ck:?}");
        // queries over the tiered table still see every row
        let snap = tables.trades.snapshot();
        let rows: usize = snap.chunks.iter().map(ChunkRef::num_rows).sum();
        assert_eq!(rows as u64, rows_before);
        assert!(snap.chunks.iter().any(|c| c.tier() == Tier::Cold));
        // flush everything sealed at shutdown
        tables.seal_all();
        tiering.flush_all(&tables, &metrics);
        assert!(tables.trades.stats().sealed_chunks > 0);

        // 2. restart: restore, then replay the same LSNs → nothing is duplicated
        let mut tables2 = Tables::new(false);
        let mut tiering2 = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        let st = tiering2.restore(&tables2).unwrap();
        assert!(st.segments >= 2, "{st:?}");
        assert_eq!(st.bad_files, 0);
        let restored = total_rows(&tables2);
        assert_eq!(restored, rows_before);
        let mut lsn = 1u64;
        let mut i = 0;
        while i < n {
            tables2.set_lsn(lsn);
            tables2.ingest(&trades(i, 1_000.min(n - i)));
            i += 1_000;
            lsn += 1;
        }
        assert_eq!(
            total_rows(&tables2),
            rows_before,
            "replayed LSNs must be skipped"
        );
        // 3. new frames after recovery are appended normally
        tables2.set_lsn(lsn);
        tables2.ingest(&trades(n, 10));
        assert_eq!(total_rows(&tables2), rows_before + 10);
        let snap = tables2.trades.snapshot();
        let rows: usize = snap.chunks.iter().map(ChunkRef::num_rows).sum();
        assert_eq!(rows as u64, rows_before + 10);
    }

    /// A quiet partition must not keep the whole log alive: under WAL pressure its open chunk
    /// is sealed and persisted although the hot window would keep it, and the checkpoint moves.
    #[test]
    fn wal_pressure_persists_the_partitions_that_hold_the_log_back() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Metrics::default();
        let mut tables = Tables::new(false);
        // one symbol trades once at LSN 1; the other keeps trading from LSN 150 on
        tables.set_lsn(1);
        let mut first = trades(0, 2);
        first.trades.retain(|t| t.symbol.0 == 0);
        tables.ingest(&first);
        for lsn in 150..200u64 {
            tables.set_lsn(lsn);
            let mut b = trades(lsn as usize * 2, 2);
            b.trades.retain(|t| t.symbol.0 == 1);
            tables.ingest(&b);
        }
        let quiet = tables.trades.partition_for(0);
        assert_eq!(
            tables.trades.with_partition(quiet, |p| p.checkpoint_lsn()),
            1
        );
        let mut tiering = Tiering::new(
            Some(dir.path().to_path_buf()),
            4, // hot window: without pressure nothing here would be persisted
            usize::MAX,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        assert!(tiering.tick(&tables, &metrics).is_none());
        // a checkpoint that became due after the tick that wrote the segments is still handed
        // out (relief can span many ticks, and the interval may not have elapsed on the first)
        {
            let mut slow = Tiering::new(
                Some(dir.path().join("slow")),
                4,
                usize::MAX,
                Duration::from_secs(10),
                Duration::from_millis(150),
            );
            let mut t2 = Tables::new(false);
            for lsn in 1..5u64 {
                t2.set_lsn(lsn);
                t2.ingest(&trades(lsn as usize * 2, 2));
            }
            assert!(slow.tick(&t2, &metrics).is_none());
            slow.last_truncate = Some(Instant::now());
            assert!(slow.relieve_wal(&t2, 100) >= 1);
            assert!(
                slow.tick(&t2, &metrics).is_none(),
                "persisted, but not due yet"
            );
            std::thread::sleep(Duration::from_millis(200));
            assert!(
                slow.tick(&t2, &metrics).is_some(),
                "due now, although nothing was persisted in this tick"
            );
        }
        // the log is over budget and its second segment starts at LSN 100
        assert_eq!(
            tiering.relieve_wal(&tables, 100),
            1,
            "only the quiet partition"
        );
        let ckpt = tiering.tick(&tables, &metrics).expect("checkpoint moved");
        assert!(ckpt > 1, "{ckpt}");
        assert_eq!(tiering.moves_to_warm, 1);
        // asking again while nothing new is below the target does nothing
        assert_eq!(tiering.relieve_wal(&tables, 100), 0);
        // the busy partition was left alone
        let busy = tables.trades.partition_for(1);
        assert_eq!(tables.trades.with_partition(busy, |p| p.sealed().len()), 0);
    }

    /// Segments written with another column set (or unreadable ones) stop the start: skipping
    /// them would hide their rows and later overwrite the files.
    #[test]
    fn restore_refuses_segments_it_cannot_use() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Metrics::default();
        let mut tables = Tables::new(false);
        let n = CHUNK_ROWS * 2 + 10;
        let mut i = 0;
        let mut lsn = 1;
        while i < n {
            tables.set_lsn(lsn);
            tables.ingest(&trades(i, 1_000.min(n - i)));
            i += 1_000;
            lsn += 1;
        }
        tables.seal_all();
        let mut tiering = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        tiering.flush_all(&tables, &metrics);
        let new = || {
            Tiering::new(
                Some(dir.path().to_path_buf()),
                1,
                0,
                Duration::from_secs(10),
                Duration::ZERO,
            )
        };
        // same schema: fine
        assert!(new().restore(&Tables::new(false)).unwrap().segments >= 1);
        // a user table that now claims the name `trades`-like files with other columns
        let mut other = Tables::new(false);
        let schema = ankaa_store::Schema::new(vec![
            ankaa_store::Field::new("ts", ankaa_store::ColumnType::TimestampNs),
            ankaa_store::Field::new("v", ankaa_store::ColumnType::Int64),
        ])
        .unwrap();
        other.add_user_table("telemetry", schema, None, 0);
        std::fs::rename(dir.path().join("trades"), dir.path().join("telemetry")).unwrap();
        // the files still say `trades` in their metadata: they belong to a configured table
        // with the trades schema, so they are still usable
        assert!(new().restore(&other).is_ok());
        // an unreadable segment file
        let seg = std::fs::read_dir(dir.path().join("telemetry"))
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .path();
        std::fs::write(&seg, b"not a segment").unwrap();
        let err = new().restore(&Tables::new(false)).unwrap_err().to_string();
        assert!(
            err.contains("cannot be used") && err.contains("unreadable"),
            "{err}"
        );
    }

    /// Crash (no `seal_all`/`flush_all`) after a checkpoint: WAL records that straddled a chunk
    /// boundary must be replayed exactly once (M1 from the QA review). Records are 700 rows so
    /// they do straddle 65,536-row chunks without record-aligned sealing.
    #[test]
    fn crash_recovery_replays_straddling_records_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Metrics::default();
        let mut tables = Tables::new(false);
        let per_record = 700usize;
        let records = 4 * CHUNK_ROWS / per_record + 40; // ~2 sealed chunks per symbol + open
        let mut frames: Vec<(u64, EventBatch)> = Vec::new();
        for i in 0..records {
            frames.push(((i + 1) as u64, trades(i * per_record, per_record)));
        }
        for (lsn, b) in &frames {
            tables.set_lsn(*lsn);
            tables.ingest(b);
        }
        let rows_before = total_rows(&tables);
        let mut tiering = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        let ck = tiering.tick(&tables, &metrics).expect("checkpoint");
        // crash: drop `tables` without sealing the open chunks
        drop(tables);
        let mut tables2 = Tables::new(false);
        let mut tiering2 = Tiering::new(
            Some(dir.path().to_path_buf()),
            1,
            0,
            Duration::from_secs(10),
            Duration::ZERO,
        );
        let st = tiering2.restore(&tables2).unwrap();
        assert!(st.segments > 0);
        // the WAL kept every record from the checkpoint LSN on; replay them all (a real WAL
        // replays from its oldest surviving segment, which contains at least these)
        for (lsn, b) in &frames {
            if *lsn >= ck.saturating_sub(1) {
                tables2.set_lsn(*lsn);
                tables2.ingest(b);
            }
        }
        assert_eq!(
            total_rows(&tables2),
            rows_before,
            "rows lost or duplicated across the crash"
        );
        // every row is present exactly once: seq values are unique and dense
        let snap = tables2.trades.snapshot();
        let mut seqs: Vec<i64> = Vec::new();
        for c in &snap.chunks {
            match c {
                ChunkRef::Hot(h) => seqs.extend_from_slice(h.column(5).unwrap().as_i64().unwrap()),
                ChunkRef::Encoded(e) => {
                    seqs.extend_from_slice(e.decode(&[5]).unwrap()[0].as_i64().unwrap());
                }
            }
        }
        seqs.sort_unstable();
        seqs.dedup();
        assert_eq!(seqs.len() as u64, rows_before);
    }
}
