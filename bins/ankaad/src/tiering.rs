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
//! On start, [`Tiering::restore`] lists the segments and re-attaches them as cold chunks, then
//! the WAL replay skips records whose LSN is already covered by a segment.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ankaa_store::{ChunkId, ChunkRef, EncodedChunk as _, Table, Tier, TierStats};
use ankaa_tier::{ColdChunk, Encoder};
use tracing::{info, warn};

use crate::metrics::Metrics;
use crate::tables::Tables;

/// Key of a sealed chunk across tables.
type Key = (String, u32, ChunkId);

/// Tiering state.
pub struct Tiering {
    enc: Arc<Encoder>,
    checkpoint_dir: Option<PathBuf>,
    hot_sealed: usize,
    warm_budget_bytes: usize,
    budget: Duration,
    checkpoint_interval: Duration,
    last_truncate: Option<Instant>,
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
            cold: HashMap::new(),
            forced: std::collections::HashSet::new(),
            retention: HashMap::new(),
            graveyard: Vec::new(),
            checkpoint_pending: false,
            moves_to_warm: 0,
            moves_to_cold: 0,
            checkpoints: 0,
            errors: 0,
        }
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
    /// Fails, without attaching anything that could be overwritten later, when a segment cannot
    /// be read or was written with a different column set than the table has now. Starting
    /// without those files would hide their rows and then reuse their chunk ids, replacing the
    /// old files with new data. The message names the files; the operator decides (restore the
    /// previous table definition, or move the files out of the directory).
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
        let all = tables.persistent();
        let names: HashMap<&str, &Table> = all.iter().copied().collect();
        for seg in &segments {
            let meta = seg.meta();
            if let Some(table) = names.get(meta.table.as_str())
                && *table.schema() != meta.schema
            {
                problems.push(format!(
                    "{}: written with a different column set than table {:?} has now",
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
                "{} segment file(s) under {} cannot be used:\n  {shown}\nThere is no schema migration yet. Restore the previous table definition, or move these files out of the directory to start without their rows.",
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
            table.with_partition(part, |p| p.restore_sealed(ChunkRef::Encoded(seg.clone())));
            st.segments += 1;
            st.rows += meta.num_rows as u64;
            st.max_lsn = st.max_lsn.max(meta.lsn_range.1);
            self.cold
                .insert((meta.table.clone(), meta.partition, meta.id), seg);
        }
        Ok(st)
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
        // 1. hot → warm (+ segment)
        'outer: for (name, table) in tables.persistent() {
            let mut n_parts = table.num_partitions();
            let mut idx = 0;
            while idx < n_parts {
                if t0.elapsed() > self.budget {
                    break 'outer;
                }
                // pick the oldest hot sealed chunk beyond the hot window, under the lock
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
                    sealed
                        .iter()
                        .find(|c| c.tier() == Tier::Hot)
                        .and_then(|c| c.as_hot().cloned())
                        .map(|c| (p.key(), c))
                });
                let Some((key, chunk)) = candidate else {
                    idx += 1;
                    n_parts = table.num_partitions();
                    continue;
                };
                match self.enc.encode_warm(name, key, &chunk) {
                    Ok(warm) => {
                        let mut cold_ref = None;
                        if let Some(dir) = &self.checkpoint_dir {
                            match self.enc.write_cold(dir, &warm) {
                                Ok(c) => cold_ref = Some(c),
                                Err(e) => {
                                    self.errors += 1;
                                    warn!(table = name, partition = key, chunk = chunk.id().0, error = %e, "segment write failed; chunk stays hot");
                                    idx += 1;
                                    continue;
                                }
                            }
                        }
                        let (_, last_lsn) = chunk.lsn_range();
                        let last_rows = chunk.last_lsn_rows();
                        let id = chunk.id();
                        let replaced = table.with_partition(idx, |p| {
                            let ok = p.replace_sealed(id, ChunkRef::Encoded(warm.clone()));
                            if ok && cold_ref.is_some() {
                                p.set_persisted_through_lsn(last_lsn, last_rows);
                            }
                            ok
                        });
                        if replaced {
                            self.moves_to_warm += 1;
                            if let Some(c) = cold_ref {
                                self.cold.insert((name.to_owned(), key, id), c);
                                persisted_any = true;
                            }
                        }
                    }
                    Err(e) => {
                        self.errors += 1;
                        warn!(table = name, partition = key, chunk = chunk.id().0, error = %e, "encode failed; chunk stays hot");
                        idx += 1;
                    }
                }
            }
        }
        // 2. warm → cold when over budget (only chunks that have a segment)
        if self.checkpoint_dir.is_some() {
            let mut warm_bytes: usize = tables
                .persistent()
                .iter()
                .map(|(_, t)| t.tier_stats().warm_bytes)
                .sum();
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
        let mut total = TierStats::default();
        for (_, t) in tables.persistent() {
            total.merge(&t.tier_stats());
        }
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

    /// Persist everything sealed (shutdown): hot window ignored, no time budget.
    pub fn flush_all(&mut self, tables: &Tables, metrics: &Metrics) -> Option<u64> {
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
