//! `ankaad serve`: live ingest → tables → snapshot swaps → Arrow Flight + /metrics, with an
//! optional write-ahead log (recovery on start, group commit while running) and REST depth
//! snapshots for exact order books.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ankaa_core::{Config, Secret};
use ankaa_feeds::binance::{BinanceJsonDecoder, BinanceSbeDecoder};
use ankaa_feeds::capture::{Frame, FrameKind};
use ankaa_feeds::live::{
    binance_url, depth_symbols, fetch_binance_depth_snapshot, now_ns, run_ws_source_with_key,
};
use ankaa_flight::{AnkaaFlight, TlsFiles};
use ankaa_ingest::{DecodeError, Decoder, EventBatch, SymbolId};
use ankaa_sql::{ChunkTable, register_functions};
use datafusion::prelude::SessionContext;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::map_pool::MapPool;
use crate::metrics::{Metrics, serve_metrics};
use crate::source_state::SourceStates;
use crate::tables::Tables;
use crate::tiering::Tiering;
use crate::wal_writer::{self, WalHandle};
use ankaa_core::config::{ColumnKind, SourceFormat, SourceKind};
use ankaa_ingest::generic::{RowMapper, TsUnit};

/// Dictionaries larger than this are saved and republished at most once a second while they
/// grow (both rewrite the whole dictionary); smaller ones follow every snapshot tick.
const DICT_THROTTLE_ABOVE: usize = 10_000;

/// Options for `serve`: the validated [`Config`] plus run-time only settings.
#[derive(Clone)]
pub struct ServeOpts {
    /// Validated configuration (file + CLI overrides).
    pub config: Config,
    /// Stop after this long (None = run until Ctrl-C).
    pub duration: Option<Duration>,
    /// Exchange API key for the SBE endpoint (sent as `X-MBX-APIKEY`; never logged).
    pub api_key: Option<Secret>,
}

impl std::fmt::Debug for ServeOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeOpts")
            .field("config", &self.config)
            .field("duration", &self.duration)
            .field("api_key", &self.api_key.as_ref().map(|_| "***"))
            .finish()
    }
}

/// Everything the ingest step needs; shared by recovery and live ingest.
struct Ingest {
    tables: Tables,
    dec: BinanceJsonDecoder,
    sbe: BinanceSbeDecoder,
    batch: EventBatch,
    metrics: Arc<Metrics>,
    need_snapshot: Vec<SymbolId>,
    /// Source ids and read positions.
    states: SourceStates,
    /// LSN of the frame being handled (0 = not in the WAL).
    lsn: u64,
    /// True while the WAL is being replayed at start.
    recovering: bool,
    /// Set when a frame of a user source could not be logged: read positions reported after
    /// it would skip that message on the next start, so they are ignored until a restart
    /// (the source then re-reads from the last good position).
    positions_frozen: bool,
    /// LSNs of the `connect` frames still in the log, oldest first (see
    /// [`Self::truncation_point`]).
    connects: Vec<u64>,
    /// The order books must be re-based on fresh snapshots: a snapshot or `connect` frame did
    /// not reach the log (the books would wait for it forever), or the log has outgrown its
    /// budget and can only be truncated behind a new starting point for the books.
    rebase_books: bool,
    /// LSNs of the Sparkplug definitions frames still in the log, oldest first (see
    /// [`Self::truncation_point`]).
    sp_marks: Vec<u64>,
    /// [`Tables::sparkplug_generation`] when the last definitions frame was handled: a new
    /// frame is due once the definitions have changed.
    sp_marked_gen: u64,
    /// Counters of the frames handled since the last [`Self::flush_tally`]: plain integers in
    /// the ingest thread, published to the shared atomics once per batch rather than with
    /// three atomic read-modify-writes per message.
    tally: Tally,
}

/// Per-batch counters (see [`Ingest::tally`]).
#[derive(Default)]
struct Tally {
    frames: u64,
    user_rows: u64,
    user_rejected: u64,
}

impl Ingest {
    /// Handle one frame that the WAL numbered `lsn` (0 = not in the WAL).
    fn handle_frame_lsn(&mut self, frame: &Frame, lsn: u64) {
        self.handle_frame_lsn_batched(frame, lsn);
        self.flush_tally();
    }

    /// [`Self::handle_frame_lsn`] without publishing the counters: for frames handled in a
    /// batch, which calls [`Self::flush_tally`] once at its end.
    fn handle_frame_lsn_batched(&mut self, frame: &Frame, lsn: u64) {
        self.tables.set_lsn(lsn);
        self.lsn = lsn;
        self.handle_frame(frame);
    }

    /// Count what one user message did.
    fn note_user(&mut self, source_id: u16, done: crate::tables::UserIngest) {
        self.tally.user_rows += done.rows as u64;
        if done.rejected > 0 {
            self.tally.user_rejected += done.rejected as u64;
            if let Some(e) = done.error {
                tracing::debug!(source_id, rejected = done.rejected, error = %e, "user rows rejected");
            }
        }
    }

    /// Apply a job a mapping worker returned, frame by frame in log order: mapped user messages
    /// through [`Tables::apply_mapped`], every other frame as [`Self::handle_frame`] would; the
    /// used buffers go back to the worker.
    fn apply_mapped_job(&mut self, pool: &crate::map_pool::MapPool, job: crate::map_pool::Done) {
        let crate::map_pool::Done {
            worker,
            frames,
            first_lsn,
            step,
            mapped,
            ..
        } = job;
        let mut lsn = first_lsn;
        for (frame, m) in frames.iter().zip(mapped) {
            match (m, ankaa_feeds::capture::split_user_payload(&frame.payload)) {
                (Some(result), Some((source_id, _))) => {
                    self.tally.frames += 1;
                    self.tables.set_lsn(lsn);
                    self.lsn = lsn;
                    match result {
                        Ok(mut m) => {
                            let done =
                                self.tables
                                    .apply_mapped(source_id, frame.recv_ns, Ok(&mut m));
                            self.note_user(source_id, done);
                            pool.recycle(worker, source_id, m);
                        }
                        Err(e) => {
                            let done = self.tables.apply_mapped(source_id, frame.recv_ns, Err(e));
                            self.note_user(source_id, done);
                        }
                    }
                }
                _ => self.handle_frame_lsn_batched(frame, lsn),
            }
            lsn += step;
        }
        self.flush_tally();
    }

    /// Publish the counters gathered since the last call.
    fn flush_tally(&mut self) {
        let t = std::mem::take(&mut self.tally);
        if t.frames > 0 {
            self.metrics.frames.fetch_add(t.frames, Ordering::Relaxed);
        }
        if t.user_rows > 0 {
            self.metrics
                .user_rows
                .fetch_add(t.user_rows, Ordering::Relaxed);
            self.metrics
                .events
                .fetch_add(t.user_rows, Ordering::Relaxed);
        }
        if t.user_rejected > 0 {
            self.metrics
                .user_rejected
                .fetch_add(t.user_rejected, Ordering::Relaxed);
        }
    }

    /// A frame the WAL did not take (stalled disk, or larger than a record may be): it is
    /// dropped, not ingested. The WAL handle has counted it. Returns `true` when the server
    /// has to stop: a message of a user source was lost, and the only way to get it back is to
    /// restart and let the source read again from its last saved position. From here on no
    /// user frame is ingested and no position is kept, so that re-read finds the table where
    /// the lost message belongs.
    fn frame_not_logged(&mut self, frame: &Frame) -> bool {
        match frame.kind {
            FrameKind::User => {
                let first = !self.positions_frozen;
                self.positions_frozen = true;
                first
            }
            // the books would wait forever for a snapshot or a connect that never arrived
            FrameKind::Snapshot => {
                self.rebase_books = true;
                false
            }
            FrameKind::Control if frame.payload.as_ref() == b"connect" => {
                self.rebase_books = true;
                false
            }
            _ => false,
        }
    }

    /// True for frames that must be discarded once a user-source message has been lost.
    fn discard_after_loss(&self, frame: &Frame) -> bool {
        self.positions_frozen
            && (frame.kind == FrameKind::User
                || (frame.kind == FrameKind::Control
                    && ankaa_feeds::capture::split_source_state(&frame.payload).is_some()))
    }

    /// Where the WAL may be truncated, given the checkpoint `ckpt` (every row below it is in a
    /// segment). `None` = not at all for now.
    ///
    /// Trades and user rows are a pure function of their own frame, so `ckpt` is enough for
    /// them. Book rows (`book_deltas`, `quotes`) are not: which diffs apply, and the top of
    /// book they produce, depend on the snapshot and the diffs since the connection began. To
    /// regenerate the unpersisted ones after a crash the replay has to start at that
    /// connection, so the log is kept back to the last `connect` at or before `ckpt`.
    ///
    /// Sparkplug rows depend on the BIRTH that defined their metrics, which may be days older
    /// than any checkpoint: with Sparkplug definitions in use, the log is kept back to the last
    /// definitions frame at or before `ckpt` (a replay from there knows them all).
    fn truncation_point(&mut self, ckpt: u64) -> Option<u64> {
        let mut at = ckpt;
        if self.tables.book_sync_active() {
            let base = self.connects.iter().rev().find(|c| **c <= ckpt).copied()?;
            self.connects.retain(|c| *c >= base);
            at = base;
        }
        if self.tables.sparkplug_active() {
            let mark = self.sp_marks.iter().rev().find(|m| **m <= ckpt).copied()?;
            self.sp_marks.retain(|m| *m >= mark);
            at = at.min(mark);
        }
        Some(at)
    }

    fn handle_frame(&mut self, frame: &Frame) {
        self.tally.frames += 1;
        match frame.kind {
            FrameKind::Control => {
                if frame.payload.as_ref() == b"connect" {
                    self.metrics.connects.fetch_add(1, Ordering::Relaxed);
                    // a new connection is a new sequence: no gap accounting across it, and books
                    // wait for fresh snapshots instead of detecting a gap
                    self.dec.reset_sequences();
                    self.sbe.reset_sequences();
                    self.tables.on_reconnect();
                    if self.lsn != 0 && self.connects.last().is_none_or(|l| *l < self.lsn) {
                        self.connects.push(self.lsn);
                    }
                } else if let Some(entries) =
                    ankaa_feeds::capture::split_sparkplug_defs(&frame.payload)
                {
                    // the definitions as of this point of the log: what a replay that starts
                    // here needs to read the Sparkplug DATA messages after it
                    match self.tables.restore_sparkplug(&entries) {
                        Ok(()) => {
                            if self.lsn != 0 && self.sp_marks.last().is_none_or(|l| *l < self.lsn) {
                                self.sp_marks.push(self.lsn);
                            }
                            self.sp_marked_gen = self.tables.sparkplug_generation();
                        }
                        Err(e) => {
                            self.metrics.frames_bad.fetch_add(1, Ordering::Relaxed);
                            warn!(error = %e, "sparkplug definitions frame rejected");
                        }
                    }
                } else if let Some((id, name)) =
                    ankaa_feeds::capture::split_source_id(&frame.payload)
                {
                    // the log's own numbering of sources wins over this node's config order
                    if let Some(other) = self.states.adopt(id, name) {
                        self.tables.swap_sources(id, other);
                        info!(source = name, id, "source id taken from the log");
                    }
                } else if let Some((id, state)) =
                    ankaa_feeds::capture::split_source_state(&frame.payload)
                {
                    // a source reports how far it has read; the position becomes permanent
                    // once the frames before it are durable (a replayed one already is)
                    if self.recovering {
                        self.states.note_recovered(id, state, self.lsn);
                    } else if !self.positions_frozen {
                        self.states.note(id, state, self.lsn);
                    }
                }
            }
            FrameKind::User => {
                let Some((source_id, msg)) =
                    ankaa_feeds::capture::split_user_payload(&frame.payload)
                else {
                    self.metrics.frames_bad.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                let done = self.tables.ingest_user(source_id, msg, frame.recv_ns);
                self.note_user(source_id, done);
            }
            FrameKind::Snapshot => {
                self.batch.clear();
                match self
                    .dec
                    .decode_snapshot(&frame.payload, frame.recv_ns, &mut self.batch)
                {
                    Ok(_) => self.apply_batch(frame.recv_ns),
                    Err(e) => {
                        self.metrics.frames_bad.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %e, "snapshot frame rejected");
                    }
                }
            }
            FrameKind::Text | FrameKind::Binary => {
                self.batch.clear();
                let decoded = if frame.kind == FrameKind::Binary {
                    self.sbe.decode(
                        &frame.payload,
                        frame.recv_ns,
                        self.dec.symbols_mut(),
                        &mut self.batch,
                    )
                } else {
                    self.dec
                        .decode(&frame.payload, frame.recv_ns, &mut self.batch)
                };
                match decoded {
                    Ok(_) => self.apply_batch(frame.recv_ns),
                    Err(DecodeError::Ignored) => {
                        self.metrics.frames_ignored.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        self.metrics.frames_bad.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %e, "frame rejected");
                    }
                }
            }
        }
    }

    fn apply_batch(&mut self, recv_ns: i64) {
        let out = self.tables.ingest(&self.batch);
        self.metrics
            .events
            .fetch_add(out.events as u64, Ordering::Relaxed);
        self.metrics
            .dropped
            .fetch_add(out.dropped as u64, Ordering::Relaxed);
        let lat = u64::try_from((now_ns() - recv_ns).max(0)).unwrap_or(0);
        self.metrics
            .last_ingest_latency_ns
            .store(lat, Ordering::Relaxed);
        self.metrics.ingest_latency.record(lat);
        self.metrics
            .book_crossed
            .store(self.tables.crossed_messages, Ordering::Relaxed);
        self.metrics
            .book_stale
            .store(self.tables.stale_messages(), Ordering::Relaxed);
        if !out.need_snapshot.is_empty() {
            self.metrics
                .book_resyncs
                .fetch_add(out.need_snapshot.len() as u64, Ordering::Relaxed);
            self.need_snapshot.extend(out.need_snapshot);
        }
    }
}

/// Run the server until Ctrl-C or `duration`.
#[allow(clippy::too_many_lines)]
pub async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let cfg = &opts.config;
    cfg.validate()?;
    cfg.check_resources()?;
    let token: Option<String> = cfg.server.token.as_ref().map(|t| t.expose().to_owned());
    let tls = match (&cfg.server.tls_cert, &cfg.server.tls_key) {
        (Some(cert), Some(key)) => Some(TlsFiles {
            cert: cert.clone(),
            key: key.clone(),
            client_ca: cfg.server.tls_client_ca.clone(),
        }),
        _ => None,
    };
    let started = Instant::now();
    let metrics = Arc::new(Metrics::default());
    // Query runtime: bounded memory pool so one query cannot take the ingest process down.
    let query_mem = usize::try_from(cfg.resources.query_memory_mib.saturating_mul(1024 * 1024))
        .unwrap_or(usize::MAX);
    // No spilling to disk: an operator that needs more than the pool fails. (The default would
    // write up to 100 GB of temporary files under the system temp directory.)
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_limit(query_mem, 1.0)
        .with_disk_manager_builder(
            datafusion::execution::disk_manager::DiskManagerBuilder::default()
                .with_mode(datafusion::execution::disk_manager::DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let ctx = Arc::new(ankaa_sql::session(
        datafusion::prelude::SessionConfig::new(),
        runtime,
    ));

    let mut ingest = Ingest {
        tables: Tables::new(cfg.feed.depth_snapshots),
        dec: BinanceJsonDecoder::new(),
        sbe: BinanceSbeDecoder::new(),
        batch: EventBatch::with_capacity(1024),
        metrics: metrics.clone(),
        need_snapshot: Vec::new(),
        // kept next to the WAL when there is one (positions follow WAL durability), else
        // next to the segments
        states: SourceStates::load(
            cfg.storage
                .wal_dir
                .as_deref()
                .or(cfg.storage.checkpoint_dir.as_deref()),
        )?,
        lsn: 0,
        recovering: false,
        positions_frozen: false,
        connects: Vec::new(),
        rebase_books: false,
        tally: Tally::default(),
        sp_marks: Vec::new(),
        sp_marked_gen: 0,
    };

    // User-defined tables, their sources and the continuous queries (before recovery so that
    // replayed user frames and rows find their tables).
    let standby = cfg.replication.standby_of.is_some();
    for t in &cfg.tables {
        let fields = t
            .columns
            .iter()
            .map(|c| {
                let ty = match c.kind {
                    ColumnKind::TimestampNs => ankaa_store::ColumnType::TimestampNs,
                    ColumnKind::Fixed8 => ankaa_store::ColumnType::FixedPoint { scale: 8 },
                    ColumnKind::Int64 => ankaa_store::ColumnType::Int64,
                    ColumnKind::Dict => ankaa_store::ColumnType::DictU32,
                    ColumnKind::U8 => ankaa_store::ColumnType::U8,
                    ColumnKind::Float64 => ankaa_store::ColumnType::Float64,
                };
                if c.nullable {
                    ankaa_store::Field::nullable(c.name.clone(), ty)
                } else {
                    ankaa_store::Field::new(c.name.clone(), ty)
                }
            })
            .collect();
        let schema = ankaa_store::Schema::new(fields)?;
        let lateness = i64::try_from(t.max_lateness_ms)
            .unwrap_or(i64::MAX / 1_000_000)
            .saturating_mul(1_000_000);
        ingest
            .tables
            .add_user_table(&t.name, schema, t.partition_by.as_deref(), lateness);
    }
    let mut source_transports = Vec::new();
    for s in &cfg.sources {
        let (ti, t) = cfg
            .tables
            .iter()
            .enumerate()
            .find(|(_, t)| t.name == s.table)
            .ok_or_else(|| anyhow::anyhow!("source {} names unknown table {}", s.name, s.table))?;
        let schema = ingest.tables.user[ti].table.schema().clone();
        let csv = s.format == SourceFormat::Csv;
        let unit = match s.ts_unit {
            ankaa_core::config::TimeUnit::Seconds => TsUnit::Seconds,
            ankaa_core::config::TimeUnit::Millis => TsUnit::Millis,
            ankaa_core::config::TimeUnit::Micros => TsUnit::Micros,
            ankaa_core::config::TimeUnit::Nanos => TsUnit::Nanos,
            ankaa_core::config::TimeUnit::Rfc3339 => TsUnit::Rfc3339,
        };
        let map: std::collections::HashMap<String, String> =
            s.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let mapper = RowMapper::new(
            schema,
            &map,
            csv,
            unit,
            s.ts_from_receive,
            t.partition_by.as_deref(),
        )
        .map_err(|e| anyhow::anyhow!("source {}: {e}", s.name))?
        .with_max_dict_entries(t.max_dict_entries)
        .with_topic_prefix(s.kind == SourceKind::Mqtt)
        .with_rows(s.rows.as_deref())
        .and_then(|m| m.with_sparkplug(s.format == SourceFormat::Sparkplug))
        .map_err(|e| anyhow::anyhow!("source {}: {e}", s.name))?;
        // ids are stable across restarts and config edits: WAL frames name their source by id
        let id = ingest.states.id_for(&s.name)?;
        ingest.tables.add_source(id, &s.name, ti, mapper, csv);
        let transport = match s.kind {
            SourceKind::File => {
                ankaa_feeds::sources::Transport::File(std::path::PathBuf::from(&s.url))
            }
            SourceKind::Nats => ankaa_feeds::sources::Transport::Nats {
                url: s.url.clone(),
                subject: s.subject.clone(),
            },
            SourceKind::Kafka => ankaa_feeds::sources::Transport::Kafka {
                bootstrap: s.url.clone(),
                topic: s.subject.clone(),
            },
            SourceKind::Mqtt => ankaa_feeds::sources::Transport::Mqtt {
                url: s.url.clone(),
                topic: s.subject.clone(),
            },
        };
        let opts = ankaa_feeds::sources::SourceOpts {
            max_message_bytes: s.max_message_bytes,
            start_earliest: s.start == ankaa_core::config::SourceStart::Earliest,
            resume: None, // filled in after recovery
            client_id: Some(format!("ankaa-{}", s.name)),
            tls: ankaa_feeds::sources::TlsFiles {
                ca: s.tls_ca.clone(),
                cert: s.tls_cert.clone(),
                key: s.tls_key.clone(),
            },
            sparkplug_rebirth: s.sparkplug_rebirth,
        };
        // a bad certificate file stops the start, with the reason, rather than the source later
        if s.kind == SourceKind::Mqtt && s.url.starts_with("mqtts://") {
            ankaa_feeds::sources::mqtt::tls_config(&opts.tls)
                .map_err(|e| anyhow::anyhow!("source {}: TLS: {e}", s.name))?;
        }
        source_transports.push((id, s.name.clone(), transport, opts));
    }
    // validate the continuous queries now (fail fast); they are attached after recovery
    crate::continuous::Continuous::new(&cfg.continuous, &ingest.tables.schemas())?;
    if !cfg.tables.is_empty() || !cfg.continuous.is_empty() {
        info!(
            tables = cfg.tables.len(),
            sources = cfg.sources.len(),
            continuous = cfg.continuous.len(),
            standby,
            "user tables and continuous queries"
        );
    }
    if !cfg.feed.enabled && cfg.sources.is_empty() && !standby {
        warn!(
            "no feed and no [[sources]]: nothing is ingested; the server serves the data it has stored"
        );
    }

    // Tiering + checkpoints: re-attach persisted segments before replaying the WAL.
    let mut tiering = Tiering::new(
        cfg.storage.checkpoint_dir.clone(),
        cfg.storage.hot_sealed_chunks,
        usize::try_from(cfg.storage.warm_budget_mib.saturating_mul(1024 * 1024))
            .unwrap_or(usize::MAX),
        Duration::from_millis(
            cfg.storage.snapshot_ms * u64::from(cfg.storage.tier_budget_pct) / 100,
        ),
        Duration::from_secs(cfg.storage.checkpoint_interval_s),
    )
    .with_retention(retention_windows(cfg));
    let mut symbols_saved = 0usize;
    let mut dict_values_saved = 0usize;
    let mut restored_max_lsn = 0u64;
    if tiering.checkpoint_dir().is_some() {
        let t0 = Instant::now();
        symbols_saved = tiering.load_symbols(ingest.dec.symbols_mut())?;
        // user-table dictionaries: segments and truncated WAL records refer to their ids
        dict_values_saved = tiering.load_dictionaries(&mut ingest.tables)?;
        let st = tiering.restore(&ingest.tables)?;
        restored_max_lsn = st.max_lsn;
        info!(
            segments = st.segments,
            rows = st.rows,
            max_lsn = st.max_lsn,
            bad_files = st.bad_files,
            evolved = st.evolved,
            symbols = symbols_saved,
            dictionary_values = dict_values_saved,
            ms = t0.elapsed().as_millis(),
            "segments restored"
        );
    }

    // Durability: recover, then start the WAL owner thread.
    let wal: Option<WalHandle> = match &cfg.storage.wal_dir {
        Some(dir) => {
            let t0 = Instant::now();
            ingest.recovering = true;
            ingest.tables.replaying = true;
            let n = wal_writer::recover(dir, |frame, lsn| {
                ingest.handle_frame_lsn_batched(&frame, lsn);
            })?;
            ingest.flush_tally();
            ingest.recovering = false;
            ingest.tables.replaying = false;
            metrics.wal_recovered_frames.store(n, Ordering::Relaxed);
            info!(
                frames = n,
                ms = t0.elapsed().as_millis(),
                rows = ingest.tables.trades.stats().rows + ingest.tables.deltas.stats().rows,
                "wal recovery done"
            );
            // Books recovered from the WAL are only as good as the frames; force fresh snapshots live.
            ingest.need_snapshot.clear();
            ingest.tables.set_lsn(0);
            ingest.lsn = 0;
            // LSNs are never reused: start above everything segments and saved positions refer
            // to, even when the log itself holds less (wiped directory, records lost in a
            // power cut after their rows had reached a segment)
            // (a standby numbers nothing itself: its log is a copy of the primary's and it must
            // ask for exactly what follows its own last record)
            let floor = if standby {
                1
            } else {
                restored_max_lsn.max(ingest.states.max_lsn()) + 1
            };
            let w = wal_writer::start(dir, metrics.clone(), floor)?;
            if w.next_lsn() == floor && floor > 1 && n == 0 {
                info!(
                    next_lsn = floor,
                    "wal numbering continues above the persisted data"
                );
            }
            Some(w)
        }
        None => None,
    };
    // Tell the log which id each source has, so that recovery and standbys bind user frames
    // to the same sources whatever their own configuration order is.
    if !standby && let Some(w) = &wal {
        for (id, name, _, _) in &source_transports {
            let frame = Frame {
                recv_ns: now_ns(),
                kind: FrameKind::Control,
                payload: ankaa_feeds::capture::source_id_payload(*id, name),
            };
            if let Some(lsn) = w.push(frame.clone(), &metrics) {
                ingest.handle_frame_lsn(&frame, lsn);
            }
        }
        ingest.tables.set_lsn(0);
        ingest.lsn = 0;
    }
    // positions and ids found during recovery are durable: write them before sources start
    ingest
        .states
        .commit(metrics.wal_durable_lsn.load(Ordering::Relaxed), false);

    // Continuous queries are derived state: rebuild them from what the tables hold now
    // (restored segments + replayed WAL), so results do not depend on when the server stopped.
    ingest.tables.continuous = crate::continuous::Continuous::with_partitioning(
        &cfg.continuous,
        &ingest.tables.schemas(),
        &ingest.tables.partition_columns(),
    )?;
    ingest
        .tables
        .continuous
        .set_lateness(&ingest.tables.lateness());
    if !cfg.continuous.is_empty() {
        let t0 = Instant::now();
        let mut cq = std::mem::take(&mut ingest.tables.continuous);
        let st = {
            let inputs: std::collections::HashMap<&str, &ankaa_store::Table> =
                ingest.tables.persistent().into_iter().collect();
            cq.rebuild(&inputs)
        };
        ingest.tables.continuous = cq;
        if st.bad_chunks > 0 {
            warn!(
                chunks = st.bad_chunks,
                "continuous queries rebuilt without some chunks (undecodable)"
            );
        }
        info!(
            rows = st.rows,
            ms = t0.elapsed().as_millis(),
            "continuous queries rebuilt from the input tables"
        );
    }

    // Long-lived providers: registered once, swapped every snapshot interval.
    let live_trades = Arc::new(ChunkTable::from_snapshot(ingest.tables.trades.snapshot()));
    let live_deltas = Arc::new(ChunkTable::from_snapshot(ingest.tables.deltas.snapshot()));
    let live_quotes = Arc::new(ChunkTable::from_snapshot(ingest.tables.quotes.snapshot()));
    ctx.register_table("trades", live_trades.clone())?;
    ctx.register_table("book_deltas", live_deltas.clone())?;
    ctx.register_table("quotes", live_quotes.clone())?;
    // small tables that are replaced whole: exchanged in place, never deregistered
    let mut published = ankaa_sql::BatchTables::default();
    published.publish(&ctx, "symbols", symbols_batch(&ingest.dec)?)?;
    register_functions(&ctx);
    let mut symbols_registered = ingest.dec.symbols().len();
    let mut live_extra: Vec<(String, Arc<ChunkTable>)> = Vec::new();
    for u in &ingest.tables.user {
        let t = Arc::new(ChunkTable::from_snapshot(u.table.snapshot()));
        ctx.register_table(u.name.as_str(), t.clone())?;
        live_extra.push((u.name.clone(), t));
    }
    for q in ingest.tables.continuous.queries() {
        let t = Arc::new(ChunkTable::from_snapshot(q.out.snapshot()));
        ctx.register_table(q.out.name(), t.clone())?;
        live_extra.push((q.out.name().to_owned(), t));
    }
    published.publish(&ctx, "dictionaries", dictionaries_batch(&ingest.tables)?)?;
    publish_live_cq(&ctx, &mut published, &ingest.tables)?;

    // Flight with graceful shutdown
    let flight_listener = tokio::net::TcpListener::bind(cfg.server.flight_addr).await?;
    info!(addr = %cfg.server.flight_addr, auth = token.is_some(), tls = tls.is_some(), "flight listening");
    let serve_wal = cfg.storage.wal_dir.is_some() && cfg.replication.serve_wal;
    let repl_token: Option<String> = cfg
        .replication
        .token
        .as_ref()
        .map(|t| t.expose().to_owned());
    let durable = metrics.clone();
    let flight = AnkaaFlight::with_metrics(ctx.clone(), token.clone(), metrics.query.clone())
        .with_query_timeout(Duration::from_secs(cfg.server.query_timeout_s))
        .with_max_concurrent_queries(cfg.server.max_concurrent_queries)
        .with_wal_dir(serve_wal.then(|| cfg.storage.wal_dir.clone()).flatten())
        .with_wal_token(repl_token.clone())
        .with_max_wal_streams(cfg.replication.max_standbys)
        // standbys only receive what a power loss can no longer undo here
        .with_wal_durable_lsn(Arc::new(move || {
            durable.wal_durable_lsn.load(Ordering::Relaxed)
        }));
    if serve_wal {
        info!(
            separate_token = repl_token.is_some(),
            max_standbys = cfg.replication.max_standbys,
            "serving the WAL to standbys over Flight (ticket __wal__:<lsn>)"
        );
        if repl_token.is_none() && token.is_some() {
            warn!(
                "replication.token_env is not set: every client holding the query token can read the raw WAL"
            );
        }
    }
    let metrics_token: Option<String> = cfg
        .server
        .metrics_token
        .as_ref()
        .map(|t| t.expose().to_owned())
        .or_else(|| token.clone());
    let (flight_stop_tx, flight_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let mut flight_task = tokio::spawn(async move {
        ankaa_flight::serve(flight, flight_listener, tls.as_ref(), async {
            let _ = flight_stop_rx.await;
        })
        .await
        .map_err(|e| anyhow::anyhow!("flight: {e}"))
    });

    // Optional PostgreSQL wire protocol (read-only)
    let (pg_stop_tx, pg_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let mut pg_task = match (cfg.server.pg_addr, &token) {
        (Some(addr), Some(tok)) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            info!(%addr, "postgres wire listening (read-only, SCRAM)");
            let handlers = Arc::new(ankaa_pg::PgHandlers::new(ctx.clone(), tok));
            Some(tokio::spawn(async move {
                ankaa_pg::serve(listener, handlers, async {
                    let _ = pg_stop_rx.await;
                })
                .await
            }))
        }
        _ => None,
    };

    // Metrics
    let metrics_listener = tokio::net::TcpListener::bind(cfg.server.metrics_addr).await?;
    info!(addr = %cfg.server.metrics_addr, "metrics listening");
    let mut metrics_task = tokio::spawn(serve_metrics(
        metrics_listener,
        metrics.clone(),
        metrics_token,
        started,
    ));

    // Source
    let (tx, mut rx) = mpsc::channel::<Frame>(65_536);
    // frames taken from the channel in one wake-up (reused)
    let mut inbox: Vec<Frame> = Vec::with_capacity(INGEST_BATCH);
    // mapping workers (RFC-0001): only when a source can use them
    let workers = cfg.storage.ingest_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .saturating_sub(2)
            .min(32)
    });
    let mappers = ingest.tables.worker_mappers();
    let mut pool: Option<MapPool> = if workers > 0 && mappers.iter().any(Option::is_some) {
        let p = MapPool::start(workers, &mappers)?;
        info!(workers = p.workers(), "mapping workers started");
        Some(p)
    } else {
        None
    };
    let coalesce = Duration::from_micros(cfg.storage.ingest_coalesce_us);
    let mut last_wake = Instant::now()
        .checked_sub(BUSY_GAP)
        .unwrap_or_else(Instant::now);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let streams: Vec<&str> = cfg.feed.streams.iter().map(String::as_str).collect();
    let depth_syms = depth_symbols(&streams);
    // `file://<capture.ankc>[,<more.ankc>]` serves recorded frames instead of a live stream
    // (offline demos, Flight/Postgres benchmarks); REST snapshot fetching is off in that mode.
    let file_mode = cfg.feed.url.starts_with("file://");
    let fetch_depth = cfg.feed.depth_snapshots && !file_mode && !standby;
    // user sources (NATS / Kafka / file): one task each, all into the same channel
    let mut user_source_tasks = Vec::new();
    if !standby {
        for (id, name, transport, mut opts) in source_transports {
            opts.resume = ingest.states.position(id).map(str::to_owned);
            info!(source = %name, id, resumes = opts.resume.is_some(), "starting user source");
            user_source_tasks.push(tokio::spawn(ankaa_feeds::sources::run_source(
                id,
                transport,
                opts,
                tx.clone(),
                shutdown_rx.clone(),
            )));
        }
    }
    // standby: the primary's WAL records arrive on their own channel, with their LSNs
    let (repl_tx, mut repl_rx) = mpsc::channel::<crate::replication::Replicated>(65_536);
    let mut repl_task: Option<
        tokio::task::JoinHandle<Result<ankaa_feeds::live::SourceStats, String>>,
    > = None;
    let idle_source = |mut stop: watch::Receiver<bool>| async move {
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
        ankaa_feeds::live::SourceStats::default()
    };
    let source_task = if let Some(primary_addr) = cfg.replication.standby_of.clone() {
        // the local log mirrors the primary's, record for record: continue at its end
        let from_lsn = wal.as_ref().map_or(1, WalHandle::next_lsn);
        let primary_token = cfg
            .replication
            .primary_token_env
            .as_deref()
            .and_then(|v| std::env::var(v).ok())
            .filter(|v| !v.is_empty())
            .or_else(|| token.clone());
        let tls_ca = match &cfg.replication.primary_tls_ca {
            Some(p) => Some(std::fs::read(p)?),
            None => None,
        };
        info!(primary = %primary_addr, from_lsn, "standby mode: ingesting the primary's WAL");
        repl_task = Some(tokio::spawn(crate::replication::run_wal_source(
            crate::replication::Primary {
                addr: primary_addr,
                token: primary_token,
                tls_ca,
                tls_domain: cfg.replication.primary_tls_domain.clone(),
            },
            from_lsn,
            repl_tx,
            shutdown_rx.clone(),
            metrics.clone(),
        )));
        tokio::spawn(idle_source(shutdown_rx))
    } else if !cfg.feed.enabled {
        drop(repl_tx);
        info!("feed.enabled = false: only user sources feed this server");
        tokio::spawn(idle_source(shutdown_rx))
    } else if file_mode {
        drop(repl_tx);
        let paths: Vec<std::path::PathBuf> = cfg.feed.url["file://".len()..]
            .split(',')
            .filter(|p| !p.is_empty())
            .map(std::path::PathBuf::from)
            .collect();
        info!(
            files = paths.len(),
            "serving capture files (file:// source)"
        );
        tokio::spawn(run_file_source(paths, tx.clone(), shutdown_rx))
    } else {
        drop(repl_tx);
        let url = binance_url(&cfg.feed.url, &streams);
        tokio::spawn(run_ws_source_with_key(
            url,
            opts.api_key.as_ref().map(|k| k.expose().to_owned()),
            tx.clone(),
            shutdown_rx,
        ))
    };

    // Snapshot fetcher: on connect and on resync requests; results come back as Snapshot frames.
    let fetch_snapshot =
        |symbol: String, tx: mpsc::Sender<Frame>, metrics: Arc<Metrics>, rest: String| {
            tokio::spawn(async move {
                match fetch_binance_depth_snapshot(&rest, &symbol, 1000).await {
                    Ok(frame) => {
                        metrics.snapshots_fetched.fetch_add(1, Ordering::Relaxed);
                        let _ = tx.send(frame).await;
                    }
                    Err(e) => warn!(%symbol, error = %e, "depth snapshot fetch failed"),
                }
            });
        };
    let mut snapshot_backoff: std::collections::HashMap<String, tokio::time::Instant> =
        std::collections::HashMap::new();

    let mut snapshot_tick = tokio::time::interval(Duration::from_millis(cfg.storage.snapshot_ms));
    let mut log_tick = tokio::time::interval(Duration::from_secs(10));
    let deadline = opts.duration.map(|d| tokio::time::Instant::now() + d);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(not(unix))]
    let mut sigterm = {
        struct Never;
        impl Never {
            async fn recv(&mut self) -> Option<()> {
                std::future::pending().await
            }
        }
        Never
    };

    // Why the loop ended, when it was not a signal: the shutdown sequence below still runs (the
    // WAL is committed, sealed data is flushed) before the error is returned.
    let mut exit_err: Option<anyhow::Error> = None;
    let mut dict_values_registered = usize::MAX;
    // a task handle that has completed must not be awaited again during shutdown
    let (mut flight_done, mut pg_done) = (false, false);
    let mut last_dict_save = Instant::now();
    let mut last_dict_register = Instant::now();
    let mut last_wal_check = Instant::now();
    let mut last_rebase: Option<Instant> = None;
    // Sparkplug definitions frames: when the last was written, and the newest checkpoint
    let mut last_sp_mark: Option<Instant> = None;
    // uncompressed rows: bounded in bytes and in age (see `Tiering::relieve_memory`)
    let mut last_mem_check = Instant::now();
    let hot_budget = usize::try_from(cfg.storage.hot_budget_mib.saturating_mul(1024 * 1024))
        .unwrap_or(usize::MAX);
    let max_open_age =
        (cfg.storage.max_open_age_s > 0).then(|| Duration::from_secs(cfg.storage.max_open_age_s));
    let mut last_ckpt = 0u64;
    let sp_mark_every = Duration::from_secs(cfg.storage.checkpoint_interval_s.max(1));
    let wal_budget = cfg.storage.wal_budget_mib.saturating_mul(1024 * 1024);
    if opts.api_key.is_some() && cfg.feed.url.starts_with("ws://") && cfg.feed.enabled && !standby {
        anyhow::bail!("an exchange API key must not be sent over ws:// (clear text); use wss://");
    }
    loop {
        tokio::select! {
            _ = &mut ctrl_c => { info!("ctrl-c"); break; }
            _ = sigterm.recv() => { info!("sigterm"); break; }
            r = &mut flight_task => {
                flight_done = true;
                exit_err = Some(anyhow::anyhow!("flight server stopped unexpectedly: {}", describe_join(r)));
                break;
            }
            r = &mut metrics_task => {
                exit_err = Some(anyhow::anyhow!("metrics server stopped unexpectedly: {}", describe_join(r.map(|()| Ok(())))));
                break;
            }
            r = async { match pg_task.as_mut() { Some(t) => t.await, None => std::future::pending().await } } => {
                pg_done = true;
                exit_err = Some(anyhow::anyhow!("postgres listener stopped unexpectedly: {}", describe_join(r.map(|x| x.map_err(anyhow::Error::from)))));
                break;
            }
            () = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => { info!("duration reached"); break; }
            rec = async { if repl_task.is_some() { repl_rx.recv().await } else { std::future::pending().await } } => {
                let Some(rec) = rec else {
                    // the replication task ended: report why (it only ends on shutdown or on
                    // an error that retrying cannot fix)
                    let why = match repl_task.take() {
                        Some(t) => match t.await {
                            Ok(Err(e)) => e,
                            Ok(Ok(_)) => "the replication task stopped".to_owned(),
                            Err(e) => format!("the replication task failed: {e}"),
                        },
                        None => "the replication task stopped".to_owned(),
                    };
                    exit_err = Some(anyhow::anyhow!("replication stopped: {why}"));
                    break;
                };
                // same LSN in the local log as on the primary, or stop
                if let Some(w) = &wal
                    && let Err(e) = w.push_replicated(rec.lsn, rec.epoch, rec.payload.clone())
                {
                    exit_err = Some(anyhow::anyhow!("replication stopped: {e}; re-seed this standby"));
                    break;
                }
                match ankaa_feeds::capture::decode_frame(&rec.payload, 0) {
                    Ok((frame, _)) => ingest.handle_frame_lsn(&frame, rec.lsn),
                    Err(e) => warn!(lsn = rec.lsn, error = %e, "replication: record is not a frame; kept in the log, not applied"),
                }
                metrics.replication_applied_lsn.store(rec.lsn, Ordering::Relaxed);
            }
            r = async { match pool.as_mut() { Some(p) if p.in_flight() > 0 => p.arrived().await, _ => std::future::pending().await } } => {
                if let Err(e) = r {
                    exit_err = Some(e);
                    break;
                }
                if let Some(p) = pool.as_mut() {
                    while let Some(done) = p.try_next() {
                        ingest.apply_mapped_job(p, done);
                    }
                }
            }
            n = rx.recv_many(&mut inbox, INGEST_BATCH) => {
                if n == 0 { warn!("source ended"); break; }
                let woke = Instant::now();
                let busy = woke.duration_since(last_wake) < BUSY_GAP;
                last_wake = woke;
                let is_connect = match ingest_frames(&mut ingest, wal.as_ref(), &metrics, &mut inbox, &mut pool).await {
                    Ok(c) => c,
                    Err(IngestStop::Lost) => {
                        exit_err = Some(anyhow::anyhow!(
                            "the WAL did not accept a user-source message within {} s; stopping so that the sources read again from their last saved position on restart",
                            wal_writer::PUSH_WAIT.as_secs()
                        ));
                        break;
                    }
                    Err(IngestStop::Worker(e)) => {
                        exit_err = Some(e);
                        break;
                    }
                };
                if fetch_depth {
                    // (re)connect: every depth symbol needs a snapshot; resyncs: the named symbols
                    let mut wanted: Vec<String> = Vec::new();
                    if is_connect {
                        wanted.extend(depth_syms.iter().cloned());
                    }
                    for sym in ingest.need_snapshot.drain(..) {
                        if let Some(name) = ingest.dec.symbols().name(sym) {
                            wanted.push(name.to_owned());
                        }
                    }
                    wanted.sort();
                    wanted.dedup();
                    let now = tokio::time::Instant::now();
                    for sym in wanted {
                        // at most one fetch per symbol per 2 s (REST weight limits, flapping streams)
                        if snapshot_backoff.get(&sym).is_none_or(|t| now >= *t) {
                            snapshot_backoff.insert(sym.clone(), now + Duration::from_secs(2));
                            fetch_snapshot(sym, tx.clone(), metrics.clone(), cfg.feed.rest_url.clone());
                        }
                    }
                } else {
                    ingest.need_snapshot.clear();
                }
                // Interrupt moderation (as a NIC's rx-usecs): frames keep coming and the queue
                // is drained, so wait a little before the next batch. The sources keep queueing
                // meanwhile without waking this thread; the next `recv_many` finds them ready.
                // A full batch means a backlog: no wait. A quiet source: no wait either.
                if busy && n < INGEST_BATCH && !coalesce.is_zero() {
                    std::thread::sleep(coalesce);
                }
            }
            _ = snapshot_tick.tick() => {
                // everything logged so far is applied before positions, definitions and
                // checkpoints are looked at
                if let Err(e) = drain_pool(&mut ingest, &mut pool).await {
                    exit_err = Some(e);
                    break;
                }
                // dictionaries first: a segment must never reference an id that is not persisted
                let mut dicts_durable = true;
                if ingest.dec.symbols().len() != symbols_saved {
                    match tiering.save_symbols(ingest.dec.symbols()) {
                        Ok(()) => symbols_saved = ingest.dec.symbols().len(),
                        Err(e) => { dicts_durable = false; warn!(error = %e, "symbol dictionary save failed"); }
                    }
                }
                let dict_values = ingest.tables.dictionary_values();
                if dict_values != dict_values_saved {
                    // the file is rewritten whole: once it is large, at most once a second
                    // while it grows (small dictionaries are saved at once)
                    if dict_values <= DICT_THROTTLE_ABOVE
                        || last_dict_save.elapsed() >= Duration::from_secs(1)
                    {
                        last_dict_save = Instant::now();
                        match tiering.save_dictionaries(&ingest.tables) {
                            Ok(()) => dict_values_saved = dict_values,
                            Err(e) => { dicts_durable = false; warn!(error = %e, "user dictionary save failed"); }
                        }
                    } else {
                        dicts_durable = false;
                    }
                }
                metrics.dict_entries.store(dict_values as u64, Ordering::Relaxed);
                // no segment is written (and no WAL truncated) while a dictionary is not on disk
                if dicts_durable {
                    // a log over its budget: persist the partitions that hold it back
                    if let Some(dir) = &cfg.storage.wal_dir
                        && last_wal_check.elapsed() >= Duration::from_secs(5)
                    {
                        last_wal_check = Instant::now();
                        let (bytes, second_segment) = wal_writer::log_size(dir);
                        metrics.wal_bytes.store(bytes, Ordering::Relaxed);
                        if bytes > wal_budget
                            && let Some(target) = second_segment
                        {
                            if !ingest.tables.book_sync_active()
                                || ingest.connects.iter().any(|c| *c >= target)
                            {
                                let n = tiering.relieve_wal(&ingest.tables, target);
                                if n > 0 {
                                    info!(partitions = n, wal_mib = bytes / (1024 * 1024), "wal over budget: persisting the partitions that hold it back");
                                }
                            } else if fetch_depth
                                && last_rebase.is_none_or(|t| t.elapsed() >= Duration::from_secs(600))
                            {
                                // the books depend on a connection older than the segment we
                                // want to drop: give them a new starting point
                                ingest.rebase_books = true;
                            }
                        }
                    }
                    if ingest.rebase_books && fetch_depth {
                        // a `connect` in the log: books wait for fresh snapshots (fetched by the
                        // connect handling above), and later checkpoints can truncate up to here
                        let frame = Frame {
                            recv_ns: now_ns(),
                            kind: FrameKind::Control,
                            payload: bytes::Bytes::from_static(b"connect"),
                        };
                        if tx.try_send(frame).is_ok() {
                            ingest.rebase_books = false;
                            last_rebase = Some(Instant::now());
                            info!("order books are being re-based on fresh snapshots");
                        }
                    }
                    if last_mem_check.elapsed() >= Duration::from_secs(1) {
                        last_mem_check = Instant::now();
                        let (aged, pressed) =
                            tiering.relieve_memory(&ingest.tables, hot_budget, max_open_age);
                        if aged + pressed > 0 {
                            metrics.tier.sealed_for_age.fetch_add(aged as u64, Ordering::Relaxed);
                            metrics.tier.sealed_for_memory.fetch_add(pressed as u64, Ordering::Relaxed);
                        }
                    }
                    // Sparkplug definitions into the log, so that truncation can pass the BIRTHs
                    // they came from: when they changed, or when the last such frame holds
                    // truncation back, at most once per checkpoint interval
                    if !standby
                        && let Some(w) = &wal
                        && ingest.tables.sparkplug_active()
                        && last_sp_mark.is_none_or(|t| t.elapsed() >= sp_mark_every)
                        && (ingest.tables.sparkplug_generation() != ingest.sp_marked_gen
                            || ingest.sp_marks.last().is_none_or(|m| *m < last_ckpt))
                    {
                        let frame = Frame {
                            recv_ns: now_ns(),
                            kind: FrameKind::Control,
                            payload: ingest.tables.sparkplug_defs_payload(),
                        };
                        if !wal_writer::WalHandle::too_large(&frame, &metrics)
                            && let Some(lsn) = w.push(frame.clone(), &metrics)
                        {
                            ingest.handle_frame_lsn(&frame, lsn);
                            last_sp_mark = Some(Instant::now());
                        }
                    }
                    if let Some(ckpt) = tiering.tick(&ingest.tables, &metrics) {
                        last_ckpt = last_ckpt.max(ckpt);
                        if let Some(w) = &wal
                            && let Some(at) = ingest.truncation_point(ckpt)
                        {
                            w.truncate_before(at);
                        }
                    }
                }
                // source positions whose frames are durable by now
                let saved = ingest.states.commit(metrics.wal_durable_lsn.load(Ordering::Relaxed), false);
                metrics.source_positions_saved.fetch_add(saved, Ordering::Relaxed);
                live_trades.swap_snapshot(ingest.tables.trades.snapshot());
                live_deltas.swap_snapshot(ingest.tables.deltas.snapshot());
                live_quotes.swap_snapshot(ingest.tables.quotes.snapshot());
                for (name, t) in &live_extra {
                    if let Some((_, src)) = ingest.tables.all().into_iter().find(|(n, _)| *n == name.as_str()) {
                        t.swap_snapshot(src.snapshot());
                    }
                }
                let mut registered = Ok(());
                if dict_values != dict_values_registered
                    && (dict_values <= DICT_THROTTLE_ABOVE
                        || last_dict_register.elapsed() >= Duration::from_secs(1))
                {
                    last_dict_register = Instant::now();
                    registered = dictionaries_batch(&ingest.tables)
                        .and_then(|b| Ok(published.publish(&ctx, "dictionaries", b)?));
                    dict_values_registered = dict_values;
                }
                if registered.is_ok() && !ingest.tables.continuous.queries().is_empty() {
                    registered = publish_live_cq(&ctx, &mut published, &ingest.tables);
                    let qs = ingest.tables.continuous.queries();
                    metrics.cq_late_rows.store(qs.iter().map(|q| q.late_rows).sum(), Ordering::Relaxed);
                    metrics.cq_rejected_rows.store(qs.iter().map(|q| q.rejected_rows).sum(), Ordering::Relaxed);
                }
                if registered.is_ok() && ingest.dec.symbols().len() != symbols_registered {
                    registered = symbols_batch(&ingest.dec)
                        .and_then(|b| Ok(published.publish(&ctx, "symbols", b)?));
                    symbols_registered = ingest.dec.symbols().len();
                }
                if let Err(e) = registered {
                    exit_err = Some(e.context("registering query tables"));
                    break;
                }
                let s = ingest.dec.stats();
                metrics.trades.store(s.trades, Ordering::Relaxed);
                metrics.deltas.store(s.deltas, Ordering::Relaxed);
                metrics.gaps.store(s.gaps + ingest.sbe.stats().gaps, Ordering::Relaxed);
                let st: Vec<_> = ingest.tables.persistent().iter().map(|(_, t)| t.stats()).collect();
                metrics.rows.store(st.iter().map(|x| x.rows).sum(), Ordering::Relaxed);
                metrics.late_rows.store(st.iter().map(|x| x.late_rows).sum(), Ordering::Relaxed);
                metrics.sealed_chunks.store(st.iter().map(|x| x.sealed_chunks).sum(), Ordering::Relaxed);
                metrics.snapshots.fetch_add(1, Ordering::Relaxed);
            }
            _ = log_tick.tick() => {
                let s = ingest.dec.stats();
                info!(frames = metrics.frames.load(Ordering::Relaxed), events = metrics.events.load(Ordering::Relaxed), trades = s.trades, deltas = s.deltas, gaps = s.gaps,
                      book_snapshots = s.snapshots, resyncs = ingest.tables.resyncs(), rows = metrics.rows.load(Ordering::Relaxed),
                      wal_lsn = metrics.wal_durable_lsn.load(Ordering::Relaxed), uptime_s = started.elapsed().as_secs(), "live");
            }
        }
    }

    // Orderly shutdown: stop the source (bounded), close the WAL (flush + fsync), stop Flight gracefully.
    if let Err(e) = drain_pool(&mut ingest, &mut pool).await {
        warn!(error = %e, "mapping workers: results lost at shutdown");
    }
    drop(pool.take());
    let _ = shutdown_tx.send(true);
    drop(tx);
    // Keep taking frames while the sources stop: they deliver what they hold and report their
    // final position, and the channel closes once every one of them has returned.
    let drain_until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() >= drain_until {
            warn!("still receiving frames 5 s into shutdown; continuing without the rest");
            break;
        }
        match tokio::time::timeout_at(drain_until, rx.recv()).await {
            Ok(Some(frame)) => {
                if ingest.discard_after_loss(&frame) {
                    continue;
                }
                let lsn = if let Some(w) = &wal {
                    let Some(lsn) = w.push(frame.clone(), &metrics) else {
                        // the log is not taking frames: waiting 2 s for each remaining one
                        // would only delay the flush of what is already safe
                        ingest.frame_not_logged(&frame);
                        warn!(
                            "the WAL is not accepting frames; the frames still queued are dropped"
                        );
                        break;
                    };
                    lsn
                } else {
                    0
                };
                ingest.handle_frame_lsn(&frame, lsn);
            }
            Ok(None) => break,
            Err(_) => {
                warn!("sources did not stop within 5 s; continuing shutdown");
                break;
            }
        }
    }
    for mut t in user_source_tasks {
        match tokio::time::timeout(Duration::from_millis(200), &mut t).await {
            Ok(Ok(st)) => info!(
                messages = st.messages,
                dropped_full = st.dropped_full,
                oversized = st.oversized,
                "user source stopped"
            ),
            Ok(Err(e)) => warn!(error = %e, "user source task failed"),
            Err(_) => {
                warn!("user source did not stop; aborting it");
                t.abort();
            }
        }
    }
    if let Some(mut t) = repl_task.take()
        && tokio::time::timeout(Duration::from_secs(5), &mut t)
            .await
            .is_err()
    {
        warn!("replication task did not stop within 5 s; aborting it");
        t.abort();
    }
    // records the standby had already received: keep the local log gap-free up to the last one
    while let Ok(rec) = repl_rx.try_recv() {
        if let Some(w) = &wal
            && w.push_replicated(rec.lsn, rec.epoch, rec.payload.clone())
                .is_err()
        {
            break;
        }
        if let Ok((frame, _)) = ankaa_feeds::capture::decode_frame(&rec.payload, 0) {
            ingest.handle_frame_lsn(&frame, rec.lsn);
        }
    }
    match tokio::time::timeout(Duration::from_secs(1), source_task).await {
        Ok(Ok(stats)) => {
            metrics
                .source_dropped_full
                .store(stats.dropped_full, Ordering::Relaxed);
            info!(
                connects = stats.connects,
                frames = stats.frames,
                dropped_full = stats.dropped_full,
                "source stopped"
            );
        }
        Ok(Err(e)) => warn!(error = %e, "source task failed"),
        Err(_) => warn!("source did not stop in time; continuing shutdown"),
    }
    for src in ingest.tables.sources.iter().flatten() {
        info!(source = %src.name, mapped = src.mapped, rejected = src.rejected, "user source totals");
    }
    ingest.tables.seal_all();
    let mut flushed = false;
    if tiering.checkpoint_dir().is_some() {
        let t0 = Instant::now();
        let mut dicts_durable = true;
        if let Err(e) = tiering.save_symbols(ingest.dec.symbols()) {
            dicts_durable = false;
            warn!(error = %e, "symbol dictionary save failed");
        }
        if let Err(e) = tiering.save_dictionaries(&ingest.tables) {
            dicts_durable = false;
            warn!(error = %e, "user dictionary save failed");
        }
        if dicts_durable {
            let ckpt = tiering.flush_all(&ingest.tables, &metrics);
            flushed = tiering.errors == 0;
            info!(
                ms = t0.elapsed().as_millis(),
                checkpoint_lsn = ckpt,
                "segments flushed"
            );
            if let (Some(l), Some(w)) = (ckpt, &wal)
                && let Some(at) = ingest.truncation_point(l)
            {
                w.truncate_before(at);
            }
        } else {
            warn!("segments not flushed: a dictionary could not be saved (the WAL keeps the data)");
        }
    }
    let had_wal = wal.is_some();
    if let Some(w) = wal {
        w.shutdown();
    }
    // everything that was queued is committed now (WAL), or in segments (no WAL): the last
    // reported source positions are safe to keep. Without either, nothing persists anyway.
    let durable_lsn = metrics.wal_durable_lsn.load(Ordering::Relaxed);
    // with a WAL every position carries its LSN and follows the durable LSN on its own
    let all_durable = !had_wal && flushed;
    ingest.states.commit(durable_lsn, all_durable);
    let _ = flight_stop_tx.send(());
    if !flight_done {
        match tokio::time::timeout(Duration::from_secs(5), &mut flight_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(r) => warn!(result = %describe_join(r), "flight server ended with an error"),
            Err(_) => warn!("flight server did not stop within 5 s"),
        }
    }
    let _ = pg_stop_tx.send(());
    if !pg_done && let Some(t) = pg_task.as_mut() {
        let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
    }
    metrics_task.abort();
    info!(
        events = metrics.events.load(Ordering::Relaxed),
        wal_lsn = metrics.wal_durable_lsn.load(Ordering::Relaxed),
        uptime_s = started.elapsed().as_secs(),
        "serve done"
    );
    match exit_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Register the symbol dictionary as a small Arrow table (one-shot sessions such as `replay`).
pub fn register_symbols(ctx: &SessionContext, dec: &BinanceJsonDecoder) -> anyhow::Result<()> {
    let _ = ctx.deregister_table("symbols")?;
    ctx.register_batch("symbols", symbols_batch(dec)?)?;
    Ok(())
}

/// `symbols(symbol, name)`: the exchange symbol dictionary.
fn symbols_batch(dec: &BinanceJsonDecoder) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field as AField, Schema as ASchema};
    use arrow::record_batch::RecordBatch;
    let n = dec.symbols().len();
    let ids: Vec<u32> = (0..n)
        .map(|i| u32::try_from(i).unwrap_or(u32::MAX))
        .collect();
    let names: Vec<String> = ids
        .iter()
        .map(|&id| {
            dec.symbols()
                .name(ankaa_ingest::SymbolId(id))
                .unwrap_or("?")
                .to_owned()
        })
        .collect();
    let schema = Arc::new(ASchema::new(vec![
        AField::new("symbol", DataType::UInt32, false),
        AField::new("name", DataType::Utf8, false),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )?;
    Ok(rb)
}

/// Replay capture files into the ingest channel (awaiting on backpressure instead of dropping),
/// then stay idle until shutdown so the tables remain queryable.
async fn run_file_source(
    paths: Vec<std::path::PathBuf>,
    tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
) -> ankaa_feeds::live::SourceStats {
    use ankaa_feeds::capture::decode_all_bytes_tolerant;
    let mut stats = ankaa_feeds::live::SourceStats::default();
    for path in paths {
        let bytes = match std::fs::read(&path) {
            Ok(b) => bytes::Bytes::from(b),
            Err(e) => {
                warn!(path = %path.display(), error = %e, "capture unreadable");
                continue;
            }
        };
        let frames = match decode_all_bytes_tolerant(&bytes) {
            Ok((_, f, tail)) => {
                if let Some(e) = tail {
                    warn!(path = %path.display(), error = %e, frames = f.len(), "capture ends in an invalid frame; serving the frames before it");
                }
                f
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "capture undecodable");
                continue;
            }
        };
        stats.connects += 1;
        for f in frames {
            tokio::select! {
                _ = shutdown.changed() => return stats,
                r = tx.send(f) => {
                    if r.is_err() {
                        return stats;
                    }
                    stats.frames += 1;
                }
            }
        }
        info!(path = %path.display(), frames = stats.frames, "capture served");
    }
    let _ = shutdown.changed().await;
    stats
}

/// Render a task join result for logs.
/// Frames the ingest loop takes from the source channel per wake-up. Under load the loop
/// finds many frames queued and handles them as one batch (one WAL hand-off, one counter
/// update); at a trickle a batch is a single frame and nothing waits to fill one. 1,024 frames
/// of ~100 bytes stay well inside the L2 cache while they are logged and ingested.
const INGEST_BATCH: usize = 1024;
/// Wake-ups closer together than this mean steady traffic: the ingest loop then coalesces
/// (`storage.ingest_coalesce_us`).
const BUSY_GAP: Duration = Duration::from_millis(1);

/// Runs shorter than this are ingested in the loop itself when no job is out: at a trickle a job
/// would only add two thread wake-ups per message.
const MIN_PARALLEL_RUN: usize = 8;

/// Log and ingest the frames in `inbox` (emptied), in order. With a WAL, a frame is ingested
/// only once the log has taken it (the push waits for the log when it is behind), so tables
/// and log always agree; the frames go to the log as runs, one hand-off per run. With mapping
/// workers a logged run is handed to them and applied when it comes back, in log order.
/// Returns whether a `connect` frame was among them, or `Err` when a user-source message could
/// not be logged (the server has to stop, see [`Ingest::frame_not_logged`]) or a worker failed.
async fn ingest_frames(
    ingest: &mut Ingest,
    wal: Option<&WalHandle>,
    metrics: &Metrics,
    inbox: &mut Vec<Frame>,
    pool: &mut Option<MapPool>,
) -> Result<bool, IngestStop> {
    let mut saw_connect = false;
    let mut run: Vec<Frame> = Vec::with_capacity(inbox.len());
    let frames: Vec<Frame> = std::mem::take(inbox);
    for f in frames {
        // must not reach the log either: recovery would take it as valid
        if ingest.discard_after_loss(&f) {
            continue;
        }
        // too large for a record: not logged, so not ingested; the run before it goes first
        if wal.is_some_and(|_| WalHandle::too_large(&f, metrics)) {
            let r = std::mem::take(&mut run);
            if log_and_ingest(ingest, wal, metrics, r, &mut saw_connect, pool).await?
                || ingest.frame_not_logged(&f)
            {
                return Err(IngestStop::Lost);
            }
            continue;
        }
        run.push(f);
    }
    if log_and_ingest(ingest, wal, metrics, run, &mut saw_connect, pool).await? {
        return Err(IngestStop::Lost);
    }
    ingest.flush_tally();
    Ok(saw_connect)
}

/// Why the ingest loop stops.
enum IngestStop {
    /// A user-source message could not be logged.
    Lost,
    /// A mapping worker failed.
    Worker(anyhow::Error),
}

/// Hand `run` to the log in one push, then ingest it under consecutive LSNs (in the loop, or by
/// the mapping workers). Returns `true` when the log did not take it and a user-source message
/// was among the frames lost.
async fn log_and_ingest(
    ingest: &mut Ingest,
    wal: Option<&WalHandle>,
    metrics: &Metrics,
    run: Vec<Frame>,
    saw_connect: &mut bool,
    pool: &mut Option<MapPool>,
) -> Result<bool, IngestStop> {
    if run.is_empty() {
        return Ok(false);
    }
    let run = Arc::new(run);
    // LSN of the first frame, and the step: 0 and 0 without a log
    let (first, step) = match wal {
        None => (0, 0),
        Some(w) => {
            let Some(first) = w.push_batch(&run, metrics) else {
                let mut stop = false;
                for f in run.iter() {
                    stop |= ingest.frame_not_logged(f);
                }
                return Ok(stop);
            };
            (first, 1)
        }
    };
    for f in run.iter() {
        *saw_connect |= f.kind == FrameKind::Control && f.payload.as_ref() == b"connect";
    }
    if let Some(p) = pool.as_mut()
        && (p.in_flight() > 0 || run.len() >= MIN_PARALLEL_RUN)
    {
        // a full set of queues: apply what came back (in order) before handing out more
        while p.full() {
            match p.next().await {
                Ok(Some(done)) => ingest.apply_mapped_job(p, done),
                Ok(None) => break,
                Err(e) => return Err(IngestStop::Worker(e)),
            }
        }
        p.dispatch(run, first, step);
        while let Some(done) = p.try_next() {
            ingest.apply_mapped_job(p, done);
        }
        return Ok(false);
    }
    let mut lsn = first;
    for f in run.iter() {
        ingest.handle_frame_lsn_batched(f, lsn);
        lsn += step;
    }
    Ok(false)
}

/// Apply every job still out, in order (before a checkpoint tick, a definitions frame or
/// shutdown, which must see everything that was logged).
async fn drain_pool(ingest: &mut Ingest, pool: &mut Option<MapPool>) -> anyhow::Result<()> {
    if let Some(p) = pool.as_mut() {
        while let Some(done) = p.next().await? {
            ingest.apply_mapped_job(p, done);
        }
    }
    Ok(())
}

fn describe_join<T: std::fmt::Debug>(
    r: Result<anyhow::Result<T>, tokio::task::JoinError>,
) -> String {
    match r {
        Ok(Ok(v)) => format!("finished: {v:?}"),
        Ok(Err(e)) => format!("error: {e}"),
        Err(e) if e.is_panic() => "panicked".to_owned(),
        Err(e) => format!("cancelled: {e}"),
    }
}

/// `dictionaries(table, column, id, name)`: names behind the `dict` columns of user tables.
fn dictionaries_batch(tables: &Tables) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field as AField, Schema as ASchema};
    use arrow::record_batch::RecordBatch;
    let (mut t, mut c, mut ids, mut names) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for u in &tables.user {
        for (ci, d) in u.dicts.iter().enumerate() {
            let Some(d) = d else { continue };
            for (i, name) in d.names().iter().enumerate() {
                t.push(u.name.clone());
                c.push(u.table.schema().fields()[ci].name.clone());
                ids.push(u32::try_from(i).unwrap_or(u32::MAX));
                names.push(name.clone());
            }
        }
    }
    let schema = Arc::new(ASchema::new(vec![
        AField::new("table", DataType::Utf8, false),
        AField::new("column", DataType::Utf8, false),
        AField::new("id", DataType::UInt32, false),
        AField::new("name", DataType::Utf8, false),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(t)),
            Arc::new(StringArray::from(c)),
            Arc::new(UInt32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )?;
    Ok(rb)
}

/// `cq_<name>_live`: the open bar / window per key of every continuous query.
fn publish_live_cq(
    ctx: &SessionContext,
    published: &mut ankaa_sql::BatchTables,
    tables: &Tables,
) -> anyhow::Result<()> {
    for (name, batch) in tables.continuous.live_batches() {
        published.publish(ctx, name.as_str(), batch)?;
    }
    Ok(())
}

/// Retention window per table (nanoseconds), from the table and storage settings.
fn retention_windows(cfg: &ankaa_core::config::Config) -> std::collections::HashMap<String, i64> {
    use ankaa_core::config::parse_duration_ns;
    let mut out = std::collections::HashMap::new();
    if let Some(ns) = cfg
        .storage
        .market_retention
        .as_deref()
        .and_then(parse_duration_ns)
    {
        for t in ["trades", "book_deltas", "quotes"] {
            out.insert(t.to_owned(), ns);
        }
    }
    for t in &cfg.tables {
        if let Some(ns) = t.retention.as_deref().and_then(parse_duration_ns) {
            out.insert(t.name.clone(), ns);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ingest(expect_snapshots: bool) -> Ingest {
        Ingest {
            tables: Tables::new(expect_snapshots),
            dec: BinanceJsonDecoder::new(),
            sbe: BinanceSbeDecoder::new(),
            batch: EventBatch::with_capacity(16),
            metrics: Arc::new(Metrics::default()),
            need_snapshot: Vec::new(),
            states: SourceStates::load(None).unwrap(),
            lsn: 0,
            recovering: false,
            positions_frozen: false,
            connects: Vec::new(),
            rebase_books: false,
            tally: Tally::default(),
            sp_marks: Vec::new(),
            sp_marked_gen: 0,
        }
    }

    fn control(text: &'static [u8]) -> Frame {
        Frame {
            recv_ns: 1,
            kind: FrameKind::Control,
            payload: bytes::Bytes::from_static(text),
        }
    }

    const DEPTH: &[u8] = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794231662214,"s":"BTCUSDT","U":100,"u":105,"b":[["74663.56000000","0.00015000"]],"a":[["74670.00000000","0.00000000"]]}}"#;

    /// Sparkplug rows depend on the BIRTH that defined their metrics: once a source holds
    /// definitions, the log is only truncated up to a definitions frame at or before the
    /// checkpoint, and older such frames are forgotten once passed.
    #[test]
    fn wal_truncation_keeps_the_sparkplug_definitions() {
        use ankaa_store::{ColumnType, Field, Schema};
        let mut i = ingest(false);
        let schema = Schema::new(vec![
            Field::new("ts", ColumnType::TimestampNs),
            Field::new("metric", ColumnType::DictU32),
            Field::nullable("value", ColumnType::Float64),
        ])
        .unwrap();
        let t = i.tables.add_user_table("scada", schema.clone(), None, 0);
        let map: std::collections::HashMap<String, String> = [
            ("ts", "$timestamp"),
            ("metric", "$metric"),
            ("value", "$value"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
        let mapper = RowMapper::new(schema, &map, false, TsUnit::Millis, true, None)
            .unwrap()
            .with_topic_prefix(true)
            .with_sparkplug(true)
            .unwrap();
        i.tables.add_source(0, "scada", t, mapper, false);
        assert_eq!(i.truncation_point(500), Some(500), "no definitions yet");
        // an NBIRTH: metric "T", alias 0, Double 1.5
        let mut metric = vec![0x0a, 1, b'T', 0x10, 0, 0x20, 10, 0x69];
        metric.extend_from_slice(&1.5f64.to_le_bytes());
        let mut msg = b"spBv1.0/g/NBIRTH/n\0".to_vec();
        msg.extend_from_slice(&[0x12, u8::try_from(metric.len()).unwrap()]);
        msg.extend_from_slice(&metric);
        let birth = Frame {
            recv_ns: 1,
            kind: FrameKind::User,
            payload: ankaa_feeds::capture::user_payload(0, &msg),
        };
        i.handle_frame_lsn(&birth, 5);
        assert!(i.tables.sparkplug_active());
        assert_eq!(i.truncation_point(500), None, "the BIRTH at 5 must stay");
        let mark = |i: &mut Ingest, lsn| {
            let f = Frame {
                recv_ns: 1,
                kind: FrameKind::Control,
                payload: i.tables.sparkplug_defs_payload(),
            };
            i.handle_frame_lsn(&f, lsn);
        };
        mark(&mut i, 100);
        assert_eq!(i.sp_marked_gen, i.tables.sparkplug_generation());
        assert_eq!(i.truncation_point(50), None);
        assert_eq!(i.truncation_point(500), Some(100));
        mark(&mut i, 400);
        assert_eq!(i.truncation_point(300), Some(100));
        assert_eq!(i.truncation_point(500), Some(400));
        assert_eq!(i.sp_marks, vec![400]);
        // the frame restores what it holds: a fresh server knows alias 0 without the BIRTH
        let payload = i.tables.sparkplug_defs_payload();
        let mut fresh = ingest(false);
        let schema = i.tables.user[0].table.schema().clone();
        let t = fresh
            .tables
            .add_user_table("scada", schema.clone(), None, 0);
        let mapper = RowMapper::new(schema, &map, false, TsUnit::Millis, true, None)
            .unwrap()
            .with_topic_prefix(true)
            .with_sparkplug(true)
            .unwrap();
        fresh.tables.add_source(0, "scada", t, mapper, false);
        let f = Frame {
            recv_ns: 1,
            kind: FrameKind::Control,
            payload,
        };
        fresh.handle_frame_lsn(&f, 7);
        assert!(fresh.tables.sparkplug_active());
        assert_eq!(fresh.sp_marks, vec![7]);
    }

    /// Book rows can only be regenerated by replaying from the connection they belong to, so
    /// with synchronised books the log is kept back to the last `connect` at or before the
    /// checkpoint; without them the checkpoint itself is the truncation point.
    #[test]
    fn wal_truncation_keeps_the_connection_books_depend_on() {
        // trades only (no book state): plain checkpoint
        let mut i = ingest(true);
        i.handle_frame_lsn(&control(b"connect"), 10);
        assert_eq!(i.truncation_point(500), Some(500));
        // books synchronised from snapshots
        let depth = Frame {
            recv_ns: 1,
            kind: FrameKind::Text,
            payload: bytes::Bytes::from_static(DEPTH),
        };
        i.handle_frame_lsn(&depth, 11);
        assert!(i.tables.book_sync_active());
        assert_eq!(
            i.truncation_point(500),
            Some(10),
            "back to the connect at lsn 10"
        );
        // a reconnect later: checkpoints before it still need the first connection
        i.handle_frame_lsn(&control(b"connect"), 300);
        assert_eq!(i.truncation_point(250), Some(10));
        assert_eq!(i.truncation_point(300), Some(300));
        assert_eq!(i.truncation_point(900), Some(300));
        assert_eq!(
            i.connects,
            vec![300],
            "older connects are forgotten once passed"
        );
        // a checkpoint older than every known connect: nothing may be truncated
        assert_eq!(i.truncation_point(100), None);
        // cold books (no snapshots expected): not replay-exact anyway, plain checkpoint
        let mut cold = ingest(false);
        cold.handle_frame_lsn(&control(b"connect"), 10);
        cold.handle_frame_lsn(&depth, 11);
        assert_eq!(cold.truncation_point(500), Some(500));
    }

    /// The log names each source's id; a node whose configuration lists the sources in another
    /// order adopts the log's numbering, and positions reported after a frame was lost to the
    /// log are ignored.
    #[test]
    fn source_ids_follow_the_log_and_positions_freeze_after_a_lost_frame() {
        let mut i = ingest(false);
        assert_eq!(i.states.id_for("a").unwrap(), 0);
        assert_eq!(i.states.id_for("b").unwrap(), 1);
        let frame = |payload: bytes::Bytes| Frame {
            recv_ns: 1,
            kind: FrameKind::Control,
            payload,
        };
        // the primary's log says b = 0, a = 1
        i.handle_frame_lsn(&frame(ankaa_feeds::capture::source_id_payload(0, "b")), 1);
        i.handle_frame_lsn(&frame(ankaa_feeds::capture::source_id_payload(1, "a")), 2);
        assert_eq!(i.states.id_of("b"), Some(0));
        assert_eq!(i.states.id_of("a"), Some(1));
        // a position is noted normally
        i.handle_frame_lsn(
            &frame(ankaa_feeds::capture::source_state_payload(0, "0=5")),
            3,
        );
        i.states.commit(3, false);
        assert_eq!(i.states.position(0), Some("0=5"));
        // a user frame could not be logged: later positions must not be kept
        let user = Frame {
            recv_ns: 1,
            kind: FrameKind::User,
            payload: ankaa_feeds::capture::user_payload(0, b"{}"),
        };
        assert!(i.frame_not_logged(&user), "the server has to stop");
        assert!(i.positions_frozen);
        assert!(i.discard_after_loss(&user));
        // a snapshot that is not logged asks for a re-base instead (books would wait forever)
        let snap = Frame {
            recv_ns: 1,
            kind: FrameKind::Snapshot,
            payload: bytes::Bytes::from_static(b"{}"),
        };
        assert!(!i.frame_not_logged(&snap));
        assert!(i.rebase_books);
        i.handle_frame_lsn(
            &frame(ankaa_feeds::capture::source_state_payload(0, "0=9")),
            4,
        );
        i.states.commit(u64::MAX, true);
        assert_eq!(i.states.position(0), Some("0=5"));
    }
}
