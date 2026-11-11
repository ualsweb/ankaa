//! WAL owner thread: receives raw frames from the ingest loop, appends them (frame encoding as
//! payload) and group-commits them. The ingest loop never performs a syscall for durability; it
//! only pushes into a bounded queue.
//!
//! **Backpressure, not loss.** When the queue is full the ingest loop waits for the log (up to
//! [`PUSH_WAIT`]) instead of ingesting a frame that is not logged: what is in the tables is in
//! the log, which is what crash recovery and standbys rely on. Ingest is therefore bounded by
//! what the disk can make durable; a real-time source that outruns it drops at the source
//! channel, where it is counted. Only when the log does not accept a frame within the wait (a
//! stalled or failing disk) is the frame dropped, entirely: it is not ingested either.
//!
//! **Group commit.** A commit happens 10 ms after the first buffered record, or as soon as
//! 256 KiB are buffered and the queue is empty, or at 8 MiB: under load the batch grows with
//! the time `fdatasync` takes, so throughput follows the disk instead of a fixed batch size.
//! A failed commit is retried until it succeeds (nothing is received meanwhile, so the queue
//! pushes back) and the buffer is never discarded.
//!
//! **Batches.** The ingest loop hands over every frame it found queued in one message
//! ([`WalHandle::push_batch`]): one channel operation, and at most one thread wake-up, per
//! batch instead of per frame. The frames are shared (`Arc`), not copied: the ingest loop
//! reads them while this thread encodes them straight into the log's buffer.
//!
//! Recovery replays every durable frame through the same decoder/appender path as live data.

use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError, sync_channel,
};
use std::time::{Duration, Instant};

use ankaa_feeds::capture::{Frame, decode_frame, encode_frame};
use ankaa_wal::{Wal, WalConfig, replay};
use tracing::{info, warn};

use crate::metrics::Metrics;

/// Messages to the owner thread.
enum Msg {
    Frame(Frame),
    /// Frames appended in order under consecutive LSNs.
    Frames(Arc<Vec<Frame>>),
    /// A record replicated from a primary: appended verbatim (same payload, same epoch).
    Raw {
        epoch: u64,
        payload: bytes::Bytes,
    },
    Truncate(u64),
}

/// Largest frame payload the log takes: what a frame may hold (recovery and standbys decode
/// records back into frames, and refuse longer ones).
pub const MAX_FRAME_PAYLOAD: usize = ankaa_feeds::capture::MAX_FRAME_LEN as usize;

/// How long the ingest loop waits for space in the WAL queue before giving a frame up.
pub const PUSH_WAIT: Duration = Duration::from_secs(2);
/// How long a standby waits for its own log before it stops.
const REPLICATED_PUSH_WAIT: Duration = Duration::from_secs(30);
/// Frames queued for the writer at most (single frames and batches alike): the bound on the
/// memory the queue holds, whatever the batch size.
const MAX_QUEUED_FRAMES: usize = 65_536;
/// Oldest buffered record → commit.
const COMMIT_INTERVAL: Duration = Duration::from_millis(10);
/// Buffered bytes that force a commit even while more records are queued.
const COMMIT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Handle held by the ingest loop (single producer).
pub struct WalHandle {
    /// `None` once shut down.
    tx: Option<SyncSender<Msg>>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// LSN the next accepted frame will receive: the owner thread appends in channel order and
    /// `Wal::append` numbers records consecutively, so the producer can predict it.
    next_lsn: Cell<u64>,
    /// Tells the owner to stop retrying a failing commit (shutdown).
    stop: Arc<AtomicBool>,
    /// Frames sent and not yet appended (see [`MAX_QUEUED_FRAMES`]).
    queued: Arc<AtomicUsize>,
}

/// Run a short blocking wait without starving the async runtime's other tasks.
fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Wait (up to `wait`) until `n` more frames fit in the queue; `false` on timeout. A batch
/// larger than the bound is let through once the queue is empty, so it can never wait forever.
fn room_for(queued: &AtomicUsize, n: usize, wait: Duration) -> bool {
    let fits = || {
        let q = queued.load(Ordering::Relaxed);
        q == 0 || q + n <= MAX_QUEUED_FRAMES
    };
    if fits() {
        return true;
    }
    blocking(|| {
        let deadline = Instant::now() + wait;
        loop {
            std::thread::sleep(Duration::from_micros(200));
            if fits() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
        }
    })
}

/// Send with a deadline (`std`'s bounded channel has no timed send).
fn send_within(tx: &SyncSender<Msg>, mut msg: Msg, wait: Duration) -> Result<(), bool> {
    match tx.try_send(msg) {
        Ok(()) => return Ok(()),
        Err(TrySendError::Disconnected(_)) => return Err(false),
        Err(TrySendError::Full(m)) => msg = m,
    }
    blocking(|| {
        let deadline = Instant::now() + wait;
        loop {
            std::thread::sleep(Duration::from_micros(200));
            match tx.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(_)) => return Err(false),
                Err(TrySendError::Full(m)) => {
                    if Instant::now() >= deadline {
                        return Err(true);
                    }
                    msg = m;
                }
            }
        }
    })
}

impl WalHandle {
    /// Enqueue a frame for durable append and return the LSN it will get. When the queue is
    /// full this waits for the log (up to [`PUSH_WAIT`]). `None` means the frame is **not** in
    /// the log (the log did not take it in time, the writer has stopped, or the frame is too
    /// large): the caller must not ingest it, so that tables and log keep agreeing.
    ///
    /// Frames whose payload exceeds [`MAX_FRAME_PAYLOAD`] are refused here: the owner would
    /// refuse them without consuming an LSN and every later prediction would be off by one.
    pub fn push(&self, frame: Frame, metrics: &Metrics) -> Option<u64> {
        if frame.payload.len() > MAX_FRAME_PAYLOAD {
            metrics.wal_oversized.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if !self.send(Msg::Frame(frame), 1) {
            metrics.wal_dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let lsn = self.next_lsn.get();
        self.next_lsn.set(lsn + 1);
        Some(lsn)
    }

    /// True (and counted) when `frame` is larger than a log record may be: [`Self::push`]
    /// refuses it, and it must be kept out of a [`Self::push_batch`].
    pub fn too_large(frame: &Frame, metrics: &Metrics) -> bool {
        let big = frame.payload.len() > MAX_FRAME_PAYLOAD;
        if big {
            metrics.wal_oversized.fetch_add(1, Ordering::Relaxed);
        }
        big
    }

    /// Enqueue frames for durable append, in order, and return the LSN of the first; frame
    /// `i` gets that LSN + `i`. All or nothing: `None` means **none** of them is in the log
    /// (same cases as [`Self::push`]). Every frame must have passed [`Self::too_large`].
    pub fn push_batch(&self, frames: &Arc<Vec<Frame>>, metrics: &Metrics) -> Option<u64> {
        debug_assert!(frames.iter().all(|f| f.payload.len() <= MAX_FRAME_PAYLOAD));
        let n = frames.len() as u64;
        if n == 0 {
            return Some(self.next_lsn.get());
        }
        if !self.send(Msg::Frames(Arc::clone(frames)), frames.len()) {
            metrics.wal_dropped.fetch_add(n, Ordering::Relaxed);
            return None;
        }
        let lsn = self.next_lsn.get();
        self.next_lsn.set(lsn + n);
        Some(lsn)
    }

    /// Queue `msg` holding `frames` frames, waiting (up to [`PUSH_WAIT`]) for room.
    fn send(&self, msg: Msg, frames: usize) -> bool {
        let Some(tx) = self.tx.as_ref() else {
            return false;
        };
        if !room_for(&self.queued, frames, PUSH_WAIT) {
            return false;
        }
        // counted before the send: the writer may append (and subtract) before it returns
        self.queued.fetch_add(frames, Ordering::Relaxed);
        if send_within(tx, msg, PUSH_WAIT).is_err() {
            self.queued.fetch_sub(frames, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// LSN the next accepted record will receive.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.get()
    }

    /// Standby: append a record received from the primary under the primary's own LSN. Waits
    /// for queue space (a standby never drops). Fails when `lsn` is not the next local LSN: the
    /// two logs have diverged and the standby must be re-seeded.
    pub fn push_replicated(
        &self,
        lsn: u64,
        epoch: u64,
        payload: bytes::Bytes,
    ) -> Result<(), String> {
        let next = self.next_lsn.get();
        if lsn != next {
            return Err(format!(
                "primary sent WAL record {lsn} but the local log expects {next}"
            ));
        }
        if payload.len() > ankaa_wal::MAX_PAYLOAD {
            return Err(format!("WAL record {lsn} is larger than a record may be"));
        }
        let tx = self.tx.as_ref().ok_or("the WAL writer has stopped")?;
        send_within(tx, Msg::Raw { epoch, payload }, REPLICATED_PUSH_WAIT).map_err(|full| {
            if full {
                "the local WAL did not accept a record for 30 s (disk stalled or full)".to_owned()
            } else {
                "the WAL writer has stopped".to_owned()
            }
        })?;
        self.next_lsn.set(next + 1);
        Ok(())
    }

    /// Ask the owner to delete segments entirely below `checkpoint_lsn` (after the next commit).
    /// When the queue is full the request is dropped; the next checkpoint asks again.
    pub fn truncate_before(&self, checkpoint_lsn: u64) {
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(Msg::Truncate(checkpoint_lsn));
        }
    }

    /// Stop: flush, fdatasync and join.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        // closing the channel makes the owner commit what it holds and exit; the flag ends a
        // commit that keeps failing, so shutdown cannot hang on a dead disk
        self.stop.store(true, Ordering::Relaxed);
        self.tx = None;
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for WalHandle {
    /// An early return from the server (a listener that cannot bind, a failed registration)
    /// still commits what was queued before the process exits.
    fn drop(&mut self) {
        self.stop();
    }
}

/// Replay durable frames from `dir` in LSN order, calling `f(frame, lsn)` for each; returns the count.
pub fn recover(dir: &Path, mut f: impl FnMut(Frame, u64)) -> anyhow::Result<u64> {
    std::fs::create_dir_all(dir)?;
    let mut n = 0u64;
    for foreign in ankaa_wal::list_foreign_files(dir).unwrap_or_default() {
        // the server keeps its source bookkeeping next to the log, and the log its own markers
        let ours = foreign
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("sources.json"))
            || foreign
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case(ankaa_wal::JUMP_EXT));
        if ours {
            continue;
        }
        warn!(path = %foreign.display(), "ignoring non-WAL file in WAL directory");
    }
    for rec in replay(dir)? {
        let rec = rec?;
        match decode_frame(&rec.payload, 0) {
            Ok((frame, _)) => {
                f(frame, rec.lsn);
                n += 1;
            }
            Err(e) => {
                warn!(lsn = rec.lsn, error = %e, "WAL record is not a capture frame; skipped");
            }
        }
    }
    Ok(n)
}

/// Start the WAL owner thread. `min_next_lsn` is the lowest LSN the next record may get: one
/// above every LSN that segments or saved source positions already refer to.
#[allow(clippy::too_many_lines)]
pub fn start(dir: &Path, metrics: Arc<Metrics>, min_next_lsn: u64) -> anyhow::Result<WalHandle> {
    let (mut wal, last) = Wal::open_at(
        dir,
        WalConfig {
            group_commit_bytes: 256 * 1024,
            segment_bytes: 256 << 20,
        },
        min_next_lsn,
    )?;
    info!(dir = %dir.display(), durable_lsn = last, "wal opened");
    metrics.wal_durable_lsn.store(last, Ordering::Relaxed);
    let (tx, rx): (SyncSender<Msg>, Receiver<Msg>) = sync_channel(65_536);
    let stop = Arc::new(AtomicBool::new(false));
    let stopping = stop.clone();
    let queued = Arc::new(AtomicUsize::new(0));
    let appended_frames = queued.clone();
    let soft_bytes = 256 * 1024;
    let thread = std::thread::Builder::new()
        .name("ankaa-wal".into())
        .spawn(move || {
            let epoch = 1u64;
            // Commit, retrying until it succeeds: while it fails nothing is received, so the
            // queue fills and the ingest loop waits. Gives up only when the server is stopping.
            let commit = |wal: &mut Wal, metrics: &Metrics| {
                let mut failures = 0u32;
                loop {
                    match wal.commit() {
                        Ok(lsn) => {
                            metrics.wal_durable_lsn.store(lsn, Ordering::Relaxed);
                            metrics.wal_commits.fetch_add(1, Ordering::Relaxed);
                            metrics.wal_pending_bytes.store(0, Ordering::Relaxed);
                            if failures > 0 {
                                info!(failures, "wal commit succeeded again");
                            }
                            return;
                        }
                        Err(e) => {
                            metrics.wal_errors.fetch_add(1, Ordering::Relaxed);
                            // first failure, then about once a minute
                            if failures.is_multiple_of(120) {
                                warn!(error = %e, pending_bytes = wal.pending_bytes(), "wal commit failed; ingest waits until the log is writable again");
                            }
                            failures = failures.saturating_add(1);
                            if stopping.load(Ordering::Relaxed) {
                                warn!(pending_bytes = wal.pending_bytes(), "wal: giving up on the last commit (shutdown); the records after the durable LSN are lost");
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(500));
                        }
                    }
                }
            };
            // when the oldest buffered record arrived
            let mut first_pending: Option<Instant> = None;
            loop {
                let next = if wal.pending_bytes() == 0 {
                    rx.recv_timeout(Duration::from_millis(100))
                } else if wal.pending_bytes() >= soft_bytes {
                    // enough for a commit: keep batching only while more is already queued
                    rx.try_recv().map_err(|e| match e {
                        TryRecvError::Empty => RecvTimeoutError::Timeout,
                        TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                    })
                } else {
                    let waited = first_pending.map_or(Duration::ZERO, |t| t.elapsed());
                    rx.recv_timeout(COMMIT_INTERVAL.saturating_sub(waited))
                };
                match next {
                    Ok(Msg::Truncate(lsn)) => {
                        if wal.pending_bytes() > 0 {
                            commit(&mut wal, &metrics);
                            first_pending = None;
                        }
                        match wal.truncate_before(lsn) {
                            Ok(n) if n > 0 => {
                                metrics
                                    .tier
                                    .wal_truncated_segments
                                    .fetch_add(n as u64, Ordering::Relaxed);
                                info!(checkpoint_lsn = lsn, segments = n, "wal truncated");
                            }
                            Ok(_) => {}
                            Err(e) => {
                                metrics.wal_errors.fetch_add(1, Ordering::Relaxed);
                                warn!(error = %e, "wal truncate failed");
                            }
                        }
                    }
                    Ok(Msg::Frames(frames)) => {
                        for frame in frames.iter() {
                            let appended = wal.append_with(epoch, |b| encode_frame(frame, b));
                            note_append(appended, &metrics);
                            // a batch of large frames must not grow the buffer without bound
                            if wal.pending_bytes() >= COMMIT_MAX_BYTES {
                                commit(&mut wal, &metrics);
                                first_pending = None;
                            }
                        }
                        appended_frames.fetch_sub(frames.len(), Ordering::Relaxed);
                        // the last reference usually: the frames are freed here, off the
                        // ingest thread
                        drop(frames);
                        first_pending.get_or_insert_with(Instant::now);
                        metrics
                            .wal_pending_bytes
                            .store(wal.pending_bytes() as u64, Ordering::Relaxed);
                        if wal.pending_bytes() >= COMMIT_MAX_BYTES
                            || first_pending.is_some_and(|t| t.elapsed() >= COMMIT_INTERVAL)
                        {
                            commit(&mut wal, &metrics);
                            first_pending = None;
                        }
                    }
                    Ok(msg @ (Msg::Frame(_) | Msg::Raw { .. })) => {
                        let appended = match msg {
                            Msg::Frame(frame) => {
                                let r = wal.append_with(epoch, |b| encode_frame(&frame, b));
                                appended_frames.fetch_sub(1, Ordering::Relaxed);
                                r
                            }
                            Msg::Raw { epoch, payload } => wal.append(epoch, &payload),
                            Msg::Truncate(_) | Msg::Frames(_) => Ok(0),
                        };
                        note_append(appended, &metrics);
                        first_pending.get_or_insert_with(Instant::now);
                        metrics
                            .wal_pending_bytes
                            .store(wal.pending_bytes() as u64, Ordering::Relaxed);
                        if wal.pending_bytes() >= COMMIT_MAX_BYTES
                            || first_pending.is_some_and(|t| t.elapsed() >= COMMIT_INTERVAL)
                        {
                            commit(&mut wal, &metrics);
                            first_pending = None;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if wal.pending_bytes() > 0 {
                            commit(&mut wal, &metrics);
                            first_pending = None;
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        commit(&mut wal, &metrics);
                        info!(durable_lsn = wal.durable_lsn(), "wal closed");
                        return;
                    }
                }
            }
        })?;
    Ok(WalHandle {
        tx: Some(tx),
        thread: Some(thread),
        next_lsn: Cell::new(last + 1),
        stop,
        queued,
    })
}

/// Count a failed append (unreachable for frames: sizes are checked before they are queued).
fn note_append(appended: Result<u64, ankaa_wal::WalError>, metrics: &Metrics) {
    if let Err(e) = appended {
        metrics.wal_errors.fetch_add(1, Ordering::Relaxed);
        warn!(error = %e, "wal append failed");
    }
}

/// Size of the log on disk and the first LSN of its second-oldest segment (what a checkpoint
/// has to pass before the oldest segment can be deleted).
pub fn log_size(dir: &Path) -> (u64, Option<u64>) {
    let segments = ankaa_wal::list_segments(dir).unwrap_or_default();
    let bytes = segments
        .iter()
        .map(|(_, p)| std::fs::metadata(p).map_or(0, |m| m.len()))
        .sum();
    (bytes, segments.get(1).map(|(first, _)| *first))
}
