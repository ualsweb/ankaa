//! Mapping workers (RFC-0001, stage 1): user messages are mapped to rows on N threads while the
//! ingest loop applies the results **in log order**, so tables, dictionaries and counters end up
//! exactly as with one thread.
//!
//! - The ingest loop logs a run of frames, then hands it over as a job numbered in order; job `s`
//!   goes to worker `s mod N` through that worker's own queue (no queue or lock shared by workers).
//! - A worker maps the user messages of JSON, CSV and line protocol sources with its own clones of
//!   their mappers and without the dictionaries ([`ankaa_ingest::Mapped`] keeps dictionary values
//!   as names). It touches no shared state: nothing per row is atomic or contended.
//! - Results come back in any order and wait in a reorder buffer until their number is next.
//! - Used result buffers go back to the worker that filled them: no allocation per message in
//!   steady state, and each worker keeps reusing memory in its own caches. On the way back a
//!   buffer carries the dictionary ids the ingest loop gave to the values the worker left
//!   pending; the worker remembers them ([`RowMapper::learn`]) and resolves those values itself
//!   from then on, so the loop hashes only values it has never seen.
//!
//! Frames a worker does not map (market data, control frames, Sparkplug sources) travel with the
//! job unmapped and are handled by the ingest loop at their place.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, SyncSender, sync_channel};

use ankaa_feeds::capture::{Frame, FrameKind, split_user_payload};
use ankaa_ingest::generic::{MapError, Mapped, RowMapper};

/// Jobs queued per worker before the ingest loop applies results first (backpressure).
const JOBS_PER_WORKER: usize = 2;

/// A run of logged frames.
struct Job {
    seq: u64,
    frames: Arc<Vec<Frame>>,
    first_lsn: u64,
    step: u64,
}

/// A mapped job: per frame, the mapping of a user message (`None`: the ingest loop handles the
/// frame itself).
pub struct Done {
    seq: u64,
    /// Worker that mapped it (its buffers go back there).
    pub worker: usize,
    /// The frames, in log order.
    pub frames: Arc<Vec<Frame>>,
    /// LSN of the first frame, and the step between frames (0 without a log).
    pub first_lsn: u64,
    /// See [`Self::first_lsn`].
    pub step: u64,
    /// Per frame.
    pub mapped: Vec<Option<Result<Mapped, MapError>>>,
}

/// The workers and the reorder buffer.
pub struct MapPool {
    jobs: Vec<SyncSender<Job>>,
    recycle: Vec<Sender<(u16, Mapped)>>,
    done_rx: tokio::sync::mpsc::UnboundedReceiver<Done>,
    threads: Vec<std::thread::JoinHandle<()>>,
    next_seq: u64,
    apply_seq: u64,
    waiting: BTreeMap<u64, Done>,
}

impl MapPool {
    /// Start `n` workers, each with its own copy of `mappers` (index = source id; `None` for a
    /// source the workers do not map, and whether a source reads CSV).
    pub fn start(n: usize, mappers: &[Option<(RowMapper, bool)>]) -> std::io::Result<Self> {
        let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut jobs = Vec::with_capacity(n);
        let mut recycle = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n);
        for w in 0..n {
            let (job_tx, job_rx) = sync_channel::<Job>(JOBS_PER_WORKER + 1);
            let (rec_tx, rec_rx) = std::sync::mpsc::channel::<(u16, Mapped)>();
            let mine = mappers.to_vec();
            let done_tx = done_tx.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("ankaa-map-{w}"))
                    .spawn(move || work(w, &job_rx, &rec_rx, mine, &done_tx))?,
            );
            jobs.push(job_tx);
            recycle.push(rec_tx);
        }
        Ok(Self {
            jobs,
            recycle,
            done_rx,
            threads,
            next_seq: 0,
            apply_seq: 0,
            waiting: BTreeMap::new(),
        })
    }

    /// Workers.
    pub fn workers(&self) -> usize {
        self.jobs.len()
    }

    /// Jobs handed out and not yet returned in order.
    pub fn in_flight(&self) -> usize {
        usize::try_from(self.next_seq - self.apply_seq).unwrap_or(usize::MAX)
    }

    /// Jobs handed out since the start.
    pub fn jobs_total(&self) -> u64 {
        self.next_seq
    }

    /// True when another job would exceed the queues: apply results first.
    pub fn full(&self) -> bool {
        self.in_flight() >= self.jobs.len() * JOBS_PER_WORKER
    }

    /// Hand a logged run over. Never blocks while [`Self::full`] is false.
    pub fn dispatch(&mut self, frames: Arc<Vec<Frame>>, first_lsn: u64, step: u64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let w = usize::try_from(seq % self.jobs.len() as u64).unwrap_or(0);
        // a worker that has gone (it panicked) leaves its job unanswered; the ingest loop then
        // waits forever for that number, which `next` reports as an error instead
        let _ = self.jobs[w].send(Job {
            seq,
            frames,
            first_lsn,
            step,
        });
    }

    /// The next job in log order when it is back already.
    pub fn try_next(&mut self) -> Option<Done> {
        while let Ok(d) = self.done_rx.try_recv() {
            self.waiting.insert(d.seq, d);
        }
        self.pop_ready()
    }

    /// The next job in log order, waiting for it; `None` when nothing is in flight.
    pub async fn next(&mut self) -> anyhow::Result<Option<Done>> {
        loop {
            if self.in_flight() == 0 {
                return Ok(None);
            }
            if let Some(d) = self.pop_ready() {
                return Ok(Some(d));
            }
            match self.done_rx.recv().await {
                Some(d) => {
                    self.waiting.insert(d.seq, d);
                }
                None => anyhow::bail!("a mapping worker stopped"),
            }
        }
    }

    /// Wait for any result to arrive (for a `select!` branch; take it with [`Self::try_next`]).
    pub async fn arrived(&mut self) -> anyhow::Result<()> {
        match self.done_rx.recv().await {
            Some(d) => {
                self.waiting.insert(d.seq, d);
                Ok(())
            }
            None => anyhow::bail!("a mapping worker stopped"),
        }
    }

    fn pop_ready(&mut self) -> Option<Done> {
        let d = self.waiting.remove(&self.apply_seq)?;
        self.apply_seq += 1;
        Some(d)
    }

    /// Give a used result of `source` back to the worker that filled it (after it was applied:
    /// the worker learns the dictionary ids it holds).
    pub fn recycle(&self, worker: usize, source: u16, m: Mapped) {
        if let Some(tx) = self.recycle.get(worker) {
            let _ = tx.send((source, m));
        }
    }
}

impl Drop for MapPool {
    fn drop(&mut self) {
        // closing the queues ends the workers
        self.jobs.clear();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn work(
    me: usize,
    jobs: &Receiver<Job>,
    recycled: &Receiver<(u16, Mapped)>,
    mut mappers: Vec<Option<(RowMapper, bool)>>,
    done: &tokio::sync::mpsc::UnboundedSender<Done>,
) {
    let mut spare: Vec<Mapped> = Vec::new();
    while let Ok(job) = jobs.recv() {
        for (source, m) in recycled.try_iter() {
            if let Some(Some((mapper, _))) = mappers.get_mut(usize::from(source)) {
                mapper.learn(&m);
            }
            spare.push(m);
        }
        let mapped = job
            .frames
            .iter()
            .map(|f| {
                if f.kind != FrameKind::User {
                    return None;
                }
                let (source, msg) = split_user_payload(&f.payload)?;
                let (mapper, csv) = mappers.get_mut(usize::from(source))?.as_mut()?;
                Some(match mapper.parse(msg, f.recv_ns, *csv, &[]) {
                    Ok(_) => {
                        let mut m = spare.pop().unwrap_or_else(|| mapper.empty_mapped());
                        mapper.swap_mapped(&mut m);
                        Ok(m)
                    }
                    Err(e) => Err(e),
                })
            })
            .collect();
        let d = Done {
            seq: job.seq,
            worker: me,
            frames: job.frames,
            first_lsn: job.first_lsn,
            step: job.step,
            mapped,
        };
        if done.send(d).is_err() {
            return;
        }
    }
}
