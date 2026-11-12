//! Writers (RFC-0001, stage 2): the rows of a mapped run are appended by several threads, each
//! taking the rows of its share of the partitions, while the ingest loop waits for all of them.
//!
//! What is order-dependent across partitions stays in the ingest loop and happens before the
//! writers start or after they finish: dictionary ids (a run goes to the writers only when
//! every value of every row has its id already, see [`ankaa_ingest::Mapped::has_pending`]),
//! counters, source positions, the log sequence. What a writer does depends on one partition
//! only: the stale check against the partition's watermark, and the append. A partition's
//! rows all go to one writer (`key mod writers`), in log order, so every partition ends up
//! exactly as when one thread appends the run row by row.
//!
//! A run is handed over and the ingest loop goes on to the next one (waiting for the writers
//! after every run costs two thread wake-ups per writer, more than a run's appends). Each
//! writer takes its runs in the order they were handed over, so a partition still sees its
//! rows in log order. The loop waits for the writers only where everything has to be applied:
//! before a run that goes row by row, before a snapshot tick, at shutdown
//! ([`WritePool::collect`] with `wait`). Snapshots, tiering and checkpoints therefore still
//! see tables at a message boundary.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, SyncSender, sync_channel};

use ankaa_ingest::generic::{MapError, Mapped};
use ankaa_store::Table;

use crate::tables::{MAX_FUTURE_NS, STALE};

/// One mapped message of a run: where its rows go and what a writer needs to check them.
pub struct FrameWork {
    /// Index into [`Run::tables`].
    pub table: usize,
    /// Receive time of the message (rows more than a day ahead of it are refused).
    pub recv_ns: i64,
    /// The message's WAL record.
    pub lsn: u64,
    /// Its rows, every dictionary value resolved.
    pub mapped: Mapped,
}

/// A run of mapped messages, shared by the writers while they work on it.
pub struct Run {
    /// The tables of the run and how late a row may be in each (ns).
    pub tables: Vec<(Arc<Table>, i64)>,
    /// The messages, in log order.
    pub frames: Vec<FrameWork>,
    /// Source of every message (for the counters once the run is stored).
    pub sources: Vec<u16>,
    /// Mapping worker the messages' buffers go back to.
    pub worker: usize,
}

/// What the rows one writer took of one message came to.
#[derive(Debug, Default)]
pub struct FrameTally {
    /// Index into [`Run::frames`].
    pub frame: u32,
    /// Rows stored (or skipped by recovery).
    pub rows: u32,
    /// Rows refused.
    pub rejected: u32,
    /// The first refused row and why.
    pub first: Option<(u32, MapError)>,
}

struct Job {
    id: u64,
    run: Arc<Run>,
    /// `(frame, row)`, in log order.
    rows: Vec<(u32, u32)>,
}

struct Done {
    id: u64,
    tallies: Vec<FrameTally>,
    /// The job's list, given back to be filled again.
    rows: Vec<(u32, u32)>,
}

/// The writer threads.
pub struct WritePool {
    jobs: Vec<SyncSender<Job>>,
    done_rx: Receiver<Done>,
    threads: Vec<std::thread::JoinHandle<()>>,
    /// Row lists, one per writer, filled for the next run; used ones come back in `spare`.
    lists: Vec<Vec<(u32, u32)>>,
    spare: Vec<Vec<(u32, u32)>>,
    next_id: u64,
    /// Runs the writers have not all reported on: the run, reports missing, tallies so far.
    out: std::collections::HashMap<u64, (Arc<Run>, usize, Vec<FrameTally>)>,
}

/// Runs a writer may have queued: beyond it the ingest loop waits (backpressure).
const QUEUED_RUNS: usize = 4;

impl WritePool {
    /// Start `n` writers.
    pub fn start(n: usize) -> std::io::Result<Self> {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut jobs = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n);
        for w in 0..n {
            let (tx, rx) = sync_channel::<Job>(QUEUED_RUNS);
            let done_tx = done_tx.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("ankaa-write-{w}"))
                    .spawn(move || work(&rx, &done_tx))?,
            );
            jobs.push(tx);
        }
        Ok(Self {
            jobs,
            done_rx,
            threads,
            lists: (0..n).map(|_| Vec::new()).collect(),
            spare: Vec::new(),
            next_id: 0,
            out: std::collections::HashMap::new(),
        })
    }

    /// Writers.
    pub fn writers(&self) -> usize {
        self.jobs.len()
    }

    /// The (empty) row list of the writer that owns the partition of `key`.
    pub fn list_for(&mut self, key: u32) -> &mut Vec<(u32, u32)> {
        let n = self.lists.len().max(1);
        &mut self.lists[key as usize % n]
    }

    /// Hand the rows put into the lists to their writers and return at once. `Err`: a writer
    /// is gone.
    pub fn submit(&mut self, run: Run) -> anyhow::Result<()> {
        let id = self.next_id;
        self.next_id += 1;
        let run = Arc::new(run);
        let mut parts = 0;
        for (w, list) in self.lists.iter_mut().enumerate() {
            if list.is_empty() {
                continue;
            }
            let job = Job {
                id,
                run: run.clone(),
                rows: std::mem::replace(list, self.spare.pop().unwrap_or_default()),
            };
            // (blocks while the writer has QUEUED_RUNS runs waiting)
            if self.jobs[w].send(job).is_err() {
                anyhow::bail!("a writer stopped");
            }
            parts += 1;
        }
        self.out.insert(id, (run, parts, Vec::new()));
        Ok(())
    }

    /// Runs handed over and not yet collected.
    pub fn outstanding(&self) -> usize {
        self.out.len()
    }

    /// The runs every writer has finished, each with what its messages' rows came to (several
    /// entries per message when its rows went to several writers). With `wait`, every run
    /// handed over so far: when this returns, the tables hold all of them.
    pub fn collect(&mut self, wait: bool) -> anyhow::Result<Vec<(Run, Vec<FrameTally>)>> {
        let mut finished = Vec::new();
        loop {
            // runs nobody had rows for, and runs whose last report came in
            let ready: Vec<u64> = self
                .out
                .iter()
                .filter(|(_, (_, left, _))| *left == 0)
                .map(|(id, _)| *id)
                .collect();
            for id in ready {
                if let Some((run, _, tallies)) = self.out.remove(&id) {
                    // every writer dropped its reference before it reported
                    let run = Arc::try_unwrap(run).map_err(|_| {
                        anyhow::anyhow!("a writer still holds a run it reported on")
                    })?;
                    finished.push((run, tallies));
                }
            }
            let done = if wait && !self.out.is_empty() {
                match self.done_rx.recv() {
                    Ok(d) => d,
                    Err(_) => anyhow::bail!("a writer stopped"),
                }
            } else {
                match self.done_rx.try_recv() {
                    Ok(d) => d,
                    Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(finished),
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        anyhow::bail!("a writer stopped")
                    }
                }
            };
            if let Some((_, left, tallies)) = self.out.get_mut(&done.id) {
                *left = left.saturating_sub(1);
                tallies.extend(done.tallies);
            }
            let mut rows = done.rows;
            rows.clear();
            self.spare.push(rows);
        }
    }
}

impl Drop for WritePool {
    fn drop(&mut self) {
        self.jobs.clear();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn work(jobs: &Receiver<Job>, done: &Sender<Done>) {
    while let Ok(Job { id, run, rows }) = jobs.recv() {
        let mut tallies: Vec<FrameTally> = Vec::new();
        for &(f, r) in &rows {
            if tallies.last().is_none_or(|t| t.frame != f) {
                tallies.push(FrameTally {
                    frame: f,
                    ..FrameTally::default()
                });
            }
            let outcome = append_row(&run, f, r);
            let Some(t) = tallies.last_mut() else {
                continue;
            };
            match outcome {
                Ok(()) => t.rows += 1,
                Err(e) => {
                    t.rejected += 1;
                    if t.first.is_none() {
                        t.first = Some((r, e));
                    }
                }
            }
        }
        // the run goes back whole: this reference is dropped before the report
        drop(run);
        if done.send(Done { id, tallies, rows }).is_err() {
            return;
        }
    }
}

/// One row, checked and appended exactly as the ingest loop does it row by row.
fn append_row(run: &Run, frame: u32, row: u32) -> Result<(), MapError> {
    let Some(fw) = run.frames.get(frame as usize) else {
        return Err(MapError::Malformed("row of an unknown message"));
    };
    let parsed = fw.mapped.parsed(row as usize)?;
    if parsed.ts > fw.recv_ns.saturating_add(MAX_FUTURE_NS) {
        return Err(MapError::Malformed(
            "timestamp is more than a day ahead of the receive time (check ts_unit)",
        ));
    }
    let Some((table, lateness)) = run.tables.get(fw.table) else {
        return Err(MapError::Malformed("row of an unknown table"));
    };
    let (key, values) = fw.mapped.row(row as usize);
    match table.append_shared(key, fw.lsn, values, Some(*lateness)) {
        Ok(Some(_stored)) => Ok(()),
        Ok(None) | Err(_) => Err(MapError::Malformed(STALE)),
    }
}
