//! Durable bookkeeping for user sources: stable source ids and read positions.
//!
//! **Ids.** A user frame in the WAL names its source by a `u16` id. The id of a source name is
//! assigned once and kept in `sources.json`, so reordering, adding or removing `[[sources]]`
//! entries never makes recorded frames point at another source.
//!
//! **Positions.** File and Kafka sources report how far they have read with in-band control
//! frames. A position is written to `sources.json` only once the frames before it are durable
//! (WAL fsync, or the final segment flush when there is no WAL), and handed back to the source
//! on the next start. A crash can therefore repeat the messages read after the last durable
//! position (at least once), and a clean stop repeats nothing.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

const FILE: &str = "sources.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct Entry {
    name: String,
    /// Position reported by the source (opaque).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    /// WAL LSN of the frame that reported it (0 = no WAL).
    #[serde(default)]
    lsn: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Doc {
    /// Index = source id.
    sources: Vec<Entry>,
}

/// Source ids and positions, persisted next to the WAL (or the segments).
#[derive(Debug, Default)]
pub struct SourceStates {
    path: Option<PathBuf>,
    doc: Doc,
    /// Positions waiting for their frames to become durable: `(lsn, id, state)`.
    pending: VecDeque<(u64, u16, String)>,
    dirty: bool,
    /// Writes the file off the caller's thread ([`Self::with_background_saves`]).
    saver: Option<Saver>,
}

/// A thread that writes the positions file, so that the two `fsync`s of a save (milliseconds
/// each) are not paid by the ingest loop at every snapshot tick. It always writes the latest
/// document it was given: one that arrives while another is being written waits, and replaces
/// any that was still waiting. Positions are only ever handed over once their frames are
/// durable, so the file may lag by one write and is never ahead.
struct Saver {
    shared: std::sync::Arc<(std::sync::Mutex<Slot>, std::sync::Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Default)]
struct Slot {
    next: Option<Vec<u8>>,
    writing: bool,
    failed: bool,
    stop: bool,
}

fn lock(m: &std::sync::Mutex<Slot>) -> std::sync::MutexGuard<'_, Slot> {
    // every statement under the lock leaves the slot consistent
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Write `bytes` as `path`, atomically and durably (temporary file, `fsync`, rename, `fsync`
/// of the directory).
fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let Some(dir) = path.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(dir)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(dir)?.sync_all()
}

impl std::fmt::Debug for Saver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Saver")
    }
}

impl Saver {
    fn start(path: PathBuf) -> std::io::Result<Self> {
        let shared = std::sync::Arc::new((
            std::sync::Mutex::new(Slot::default()),
            std::sync::Condvar::new(),
        ));
        let theirs = shared.clone();
        let thread = std::thread::Builder::new()
            .name("ankaa-positions".into())
            .spawn(move || {
                let (slot, wake) = &*theirs;
                let mut s = lock(slot);
                loop {
                    if let Some(bytes) = s.next.take() {
                        s.writing = true;
                        drop(s);
                        let result = write_file(&path, &bytes);
                        s = lock(slot);
                        s.writing = false;
                        if let Err(e) = result {
                            s.failed = true;
                            warn!(error = %e, "source positions could not be saved; retrying");
                        }
                        wake.notify_all();
                    } else if s.stop {
                        return;
                    } else {
                        s = wake
                            .wait(s)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                }
            })?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    /// Hand over a document to write (replacing one that is still waiting).
    fn submit(&self, bytes: Vec<u8>) {
        let (slot, wake) = &*self.shared;
        lock(slot).next = Some(bytes);
        wake.notify_all();
    }

    /// Whether a write failed since the last call (the caller hands the document over again).
    fn take_failed(&self) -> bool {
        std::mem::take(&mut lock(&self.shared.0).failed)
    }

    /// Drop what is waiting and wait for a write in progress: after this the caller may write
    /// the file itself.
    fn quiesce(&self) {
        let (slot, wake) = &*self.shared;
        let mut s = lock(slot);
        s.next = None;
        while s.writing {
            s = wake
                .wait(s)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for Saver {
    fn drop(&mut self) {
        let (slot, wake) = &*self.shared;
        lock(slot).stop = true;
        wake.notify_all();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl SourceStates {
    /// Load `dir/sources.json` (`dir = None`: nothing is persisted, ids follow the config order).
    pub fn load(dir: Option<&Path>) -> anyhow::Result<Self> {
        let Some(dir) = dir else {
            return Ok(Self::default());
        };
        let path = dir.join(FILE);
        let doc = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Doc::default(),
            Err(e) => return Err(anyhow::anyhow!("{}: {e}", path.display())),
        };
        Ok(Self {
            path: Some(path),
            doc,
            pending: VecDeque::new(),
            dirty: false,
            saver: None,
        })
    }

    /// Id of the source called `name`, assigning the next free one on first sight.
    pub fn id_for(&mut self, name: &str) -> anyhow::Result<u16> {
        if let Some(i) = self.doc.sources.iter().position(|e| e.name == name) {
            return Ok(u16::try_from(i)?);
        }
        anyhow::ensure!(!name.is_empty(), "a source needs a name");
        let id = u16::try_from(self.doc.sources.len())
            .map_err(|_| anyhow::anyhow!("too many source names were ever configured"))?;
        self.doc.sources.push(Entry {
            name: name.to_owned(),
            state: None,
            lsn: 0,
        });
        self.dirty = true;
        Ok(id)
    }

    /// Highest WAL LSN any saved position refers to (the log must never number below it).
    pub fn max_lsn(&self) -> u64 {
        self.doc.sources.iter().map(|e| e.lsn).max().unwrap_or(0)
    }

    /// Current id of `name`, if it has one.
    #[cfg(test)]
    pub fn id_of(&self, name: &str) -> Option<u16> {
        self.doc
            .sources
            .iter()
            .position(|e| e.name == name)
            .and_then(|i| u16::try_from(i).ok())
    }

    /// A log record says `name` has id `id` (written by the server that produced the log).
    /// Adopt it. Returns `Some(other)` when slot `id` and slot `other` were exchanged to make
    /// room, so the caller can exchange its own per-id state the same way.
    pub fn adopt(&mut self, id: u16, name: &str) -> Option<u16> {
        let idx = usize::from(id);
        let old = self.doc.sources.iter().position(|e| e.name == name);
        if old == Some(idx) {
            return None;
        }
        if self.doc.sources.len() <= idx {
            self.doc.sources.resize_with(idx + 1, Entry::default);
        }
        self.dirty = true;
        if let Some(j) = old {
            self.doc.sources.swap(j, idx);
            let other = u16::try_from(j).ok()?;
            self.swap_pending(id, other);
            return Some(other);
        }
        // a name this node does not know; the slot's previous owner (if any) moves to a new id
        // at the end, which for the caller is again an exchange of two slots
        let displaced = std::mem::take(&mut self.doc.sources[idx]);
        name.clone_into(&mut self.doc.sources[idx].name);
        if displaced.name.is_empty() {
            return None;
        }
        self.doc.sources.push(displaced);
        let other = u16::try_from(self.doc.sources.len() - 1).ok()?;
        self.swap_pending(id, other);
        Some(other)
    }

    /// Positions waiting to become durable follow their source when two ids are exchanged.
    fn swap_pending(&mut self, a: u16, b: u16) {
        for (_, id, _) in &mut self.pending {
            if *id == a {
                *id = b;
            } else if *id == b {
                *id = a;
            }
        }
    }

    /// Number of ids assigned (including sources no longer configured).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.doc.sources.len()
    }

    /// Saved position of a source.
    pub fn position(&self, id: u16) -> Option<&str> {
        self.doc
            .sources
            .get(usize::from(id))
            .and_then(|e| e.state.as_deref())
    }

    /// A position reported by a live source in the frame numbered `lsn` (0 = not in a WAL).
    pub fn note(&mut self, id: u16, state: &str, lsn: u64) {
        // only the newest pending position per source matters
        self.pending.retain(|(_, i, _)| *i != id);
        self.pending.push_back((lsn, id, state.to_owned()));
    }

    /// A position found in the WAL during recovery: durable by definition.
    pub fn note_recovered(&mut self, id: u16, state: &str, lsn: u64) {
        self.apply(id, state, lsn);
    }

    fn apply(&mut self, id: u16, state: &str, lsn: u64) {
        let Some(e) = self.doc.sources.get_mut(usize::from(id)) else {
            return;
        };
        if lsn != 0 && lsn <= e.lsn {
            return; // older than what is saved
        }
        if e.state.as_deref() != Some(state) || e.lsn != lsn {
            e.state = Some(state.to_owned());
            e.lsn = lsn;
            self.dirty = true;
        }
    }

    /// Make positions whose frames are durable (`lsn <= durable_lsn`) permanent. With
    /// `everything = true` (clean shutdown after the final flush) all pending positions are.
    /// Returns how many positions became permanent.
    pub fn commit(&mut self, durable_lsn: u64, everything: bool) -> u64 {
        let mut keep = VecDeque::new();
        let mut applied = 0;
        while let Some((lsn, id, state)) = self.pending.pop_front() {
            if everything || (lsn != 0 && lsn <= durable_lsn) {
                self.apply(id, &state, lsn);
                applied += 1;
            } else {
                keep.push_back((lsn, id, state));
            }
        }
        self.pending = keep;
        if self.saver.as_ref().is_some_and(Saver::take_failed) {
            self.dirty = true;
        }
        if self.dirty {
            if let (Some(saver), false) = (&self.saver, everything) {
                // off this thread; a failure comes back as `take_failed`
                match serde_json::to_vec_pretty(&self.doc) {
                    Ok(bytes) => {
                        saver.submit(bytes);
                        self.dirty = false;
                    }
                    Err(e) => warn!(error = %e, "source positions could not be encoded"),
                }
            } else {
                match self.save() {
                    Ok(()) => self.dirty = false,
                    Err(e) => warn!(error = %e, "source positions could not be saved; retrying"),
                }
            }
        }
        applied
    }

    /// Save positions on a thread of their own from now on (a save is two `fsync`s; the caller
    /// is the ingest loop). Without a file there is nothing to do.
    pub fn with_background_saves(mut self) -> std::io::Result<Self> {
        if let Some(path) = &self.path {
            self.saver = Some(Saver::start(path.clone())?);
        }
        Ok(self)
    }

    /// Write the file now, on this thread (the saver thread, if any, is made to stand by first:
    /// two writers must never share the temporary file, and an older document must not land
    /// after this one).
    fn save(&self) -> std::io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(saver) = &self.saver {
            saver.quiesce();
        }
        write_file(path, &serde_json::to_vec_pretty(&self.doc)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With background saves a commit does not write the file itself; the file gets the latest
    /// positions soon after, a commit of everything (shutdown) writes at once, and what a
    /// restart reads is never ahead of what was committed.
    #[test]
    fn background_saves_write_the_latest_positions() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SourceStates::load(Some(dir.path()))
            .unwrap()
            .with_background_saves()
            .unwrap();
        let id = s.id_for("a").unwrap();
        for lsn in 1..=200u64 {
            s.note(id, &format!("0={lsn}"), lsn);
            s.commit(lsn, false);
        }
        let read = || {
            SourceStates::load(Some(dir.path()))
                .unwrap()
                .position(id)
                .map(str::to_owned)
        };
        let t0 = std::time::Instant::now();
        while read().as_deref() != Some("0=200") {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(10),
                "{:?}",
                read()
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // shutdown: written by the time the call returns, and nothing older lands after it
        s.note(id, "0=201", 201);
        s.note(id, "0=202", 0);
        s.commit(201, true);
        assert_eq!(read().as_deref(), Some("0=202"));
        drop(s);
        assert_eq!(read().as_deref(), Some("0=202"));
    }

    #[test]
    fn ids_are_stable_and_positions_wait_for_durability() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SourceStates::load(Some(dir.path())).unwrap();
        assert_eq!(s.id_for("a").unwrap(), 0);
        assert_eq!(s.id_for("b").unwrap(), 1);
        assert_eq!(s.id_for("a").unwrap(), 0);
        s.note(1, "0=10", 5);
        s.note(1, "0=20", 9);
        s.note(0, "100:abc", 7);
        s.commit(6, false);
        assert_eq!(s.position(1), None, "lsn 9 is not durable yet");
        assert_eq!(s.position(0), None);
        s.commit(8, false);
        assert_eq!(s.position(0), Some("100:abc"));
        assert_eq!(s.position(1), None);
        s.commit(9, false);
        assert_eq!(s.position(1), Some("0=20"));
        // restart: a reordered config keeps its ids and positions; a new name gets a new id
        let mut s = SourceStates::load(Some(dir.path())).unwrap();
        assert_eq!(s.id_for("b").unwrap(), 1);
        assert_eq!(s.id_for("c").unwrap(), 2);
        assert_eq!(s.id_for("a").unwrap(), 0);
        assert_eq!(s.position(1), Some("0=20"));
        assert_eq!(s.len(), 3);
        // recovery: positions replayed from the WAL never move backwards
        s.note_recovered(1, "0=15", 7);
        assert_eq!(s.position(1), Some("0=20"));
        s.note_recovered(1, "0=30", 12);
        assert_eq!(s.position(1), Some("0=30"));
        // a log written by another node names the ids differently: adopt its numbering
        assert_eq!(s.adopt(0, "b"), Some(1), "a and b exchange ids");
        assert_eq!(s.id_of("b"), Some(0));
        assert_eq!(s.id_of("a"), Some(1));
        assert_eq!(
            s.position(0),
            Some("0=30"),
            "the position moved with its source"
        );
        assert_eq!(s.adopt(0, "b"), None);
        assert_eq!(s.adopt(1, "a"), None);
        assert_eq!(s.adopt(0, "b"), None);
        // back to the original numbering for the rest of the test
        assert_eq!(s.adopt(1, "b"), Some(0));
        assert_eq!(s.adopt(5, "z"), None, "a new name in a free slot");
        assert_eq!(s.id_of("z"), Some(5));
        assert_eq!(s.max_lsn(), 12);
        // without a WAL (lsn 0) positions only become permanent at a clean shutdown
        s.note(2, "5:ff", 0);
        s.commit(u64::MAX, false);
        assert_eq!(s.position(2), None);
        s.commit(0, true);
        assert_eq!(s.position(2), Some("5:ff"));
        let s = SourceStates::load(Some(dir.path())).unwrap();
        assert_eq!(s.position(2), Some("5:ff"));
        assert_eq!(s.position(1), Some("0=30"));
    }

    #[test]
    fn without_a_directory_nothing_is_persisted() {
        let mut s = SourceStates::load(None).unwrap();
        assert_eq!(s.id_for("x").unwrap(), 0);
        s.note(0, "p", 0);
        s.commit(0, true);
        assert_eq!(s.position(0), Some("p"));
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE), b"not json").unwrap();
        assert!(SourceStates::load(Some(dir.path())).is_err());
    }
}
