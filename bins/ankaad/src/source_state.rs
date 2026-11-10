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
        if self.dirty {
            match self.save() {
                Ok(()) => self.dirty = false,
                Err(e) => warn!(error = %e, "source positions could not be saved; retrying"),
            }
        }
        applied
    }

    fn save(&self) -> std::io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let Some(dir) = path.parent() else {
            return Ok(());
        };
        std::fs::create_dir_all(dir)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.doc)?)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, path)?;
        std::fs::File::open(dir)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
