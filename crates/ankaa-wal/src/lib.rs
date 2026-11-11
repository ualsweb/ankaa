//! Write-ahead log (PRD v4.1 §4.7, v3.2 RFC-015).
//!
//! Record: `[len u32][crc32c u32][lsn u64][epoch u64][payload]`, little-endian; the CRC covers
//! `lsn`, `epoch` and `payload`. Records are appended to segment files `wal-<first_lsn:020>.log`
//! under one directory. **Group commit**: appends land in a user-space buffer and become durable
//! on [`Wal::commit`] (called by the owner every `group_commit_ms` or when the buffer exceeds
//! `group_commit_bytes`), which writes and `fdatasync`s. Readers tolerate a torn tail (truncated
//! last record) and stop at the first record whose CRC fails, reporting the byte offset.
//!
//! Durability contract: after `commit()` returns `Ok`, every record with `lsn <= durable_lsn()`
//! survives power loss (verified by the dm-flakey rig, `scripts/power-loss-rig.sh`).
//!
//! The hot ingest path never calls into this crate synchronously: appenders hand payloads to the
//! WAL owner through a ring; the owner batches. `io_uring` `O_DIRECT` is a Linux feature for P1-05b.
#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(not(test), warn(clippy::expect_used))]

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Fixed record header size.
pub const HEADER_LEN: usize = 4 + 4 + 8 + 8;
/// Largest payload accepted (defensive bound for readers).
pub const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

/// WAL errors.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// I/O.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// Payload too large.
    #[error("payload of {0} bytes exceeds MAX_PAYLOAD")]
    TooLarge(usize),
    /// Corruption detected during replay (records before `offset` were fine).
    #[error("corrupt record in {segment} at offset {offset}: {reason}")]
    Corrupt {
        /// Segment file.
        segment: PathBuf,
        /// Byte offset of the bad record.
        offset: u64,
        /// What failed.
        reason: &'static str,
    },
    /// Segment file name not understood.
    #[error("unexpected file in WAL directory: {0}")]
    BadName(PathBuf),
    /// A follower asked for records that a checkpoint has already removed.
    #[error(
        "WAL records from lsn {wanted} are gone (oldest available: {oldest}); re-seed the follower"
    )]
    Truncated {
        /// First LSN requested.
        wanted: u64,
        /// Oldest LSN still in the log.
        oldest: u64,
    },
    /// A follower asked for records beyond the end of the log.
    #[error("follower wants lsn {wanted} but the log ends at {last}: it is ahead of this log")]
    Ahead {
        /// First LSN requested.
        wanted: u64,
        /// Last LSN in the log.
        last: u64,
    },
}

/// Writer configuration.
#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    /// Buffer bytes that trigger an early commit hint (`needs_commit()`).
    pub group_commit_bytes: usize,
    /// Rotate to a new segment after this many bytes.
    pub segment_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            group_commit_bytes: 256 * 1024,
            segment_bytes: 256 * 1024 * 1024,
        }
    }
}

/// A replayed record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Log sequence number (monotonic, starts at 1).
    pub lsn: u64,
    /// Writer-defined epoch (e.g. ingest epoch or checkpoint generation).
    pub epoch: u64,
    /// Payload.
    pub payload: Vec<u8>,
}

/// Append-only writer with group commit.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    cfg: WalConfig,
    file: File,
    segment_path: PathBuf,
    segment_len: u64,
    buf: Vec<u8>,
    next_lsn: u64,
    durable_lsn: u64,
    pending_first_lsn: u64,
    /// A failed commit could not cut its partial bytes off the segment: do it before writing.
    tail_dirty: bool,
}

#[inline]
fn le_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

#[inline]
fn le_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

fn segment_name(first_lsn: u64) -> String {
    format!("wal-{first_lsn:020}.log")
}

fn parse_segment_name(p: &Path) -> Option<u64> {
    let name = p.file_name()?.to_str()?;
    let digits = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    digits.parse().ok()
}

/// Extension of the marker placed next to a segment whose first LSN deliberately does not
/// follow the last record before it (see [`Wal::open_at`]).
pub const JUMP_EXT: &str = "jump";

/// List segment files in ascending LSN order. Files that are not WAL segments (editor swap
/// files, `.DS_Store`, ...) are ignored; callers that want to know about them use
/// [`list_foreign_files`].
pub fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut v = Vec::new();
    for entry in fs::read_dir(dir)? {
        let p = entry?.path();
        if p.is_file()
            && let Some(lsn) = parse_segment_name(&p)
        {
            v.push((lsn, p));
        }
    }
    v.sort();
    Ok(v)
}

/// Files in the WAL directory that are not segments (for a startup warning).
pub fn list_foreign_files(dir: &Path) -> Result<Vec<PathBuf>, WalError> {
    let mut v = Vec::new();
    for entry in fs::read_dir(dir)? {
        let p = entry?.path();
        if p.is_file() && parse_segment_name(&p).is_none() {
            v.push(p);
        }
    }
    v.sort();
    Ok(v)
}

impl Wal {
    /// Open (or create) the WAL in `dir`, replaying existing segments to find the next LSN.
    /// Returns the writer and the last LSN already assigned (0 when the log is new).
    pub fn open(dir: &Path, cfg: WalConfig) -> Result<(Self, u64), WalError> {
        Self::open_at(dir, cfg, 1)
    }

    /// [`Self::open`] with a floor for the next LSN. LSNs are never reused: the next one is
    /// the highest of (last record + 1), the first LSN named by the newest segment file (a
    /// checkpoint may have deleted every record and left only the empty active segment), and
    /// `min_next_lsn` (what the caller knows from elsewhere, e.g. the highest LSN its
    /// persisted data refers to). A log that restarted below LSNs already in use would have
    /// its new records mistaken for old ones.
    pub fn open_at(dir: &Path, cfg: WalConfig, min_next_lsn: u64) -> Result<(Self, u64), WalError> {
        fs::create_dir_all(dir)?;
        let mut last_lsn = 0u64;
        let mut rp = replay(dir)?;
        for r in rp.by_ref() {
            let r = r?;
            last_lsn = r.lsn;
        }
        if let Some((path, valid_len)) = rp.torn_tail().cloned() {
            // Repair: drop the incomplete record so later segments stay consistent.
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(valid_len)?;
            f.sync_all()?;
        }
        let named = list_segments(dir)?.last().map_or(0, |(first, _)| *first);
        let next_lsn = (last_lsn + 1).max(named).max(min_next_lsn).max(1);
        let segment_path = dir.join(segment_name(next_lsn));
        if last_lsn > 0 && next_lsn > last_lsn + 1 {
            // The log continues above a hole on purpose. Say so next to the segment: replay
            // accepts a jump in the numbering only where this marker is, so a segment file
            // that has gone missing is still reported as damage.
            File::create(segment_path.with_extension(JUMP_EXT))?.sync_all()?;
        }
        let last_lsn = next_lsn - 1;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;
        let segment_len = file.metadata()?.len();
        // Make the directory entry durable too (new file creation).
        sync_dir(dir)?;
        Ok((
            Self {
                dir: dir.to_path_buf(),
                cfg,
                file,
                segment_path,
                segment_len,
                buf: Vec::with_capacity(cfg.group_commit_bytes * 2),
                next_lsn,
                durable_lsn: last_lsn,
                pending_first_lsn: next_lsn,
                tail_dirty: false,
            },
            last_lsn,
        ))
    }

    /// Buffer a record; returns its LSN. Not durable until [`Self::commit`].
    pub fn append(&mut self, epoch: u64, payload: &[u8]) -> Result<u64, WalError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(WalError::TooLarge(payload.len()));
        }
        self.buf.reserve(HEADER_LEN + payload.len());
        self.append_with(epoch, |b| b.extend_from_slice(payload))
    }

    /// Buffer a record whose payload `write` appends to the log's own buffer: the payload is
    /// encoded in place, without an intermediate copy. `write` must only append. The checksum
    /// is one pass over `lsn | epoch | payload`, which lie contiguous after the length and
    /// checksum words. Returns the LSN; a payload over [`MAX_PAYLOAD`] is taken back out.
    pub fn append_with(
        &mut self,
        epoch: u64,
        write: impl FnOnce(&mut Vec<u8>),
    ) -> Result<u64, WalError> {
        let start = self.buf.len();
        let lsn = self.next_lsn;
        self.buf.extend_from_slice(&[0u8; 8]);
        self.buf.extend_from_slice(&lsn.to_le_bytes());
        self.buf.extend_from_slice(&epoch.to_le_bytes());
        write(&mut self.buf);
        let len = self.buf.len() - start - HEADER_LEN;
        let Some(len32) = u32::try_from(len).ok().filter(|_| len <= MAX_PAYLOAD) else {
            self.buf.truncate(start);
            return Err(WalError::TooLarge(len));
        };
        let crc = crc32c::crc32c(&self.buf[start + 8..]);
        self.buf[start..start + 4].copy_from_slice(&len32.to_le_bytes());
        self.buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
        self.next_lsn += 1;
        Ok(lsn)
    }

    /// True when the buffer has crossed `group_commit_bytes` (owner should commit now).
    #[must_use]
    pub fn needs_commit(&self) -> bool {
        self.buf.len() >= self.cfg.group_commit_bytes
    }

    /// Bytes buffered but not yet durable.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Write the buffer and `fdatasync`. Afterwards `durable_lsn() == last appended LSN`.
    pub fn commit(&mut self) -> Result<u64, WalError> {
        if self.buf.is_empty() {
            return Ok(self.durable_lsn);
        }
        if self.tail_dirty {
            self.file.set_len(self.segment_len)?;
            self.tail_dirty = false;
        }
        if let Err(e) = self
            .file
            .write_all(&self.buf)
            .and_then(|()| self.file.sync_data())
        {
            // Nothing of this batch counts as written: cut the segment back to its last durable
            // length so that a retry (the buffer is kept) cannot leave half a batch followed by
            // the whole batch again, which replay would report as corruption. If even that
            // fails, the next commit does it before it writes.
            self.tail_dirty = self.file.set_len(self.segment_len).is_err();
            return Err(e.into());
        }
        self.segment_len += self.buf.len() as u64;
        self.buf.clear();
        self.durable_lsn = self.next_lsn - 1;
        self.pending_first_lsn = self.next_lsn;
        if self.segment_len >= self.cfg.segment_bytes {
            self.rotate()?;
        }
        Ok(self.durable_lsn)
    }

    fn rotate(&mut self) -> Result<(), WalError> {
        let path = self.dir.join(segment_name(self.next_lsn));
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        if let Err(e) = sync_dir(&self.dir) {
            // keep writing to the current segment; an empty file with a higher first LSN
            // would make the names lie about where records are
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(e.into());
        }
        self.file = file;
        self.segment_path = path;
        self.segment_len = 0;
        Ok(())
    }

    /// Highest LSN guaranteed durable.
    #[must_use]
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn
    }

    /// Next LSN that `append` will assign.
    #[must_use]
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// LSN of the first buffered (not yet durable) record, if any.
    #[must_use]
    pub fn pending_first_lsn(&self) -> Option<u64> {
        (self.pending_first_lsn < self.next_lsn).then_some(self.pending_first_lsn)
    }

    /// Current segment path.
    #[must_use]
    pub fn segment_path(&self) -> &Path {
        &self.segment_path
    }

    /// Delete whole segments whose every record has `lsn < checkpoint_lsn`
    /// (the checkpoint has made them redundant). The active segment is never deleted.
    /// Returns the number of segments removed.
    pub fn truncate_before(&mut self, checkpoint_lsn: u64) -> Result<usize, WalError> {
        let segs = list_segments(&self.dir)?;
        let mut removed = 0;
        for (i, (first, path)) in segs.iter().enumerate() {
            if *path == self.segment_path {
                break;
            }
            // A segment is removable when the next segment starts at or below the checkpoint:
            // then all records in this one are < checkpoint_lsn.
            let next_first = segs.get(i + 1).map_or(u64::MAX, |(f, _)| *f);
            if *first < checkpoint_lsn && next_first <= checkpoint_lsn {
                fs::remove_file(path)?;
                let _ = fs::remove_file(path.with_extension(JUMP_EXT));
                removed += 1;
            } else {
                break;
            }
        }
        if removed > 0 {
            sync_dir(&self.dir)?;
        }
        Ok(removed)
    }
}

/// Iterate all records in `dir` in LSN order. Stops cleanly at a torn tail of the **last**
/// segment; any other inconsistency yields `Err(WalError::Corrupt)` and ends iteration.
pub fn replay(dir: &Path) -> Result<Replay, WalError> {
    let segments = list_segments(dir)?;
    Ok(Replay {
        segments,
        idx: 0,
        cur: None,
        cur_first: 0,
        offset: 0,
        expect_lsn: None,
        done: false,
        torn: None,
    })
}

/// Replay iterator.
#[derive(Debug)]
pub struct Replay {
    segments: Vec<(u64, PathBuf)>,
    idx: usize,
    cur: Option<(Vec<u8>, PathBuf)>,
    /// First LSN named by the current segment file.
    cur_first: u64,
    offset: usize,
    expect_lsn: Option<u64>,
    done: bool,
    torn: Option<(PathBuf, u64)>,
}

impl Replay {
    /// After iteration: the torn tail of the last segment, if one was found, as
    /// `(segment, byte offset of the first incomplete record)`.
    #[must_use]
    pub fn torn_tail(&self) -> Option<&(PathBuf, u64)> {
        self.torn.as_ref()
    }

    fn load_next_segment(&mut self) -> Result<bool, WalError> {
        if self.idx >= self.segments.len() {
            return Ok(false);
        }
        let (first, path) = &self.segments[self.idx];
        let mut data = Vec::new();
        File::open(path)?.read_to_end(&mut data)?;
        self.cur_first = *first;
        self.cur = Some((data, path.clone()));
        self.offset = 0;
        self.idx += 1;
        Ok(true)
    }

    fn is_last_segment(&self) -> bool {
        self.idx >= self.segments.len()
    }
}

impl Iterator for Replay {
    type Item = Result<Record, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.cur.is_none() {
                match self.load_next_segment() {
                    Ok(true) => {}
                    Ok(false) => {
                        self.done = true;
                        return None;
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                }
            }
            let Some((data, path)) = self.cur.as_ref() else {
                continue;
            };
            let rest = &data[self.offset..];
            if rest.is_empty() {
                self.cur = None;
                continue;
            }
            let corrupt = |reason: &'static str, offset: usize| WalError::Corrupt {
                segment: path.clone(),
                offset: offset as u64,
                reason,
            };
            if rest.len() < HEADER_LEN {
                if self.is_last_segment() {
                    self.torn = Some((path.clone(), self.offset as u64));
                    self.done = true;
                    return None; // torn tail
                }
                self.done = true;
                return Some(Err(corrupt(
                    "truncated header in non-final segment",
                    self.offset,
                )));
            }
            let len = le_u32(&rest[0..4]) as usize;
            let crc = le_u32(&rest[4..8]);
            let lsn = le_u64(&rest[8..16]);
            let epoch = le_u64(&rest[16..24]);
            if len > MAX_PAYLOAD {
                if self.is_last_segment() && rest.iter().all(|&b| b == 0) {
                    // Zero-filled tail (pre-allocated or writeback-lost blocks): torn, not corrupt.
                    self.torn = Some((path.clone(), self.offset as u64));
                    self.done = true;
                    return None;
                }
                self.done = true;
                return Some(Err(corrupt("length exceeds MAX_PAYLOAD", self.offset)));
            }
            if rest.len() < HEADER_LEN + len {
                if self.is_last_segment() {
                    self.torn = Some((path.clone(), self.offset as u64));
                    self.done = true;
                    return None; // torn tail
                }
                self.done = true;
                return Some(Err(corrupt(
                    "truncated payload in non-final segment",
                    self.offset,
                )));
            }
            let payload = &rest[HEADER_LEN..HEADER_LEN + len];
            let mut h = crc32c::crc32c(&rest[8..16]);
            h = crc32c::crc32c_append(h, &rest[16..24]);
            h = crc32c::crc32c_append(h, payload);
            if h != crc {
                if self.is_last_segment() && rest.iter().all(|&b| b == 0) {
                    self.torn = Some((path.clone(), self.offset as u64));
                    self.done = true;
                    return None;
                }
                self.done = true;
                return Some(Err(corrupt("crc mismatch", self.offset)));
            }
            // LSNs are consecutive, except that a segment may begin at a later LSN than the
            // previous one ended (the log was reopened above a floor, see `Wal::open_at`): the
            // records in between were never written
            if let Some(e) = self.expect_lsn
                && lsn != e
                && !(self.offset == 0
                    && lsn > e
                    && lsn == self.cur_first
                    && path.with_extension(JUMP_EXT).exists())
            {
                self.done = true;
                return Some(Err(corrupt("lsn out of sequence", self.offset)));
            }
            self.expect_lsn = Some(lsn + 1);
            let rec = Record {
                lsn,
                epoch,
                payload: payload.to_vec(),
            };
            self.offset += HEADER_LEN + len;
            return Some(Ok(rec));
        }
    }
}

/// Make the entries of `dir` durable: a created, renamed or removed file survives a power loss
/// only once its directory is synced.
fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    // Miri (the interpreter CI runs the unsafe-code checks in) cannot fsync a directory.
    // Durability is verified by the power-loss rig (scripts/power-loss-rig.sh), never by Miri.
    if cfg!(miri) {
        return Ok(());
    }
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A payload written in place is byte for byte the record `append` writes (same length,
    /// checksum, LSN), and an oversized one is taken back out without using an LSN.
    #[test]
    fn append_with_matches_append() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let (mut wa, _) = Wal::open(a.path(), WalConfig::default()).unwrap();
        let (mut wb, _) = Wal::open(b.path(), WalConfig::default()).unwrap();
        for (i, p) in [&b""[..], b"x", b"hello wal", &[7u8; 5000][..]]
            .iter()
            .enumerate()
        {
            let epoch = i as u64 + 3;
            assert_eq!(
                wa.append(epoch, p).unwrap(),
                wb.append_with(epoch, |buf| buf.extend_from_slice(p))
                    .unwrap()
            );
        }
        assert_eq!(wa.buf, wb.buf);
        let before = (wb.buf.len(), wb.next_lsn);
        let too_big = wb.append_with(1, |buf| buf.resize(buf.len() + MAX_PAYLOAD + 1, 0));
        assert!(matches!(too_big, Err(WalError::TooLarge(n)) if n == MAX_PAYLOAD + 1));
        assert_eq!((wb.buf.len(), wb.next_lsn), before);
        wb.commit().unwrap();
        let (recs, err) = all(b.path());
        assert!(err.is_none());
        assert_eq!(recs.len(), 4);
        assert_eq!(&recs[2].payload[..], b"hello wal");
    }

    fn all(dir: &Path) -> (Vec<Record>, Option<WalError>) {
        let mut v = Vec::new();
        for r in replay(dir).unwrap() {
            match r {
                Ok(rec) => v.push(rec),
                Err(e) => return (v, Some(e)),
            }
        }
        (v, None)
    }

    #[test]
    fn append_commit_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut w, last) = Wal::open(tmp.path(), WalConfig::default()).unwrap();
        assert_eq!(last, 0);
        assert_eq!(w.append(7, b"hello").unwrap(), 1);
        assert_eq!(w.append(7, b"").unwrap(), 2);
        assert_eq!(w.durable_lsn(), 0);
        assert_eq!(w.commit().unwrap(), 2);
        assert_eq!(w.durable_lsn(), 2);
        let (recs, err) = all(tmp.path());
        assert!(err.is_none());
        assert_eq!(
            recs,
            vec![
                Record {
                    lsn: 1,
                    epoch: 7,
                    payload: b"hello".to_vec()
                },
                Record {
                    lsn: 2,
                    epoch: 7,
                    payload: vec![]
                }
            ]
        );
        // reopen continues the sequence
        drop(w);
        let (mut w2, last2) = Wal::open(tmp.path(), WalConfig::default()).unwrap();
        assert_eq!(last2, 2);
        assert_eq!(w2.append(8, b"x").unwrap(), 3);
        w2.commit().unwrap();
        let (recs, _) = all(tmp.path());
        assert_eq!(recs.len(), 3);
    }

    #[test]
    fn torn_tail_is_tolerated_and_corruption_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut w, _) = Wal::open(tmp.path(), WalConfig::default()).unwrap();
        for i in 0..5u64 {
            w.append(1, &i.to_le_bytes()).unwrap();
        }
        w.commit().unwrap();
        let seg = w.segment_path().to_path_buf();
        drop(w);
        // torn tail: cut the last record in half
        let bytes = fs::read(&seg).unwrap();
        fs::write(&seg, &bytes[..bytes.len() - 5]).unwrap();
        let (recs, err) = all(tmp.path());
        assert!(err.is_none());
        assert_eq!(recs.len(), 4);
        // reopen repairs the torn tail (truncates the segment to its last complete record) and
        // starts a new segment at lsn 5; a full replay must then be clean across both segments.
        let (mut w2, last) = Wal::open(tmp.path(), WalConfig::default()).unwrap();
        assert_eq!(last, 4);
        assert_eq!(
            fs::metadata(&seg).unwrap().len(),
            bytes.len() as u64 - (HEADER_LEN as u64 + 8)
        );
        w2.append(2, b"after-repair").unwrap();
        w2.commit().unwrap();
        drop(w2);
        let (recs, err) = all(tmp.path());
        assert!(err.is_none(), "{err:?}");
        assert_eq!(recs.len(), 5);
        assert_eq!(recs[4].payload, b"after-repair");
        // remove the second segment again so the corruption test below sees one segment
        for (_, p) in list_segments(tmp.path()).unwrap().into_iter().skip(1) {
            fs::remove_file(p).unwrap();
        }
        // corruption in the middle: flip a payload byte of record 2
        let bytes = fs::read(&seg).unwrap();
        let mut bad = bytes.clone();
        bad[HEADER_LEN * 2 + 8 + 3] ^= 0x55; // inside record 2's payload
        fs::write(&seg, &bad).unwrap();
        let (recs, err) = all(tmp.path());
        assert_eq!(recs.len(), 1);
        assert!(
            matches!(
                err,
                Some(WalError::Corrupt {
                    reason: "crc mismatch",
                    ..
                })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn zero_filled_tail_is_torn_and_foreign_files_are_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut w, _) = Wal::open(tmp.path(), WalConfig::default()).unwrap();
        for i in 0..3u64 {
            w.append(1, &i.to_le_bytes()).unwrap();
        }
        w.commit().unwrap();
        let seg = w.segment_path().to_path_buf();
        drop(w);
        // a writeback-style loss leaves zero blocks after the last good record
        let mut bytes = fs::read(&seg).unwrap();
        bytes.extend(std::iter::repeat_n(0u8, 4096));
        fs::write(&seg, &bytes).unwrap();
        fs::write(tmp.path().join(".DS_Store"), b"junk").unwrap();
        fs::write(tmp.path().join("notes.txt"), b"junk").unwrap();
        let (recs, err) = all(tmp.path());
        assert!(err.is_none(), "{err:?}");
        assert_eq!(recs.len(), 3);
        assert_eq!(list_foreign_files(tmp.path()).unwrap().len(), 2);
        // reopen repairs and continues
        let (mut w2, last) = Wal::open(tmp.path(), WalConfig::default()).unwrap();
        assert_eq!(last, 3);
        w2.append(1, b"x").unwrap();
        w2.commit().unwrap();
        let (recs, err) = all(tmp.path());
        assert!(err.is_none(), "{err:?}");
        assert_eq!(recs.len(), 4);
        // garbage (non-zero) tail is still reported as corruption, never silently dropped
        let mut bytes = fs::read(&seg).unwrap(); // already repaired to 3 records by the reopen
        bytes.extend_from_slice(&[0xAB; 64]);
        fs::write(&seg, &bytes).unwrap();
        for (_, p) in list_segments(tmp.path()).unwrap().into_iter().skip(1) {
            fs::remove_file(p).unwrap();
        }
        let (recs, err) = all(tmp.path());
        assert_eq!(recs.len(), 3);
        assert!(matches!(err, Some(WalError::Corrupt { .. })), "{err:?}");
    }

    /// LSNs are never reused: not after a checkpoint emptied the log, and not when the data
    /// already refers to higher LSNs than the log holds.
    #[test]
    fn numbering_never_restarts_below_what_was_assigned() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = WalConfig {
            group_commit_bytes: 64,
            segment_bytes: 100,
        };
        let (mut w, _) = Wal::open(tmp.path(), cfg).unwrap();
        for i in 0..9u8 {
            w.append(1, &[i; 20]).unwrap();
            w.commit().unwrap(); // rotates every 3 records: the active segment ends up empty
        }
        assert_eq!(w.next_lsn(), 10);
        // a checkpoint past everything deletes every record; only the empty active segment stays
        assert!(w.truncate_before(10).unwrap() >= 1);
        drop(w);
        let (recs, err) = all(tmp.path());
        assert!(err.is_none() && recs.is_empty());
        let (mut w, last) = Wal::open(tmp.path(), cfg).unwrap();
        assert_eq!(last, 9, "numbering continues after the deleted records");
        assert_eq!(w.append(1, b"x").unwrap(), 10);
        w.commit().unwrap();
        drop(w);
        // the caller knows of higher LSNs (segments written before a power loss): jump there
        let (mut w, last) = Wal::open_at(tmp.path(), cfg, 50).unwrap();
        assert_eq!(last, 49);
        assert_eq!(w.durable_lsn(), 49);
        assert_eq!(w.append(1, b"y").unwrap(), 50);
        assert_eq!(w.append(1, b"z").unwrap(), 51);
        w.commit().unwrap();
        drop(w);
        // replay accepts the jump at the segment boundary and nothing else
        let (recs, err) = all(tmp.path());
        assert!(err.is_none(), "{err:?}");
        assert_eq!(
            recs.iter().map(|r| r.lsn).collect::<Vec<_>>(),
            vec![10, 50, 51]
        );
        // without the marker the same jump is damage (a segment file went missing)
        let marker = list_segments(tmp.path())
            .unwrap()
            .last()
            .unwrap()
            .1
            .with_extension(JUMP_EXT);
        assert!(marker.exists());
        let hidden = marker.with_extension("hidden");
        fs::rename(&marker, &hidden).unwrap();
        let (recs, err) = all(tmp.path());
        assert_eq!(recs.len(), 1);
        assert!(matches!(err, Some(WalError::Corrupt { .. })), "{err:?}");
        fs::rename(&hidden, &marker).unwrap();
        // a floor below what the log holds changes nothing
        let (w, last) = Wal::open_at(tmp.path(), cfg, 7).unwrap();
        assert_eq!(last, 51);
        drop(w);
        // a follower cannot be served across the hole: it is told to re-seed
        let mut t = Tail::new(tmp.path(), 10);
        let err = loop {
            match t.poll(1) {
                Ok(v) if v.is_empty() => panic!("stalled without an error"),
                Ok(_) => {}
                Err(e) => break e,
            }
        };
        assert!(
            matches!(
                err,
                WalError::Truncated {
                    wanted: 11,
                    oldest: 50
                }
            ),
            "{err:?}"
        );
        // while one that starts after the hole is served
        assert_eq!(Tail::new(tmp.path(), 50).poll(10).unwrap().len(), 2);
    }

    #[test]
    fn rotation_and_truncate_before_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = WalConfig {
            group_commit_bytes: 64,
            segment_bytes: 100,
        };
        let (mut w, _) = Wal::open(tmp.path(), cfg).unwrap();
        for i in 0..12u64 {
            w.append(1, &[i as u8; 20]).unwrap(); // 44 bytes per record
            w.commit().unwrap(); // rotate every ~3 records
        }
        let segs = list_segments(tmp.path()).unwrap();
        assert!(segs.len() >= 4, "{segs:?}");
        let (recs, err) = all(tmp.path());
        assert!(err.is_none());
        assert_eq!(recs.len(), 12);
        // checkpoint at lsn 7: segments entirely below 7 go away, the rest stay
        let removed = w.truncate_before(7).unwrap();
        assert!(removed >= 1);
        let (recs, err) = all(tmp.path());
        assert!(err.is_none());
        assert!(recs.first().unwrap().lsn <= 7);
        assert_eq!(recs.last().unwrap().lsn, 12);
        assert!(recs.iter().all(|r| r.lsn >= recs[0].lsn));
    }
}

/// Follow a WAL directory from `from_lsn`: like [`replay`] but resumable, for replication.
/// `poll` returns the complete records written so far (never a partial tail) and remembers the
/// segment and byte offset where it stopped, so the next call only reads new bytes.
///
/// Continuity is checked against the request: if the first record available is newer than
/// `from_lsn` the tail fails with [`WalError::Truncated`] (the follower must be re-seeded), and
/// if the log ends before `from_lsn - 1` it fails with [`WalError::Ahead`] (the follower holds
/// records this log does not).
#[derive(Debug)]
pub struct Tail {
    dir: PathBuf,
    next_lsn: u64,
    /// `(first LSN in the segment name, path, byte offset of the next unread record)`.
    pos: Option<(u64, PathBuf, u64)>,
    /// True once a record has been delivered.
    started: bool,
    /// Last LSN seen while skipping records below `next_lsn` (0 = none).
    last_seen: u64,
    /// Highest LSN that may be delivered (the writer's durable LSN, when the caller knows it).
    limit: u64,
}

/// Default byte budget of one [`Tail::poll`].
pub const TAIL_POLL_BYTES: usize = 8 * 1024 * 1024;

impl Tail {
    /// Start tailing at `from_lsn` (1 = from the beginning).
    #[must_use]
    pub fn new(dir: &Path, from_lsn: u64) -> Self {
        Self {
            dir: dir.to_path_buf(),
            next_lsn: from_lsn.max(1),
            pos: None,
            started: false,
            last_seen: 0,
            limit: u64::MAX,
        }
    }

    /// Deliver nothing above `lsn`. A server that owns the writer passes its durable LSN before
    /// every poll, so followers never receive a record that a power loss could still undo.
    pub fn set_limit(&mut self, lsn: u64) {
        self.limit = lsn;
    }

    /// Next LSN this tail will deliver.
    #[must_use]
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Records with `lsn >= next_lsn`, at most `max` (and about [`TAIL_POLL_BYTES`]).
    pub fn poll(&mut self, max: usize) -> Result<Vec<Record>, WalError> {
        self.poll_bytes(max, TAIL_POLL_BYTES)
    }

    /// Current read position, finding the segment that may hold `next_lsn` when there is none
    /// (`None` = the directory has no segments yet).
    fn locate(&mut self) -> Result<Option<(u64, PathBuf, u64)>, WalError> {
        if let Some(p) = &self.pos {
            return Ok(Some(p.clone()));
        }
        let segments = list_segments(&self.dir)?;
        let Some(oldest) = segments.first().map(|(f, _)| *f) else {
            return Ok(None);
        };
        // the last segment starting at or below next_lsn
        let Some((f, p)) = segments.iter().rev().find(|(f, _)| *f <= self.next_lsn) else {
            return Err(WalError::Truncated {
                wanted: self.next_lsn,
                oldest,
            });
        };
        self.last_seen = f.saturating_sub(1);
        self.pos = Some((*f, p.clone(), 0));
        Ok(self.pos.clone())
    }

    /// [`Self::poll`] with an explicit payload byte budget (at least one record is returned when
    /// one is available, whatever its size).
    #[allow(clippy::too_many_lines)]
    pub fn poll_bytes(&mut self, max: usize, max_bytes: usize) -> Result<Vec<Record>, WalError> {
        use std::io::{BufReader, Seek, SeekFrom};
        let mut out = Vec::new();
        let mut bytes = 0usize;
        'segments: loop {
            let Some((first, path, offset)) = self.locate()? else {
                return Ok(out);
            };
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    // deleted by a checkpoint while we were behind it: locate again (which
                    // reports the truncation if the records are gone)
                    self.pos = None;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let len = file.metadata()?.len();
            let next_segment = |dir: &Path| -> Result<Option<(u64, PathBuf)>, WalError> {
                Ok(list_segments(dir)?.into_iter().find(|(f, _)| *f > first))
            };
            if len <= offset {
                // nothing new in this segment; did the writer rotate?
                let Some((f, p)) = next_segment(&self.dir)? else {
                    if !self.started && self.last_seen.saturating_add(1) < self.next_lsn {
                        return Err(WalError::Ahead {
                            wanted: self.next_lsn,
                            last: self.last_seen,
                        });
                    }
                    return Ok(out);
                };
                // the writer commits before it rotates: once a successor exists this segment is
                // final, but it may have grown between our stat and the listing
                if fs::metadata(&path).map_or(len, |m| m.len()) > offset {
                    continue;
                }
                self.pos = Some((f, p, 0));
                continue;
            }
            let mut rd = BufReader::with_capacity(1 << 20, file);
            rd.seek(SeekFrom::Start(offset))?;
            let mut at = offset;
            let corrupt = |reason: &'static str, at: u64| WalError::Corrupt {
                segment: path.clone(),
                offset: at,
                reason,
            };
            loop {
                let remaining = len - at;
                if remaining == 0 {
                    break;
                }
                // an incomplete record at the end is a write in progress (or a torn tail that the
                // writer repairs on restart) unless a later segment exists
                let mut header = [0u8; HEADER_LEN];
                let incomplete = if remaining < HEADER_LEN as u64 {
                    true
                } else {
                    rd.read_exact(&mut header)?;
                    false
                };
                let plen = le_u32(&header[0..4]) as usize;
                let crc = le_u32(&header[4..8]);
                let lsn = le_u64(&header[8..16]);
                let epoch = le_u64(&header[16..24]);
                let zero_header = !incomplete && header.iter().all(|&b| b == 0);
                if incomplete || zero_header || remaining < (HEADER_LEN + plen) as u64 {
                    if !zero_header && !incomplete && plen > MAX_PAYLOAD {
                        return Err(corrupt("length exceeds MAX_PAYLOAD", at));
                    }
                    if next_segment(&self.dir)?.is_some() {
                        // the writer may have finished this record and rotated since we
                        // measured the file: look again before calling it damage
                        if fs::metadata(&path).map_or(len, |m| m.len()) > len {
                            self.pos = Some((first, path.clone(), at));
                            continue 'segments;
                        }
                        return Err(corrupt("truncated record in non-final segment", at));
                    }
                    self.pos = Some((first, path.clone(), at));
                    return Ok(out);
                }
                if plen > MAX_PAYLOAD {
                    return Err(corrupt("length exceeds MAX_PAYLOAD", at));
                }
                let mut payload = vec![0u8; plen];
                rd.read_exact(&mut payload)?;
                let mut h = crc32c::crc32c(&header[8..16]);
                h = crc32c::crc32c_append(h, &header[16..24]);
                h = crc32c::crc32c_append(h, &payload);
                if h != crc {
                    return Err(corrupt("crc mismatch", at));
                }
                let record_start = at;
                at += (HEADER_LEN + plen) as u64;
                if lsn < self.next_lsn {
                    if self.started {
                        return Err(corrupt("lsn out of sequence", at));
                    }
                    self.last_seen = lsn;
                    self.pos = Some((first, path.clone(), at));
                    continue;
                }
                if lsn > self.next_lsn {
                    // after the first delivery a jump is only legitimate at the start of a
                    // segment named after it (records never written, see `Wal::open_at`); a
                    // follower cannot be given a log with a hole either way
                    if self.started && !(record_start == 0 && lsn == first) {
                        return Err(corrupt("lsn out of sequence", at));
                    }
                    return Err(WalError::Truncated {
                        wanted: self.next_lsn,
                        oldest: lsn,
                    });
                }
                if lsn > self.limit {
                    // written but not durable yet: stay in front of it
                    return Ok(out);
                }
                self.started = true;
                self.next_lsn = lsn + 1;
                self.pos = Some((first, path.clone(), at));
                bytes += plen;
                out.push(Record {
                    lsn,
                    epoch,
                    payload,
                });
                if out.len() >= max || bytes >= max_bytes {
                    return Ok(out);
                }
            }
            self.pos = Some((first, path, at));
            // end of what this segment holds now: loop once more to look for a successor
        }
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;

    fn wal(dir: &Path, segment_bytes: u64) -> Wal {
        Wal::open(
            dir,
            WalConfig {
                group_commit_bytes: 64,
                segment_bytes,
            },
        )
        .unwrap()
        .0
    }

    #[test]
    fn follows_commits_and_rotation_incrementally() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = wal(tmp.path(), 100);
        let mut t = Tail::new(tmp.path(), 1);
        assert_eq!(t.poll(10).unwrap().len(), 0);
        for i in 0..5u64 {
            w.append(1, &[i as u8; 20]).unwrap();
        }
        // buffered, not written: nothing to read yet
        assert_eq!(t.poll(10).unwrap().len(), 0);
        w.commit().unwrap();
        let got = t.poll(3).unwrap();
        assert_eq!(got.iter().map(|r| r.lsn).collect::<Vec<_>>(), vec![1, 2, 3]);
        let got = t.poll(10).unwrap();
        assert_eq!(got.iter().map(|r| r.lsn).collect::<Vec<_>>(), vec![4, 5]);
        assert_eq!(t.poll(10).unwrap().len(), 0);
        // across several rotations
        for i in 5..20u64 {
            w.append(1, &[i as u8; 20]).unwrap();
            w.commit().unwrap();
        }
        assert!(list_segments(tmp.path()).unwrap().len() >= 4);
        let mut all = Vec::new();
        loop {
            let got = t.poll(4).unwrap();
            if got.is_empty() {
                break;
            }
            all.extend(got.into_iter().map(|r| r.lsn));
        }
        assert_eq!(all, (6..=20).collect::<Vec<u64>>());
        assert_eq!(t.next_lsn(), 21);
        // a tail that starts in the middle skips what is behind it
        let mut mid = Tail::new(tmp.path(), 13);
        let got = mid.poll(100).unwrap();
        assert_eq!(got.first().unwrap().lsn, 13);
        assert_eq!(got.last().unwrap().lsn, 20);
        // caught up at the end of the log is not an error
        let mut end = Tail::new(tmp.path(), 21);
        assert_eq!(end.poll(10).unwrap().len(), 0);
        w.append(1, b"x").unwrap();
        w.commit().unwrap();
        assert_eq!(end.poll(10).unwrap()[0].lsn, 21);
    }

    #[test]
    fn byte_budget_bounds_a_poll() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = wal(tmp.path(), 1 << 30);
        for _ in 0..10 {
            w.append(1, &[7u8; 1000]).unwrap();
        }
        w.commit().unwrap();
        let mut t = Tail::new(tmp.path(), 1);
        assert_eq!(t.poll_bytes(100, 2500).unwrap().len(), 3);
        assert_eq!(t.poll_bytes(100, 1).unwrap().len(), 1);
        assert_eq!(t.poll_bytes(100, usize::MAX).unwrap().len(), 6);
        // the durable limit holds records back until it moves
        let mut t = Tail::new(tmp.path(), 1);
        t.set_limit(4);
        assert_eq!(t.poll(100).unwrap().len(), 4);
        assert_eq!(t.poll(100).unwrap().len(), 0);
        t.set_limit(10);
        assert_eq!(t.poll(100).unwrap().len(), 6);
    }

    #[test]
    fn truncated_and_ahead_requests_fail_loudly() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = wal(tmp.path(), 100);
        for i in 0..12u64 {
            w.append(1, &[i as u8; 20]).unwrap();
            w.commit().unwrap();
        }
        assert!(w.truncate_before(7).unwrap() >= 1);
        // records 1.. are gone
        let err = Tail::new(tmp.path(), 1).poll(10).unwrap_err();
        assert!(
            matches!(err, WalError::Truncated { wanted: 1, .. }),
            "{err:?}"
        );
        // what is still there can be read
        let oldest = list_segments(tmp.path()).unwrap()[0].0;
        let got = Tail::new(tmp.path(), oldest).poll(100).unwrap();
        assert_eq!(got.first().unwrap().lsn, oldest);
        assert_eq!(got.last().unwrap().lsn, 12);
        // a follower that claims records this log never had
        let err = Tail::new(tmp.path(), 40).poll(10).unwrap_err();
        assert!(
            matches!(
                err,
                WalError::Ahead {
                    wanted: 40,
                    last: 12
                }
            ),
            "{err:?}"
        );
        // a tail overtaken by truncation while it was idle
        let mut slow = Tail::new(tmp.path(), oldest);
        assert_eq!(slow.poll(1).unwrap().len(), 1);
        for i in 12..30u64 {
            w.append(1, &[i as u8; 20]).unwrap();
            w.commit().unwrap();
        }
        assert!(w.truncate_before(25).unwrap() >= 1);
        let err = loop {
            match slow.poll(1) {
                Ok(v) if v.is_empty() => panic!("stalled without an error"),
                Ok(_) => {}
                Err(e) => break e,
            }
        };
        assert!(matches!(err, WalError::Truncated { .. }), "{err:?}");
    }

    #[test]
    fn partial_tail_waits_and_garbage_is_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = wal(tmp.path(), 1 << 30);
        for i in 0..3u64 {
            w.append(1, &i.to_le_bytes()).unwrap();
        }
        w.commit().unwrap();
        let seg = w.segment_path().to_path_buf();
        let good = fs::read(&seg).unwrap();
        // half a record at the end: a write in progress
        let mut bytes = good.clone();
        bytes.extend_from_slice(&good[..HEADER_LEN + 3]);
        fs::write(&seg, &bytes).unwrap();
        let mut t = Tail::new(tmp.path(), 1);
        assert_eq!(t.poll(10).unwrap().len(), 3);
        assert_eq!(t.poll(10).unwrap().len(), 0);
        // zero-filled tail: also not an error
        let mut bytes = good.clone();
        bytes.extend(std::iter::repeat_n(0u8, 4096));
        fs::write(&seg, &bytes).unwrap();
        let mut t = Tail::new(tmp.path(), 1);
        assert_eq!(t.poll(10).unwrap().len(), 3);
        assert_eq!(t.poll(10).unwrap().len(), 0);
        // a complete record with a wrong checksum
        let mut bytes = good.clone();
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        fs::write(&seg, &bytes).unwrap();
        let mut t = Tail::new(tmp.path(), 1);
        assert!(matches!(t.poll(10), Err(WalError::Corrupt { .. })));
    }
}
