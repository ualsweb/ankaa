//! File source: every non-empty line is one message (lines starting with `#` are comments).
//! After the file is consumed the task idles until shutdown so the tables stay queryable.
//!
//! The file is read as a stream (memory is bounded by the longest accepted line). The position
//! reported to the server is `<byte offset>:<hash>`, the hash covering the first and the last
//! 4 KiB before the offset: on the next start the source continues at that offset when the file
//! still holds the same bytes there (so lines appended in the meantime are ingested, and
//! nothing is ingested twice), and starts over when the file was replaced. The position is
//! also reported when the source is stopped, so a clean stop repeats nothing.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::{mpsc, watch};

use super::{SourceOpts, SourceStats, deliver_wait, oversized, send_state, shutdown_wait};
use crate::capture::Frame;

/// Bytes at the start of the file that identify it.
const HEAD_BYTES: u64 = 4096;
/// Report the position every this many lines.
const STATE_EVERY_LINES: u64 = 8192;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Hash identifying the file contents before `upto`: its first and its last `HEAD_BYTES`
/// bytes in that range (a different file with the same header does not match).
async fn head_hash(path: &Path, upto: u64) -> std::io::Result<u64> {
    let n = usize::try_from(upto.min(HEAD_BYTES)).unwrap_or(0);
    let mut buf = vec![0u8; 2 * n];
    let mut f = tokio::fs::File::open(path).await?;
    f.read_exact(&mut buf[..n]).await?;
    f.seek(std::io::SeekFrom::Start(upto - n as u64)).await?;
    f.read_exact(&mut buf[n..]).await?;
    Ok(fnv1a(&buf))
}

fn parse_state(state: &str) -> Option<(u64, u64)> {
    let (offset, hash) = state.split_once(':')?;
    Some((offset.parse().ok()?, u64::from_str_radix(hash, 16).ok()?))
}

/// Offset to resume at: the saved offset when the file still starts with the same bytes and is
/// at least that long; otherwise 0.
async fn resume_offset(path: &Path, resume: Option<&str>) -> u64 {
    let Some((offset, hash)) = resume.and_then(parse_state) else {
        return 0;
    };
    let len = tokio::fs::metadata(path).await.map_or(0, |m| m.len());
    if offset == 0 || len < offset {
        return 0;
    }
    match head_hash(path, offset).await {
        Ok(h) if h == hash => offset,
        _ => 0,
    }
}

/// Replay `path` line by line.
pub async fn run(
    source_id: u16,
    path: &Path,
    opts: &SourceOpts,
    tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
) -> SourceStats {
    let mut stats = SourceStats::default();
    let start = resume_offset(path, opts.resume.as_deref()).await;
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(source_id, path = %path.display(), error = %e, "file source unreadable");
            return stats;
        }
    };
    if start > 0 {
        if let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await {
            tracing::warn!(source_id, path = %path.display(), error = %e, "file source: seek failed");
            return stats;
        }
        tracing::info!(source_id, path = %path.display(), offset = start, "file source resumes after the lines already ingested");
    }
    stats.connects = 1;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut offset = start;
    let mut line = Vec::with_capacity(1024);
    let mut lines_since_state = 0u64;
    let limit = u64::try_from(opts.max_message_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(2);
    loop {
        if *shutdown.borrow() {
            report_final(&tx, source_id, path, offset).await;
            return stats;
        }
        line.clear();
        // read at most one message worth of bytes; a longer line is skipped piece by piece
        let n = match (&mut reader).take(limit).read_until(b'\n', &mut line).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(source_id, path = %path.display(), error = %e, offset, "file source: read failed");
                break;
            }
        };
        if n == 0 {
            break;
        }
        let complete = line.last() == Some(&b'\n');
        if !complete && n as u64 == limit {
            // oversized line: drop the rest of it
            let mut total = n as u64;
            loop {
                line.clear();
                match (&mut reader).take(limit).read_until(b'\n', &mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(m) => {
                        total += m as u64;
                        if line.last() == Some(&b'\n') {
                            break;
                        }
                    }
                }
            }
            offset += total;
            let _ = oversized(usize::MAX, opts, &mut stats);
            continue;
        }
        // a last line without a newline is ingested as it is: files are expected to be complete
        // when the server starts (a line still being written would be ingested partially)
        offset += n as u64;
        let text = line.trim_ascii();
        if text.is_empty() || text.first() == Some(&b'#') {
            continue;
        }
        if oversized(text.len(), opts, &mut stats) {
            continue;
        }
        // wait on backpressure instead of dropping: files are not real time
        if !deliver_wait(&tx, source_id, text, &mut stats, &mut shutdown).await {
            // this line was not delivered: the position stays before it
            report_final(&tx, source_id, path, offset - n as u64).await;
            return stats;
        }
        lines_since_state += 1;
        if lines_since_state >= STATE_EVERY_LINES {
            lines_since_state = 0;
            if !report(&tx, source_id, path, offset, &mut shutdown).await {
                // shutdown won the race against the report: say where we are on the way out
                report_final(&tx, source_id, path, offset).await;
                return stats;
            }
        }
    }
    if !report(&tx, source_id, path, offset, &mut shutdown).await {
        report_final(&tx, source_id, path, offset).await;
        return stats;
    }
    tracing::info!(source_id, path = %path.display(), messages = stats.messages, oversized = stats.oversized, "file source consumed");
    shutdown_wait(&mut shutdown).await;
    stats
}

/// Position report on the way out (shutdown requested).
async fn report_final(tx: &mpsc::Sender<Frame>, source_id: u16, path: &Path, offset: u64) {
    if offset == 0 {
        return;
    }
    if let Ok(h) = head_hash(path, offset).await {
        super::send_state_bounded(tx, source_id, &format!("{offset}:{h:016x}")).await;
    }
}

async fn report(
    tx: &mpsc::Sender<Frame>,
    source_id: u16,
    path: &Path,
    offset: u64,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    if offset == 0 {
        return true;
    }
    match head_hash(path, offset).await {
        Ok(h) => send_state(tx, source_id, &format!("{offset}:{h:016x}"), shutdown).await,
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{FrameKind, split_source_state, split_user_payload};

    /// Run the source to the end of the file; returns the messages and the last state.
    async fn drain(path: &Path, opts: &SourceOpts) -> (Vec<String>, Option<String>, SourceStats) {
        let (tx, mut rx) = mpsc::channel(4);
        let (stop_tx, stop_rx) = watch::channel(false);
        let p = path.to_path_buf();
        let o = opts.clone();
        let task = tokio::spawn(async move { run(7, &p, &o, tx, stop_rx).await });
        let mut msgs = Vec::new();
        let mut state = None;
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(300), rx.recv()).await {
                Ok(Some(f)) if f.kind == FrameKind::User => {
                    let (id, m) = split_user_payload(&f.payload).unwrap();
                    assert_eq!(id, 7);
                    msgs.push(String::from_utf8(m.to_vec()).unwrap());
                }
                Ok(Some(f)) => {
                    let (id, s) = split_source_state(&f.payload).unwrap();
                    assert_eq!(id, 7);
                    state = Some(s.to_owned());
                }
                Ok(None) | Err(_) => break,
            }
        }
        stop_tx.send(true).unwrap();
        (msgs, state, task.await.unwrap())
    }

    #[tokio::test]
    async fn reads_lines_and_resumes_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.ndjson");
        std::fs::write(&path, "# header\n{\"a\":1}\n\n  {\"a\":2}  \r\n{\"a\":3}").unwrap();
        let (msgs, position, stats) = drain(&path, &SourceOpts::default()).await;
        assert_eq!(msgs, vec!["{\"a\":1}", "{\"a\":2}", "{\"a\":3}"]);
        assert_eq!(stats.messages, 3);
        let saved = position.expect("position reported");
        // same file again with the saved position: nothing is ingested twice
        let opts = SourceOpts {
            resume: Some(saved.clone()),
            ..SourceOpts::default()
        };
        let (msgs, _, _) = drain(&path, &opts).await;
        assert_eq!(msgs.len(), 0, "{msgs:?}");
        // appended lines are picked up from the saved position
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut f, b"\n{\"a\":4}\n").unwrap();
        let (msgs, state2, _) = drain(&path, &opts).await;
        assert_eq!(msgs, vec!["{\"a\":4}"]);
        assert_ne!(state2.unwrap(), saved);
        // a different file under the same name starts over
        std::fs::write(
            &path,
            "{\"b\":1}\n{\"b\":2}\n{\"b\":3}\n{\"b\":4}\n{\"b\":5}\n",
        )
        .unwrap();
        let (msgs, _, _) = drain(&path, &opts).await;
        assert_eq!(msgs.len(), 5);
        // a different file with the same first bytes and a greater length also starts over
        let head = "{\"b\":1}\n";
        let (_, st, _) = drain(&path, &SourceOpts::default()).await;
        std::fs::write(&path, format!("{head}{}", "{\"c\":9}\n".repeat(8))).unwrap();
        let opts2 = SourceOpts {
            resume: st,
            ..SourceOpts::default()
        };
        assert_eq!(drain(&path, &opts2).await.0.len(), 9);
        // garbage state is ignored
        let opts = SourceOpts {
            resume: Some("nonsense".into()),
            ..SourceOpts::default()
        };
        assert_eq!(drain(&path, &opts).await.0.len(), 9);
    }

    #[tokio::test]
    async fn oversized_lines_are_dropped_and_counted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("in.csv");
        let big = "x".repeat(5000);
        std::fs::write(&path, format!("a,1\n{big}\nb,2\n{big}")).unwrap();
        let opts = SourceOpts {
            max_message_bytes: 64,
            ..SourceOpts::default()
        };
        let (msgs, position, stats) = drain(&path, &opts).await;
        assert_eq!(msgs, vec!["a,1", "b,2"]);
        assert_eq!(stats.oversized, 2);
        let (offset, _) = parse_state(&position.unwrap()).unwrap();
        assert_eq!(offset, std::fs::metadata(&path).unwrap().len());
        // a line of exactly the limit passes
        std::fs::write(&path, format!("{}\n", "y".repeat(64))).unwrap();
        let (msgs, _, stats) = drain(&path, &opts).await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(stats.oversized, 0);
    }
}
