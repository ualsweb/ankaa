//! `wal-torture`: writer and verifier for the power-loss rig.
//!
//! Writer: appends records with a deterministic payload (lsn repeated) and commits every
//! `--commit-every` records; after each successful commit it atomically writes the durable LSN
//! to `--ack-file` (on a *different*, stable filesystem). Killing the writer at any instant,
//! including by dropping all writes underneath it (dm-flakey), must never lose a record whose
//! LSN was acknowledged.
//!
//! Verifier: replays the WAL directory and checks (a) every record ≤ acked LSN is present with
//! the correct payload, (b) no corrupt record was accepted (CRC), (c) LSNs are contiguous.
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use ankaa_wal::{Wal, WalConfig, replay};

fn payload_for(lsn: u64, len: usize) -> Vec<u8> {
    let b = lsn.to_le_bytes();
    (0..len).map(|i| b[i % 8] ^ (i as u8)).collect()
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: wal-torture write <wal-dir> <ack-file> [--commit-every N] [--payload-len L] [--max-records M]\n       wal-torture verify <wal-dir> <ack-file>"
    );
    ExitCode::from(2)
}

#[allow(clippy::too_many_lines)]
fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        return usage();
    }
    let dir = PathBuf::from(&args[2]);
    let ack = PathBuf::from(&args[3]);
    match args[1].as_str() {
        "write" => {
            let mut commit_every = 16u64;
            let mut payload_len = 200usize;
            let mut max_records = u64::MAX;
            let mut i = 4;
            while i + 1 < args.len() {
                match args[i].as_str() {
                    "--commit-every" => commit_every = args[i + 1].parse().unwrap_or(16),
                    "--payload-len" => payload_len = args[i + 1].parse().unwrap_or(200),
                    "--max-records" => max_records = args[i + 1].parse().unwrap_or(u64::MAX),
                    _ => return usage(),
                }
                i += 2;
            }
            let (mut wal, last) = match Wal::open(
                &dir,
                WalConfig {
                    group_commit_bytes: 1 << 20,
                    segment_bytes: 8 << 20,
                },
            ) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("open failed: {e}");
                    return ExitCode::from(1);
                }
            };
            eprintln!("writer: resumed at lsn {last}");
            let mut n = 0u64;
            loop {
                let lsn = match wal.append(1, &payload_for(wal.next_lsn(), payload_len)) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("append failed: {e}");
                        return ExitCode::from(1);
                    }
                };
                n += 1;
                if lsn % commit_every == 0 {
                    match wal.commit() {
                        Ok(durable) => {
                            // Ack only after fdatasync returned. Atomic rename on the ack filesystem.
                            let tmp = ack.with_extension("tmp");
                            if std::fs::write(&tmp, durable.to_string())
                                .and_then(|()| std::fs::rename(&tmp, &ack))
                                .is_err()
                            {
                                eprintln!("ack write failed");
                                return ExitCode::from(1);
                            }
                        }
                        Err(e) => {
                            // Under dm-flakey this is the expected end: the device is gone.
                            eprintln!("commit failed (expected under fault injection): {e}");
                            return ExitCode::from(3);
                        }
                    }
                }
                if n >= max_records {
                    let _ = wal.commit();
                    return ExitCode::SUCCESS;
                }
            }
        }
        "verify" => {
            let acked: u64 = std::fs::read_to_string(&ack)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0);
            let it = match replay(&dir) {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("replay open failed: {e}");
                    return ExitCode::from(1);
                }
            };
            let mut last = 0u64;
            let mut count = 0u64;
            for r in it {
                match r {
                    Ok(rec) => {
                        if rec.lsn != last + 1 && last != 0 {
                            eprintln!("FAIL: lsn gap {} -> {}", last, rec.lsn);
                            return ExitCode::from(1);
                        }
                        let want = payload_for(rec.lsn, rec.payload.len());
                        if rec.payload != want {
                            eprintln!("FAIL: payload mismatch at lsn {}", rec.lsn);
                            return ExitCode::from(1);
                        }
                        last = rec.lsn;
                        count += 1;
                    }
                    Err(e) => {
                        eprintln!("FAIL: replay error (undetected-corruption guard tripped): {e}");
                        return ExitCode::from(1);
                    }
                }
            }
            if last < acked {
                eprintln!("FAIL: acked lsn {acked} but only {last} durable");
                return ExitCode::from(1);
            }
            println!(
                "OK: {count} records, last lsn {last}, acked {acked}, unacked-but-durable {}",
                last - acked
            );
            ExitCode::SUCCESS
        }
        _ => usage(),
    }
}
