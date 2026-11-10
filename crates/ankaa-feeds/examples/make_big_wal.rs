//! Build a large WAL from a capture for the recovery-speed check (PRD F-19):
//! `cargo run --release -p ankaa-feeds --example make_big_wal -- <capture.ankc> <wal-dir> <copies>`
//! Each copy shifts exchange timestamps by one day and all update/trade ids by 1e9 so that
//! replaying the WAL appends every row (nothing is dropped as out of order).
#![allow(
    clippy::unreadable_literal,
    clippy::map_unwrap_or,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]
use std::io::Write as _;

use ankaa_feeds::capture::{FrameKind, decode_all_bytes, encode_frame};
use ankaa_wal::{Wal, WalConfig};
use bytes::{Bytes, BytesMut};

fn shift(payload: &[u8], ts_add: i64, id_add: i64) -> Bytes {
    // JSON text: shift the integer values of keys E (event time), T (trade time), U/u (update
    // ids), t (trade id). Keys appear as "E":1234 (no spaces in Binance payloads).
    let text = std::str::from_utf8(payload).expect("json frame");
    let mut out = String::with_capacity(text.len() + 16);
    let mut rest = text;
    while let Some(pos) = rest.find('"') {
        out.push_str(&rest[..=pos]);
        rest = &rest[pos + 1..];
        let Some(end) = rest.find('"') else { break };
        let key = &rest[..end];
        out.push_str(key);
        out.push('"');
        rest = &rest[end + 1..];
        let add = match key {
            "E" | "T" => Some(ts_add),
            "U" | "u" | "t" => Some(id_add),
            _ => None,
        };
        if let Some(add) = add
            && let Some(r) = rest.strip_prefix(':')
        {
            let digits: String = r.chars().take_while(char::is_ascii_digit).collect();
            if let Ok(v) = digits.parse::<i64>() {
                out.push(':');
                out.push_str(&(v + add).to_string());
                rest = &r[digits.len()..];
            }
        }
    }
    out.push_str(rest);
    Bytes::from(out)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (capture, dir, copies) = (
        &args[1],
        std::path::Path::new(&args[2]),
        args[3].parse::<i64>().unwrap(),
    );
    let bytes = Bytes::from(std::fs::read(capture).unwrap());
    let (_, frames) = decode_all_bytes(&bytes).unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let (mut wal, _) = Wal::open(dir, WalConfig::default()).unwrap();
    let mut buf = BytesMut::with_capacity(8192);
    let mut n = 0u64;
    let day_ms = 86_400_000i64;
    for c in 0..copies {
        for f in &frames {
            let payload = match f.kind {
                FrameKind::Text => shift(&f.payload, c * day_ms, c * 1_000_000_000),
                _ => f.payload.clone(),
            };
            let frame = ankaa_feeds::capture::Frame {
                recv_ns: f.recv_ns + c * day_ms * 1_000_000,
                kind: f.kind,
                payload,
            };
            buf.clear();
            encode_frame(&frame, &mut buf);
            wal.append(1, &buf).unwrap();
            if wal.needs_commit() {
                wal.commit().unwrap();
            }
            n += 1;
        }
        if c % 50 == 0 {
            print!("\rcopy {c}/{copies}");
            std::io::stdout().flush().unwrap();
        }
    }
    wal.commit().unwrap();
    let size: u64 = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    println!(
        "\nframes={n} bytes={size} ({:.1} MiB) durable_lsn={}",
        size as f64 / 1048576.0,
        wal.durable_lsn()
    );
}
