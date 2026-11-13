//! Databento DBN replay source (PRD F-01): `.dbn` / `.dbn.zst` files → [`EventBatch`]es.
#![allow(
    clippy::similar_names,
    clippy::large_digit_groups,
    clippy::inconsistent_digit_grouping
)]
//!
//! Mapping to the ANKAA event model:
//! - `trades` (`TradeMsg`) → [`Trade`]; price scale 1e-9 → ticks at scale 8 (exact when the
//!   last digit is zero, which holds for every real venue tick size; otherwise rounded).
//! - `mbp-1` / `mbp-10` (`Mbp1Msg`, `Mbp10Msg`) → [`BookSnapshot`] with 1 or 10 levels per side:
//!   these schemas carry the absolute top of book after each update, which is exactly a
//!   snapshot of the visible book, so books rebuilt from them are exact at that depth.
//!   Prices are divided by 10 (1e-9 → 1e-8), truncating sub-1e-8 digits (none on real venues).
//! - `mbo` (order-level) and every other schema are counted as ignored: ANKAA keeps L2 books,
//!   not order-level books (PRD §3.2).
//!
//! Timestamps use `ts_event` (exchange time) as the event time and `ts_recv` (Databento
//! capture time) as the receive time, so replays preserve the original latency profile.
//! Symbols come from the metadata symbology (`instrument_id` → raw symbol per date).

use std::collections::HashMap;
use std::path::Path;

use ankaa_ingest::{BookSnapshot, EventBatch, Side, SymbolDict, Trade};
use dbn::decode::{DbnMetadata, DecodeRecordRef, DynDecoder, DynReader};
use dbn::{Mbp1Msg, Mbp10Msg, PitSymbolMap, TradeMsg, UNDEF_PRICE, VersionUpgradePolicy, rtype};

/// DBN prices are fixed point with 9 decimals; ANKAA stores 8.
const PRICE_DIV: i64 = dbn::FIXED_PRICE_SCALE / 100_000_000;

/// Counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DbnStats {
    /// Records read.
    pub records: u64,
    /// Trades emitted.
    pub trades: u64,
    /// Book snapshots emitted (mbp-1 / mbp-10).
    pub snapshots: u64,
    /// Records of schemas ANKAA does not model (mbo, ohlcv, status, ...).
    pub ignored: u64,
    /// Records whose instrument id had no symbol mapping (skipped).
    pub unmapped: u64,
}

/// The decompressed stream of a file, checked record by record on its way into `dbn`.
type GuardedStream =
    std::io::BufReader<FrameGuard<DynReader<'static, std::io::BufReader<std::fs::File>>>>;

/// The decoder of one file: a DBN stream behind [`FrameGuard`], or anything else (the legacy
/// DBZ format) through `dbn`'s own detection.
enum Source {
    Dbn(Box<dbn::decode::dbn::Decoder<GuardedStream>>),
    Other(Box<DynDecoder<'static, std::io::BufReader<std::fs::File>>>),
}

impl Source {
    fn metadata(&self) -> &dbn::Metadata {
        match self {
            Self::Dbn(d) => d.metadata(),
            Self::Other(d) => d.metadata(),
        }
    }

    fn decode_record_ref(&mut self) -> dbn::Result<Option<dbn::RecordRef<'_>>> {
        match self {
            Self::Dbn(d) => d.decode_record_ref(),
            Self::Other(d) => d.decode_record_ref(),
        }
    }
}

/// Checks the framing of a DBN stream as it is read: every record must be a whole number of
/// 8-byte words long.
///
/// `dbn` (0.73) checks a record's declared length against the header size but not its
/// alignment. A damaged length that is odd in its 4-byte units places the next record 4 bytes off
/// the 8-byte alignment `dbn` reads records with: a debug-assertion panic in `dbn`, and a
/// misaligned read in a release build. Every DBN record type is a multiple of 8 bytes, so such a
/// length is never valid; the guard ends the stream with an error at the first one. Found by the
/// nightly `dbn_replay` fuzz run.
pub(crate) struct FrameGuard<R> {
    inner: R,
    state: Frame,
}

#[derive(Debug, Clone, Copy)]
enum Frame {
    /// Collecting the 8-byte prelude: "DBN", the version, the metadata length.
    Prelude { buf: [u8; 8], got: usize },
    /// Skipping `left` more bytes (metadata, or the rest of a record).
    Skip { left: u64, version: u8 },
    /// At a record boundary: the next byte is a record's length in 4-byte units.
    Boundary { version: u8 },
    /// Not a stream this guard understands (another format, a newer version): passed through
    /// unchecked; `dbn` refuses what it cannot decode.
    Pass,
}

impl<R> FrameGuard<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            state: Frame::Prelude {
                buf: [0; 8],
                got: 0,
            },
        }
    }

    /// Advances the framing over `bytes`, the next bytes of the stream.
    fn scan(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        while !bytes.is_empty() {
            match &mut self.state {
                Frame::Pass => return Ok(()),
                Frame::Prelude { buf, got } => {
                    let n = (buf.len() - *got).min(bytes.len());
                    buf[*got..*got + n].copy_from_slice(&bytes[..n]);
                    *got += n;
                    bytes = &bytes[n..];
                    if *got == buf.len() {
                        let version = buf[3];
                        self.state = if &buf[..3] == b"DBN" && (1..=3).contains(&version) {
                            let meta = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
                            Frame::Skip {
                                left: u64::from(meta),
                                version,
                            }
                        } else {
                            Frame::Pass
                        };
                    }
                }
                Frame::Skip { left, version } => {
                    let n = usize::try_from(*left)
                        .unwrap_or(usize::MAX)
                        .min(bytes.len());
                    *left -= n as u64;
                    bytes = &bytes[n..];
                    if *left == 0 {
                        self.state = Frame::Boundary { version: *version };
                    }
                }
                Frame::Boundary { version } => {
                    // DBN 1–3: the first header byte is the record length in 4-byte units
                    let units = bytes[0];
                    if !units.is_multiple_of(2) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "record of {} bytes, not a multiple of 8: damaged DBN data",
                                usize::from(units) * 4
                            ),
                        ));
                    }
                    // a length shorter than the header is left to `dbn`, which refuses it; the
                    // guard stops checking there
                    if units == 0 {
                        self.state = Frame::Pass;
                        return Ok(());
                    }
                    self.state = Frame::Skip {
                        left: u64::from(units) * 4,
                        version: *version,
                    };
                }
            }
        }
        Ok(())
    }
}

impl<R: std::io::Read> std::io::Read for FrameGuard<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.scan(&buf[..n])?;
        Ok(n)
    }
}

/// Streaming reader over one DBN file.
pub struct DbnReplay {
    dec: Source,
    /// Per-day instrument → symbol maps (DBN symbology is date-ranged).
    maps: HashMap<i32, PitSymbolMap>,
    stats: DbnStats,
    dataset: String,
}

impl std::fmt::Debug for DbnReplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbnReplay")
            .field("dataset", &self.dataset)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

/// Open error.
#[derive(Debug, thiserror::Error)]
pub enum DbnError {
    /// DBN decoding error.
    #[error("dbn: {0}")]
    Dbn(#[from] dbn::Error),
    /// Timestamp outside the representable range.
    #[error("timestamp {0} out of range")]
    Timestamp(u64),
    /// The header declares more metadata than the file can hold.
    #[error(
        "dbn: the header declares {declared} bytes of metadata, more than the {limit} this file can hold"
    )]
    Metadata {
        /// Metadata length read from the header.
        declared: u64,
        /// The most this file could carry.
        limit: u64,
    },
}

/// Largest metadata section accepted from a compressed file. A plain file is bounded by its own
/// size; of a compressed one only the compressed size is known.
const MAX_METADATA_BYTES: u64 = 1 << 30;

/// Refuses a file whose header declares more metadata than the file can hold. The `dbn` decoder
/// reserves the declared length before it reads the section, so eleven bytes of a damaged or
/// hostile file would otherwise ask for 4 GiB. Returns whether the file is a DBN stream (as
/// opposed to the legacy DBZ format or something else, which `dbn` itself sorts out).
fn check_prelude(path: &Path) -> Result<bool, DbnError> {
    use std::io::Read as _;
    let io = |e: std::io::Error| DbnError::Dbn(dbn::Error::io(e, "reading the DBN header"));
    let mut reader = dbn::decode::DynReader::from_file(path)?;
    // "DBN", the version, the metadata length (little endian)
    let mut prelude = [0u8; 8];
    let mut got = 0;
    while got < prelude.len() {
        match reader.read(&mut prelude[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(io(e)),
        }
    }
    if got < prelude.len() || &prelude[..3] != b"DBN" {
        // not a DBN stream (or a legacy one): the decoder says what is wrong with it
        return Ok(false);
    }
    let declared = u64::from(u32::from_le_bytes([
        prelude[4], prelude[5], prelude[6], prelude[7],
    ]));
    let limit = if reader.is_compressed() {
        MAX_METADATA_BYTES
    } else {
        let len = std::fs::metadata(path).map_err(io)?.len();
        len.saturating_sub(prelude.len() as u64)
    };
    if declared > limit {
        return Err(DbnError::Metadata { declared, limit });
    }
    Ok(true)
}

impl DbnReplay {
    /// Open a `.dbn` or `.dbn.zst` file (compression is inferred).
    pub fn open(path: &Path) -> Result<Self, DbnError> {
        let is_dbn = check_prelude(path)?;
        let policy = VersionUpgradePolicy::UpgradeToV3;
        let dec = if is_dbn {
            let guarded = std::io::BufReader::new(FrameGuard::new(DynReader::from_file(path)?));
            Source::Dbn(Box::new(dbn::decode::dbn::Decoder::with_upgrade_policy(
                guarded, policy,
            )?))
        } else {
            Source::Other(Box::new(DynDecoder::from_file(path, policy)?))
        };
        let dataset = dec.metadata().dataset.clone();
        Ok(Self {
            dec,
            maps: HashMap::new(),
            stats: DbnStats::default(),
            dataset,
        })
    }

    /// Dataset code from the metadata (e.g. `XNAS.ITCH`).
    #[must_use]
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> DbnStats {
        self.stats
    }

    fn symbol_for(
        &mut self,
        ts_event: u64,
        instrument_id: u32,
    ) -> Result<Option<String>, DbnError> {
        let dt = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts_event))
            .map_err(|_| DbnError::Timestamp(ts_event))?;
        let day = dt.date().to_julian_day();
        if !self.maps.contains_key(&day) {
            if self.maps.len() >= 8 {
                // bounded cache: a hostile file could step through thousands of days
                self.maps.clear();
            }
            let map = self.dec.metadata().symbol_map_for_date(dt.date())?;
            self.maps.insert(day, map);
        }
        Ok(self
            .maps
            .get(&day)
            .and_then(|m| m.get(instrument_id))
            .cloned())
    }

    /// Decode up to `max` records into `out`; returns the number of events appended, or `None`
    /// at end of file (ignored or unmapped records do not count, so `Some(0)` means keep going).
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    pub fn next_batch(
        &mut self,
        symbols: &mut SymbolDict,
        out: &mut EventBatch,
        max: usize,
    ) -> Result<Option<usize>, DbnError> {
        let mut n = 0;
        let mut read = 0;
        let mut eof = false;
        while read < max {
            let Some(rec) = self.dec.decode_record_ref()? else {
                eof = true;
                break;
            };
            read += 1;
            self.stats.records += 1;
            let (ts_event, instrument_id, rtype) = {
                let hd = rec.header();
                (hd.ts_event, hd.instrument_id, hd.rtype)
            };
            match rtype {
                rtype::MBP_0 => {
                    let Ok(t) = rec.try_get::<TradeMsg>() else {
                        self.stats.ignored += 1;
                        continue;
                    };
                    let (price, size, side, ts_recv, seq) =
                        (t.price, t.size, t.side, t.ts_recv, t.sequence);
                    if price == UNDEF_PRICE {
                        self.stats.ignored += 1;
                        continue;
                    }
                    let Some(name) = self.symbol_for(ts_event, instrument_id)? else {
                        self.stats.unmapped += 1;
                        continue;
                    };
                    let symbol = symbols.intern(&name);
                    // `side` is a C `char`: unsigned on aarch64 Linux, signed on macOS, so the
                    // cast is needed on one and redundant on the other
                    #[allow(clippy::unnecessary_cast)]
                    let buy = side as u8 == b'B';
                    out.trades.push(Trade {
                        ts_ns: to_i64(ts_event),
                        recv_ns: to_i64(ts_recv),
                        symbol,
                        price: price / PRICE_DIV,
                        size: i64::from(size) * 100_000_000,
                        // Databento `side` is the aggressor; 'B' buy → bid-side aggressor
                        side: if buy { Side::Bid } else { Side::Ask },
                        seq: i64::from(seq),
                    });
                    self.stats.trades += 1;
                    n += 1;
                }
                rtype::MBP_1 | rtype::MBP_10 => {
                    let (levels, ts_recv, seq): (Vec<(i64, u32, i64, u32)>, u64, u32) =
                        if let Ok(m) = rec.try_get::<Mbp1Msg>() {
                            (
                                m.levels
                                    .iter()
                                    .map(|l| (l.bid_px, l.bid_sz, l.ask_px, l.ask_sz))
                                    .collect(),
                                m.ts_recv,
                                m.sequence,
                            )
                        } else if let Ok(m) = rec.try_get::<Mbp10Msg>() {
                            (
                                m.levels
                                    .iter()
                                    .map(|l| (l.bid_px, l.bid_sz, l.ask_px, l.ask_sz))
                                    .collect(),
                                m.ts_recv,
                                m.sequence,
                            )
                        } else {
                            continue;
                        };
                    let Some(name) = self.symbol_for(ts_event, instrument_id)? else {
                        self.stats.unmapped += 1;
                        continue;
                    };
                    let symbol = symbols.intern(&name);
                    let mut snap = BookSnapshot {
                        ts_ns: to_i64(ts_event),
                        recv_ns: to_i64(ts_recv),
                        symbol,
                        last_update_id: i64::from(seq),
                        bids: Vec::with_capacity(levels.len()),
                        asks: Vec::with_capacity(levels.len()),
                    };
                    for (bp, bs, ap, asz) in levels {
                        if bp != UNDEF_PRICE && bs > 0 {
                            snap.bids
                                .push((bp / PRICE_DIV, i64::from(bs) * 100_000_000));
                        }
                        if ap != UNDEF_PRICE && asz > 0 {
                            snap.asks
                                .push((ap / PRICE_DIV, i64::from(asz) * 100_000_000));
                        }
                    }
                    out.snapshots.push(snap);
                    self.stats.snapshots += 1;
                    n += 1;
                }
                _ => {
                    self.stats.ignored += 1;
                }
            }
        }
        if eof && read == 0 {
            return Ok(None);
        }
        Ok(Some(n))
    }
}

fn to_i64(ts: u64) -> i64 {
    i64::try_from(ts).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
mod tests {
    use super::*;
    use dbn::encode::EncodeRecord;
    use dbn::{FlagSet, MappingInterval, Metadata, RecordHeader, SType, Schema, SymbolMapping};

    /// The fixture with its first record one 4-byte unit short, so the next header starts 4 bytes
    /// off alignment, and a plausible length placed there (the shape the fuzzer found).
    fn misaligned_fixture(path: &Path, version: u8) -> Vec<u8> {
        write_fixture(path);
        let mut bytes = std::fs::read(path).unwrap();
        bytes[3] = version;
        let meta_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let first = 8 + meta_len;
        assert_eq!(bytes[first], 12, "fixture: a trade record is 48 bytes");
        bytes[first] = 11;
        bytes[first + 44] = 12;
        bytes[first + 45] = rtype::MBP_0;
        bytes
    }

    fn replay_all(path: &Path) -> Result<u64, DbnError> {
        let mut r = DbnReplay::open(path)?;
        let mut symbols = ankaa_ingest::SymbolDict::new();
        let mut out = ankaa_ingest::EventBatch::with_capacity(64);
        let mut n = 0;
        while let Some(k) = r.next_batch(&mut symbols, &mut out, 256)? {
            n += k as u64;
            out.clear();
        }
        Ok(n)
    }

    #[test]
    fn a_record_length_off_the_8_byte_alignment_is_refused_not_misread() {
        let dir = tempfile::tempdir().unwrap();
        for version in [1u8, 2, 3] {
            let plain = dir.path().join(format!("odd-v{version}.dbn"));
            let bytes = misaligned_fixture(&plain, version);
            std::fs::write(&plain, &bytes).unwrap();
            // the same bytes compressed: the guard sits after decompression
            let zst = dir.path().join(format!("odd-v{version}.dbn.zst"));
            let mut w = dbn::encode::DynWriter::new(
                std::fs::File::create(&zst).unwrap(),
                dbn::Compression::Zstd,
            )
            .unwrap();
            std::io::Write::write_all(&mut w, &bytes).unwrap();
            drop(w);
            for path in [&plain, &zst] {
                let err = replay_all(path).expect_err("a misaligned record must not decode");
                assert!(
                    err.to_string().contains("not a multiple of 8"),
                    "{}: {err}",
                    path.display()
                );
            }
        }
    }

    /// The guard's state survives any split of the stream into reads, down to one byte.
    #[test]
    fn frame_guard_checks_every_boundary_whatever_the_read_size() {
        struct Trickle<'a>(&'a [u8], usize);
        impl std::io::Read for Trickle<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = self.1.min(buf.len()).min(self.0.len());
                buf[..n].copy_from_slice(&self.0[..n]);
                self.0 = &self.0[n..];
                Ok(n)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let good_path = dir.path().join("good.dbn");
        write_fixture(&good_path);
        let good = std::fs::read(&good_path).unwrap();
        let bad = misaligned_fixture(&dir.path().join("bad.dbn"), 3);
        for chunk in [1, 3, 7, 8, 13, 64, 4096] {
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut FrameGuard::new(Trickle(&good, chunk)), &mut out)
                .unwrap_or_else(|e| panic!("valid stream refused at chunk {chunk}: {e}"));
            assert_eq!(out, good);
            let err = std::io::Read::read_to_end(
                &mut FrameGuard::new(Trickle(&bad, chunk)),
                &mut Vec::new(),
            )
            .expect_err("misaligned stream accepted");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "chunk {chunk}");
        }
    }

    #[test]
    fn valid_files_still_replay_through_the_guard_plain_and_compressed() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("ok.dbn");
        write_fixture(&plain);
        let bytes = std::fs::read(&plain).unwrap();
        let zst = dir.path().join("ok.dbn.zst");
        let mut w = dbn::encode::DynWriter::new(
            std::fs::File::create(&zst).unwrap(),
            dbn::Compression::Zstd,
        )
        .unwrap();
        std::io::Write::write_all(&mut w, &bytes).unwrap();
        drop(w);
        let a = replay_all(&plain).unwrap();
        assert!(a > 0);
        assert_eq!(replay_all(&zst).unwrap(), a);
    }

    fn write_fixture(path: &Path) {
        let day = time::macros::date!(2026 - 11 - 09);
        let start = day.midnight().assume_utc().unix_timestamp_nanos() as u64;
        let meta = Metadata::builder()
            .dataset("XNAS.ITCH".to_owned())
            .schema(Some(Schema::Trades))
            .start(start)
            .stype_in(Some(SType::RawSymbol))
            .stype_out(SType::InstrumentId)
            .symbols(vec!["AAPL".to_owned(), "MSFT".to_owned()])
            .mappings(vec![
                SymbolMapping {
                    raw_symbol: "AAPL".to_owned(),
                    intervals: vec![MappingInterval {
                        start_date: day,
                        end_date: day.next_day().unwrap(),
                        symbol: "1".to_owned(),
                    }],
                },
                SymbolMapping {
                    raw_symbol: "MSFT".to_owned(),
                    intervals: vec![MappingInterval {
                        start_date: day,
                        end_date: day.next_day().unwrap(),
                        symbol: "2".to_owned(),
                    }],
                },
            ])
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut enc = dbn::encode::dbn::Encoder::new(file, &meta).unwrap();
        for i in 0..50u64 {
            let t = TradeMsg {
                hd: RecordHeader::new::<TradeMsg>(
                    rtype::MBP_0,
                    1,
                    u32::from(i % 2 == 1) + 1,
                    start + i * 1_000_000,
                ),
                price: 150_000_000_000 + i as i64 * 10_000_000, // 150.00 + i cents (1e-9 scale)
                size: 100 + i as u32,
                action: b'T' as std::ffi::c_char,
                side: if i % 3 == 0 { b'B' } else { b'A' } as std::ffi::c_char,
                flags: FlagSet::default(),
                depth: 0,
                ts_recv: start + i * 1_000_000 + 500,
                ts_in_delta: 0,
                sequence: i as u32,
            };
            enc.encode_record(&t).unwrap();
        }
        // one mbp-1 record for AAPL
        let m = Mbp1Msg {
            hd: RecordHeader::new::<Mbp1Msg>(rtype::MBP_1, 1, 1, start + 60_000_000),
            price: 150_000_000_000,
            size: 1,
            action: b'M' as std::ffi::c_char,
            side: b'N' as std::ffi::c_char,
            flags: FlagSet::default(),
            depth: 0,
            ts_recv: start + 60_000_000 + 500,
            ts_in_delta: 0,
            sequence: 99,
            levels: [dbn::BidAskPair {
                bid_px: 149_990_000_000,
                ask_px: 150_010_000_000,
                bid_sz: 300,
                ask_sz: 200,
                bid_ct: 3,
                ask_ct: 2,
            }],
        };
        enc.encode_record(&m).unwrap();
    }

    /// The input the fuzzer found: a header that declares 4 GiB of metadata in an 11-byte file.
    #[test]
    fn a_header_that_declares_more_metadata_than_the_file_holds_is_refused() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.dbn");
        std::fs::write(&path, [68, 66, 78, 1, 181, 47, 40, 253, 114, 70, 10]).unwrap();
        match DbnReplay::open(&path) {
            Err(DbnError::Metadata { declared, limit }) => {
                assert_eq!(declared, 0xFD28_2FB5);
                assert_eq!(limit, 3);
            }
            other => panic!("expected the header to be refused, got {other:?}"),
        }

        // the same header inside a compressed file: only the cap applies
        let zst = dir.path().join("bad.dbn.zst");
        let mut w = dbn::encode::DynWriter::new(
            std::fs::File::create(&zst).unwrap(),
            dbn::Compression::Zstd,
        )
        .unwrap();
        w.write_all(&[68, 66, 78, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0])
            .unwrap();
        w.flush().unwrap();
        drop(w);
        assert!(matches!(
            DbnReplay::open(&zst),
            Err(DbnError::Metadata {
                declared: 0xFFFF_FFFF,
                limit: MAX_METADATA_BYTES
            })
        ));

        // a short or foreign file still gets the decoder's own error
        let short = dir.path().join("short.dbn");
        std::fs::write(&short, b"DBN\x01").unwrap();
        assert!(matches!(DbnReplay::open(&short), Err(DbnError::Dbn(_))));
    }

    #[test]
    fn long_run_of_ignored_records_does_not_end_the_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ignored.dbn");
        let day = time::macros::date!(2026 - 11 - 09);
        let start = day.midnight().assume_utc().unix_timestamp_nanos() as u64;
        let meta = Metadata::builder()
            .dataset("XNAS.ITCH".to_owned())
            .schema(Some(Schema::Mbo))
            .start(start)
            .stype_in(Some(SType::RawSymbol))
            .stype_out(SType::InstrumentId)
            .symbols(vec!["AAPL".to_owned()])
            .mappings(vec![SymbolMapping {
                raw_symbol: "AAPL".to_owned(),
                intervals: vec![MappingInterval {
                    start_date: day,
                    end_date: day.next_day().unwrap(),
                    symbol: "1".to_owned(),
                }],
            }])
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut enc = dbn::encode::dbn::Encoder::new(file, &meta).unwrap();
        // 3,000 order-level records ANKAA ignores, then one trade
        for i in 0..3_000u64 {
            let m = dbn::MboMsg {
                hd: RecordHeader::new::<dbn::MboMsg>(rtype::MBO, 1, 1, start + i),
                order_id: i,
                price: 150_000_000_000,
                size: 1,
                flags: FlagSet::default(),
                channel_id: 0,
                action: b'A' as std::ffi::c_char,
                side: b'B' as std::ffi::c_char,
                ts_recv: start + i,
                ts_in_delta: 0,
                sequence: i as u32,
            };
            enc.encode_record(&m).unwrap();
        }
        let t = TradeMsg {
            hd: RecordHeader::new::<TradeMsg>(rtype::MBP_0, 1, 1, start + 5_000),
            price: 150_000_000_000,
            size: 7,
            action: b'T' as std::ffi::c_char,
            side: b'A' as std::ffi::c_char,
            flags: FlagSet::default(),
            depth: 0,
            ts_recv: start + 5_000,
            ts_in_delta: 0,
            sequence: 9,
        };
        enc.encode_record(&t).unwrap();
        drop(enc);
        let mut r = DbnReplay::open(&path).unwrap();
        let mut symbols = SymbolDict::new();
        let mut batch = EventBatch::with_capacity(64);
        let mut total = 0;
        let mut calls = 0;
        while let Some(n) = r.next_batch(&mut symbols, &mut batch, 1024).unwrap() {
            total += n;
            calls += 1;
        }
        assert_eq!(
            total, 1,
            "the trade after 3,000 ignored records must be reached"
        );
        assert!(calls >= 3);
        assert_eq!(r.stats().ignored, 3_000);
        assert_eq!(batch.trades[0].size, 7 * 100_000_000);
    }

    #[test]
    fn replays_trades_and_mbp1_with_symbols() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dbn");
        write_fixture(&path);
        let mut r = DbnReplay::open(&path).unwrap();
        assert_eq!(r.dataset(), "XNAS.ITCH");
        let mut symbols = SymbolDict::new();
        let mut batch = EventBatch::with_capacity(1024);
        let mut total = 0;
        while let Some(n) = r.next_batch(&mut symbols, &mut batch, 16).unwrap() {
            total += n;
        }
        assert_eq!(total, 51);
        assert_eq!(batch.trades.len(), 50);
        assert_eq!(batch.snapshots.len(), 1);
        assert_eq!(symbols.len(), 2);
        let t0 = &batch.trades[0];
        assert_eq!(symbols.name(t0.symbol), Some("AAPL"));
        assert_eq!(t0.price, 150_00000000);
        assert_eq!(t0.size, 100 * 100_000_000);
        assert_eq!(t0.side, Side::Bid);
        assert_eq!(batch.trades[1].price, 150_01000000);
        assert_eq!(symbols.name(batch.trades[1].symbol), Some("MSFT"));
        assert_eq!(t0.recv_ns - t0.ts_ns, 500);
        let s = &batch.snapshots[0];
        assert_eq!(s.bids, vec![(149_99000000, 300 * 100_000_000)]);
        assert_eq!(s.asks, vec![(150_01000000, 200 * 100_000_000)]);
        assert_eq!(s.last_update_id, 99);
        assert_eq!(r.stats().unmapped, 0);
        assert_eq!(r.stats().ignored, 0);
    }
}
