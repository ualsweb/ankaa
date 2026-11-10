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
use dbn::decode::{DbnMetadata, DecodeRecordRef, DynDecoder};
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

/// Streaming reader over one DBN file.
pub struct DbnReplay {
    dec: DynDecoder<'static, std::io::BufReader<std::fs::File>>,
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
}

impl DbnReplay {
    /// Open a `.dbn` or `.dbn.zst` file (compression is inferred).
    pub fn open(path: &Path) -> Result<Self, DbnError> {
        let dec = DynDecoder::from_file(path, VersionUpgradePolicy::UpgradeToV3)?;
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
                    out.trades.push(Trade {
                        ts_ns: to_i64(ts_event),
                        recv_ns: to_i64(ts_recv),
                        symbol,
                        price: price / PRICE_DIV,
                        size: i64::from(size) * 100_000_000,
                        // Databento `side` is the aggressor; 'B' buy → bid-side aggressor
                        side: if side as u8 == b'B' {
                            Side::Bid
                        } else {
                            Side::Ask
                        },
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
