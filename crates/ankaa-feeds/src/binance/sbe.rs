//! Binance spot **SBE** market-data stream decoder (schema `spot_stream` id 1, version 0,
//! `stream_1_0.xml`), little-endian.
//!
//! A frame is an 8-byte message header (`blockLength u16, templateId u16, schemaId u16,
//! version u16`), then the root block, the repeating groups and the `symbol` var-string (`u8`
//! length, UTF-8). Prices and quantities are `mantissa64` with a per-message `exponent8`; they are
//! converted to fixed-point ticks at scale 8 without floating point. Unknown fields appended by
//! future schema versions are skipped using the encoded block lengths.
//!
//! Templates: 10000 trades, 10001 best bid/ask (ignored for now), 10002 depth snapshot,
//! 10003 depth diff.

use ankaa_ingest::{
    BookDelta, BookSnapshot, DecodeError, EventBatch, Side, SymbolDict, SymbolId, Trade,
};

/// Schema id this decoder understands.
pub const SCHEMA_ID: u16 = 1;
/// Fixed-point scale of the produced ticks.
pub const SCALE: u8 = 8;

const T_TRADES: u16 = 10000;
#[allow(dead_code)]
const T_BEST_BID_ASK: u16 = 10001;
const T_DEPTH_SNAPSHOT: u16 = 10002;
const T_DEPTH_DIFF: u16 = 10003;

/// Little-endian cursor with bounds checks (never panics on short input).
struct Cur<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(DecodeError::Malformed("sbe: length overflow"))?;
        let s = self
            .b
            .get(self.pos..end)
            .ok_or(DecodeError::Malformed("sbe: truncated"))?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn i8(&mut self) -> Result<i8, DecodeError> {
        Ok(i8::from_le_bytes([self.u8()?]))
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        let s = self.take(8)?;
        Ok(i64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }
}

/// `mantissa × 10^exponent` as ticks at `SCALE` decimals, exactly or not at all.
#[inline]
fn to_ticks(mantissa: i64, exponent: i8) -> Result<i64, DecodeError> {
    let shift = i32::from(exponent) + i32::from(SCALE);
    if shift < 0 {
        // more precision than the fixed-point scale: refuse rather than round silently
        return Err(DecodeError::Malformed(
            "sbe: exponent below fixed-point scale",
        ));
    }
    if shift > 18 {
        return Err(DecodeError::Malformed("sbe: exponent too large"));
    }
    mantissa
        .checked_mul(10i64.pow(shift as u32))
        .ok_or(DecodeError::Malformed("sbe: tick overflow"))
}

#[inline]
fn us_to_ns(us: i64) -> Result<i64, DecodeError> {
    us.checked_mul(1_000)
        .ok_or(DecodeError::Malformed("sbe: timestamp overflow"))
}

/// Counters for the SBE path.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SbeStats {
    /// Frames decoded.
    pub frames_ok: u64,
    /// Frames of a template we do not handle (e.g. best bid/ask) or another schema.
    pub frames_ignored: u64,
    /// Malformed frames.
    pub frames_bad: u64,
    /// Trades emitted.
    pub trades: u64,
    /// Book deltas emitted.
    pub deltas: u64,
    /// Snapshots emitted.
    pub snapshots: u64,
    /// Depth sequence gaps (`first != previous last + 1`).
    pub gaps: u64,
}

/// SBE decoder sharing a symbol dictionary with the JSON decoder (same symbol ids).
#[derive(Debug, Default)]
pub struct BinanceSbeDecoder {
    stats: SbeStats,
    last_update_id: Vec<Option<i64>>,
}

impl BinanceSbeDecoder {
    /// New decoder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> SbeStats {
        self.stats
    }

    /// Forget sequence state (reconnect).
    pub fn reset_sequences(&mut self) {
        self.last_update_id.iter_mut().for_each(|v| *v = None);
    }

    fn track(&mut self, symbol: SymbolId, first: i64, last: i64) {
        let i = symbol.0 as usize;
        if self.last_update_id.len() <= i {
            self.last_update_id.resize(i + 1, None);
        }
        if let Some(prev) = self.last_update_id[i]
            && Some(first) != prev.checked_add(1)
        {
            self.stats.gaps += 1;
        }
        self.last_update_id[i] = Some(last);
    }

    /// Decode one binary frame. `symbols` is shared with the JSON decoder so ids match.
    pub fn decode(
        &mut self,
        frame: &[u8],
        recv_ns: i64,
        symbols: &mut SymbolDict,
        out: &mut EventBatch,
    ) -> Result<usize, DecodeError> {
        let r = self.decode_inner(frame, recv_ns, symbols, out);
        match &r {
            Ok(_) => self.stats.frames_ok += 1,
            Err(DecodeError::Ignored) => self.stats.frames_ignored += 1,
            Err(_) => self.stats.frames_bad += 1,
        }
        r
    }

    #[allow(clippy::too_many_lines)]
    fn decode_inner(
        &mut self,
        frame: &[u8],
        recv_ns: i64,
        symbols: &mut SymbolDict,
        out: &mut EventBatch,
    ) -> Result<usize, DecodeError> {
        let mut c = Cur { b: frame, pos: 0 };
        let block_len = c.u16()? as usize;
        let template = c.u16()?;
        let schema = c.u16()?;
        let _version = c.u16()?;
        if schema != SCHEMA_ID {
            return Err(DecodeError::Ignored);
        }
        let root_start = c.pos;
        match template {
            T_TRADES => {
                let event_us = c.i64()?;
                let transact_us = c.i64()?;
                let pe = c.i8()?;
                let qe = c.i8()?;
                if block_len < c.pos - root_start {
                    return Err(DecodeError::Malformed(
                        "blockLength shorter than the root block",
                    ));
                }
                c.pos = root_start + block_len; // skip unknown appended root fields
                let ts_ns = us_to_ns(if transact_us > 0 {
                    transact_us
                } else {
                    event_us
                })?;
                // group: groupSizeEncoding (blockLength u16, numInGroup u32)
                let gbl = c.u16()? as usize;
                let n = c.u32()? as usize;
                if gbl < 8 + 8 + 8 + 1 {
                    return Err(DecodeError::Malformed("sbe: trade block too short"));
                }
                let start = out.trades.len();
                let mut entries = Vec::with_capacity(n.min(4096));
                for _ in 0..n {
                    let estart = c.pos;
                    let id = c.i64()?;
                    let price = to_ticks(c.i64()?, pe)?;
                    let qty = to_ticks(c.i64()?, qe)?;
                    let maker = c.u8()? != 0;
                    c.pos = estart + gbl;
                    if c.pos > frame.len() {
                        return Err(DecodeError::Malformed("sbe: truncated"));
                    }
                    entries.push((id, price, qty, maker));
                }
                let symbol = symbols.intern(read_var_string8(&mut c)?);
                for (id, price, qty, maker) in entries {
                    out.trades.push(Trade {
                        ts_ns,
                        recv_ns,
                        symbol,
                        price,
                        size: qty,
                        side: if maker { Side::Ask } else { Side::Bid },
                        seq: id,
                    });
                }
                let n = out.trades.len() - start;
                self.stats.trades += n as u64;
                Ok(n)
            }
            T_DEPTH_DIFF | T_DEPTH_SNAPSHOT => {
                let event_us = c.i64()?;
                let (first_id, last_id) = if template == T_DEPTH_DIFF {
                    (c.i64()?, c.i64()?)
                } else {
                    let id = c.i64()?;
                    (id, id)
                };
                let pe = c.i8()?;
                let qe = c.i8()?;
                if block_len < c.pos - root_start {
                    return Err(DecodeError::Malformed(
                        "blockLength shorter than the root block",
                    ));
                }
                c.pos = root_start + block_len;
                let ts_ns = us_to_ns(event_us)?;
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                for side in [&mut bids, &mut asks] {
                    // groupSize16Encoding (blockLength u16, numInGroup u16)
                    let gbl = c.u16()? as usize;
                    let n = c.u16()? as usize;
                    if gbl < 16 {
                        return Err(DecodeError::Malformed("sbe: level block too short"));
                    }
                    side.reserve(n);
                    for _ in 0..n {
                        let estart = c.pos;
                        let price = to_ticks(c.i64()?, pe)?;
                        let qty = to_ticks(c.i64()?, qe)?;
                        c.pos = estart + gbl;
                        if c.pos > frame.len() {
                            return Err(DecodeError::Malformed("sbe: truncated"));
                        }
                        side.push((price, qty));
                    }
                }
                let symbol = symbols.intern(read_var_string8(&mut c)?);
                if template == T_DEPTH_SNAPSHOT {
                    out.snapshots.push(BookSnapshot {
                        ts_ns,
                        recv_ns,
                        symbol,
                        last_update_id: last_id,
                        bids,
                        asks,
                    });
                    self.stats.snapshots += 1;
                    return Ok(1);
                }
                self.track(symbol, first_id, last_id);
                let n = bids.len() + asks.len();
                out.deltas.reserve(n);
                for (side, levels) in [(Side::Bid, &bids), (Side::Ask, &asks)] {
                    for &(price, size) in levels {
                        out.deltas.push(BookDelta {
                            ts_ns,
                            recv_ns,
                            symbol,
                            side,
                            price,
                            size,
                            first_update_id: first_id,
                            update_id: last_id,
                        });
                    }
                }
                self.stats.deltas += n as u64;
                Ok(n)
            }
            // best bid/ask (10001) is not materialised yet; unknown templates are skipped
            _ => Err(DecodeError::Ignored),
        }
    }
}

fn read_var_string8<'a>(c: &mut Cur<'a>) -> Result<&'a str, DecodeError> {
    let len = c.u8()? as usize;
    let bytes = c.take(len)?;
    core::str::from_utf8(bytes).map_err(|_| DecodeError::Malformed("sbe: symbol not utf-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal SBE encoder for tests (mirrors the schema).
    struct Enc(Vec<u8>);
    impl Enc {
        fn header(template: u16, block_len: u16) -> Self {
            let mut v = Vec::new();
            v.extend_from_slice(&block_len.to_le_bytes());
            v.extend_from_slice(&template.to_le_bytes());
            v.extend_from_slice(&SCHEMA_ID.to_le_bytes());
            v.extend_from_slice(&0u16.to_le_bytes());
            Enc(v)
        }
        fn i64(&mut self, v: i64) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn i8(&mut self, v: i8) -> &mut Self {
            self.0.push(v as u8);
            self
        }
        fn u8(&mut self, v: u8) -> &mut Self {
            self.0.push(v);
            self
        }
        fn u16(&mut self, v: u16) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn u32(&mut self, v: u32) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn sym(&mut self, s: &str) -> &mut Self {
            self.0.push(s.len() as u8);
            self.0.extend_from_slice(s.as_bytes());
            self
        }
    }

    #[test]
    fn trades_message() {
        let mut e = Enc::header(T_TRADES, 18);
        e.i64(1_794_231_662_266_123)
            .i64(1_794_231_662_265_000)
            .i8(-8)
            .i8(-8);
        e.u16(25).u32(2);
        e.i64(6_832_857_732)
            .i64(7_467_001_000_000)
            .i64(400_000)
            .u8(0);
        e.i64(6_832_857_733)
            .i64(7_467_002_000_000)
            .i64(1_000_000)
            .u8(1);
        e.sym("BTCUSDT");
        let mut d = BinanceSbeDecoder::new();
        let mut syms = SymbolDict::new();
        let mut out = EventBatch::default();
        assert_eq!(d.decode(&e.0, 9, &mut syms, &mut out), Ok(2));
        assert_eq!(out.trades[0].price, 7_467_001_000_000);
        assert_eq!(out.trades[0].side, Side::Bid);
        assert_eq!(out.trades[1].side, Side::Ask);
        assert_eq!(out.trades[0].ts_ns, 1_794_231_662_265_000_000);
        assert_eq!(syms.name(out.trades[0].symbol), Some("BTCUSDT"));
    }

    #[test]
    fn depth_diff_and_snapshot_with_exponent_scaling() {
        // exponent -2 → mantissa 7466356 means 74663.56 → ticks 7_466_356_000_000
        let mut e = Enc::header(T_DEPTH_DIFF, 26);
        e.i64(1_794_231_662_214_000).i64(100).i64(105).i8(-2).i8(-8);
        e.u16(16).u16(1).i64(7_466_356).i64(15_000);
        e.u16(16).u16(1).i64(7_467_000).i64(0);
        e.sym("BTCUSDT");
        let mut d = BinanceSbeDecoder::new();
        let mut syms = SymbolDict::new();
        let mut out = EventBatch::default();
        assert_eq!(d.decode(&e.0, 1, &mut syms, &mut out), Ok(2));
        assert_eq!(out.deltas[0].price, 7_466_356_000_000);
        assert_eq!(out.deltas[0].size, 15_000);
        assert_eq!(out.deltas[1].size, 0);
        assert_eq!(
            (out.deltas[1].first_update_id, out.deltas[1].update_id),
            (100, 105)
        );
        // contiguous then gap
        let mut e2 = Enc::header(T_DEPTH_DIFF, 26);
        e2.i64(1)
            .i64(106)
            .i64(110)
            .i8(-8)
            .i8(-8)
            .u16(16)
            .u16(0)
            .u16(16)
            .u16(0)
            .sym("BTCUSDT");
        d.decode(&e2.0, 2, &mut syms, &mut out).unwrap();
        assert_eq!(d.stats().gaps, 0);
        let mut e3 = Enc::header(T_DEPTH_DIFF, 26);
        e3.i64(1)
            .i64(112)
            .i64(113)
            .i8(-8)
            .i8(-8)
            .u16(16)
            .u16(0)
            .u16(16)
            .u16(0)
            .sym("BTCUSDT");
        d.decode(&e3.0, 3, &mut syms, &mut out).unwrap();
        assert_eq!(d.stats().gaps, 1);
        // snapshot template
        let mut s = Enc::header(T_DEPTH_SNAPSHOT, 18);
        s.i64(5)
            .i64(500)
            .i8(-8)
            .i8(-8)
            .u16(16)
            .u16(1)
            .i64(1)
            .i64(2)
            .u16(16)
            .u16(1)
            .i64(3)
            .i64(4)
            .sym("ETHUSDT");
        assert_eq!(d.decode(&s.0, 4, &mut syms, &mut out), Ok(1));
        assert_eq!(out.snapshots[0].last_update_id, 500);
        assert_eq!(out.snapshots[0].bids, vec![(1, 2)]);
        assert_eq!(syms.len(), 2);
    }

    #[test]
    fn forward_compatible_and_hostile_input() {
        // a future version appends 4 bytes to the root block: blockLength says 22, we skip them
        let mut e = Enc::header(T_DEPTH_DIFF, 30);
        e.i64(1).i64(1).i64(2).i8(-8).i8(-8).u32(0xDEAD_BEEF);
        e.u16(16).u16(0).u16(16).u16(0).sym("X");
        let mut d = BinanceSbeDecoder::new();
        let mut syms = SymbolDict::new();
        let mut out = EventBatch::default();
        assert_eq!(d.decode(&e.0, 1, &mut syms, &mut out), Ok(0));
        // other schema → ignored; unknown template → ignored; truncated → malformed; never panics
        let mut other = Enc::header(T_TRADES, 18);
        other.0[4] = 9;
        assert_eq!(
            d.decode(&other.0, 1, &mut syms, &mut out),
            Err(DecodeError::Ignored)
        );
        assert_eq!(
            d.decode(&Enc::header(42, 0).0, 1, &mut syms, &mut out),
            Err(DecodeError::Ignored)
        );
        for cut in 0..e.0.len() - 1 {
            let r = d.decode(&e.0[..cut], 1, &mut syms, &mut out);
            assert!(r.is_err(), "cut {cut}");
        }
        // huge group count must not allocate or loop forever
        let mut huge = Enc::header(T_TRADES, 18);
        huge.i64(1).i64(1).i8(-8).i8(-8).u16(25).u32(u32::MAX);
        assert!(d.decode(&huge.0, 1, &mut syms, &mut out).is_err());
        // exponent finer than scale is refused, not rounded
        let mut fine = Enc::header(T_DEPTH_DIFF, 26);
        fine.i64(1)
            .i64(1)
            .i64(2)
            .i8(-9)
            .i8(-8)
            .u16(16)
            .u16(1)
            .i64(1)
            .i64(1)
            .u16(16)
            .u16(0)
            .sym("X");
        assert!(matches!(
            d.decode(&fine.0, 1, &mut syms, &mut out),
            Err(DecodeError::Malformed(_))
        ));
    }
}
