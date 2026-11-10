//! Binance combined-stream JSON decoder (`@trade`, `@depth` / `@depth@100ms`) and REST depth
//! snapshots.
//!
//! Performance design (measured with samply + criterion, see `docs/benchmarks/`):
//! - **one pass per frame**: the envelope and the event are parsed together through serde
//!   `DeserializeSeed`s, dispatching on the `"e"` tag as it is read (no `RawValue`, no re-scan);
//! - **no per-message allocation**: depth levels are parsed straight into two scratch
//!   `Vec<(i64, i64)>` owned by the decoder and cleared per message; prices and quantities go
//!   from their decimal strings to fixed-point `i64` without floating point;
//! - **dense per-symbol state**: sequence tracking is a `Vec` indexed by symbol id, not a hash map.
//!
//! Strings are borrowed from the frame; the only steady-state allocation is a new symbol name.

use ankaa_ingest::{
    BookDelta, BookSnapshot, DecodeError, Decoder, EventBatch, Side, SymbolDict, Trade, parse_fixed,
};
use serde::Deserialize;
use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};

/// Fixed-point scale used for Binance prices and quantities.
pub const SCALE: u8 = 8;

// ------------------------------------------------------------------------------------------------
// Snapshot frames (rare): plain derive is fine.
// ------------------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct SnapshotWrapper<'a> {
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(borrow)]
    snapshot: RestDepth<'a>,
}

/// Body of `GET /api/v3/depth`.
#[derive(Deserialize)]
struct RestDepth<'a> {
    #[serde(rename = "lastUpdateId")]
    last_update_id: i64,
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

// ------------------------------------------------------------------------------------------------
// Stream frames: hand-written visitors (zero allocation, single pass).
// ------------------------------------------------------------------------------------------------

struct TradeMsg<'a> {
    event_ms: i64,
    symbol: &'a str,
    trade_id: i64,
    price: &'a str,
    qty: &'a str,
    trade_ms: i64,
    /// `true` when the buyer is the market maker (i.e. the trade was seller-initiated).
    buyer_is_maker: bool,
}

/// Depth message header; levels land in the scratch buffers carried by [`DepthSeed`].
struct DepthHead<'a> {
    event_ms: i64,
    symbol: &'a str,
    first_update_id: i64,
    last_update_id: i64,
}

/// Scratch buffers threaded through serde.
struct DepthSeed<'b> {
    bids: &'b mut Vec<(i64, i64)>,
    asks: &'b mut Vec<(i64, i64)>,
}

/// A JSON string as the bytes between its quotes, not checked for UTF-8: a price or a quantity
/// is digits and a point, and [`parse_fixed`] rejects every other byte. (Validating each of
/// the thousands of level strings of a depth message as text measured 4 % of the whole ingest.)
/// A string with escapes is refused, as it was when levels were read as `&str`.
struct RawStr<'de>(&'de [u8]);

impl<'de> serde::Deserialize<'de> for RawStr<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Bytes;
        impl<'de> Visitor<'de> for Bytes {
            type Value = RawStr<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string without escapes")
            }
            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<RawStr<'de>, E> {
                Ok(RawStr(v))
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<RawStr<'de>, E> {
                Ok(RawStr(v.as_bytes()))
            }
        }
        d.deserialize_bytes(Bytes)
    }
}

/// One side: a JSON array of `["price","qty"]` pairs, parsed with [`parse_fixed`] on the fly.
struct LevelsSeed<'b>(&'b mut Vec<(i64, i64)>);

impl<'de> DeserializeSeed<'de> for LevelsSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for LevelsSeed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("array of [price, qty] string pairs")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while let Some([p, q]) = seq.next_element::<[RawStr<'de>; 2]>()? {
            let price = parse_fixed(p.0, SCALE).map_err(A::Error::custom)?;
            let qty = parse_fixed(q.0, SCALE).map_err(A::Error::custom)?;
            self.0.push((price, qty));
        }
        Ok(())
    }
}

/// What a frame contained.
enum Parsed<'a> {
    Trade(TradeMsg<'a>),
    Depth(DepthHead<'a>),
    Other,
}

/// Top level: `{"stream":..,"data":{...}}` or a bare event `{"e":..,...}` (bare events must
/// start with the `"e"` tag, as Binance emits them; keys before it would be skipped).
struct FrameSeed<'b>(DepthSeed<'b>);

impl<'de> DeserializeSeed<'de> for FrameSeed<'_> {
    type Value = Parsed<'de>;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for FrameSeed<'_> {
    type Value = Parsed<'de>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("combined-stream envelope or bare event")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut depth = Some(self.0);
        let mut result = None;
        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "data" if result.is_none() => {
                    let d = depth
                        .take()
                        .ok_or_else(|| A::Error::custom("duplicate data"))?;
                    result = Some(map.next_value_seed(EventSeed(d))?);
                }
                "e" => {
                    // bare event: the tag comes first on Binance; parse the rest by type
                    let tag = map.next_value::<&'de str>()?;
                    let d = depth
                        .take()
                        .ok_or_else(|| A::Error::custom("duplicate event"))?;
                    return visit_event_body(tag, &mut map, d);
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        result.ok_or_else(|| A::Error::missing_field("data"))
    }
}

/// Event object `{"e": "...", ...}` inside the envelope.
struct EventSeed<'b>(DepthSeed<'b>);

impl<'de> DeserializeSeed<'de> for EventSeed<'_> {
    type Value = Parsed<'de>;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for EventSeed<'_> {
    type Value = Parsed<'de>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("event object with an \"e\" tag")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        match map.next_key::<&'de str>()? {
            Some("e") => {
                let tag = map.next_value::<&'de str>()?;
                visit_event_body(tag, &mut map, self.0)
            }
            Some(_) => {
                // unknown shape (e.g. {"result":null,"id":1}): drain and report
                map.next_value::<IgnoredAny>()?;
                drain(&mut map)?;
                Ok(Parsed::Other)
            }
            None => Ok(Parsed::Other),
        }
    }
}

fn drain<'de, A: MapAccess<'de>>(map: &mut A) -> Result<(), A::Error> {
    while map.next_key::<&'de str>()?.is_some() {
        map.next_value::<IgnoredAny>()?;
    }
    Ok(())
}

/// Parse the fields after the `"e"` tag for the given event type.
fn visit_event_body<'de, A: MapAccess<'de>>(
    tag: &str,
    map: &mut A,
    depth: DepthSeed<'_>,
) -> Result<Parsed<'de>, A::Error> {
    match tag {
        "trade" => {
            let (mut event_ms, mut symbol, mut trade_id, mut price, mut qty, mut maker) =
                (None, None, None, None, None, None);
            let mut trade_ms = 0i64;
            while let Some(key) = map.next_key::<&'de str>()? {
                match key {
                    "E" => event_ms = Some(map.next_value::<i64>()?),
                    "s" => symbol = Some(map.next_value::<&'de str>()?),
                    "t" => trade_id = Some(map.next_value::<i64>()?),
                    "p" => price = Some(map.next_value::<&'de str>()?),
                    "q" => qty = Some(map.next_value::<&'de str>()?),
                    "T" => trade_ms = map.next_value::<i64>()?,
                    "m" => maker = Some(map.next_value::<bool>()?),
                    _ => {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
            }
            Ok(Parsed::Trade(TradeMsg {
                event_ms: event_ms.ok_or_else(|| A::Error::missing_field("E"))?,
                symbol: symbol.ok_or_else(|| A::Error::missing_field("s"))?,
                trade_id: trade_id.ok_or_else(|| A::Error::missing_field("t"))?,
                price: price.ok_or_else(|| A::Error::missing_field("p"))?,
                qty: qty.ok_or_else(|| A::Error::missing_field("q"))?,
                trade_ms,
                buyer_is_maker: maker.ok_or_else(|| A::Error::missing_field("m"))?,
            }))
        }
        "depthUpdate" => {
            let (mut event_ms, mut symbol, mut first, mut last) = (None, None, None, None);
            let DepthSeed { bids, asks } = depth;
            while let Some(key) = map.next_key::<&'de str>()? {
                match key {
                    "E" => event_ms = Some(map.next_value::<i64>()?),
                    "s" => symbol = Some(map.next_value::<&'de str>()?),
                    "U" => first = Some(map.next_value::<i64>()?),
                    "u" => last = Some(map.next_value::<i64>()?),
                    "b" => map.next_value_seed(LevelsSeed(bids))?,
                    "a" => map.next_value_seed(LevelsSeed(asks))?,
                    _ => {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
            }
            Ok(Parsed::Depth(DepthHead {
                event_ms: event_ms.ok_or_else(|| A::Error::missing_field("E"))?,
                symbol: symbol.ok_or_else(|| A::Error::missing_field("s"))?,
                first_update_id: first.ok_or_else(|| A::Error::missing_field("U"))?,
                last_update_id: last.ok_or_else(|| A::Error::missing_field("u"))?,
            }))
        }
        _ => {
            drain(map)?;
            Ok(Parsed::Other)
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Decoder
// ------------------------------------------------------------------------------------------------

/// Counters exposed through `/metrics`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DecoderStats {
    /// Frames decoded into at least one event.
    pub frames_ok: u64,
    /// Frames of an unhandled type (e.g. subscription acks).
    pub frames_ignored: u64,
    /// Frames rejected as malformed.
    pub frames_bad: u64,
    /// Trades emitted.
    pub trades: u64,
    /// Book deltas emitted.
    pub deltas: u64,
    /// Depth sequence gaps detected (`U != previous u + 1`).
    pub gaps: u64,
    /// Book snapshots decoded.
    pub snapshots: u64,
}

/// Stateful decoder: symbol interning, per-symbol depth sequence tracking, scratch buffers.
#[derive(Debug, Default)]
pub struct BinanceJsonDecoder {
    symbols: SymbolDict,
    /// Last depth update id per symbol id (dense; `None` = no diff seen since (re)connect).
    last_update_id: Vec<Option<i64>>,
    stats: DecoderStats,
    scratch_bids: Vec<(i64, i64)>,
    scratch_asks: Vec<(i64, i64)>,
    /// simd-json parses in place and needs a mutable, padded copy of the frame.
    #[cfg(feature = "simd-json")]
    scratch_frame: Vec<u8>,
}

impl BinanceJsonDecoder {
    /// New decoder with an empty symbol dictionary.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Symbol dictionary (ids are stable for the decoder's lifetime).
    #[must_use]
    pub fn symbols(&self) -> &SymbolDict {
        &self.symbols
    }

    /// Mutable dictionary access, so an SBE decoder on the same connection shares symbol ids.
    pub fn symbols_mut(&mut self) -> &mut SymbolDict {
        &mut self.symbols
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> DecoderStats {
        self.stats
    }

    /// Forget per-symbol sequence state (call on reconnect: the new stream is a new sequence,
    /// not a gap in the old one). Symbols keep their ids.
    pub fn reset_sequences(&mut self) {
        self.last_update_id.iter_mut().for_each(|v| *v = None);
    }
}

/// Emit one trade from a parsed message (free function so the parsed borrow of the frame buffer
/// and the decoder's own fields can coexist).
fn trade_into(
    symbols: &mut SymbolDict,
    stats: &mut DecoderStats,
    m: &TradeMsg<'_>,
    recv_ns: i64,
    out: &mut EventBatch,
) -> Result<usize, DecodeError> {
    let price = parse_fixed(m.price.as_bytes(), SCALE)
        .map_err(|source| DecodeError::Number { field: "p", source })?;
    let size = parse_fixed(m.qty.as_bytes(), SCALE)
        .map_err(|source| DecodeError::Number { field: "q", source })?;
    let symbol = symbols.intern(m.symbol);
    let ts_ns = ms_to_ns(if m.trade_ms > 0 {
        m.trade_ms
    } else {
        m.event_ms
    })?;
    out.trades.push(Trade {
        ts_ns,
        recv_ns,
        symbol,
        price,
        size,
        side: if m.buyer_is_maker {
            Side::Ask
        } else {
            Side::Bid
        },
        seq: m.trade_id,
    });
    stats.trades += 1;
    Ok(1)
}

/// Emit the deltas of one depth message whose levels sit in the scratch buffers.
#[allow(clippy::too_many_arguments)]
fn depth_into(
    symbols: &mut SymbolDict,
    last_update_id: &mut Vec<Option<i64>>,
    stats: &mut DecoderStats,
    bids: &[(i64, i64)],
    asks: &[(i64, i64)],
    m: &DepthHead<'_>,
    recv_ns: i64,
    out: &mut EventBatch,
) -> Result<usize, DecodeError> {
    let symbol = symbols.intern(m.symbol);
    let ts_ns = ms_to_ns(m.event_ms)?;
    let i = symbol.0 as usize;
    if last_update_id.len() <= i {
        last_update_id.resize(i + 1, None);
    }
    if let Some(prev) = last_update_id[i]
        && Some(m.first_update_id) != prev.checked_add(1)
    {
        stats.gaps += 1;
    }
    last_update_id[i] = Some(m.last_update_id);
    let n = bids.len() + asks.len();
    out.deltas.reserve(n);
    for (side, levels) in [(Side::Bid, bids), (Side::Ask, asks)] {
        for &(price, size) in levels {
            out.deltas.push(BookDelta {
                ts_ns,
                recv_ns,
                symbol,
                side,
                price,
                size,
                first_update_id: m.first_update_id,
                update_id: m.last_update_id,
            });
        }
    }
    stats.deltas += n as u64;
    Ok(n)
}

impl BinanceJsonDecoder {
    /// Decode a snapshot frame: `{"s":"BTCUSDT","snapshot":{"lastUpdateId":..,"bids":[..],"asks":[..]}}`.
    pub fn decode_snapshot(
        &mut self,
        frame: &[u8],
        recv_ns: i64,
        out: &mut EventBatch,
    ) -> Result<usize, DecodeError> {
        let text = core::str::from_utf8(frame).map_err(|_| DecodeError::Malformed("utf8"))?;
        let w: SnapshotWrapper<'_> =
            serde_json::from_str(text).map_err(|_| DecodeError::Malformed("snapshot"))?;
        let parse =
            |levels: &[[&str; 2]], field: &'static str| -> Result<Vec<(i64, i64)>, DecodeError> {
                levels
                    .iter()
                    .map(|[p, q]| {
                        Ok((
                            parse_fixed(p.as_bytes(), SCALE)
                                .map_err(|source| DecodeError::Number { field, source })?,
                            parse_fixed(q.as_bytes(), SCALE)
                                .map_err(|source| DecodeError::Number { field, source })?,
                        ))
                    })
                    .collect()
            };
        let bids = parse(&w.snapshot.bids, "snapshot bids")?;
        let asks = parse(&w.snapshot.asks, "snapshot asks")?;
        let symbol = self.symbols.intern(w.symbol);
        // Sequence tracking stays on the diff stream; snapshot/diff alignment is the book's job.
        out.snapshots.push(BookSnapshot {
            ts_ns: recv_ns,
            recv_ns,
            symbol,
            last_update_id: w.snapshot.last_update_id,
            bids,
            asks,
        });
        self.stats.snapshots += 1;
        self.stats.frames_ok += 1;
        Ok(1)
    }
}

fn ms_to_ns(ms: i64) -> Result<i64, DecodeError> {
    ms.checked_mul(1_000_000)
        .ok_or(DecodeError::Malformed("timestamp overflow"))
}

impl Decoder for BinanceJsonDecoder {
    fn decode(
        &mut self,
        frame: &[u8],
        recv_ns: i64,
        out: &mut EventBatch,
    ) -> Result<usize, DecodeError> {
        #[cfg(feature = "simd-json")]
        let Self {
            symbols,
            last_update_id,
            stats,
            scratch_bids,
            scratch_asks,
            scratch_frame,
        } = &mut *self;
        #[cfg(not(feature = "simd-json"))]
        let Self {
            symbols,
            last_update_id,
            stats,
            scratch_bids,
            scratch_asks,
        } = &mut *self;
        scratch_bids.clear();
        scratch_asks.clear();
        #[cfg(not(feature = "simd-json"))]
        let parsed = {
            let mut de = serde_json::Deserializer::from_slice(frame);
            FrameSeed(DepthSeed {
                bids: scratch_bids,
                asks: scratch_asks,
            })
            .deserialize(&mut de)
            .map_err(|_| ())
        };
        #[cfg(feature = "simd-json")]
        let parsed = {
            // simd-json parses in place: copy the frame into the reusable scratch buffer.
            scratch_frame.clear();
            scratch_frame.extend_from_slice(frame);
            match simd_json::Deserializer::from_slice(scratch_frame) {
                Ok(mut de) => FrameSeed(DepthSeed {
                    bids: scratch_bids,
                    asks: scratch_asks,
                })
                .deserialize(&mut de)
                .map_err(|_| ()),
                Err(_) => Err(()),
            }
        };
        let Ok(parsed) = parsed else {
            // Not a frame we understand (acks, truncated or hostile input): counted, not logged.
            stats.frames_ignored += 1;
            return Err(DecodeError::Ignored);
        };
        let r = match parsed {
            Parsed::Trade(t) => trade_into(symbols, stats, &t, recv_ns, out),
            Parsed::Depth(h) => depth_into(
                symbols,
                last_update_id,
                stats,
                scratch_bids,
                scratch_asks,
                &h,
                recv_ns,
                out,
            ),
            Parsed::Other => {
                stats.frames_ignored += 1;
                return Err(DecodeError::Ignored);
            }
        };
        if r.is_ok() {
            stats.frames_ok += 1;
        } else {
            stats.frames_bad += 1;
        }
        r
    }

    fn name(&self) -> &'static str {
        "binance-json"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_trade_and_depth_from_envelope() {
        let mut d = BinanceJsonDecoder::new();
        let mut out = EventBatch::default();
        let t = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1794231662266,"s":"BTCUSDT","t":6832857732,"p":"74670.01000000","q":"0.00400000","T":1794231662265,"m":false,"M":true}}"#;
        assert_eq!(d.decode(t, 1, &mut out), Ok(1));
        let tr = out.trades[0];
        assert_eq!(tr.price, 7_467_001_000_000);
        assert_eq!(tr.size, 400_000);
        assert_eq!(tr.side, Side::Bid);
        assert_eq!(tr.ts_ns, 1_794_231_662_265_000_000);
        assert_eq!(tr.seq, 6_832_857_732);
        let dp = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794231662214,"s":"BTCUSDT","U":100,"u":105,"b":[["74663.56000000","0.00015000"]],"a":[["74670.00000000","0.00000000"]]}}"#;
        assert_eq!(d.decode(dp, 2, &mut out), Ok(2));
        assert_eq!(out.deltas[0].side, Side::Bid);
        assert_eq!(out.deltas[1].side, Side::Ask);
        assert_eq!(out.deltas[1].size, 0);
        assert_eq!(out.deltas[1].update_id, 105);
        assert_eq!(out.deltas[1].first_update_id, 100);
        let dp2 = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794231662314,"s":"BTCUSDT","U":106,"u":110,"b":[],"a":[]}}"#;
        assert_eq!(d.decode(dp2, 3, &mut out), Ok(0));
        assert_eq!(d.stats().gaps, 0);
        let dp3 = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794231662414,"s":"BTCUSDT","U":112,"u":115,"b":[],"a":[]}}"#;
        assert_eq!(d.decode(dp3, 4, &mut out), Ok(0));
        assert_eq!(d.stats().gaps, 1);
        assert_eq!(d.symbols().len(), 1);
        // bare event (single-stream endpoint): the "e" tag comes first (as Binance emits it); the
        // remaining keys may appear in any order
        let bare =
            br#"{"e":"trade","m":true,"q":"1","p":"2000.5","t":9,"s":"ETHUSDT","E":1,"T":1}"#;
        assert_eq!(d.decode(bare, 5, &mut out), Ok(1));
        assert_eq!(out.trades.last().unwrap().side, Side::Ask);
        assert_eq!(out.trades.last().unwrap().price, 200_050_000_000);
    }

    #[test]
    fn snapshot_frame_and_sequence_overflow() {
        let mut d = BinanceJsonDecoder::new();
        let mut out = EventBatch::default();
        let snap = br#"{"s":"BTCUSDT","snapshot":{"lastUpdateId":500,"bids":[["100.00000000","1.00000000"]],"asks":[["101.00000000","2.00000000"]]}}"#;
        assert_eq!(d.decode_snapshot(snap, 7, &mut out), Ok(1));
        let sn = &out.snapshots[0];
        assert_eq!(sn.last_update_id, 500);
        assert_eq!(sn.bids, vec![(10_000_000_000, 100_000_000)]);
        assert_eq!(sn.asks, vec![(10_100_000_000, 200_000_000)]);
        let dp = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":501,"u":505,"b":[],"a":[]}}"#;
        assert_eq!(d.decode(dp, 8, &mut out), Ok(0));
        assert_eq!(d.stats().gaps, 0);
        let hostile = br#"{"stream":"x","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":506,"u":9223372036854775807,"b":[],"a":[]}}"#;
        assert_eq!(d.decode(hostile, 9, &mut out), Ok(0));
        let next = br#"{"stream":"x","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]}}"#;
        assert_eq!(d.decode(next, 10, &mut out), Ok(0));
        assert_eq!(d.stats().gaps, 1);
        // a level with a bad number fails the whole frame and leaves no half-applied deltas
        let bad = br#"{"stream":"x","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":3,"u":4,"b":[["1.0","1.0"]],"a":[["x","1"]]}}"#;
        let before = out.deltas.len();
        assert!(d.decode(bad, 11, &mut out).is_err());
        assert_eq!(out.deltas.len(), before);
    }

    #[test]
    fn malformed_input_never_panics() {
        let mut d = BinanceJsonDecoder::new();
        let mut out = EventBatch::default();
        for bad in [
            &b"{"[..],
            b"\xff\xfe",
            b"{\"result\":null,\"id\":1}",
            br#"{"e":"trade","p":"abc"}"#,
            br#"{"stream":"x","data":{"e":"trade","E":1,"s":"S","t":1,"p":"1.123456789","q":"1","T":1,"m":true}}"#,
            br#"{"stream":"x","data":{"e":"depthUpdate","E":1,"s":"S","U":1,"u":2,"b":[["1"]],"a":[]}}"#,
            b"[1,2,3]",
            b"",
        ] {
            let r = d.decode(bad, 0, &mut out);
            assert!(r.is_err(), "{bad:?}");
        }
        assert!(out.is_empty());
    }
}
