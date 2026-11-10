//! A complete user-defined binary feed in ~100 lines (PRD F-03): framing + decoder.
//!
//! Wire format "TICK/1" (little-endian):
//! ```text
//! frame  := u16 len (payload bytes) | u8 type | payload
//! trade  := type 1 | i64 ts_ns | u8 sym_len | sym bytes | i64 price_ticks | i64 size_ticks | u8 side
//! hb     := type 0 (heartbeat, ignored)
//! ```
//! Run: `cargo run -p ankaa-ingest --example custom_feed`
#![allow(
    clippy::similar_names,
    clippy::inconsistent_digit_grouping,
    clippy::unreadable_literal
)]
use ankaa_ingest::framing::{Framer, LengthPrefixed, Prefix};
use ankaa_ingest::{DecodeError, Decoder, EventBatch, Side, SymbolDict, Trade};

/// Decoder state: the symbol dictionary (ids are stable for the process lifetime).
struct TickDecoder {
    symbols: SymbolDict,
}

/// Little helper: read fixed-width fields without panicking on short input.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError::Malformed("truncated field"));
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes(
            b.try_into().map_err(|_| DecodeError::Malformed("i64"))?,
        ))
    }
}

impl Decoder for TickDecoder {
    fn decode(
        &mut self,
        frame: &[u8],
        recv_ns: i64,
        out: &mut EventBatch,
    ) -> Result<usize, DecodeError> {
        let mut c = Cursor(frame);
        match c.u8()? {
            0 => Err(DecodeError::Ignored),
            1 => {
                let ts_ns = c.i64()?;
                let n = c.u8()? as usize;
                let sym = std::str::from_utf8(c.take(n)?)
                    .map_err(|_| DecodeError::Malformed("symbol utf-8"))?;
                let symbol = self.symbols.intern(sym);
                let price = c.i64()?;
                let size = c.i64()?;
                let side = if c.u8()? == 0 { Side::Bid } else { Side::Ask };
                if price <= 0 || size <= 0 {
                    return Err(DecodeError::Malformed("non-positive price or size"));
                }
                out.trades.push(Trade {
                    ts_ns,
                    recv_ns,
                    symbol,
                    price,
                    size,
                    side,
                    seq: 0,
                });
                Ok(1)
            }
            _ => Err(DecodeError::Malformed("unknown message type")),
        }
    }
    fn name(&self) -> &'static str {
        "tick/1"
    }
}

/// Encoder used only to produce a sample stream.
fn trade_frame(ts_ns: i64, sym: &str, price: i64, size: i64, side: u8) -> Vec<u8> {
    let mut p = vec![1u8];
    p.extend(ts_ns.to_le_bytes());
    p.push(u8::try_from(sym.len()).expect("short symbol"));
    p.extend(sym.as_bytes());
    p.extend(price.to_le_bytes());
    p.extend(size.to_le_bytes());
    p.push(side);
    let mut f = u16::try_from(p.len())
        .expect("small frame")
        .to_le_bytes()
        .to_vec();
    f.extend(p);
    f
}

fn main() {
    // the stream: two trades, a heartbeat, one trade, delivered in awkward TCP segments
    let mut stream = Vec::new();
    stream.extend(trade_frame(
        1_700_000_000_000_000_000,
        "BTCUSDT",
        65_000_12345678,
        150_000_000,
        0,
    ));
    stream.extend(trade_frame(
        1_700_000_000_000_100_000,
        "ETHUSDT",
        3_000_00000000,
        2_000_000_000,
        1,
    ));
    stream.extend([1u8, 0, 0]); // heartbeat frame: len=1, type=0
    stream.extend(trade_frame(
        1_700_000_000_000_200_000,
        "BTCUSDT",
        65_001_00000000,
        10_000_000,
        1,
    ));

    let layout = LengthPrefixed::new(Prefix::U16Le).payload_only();
    let mut framer = Framer::new(layout, 64 * 1024);
    let mut dec = TickDecoder {
        symbols: SymbolDict::new(),
    };
    let mut batch = EventBatch::with_capacity(1024);
    let (mut events, mut ignored) = (0usize, 0usize);
    for segment in stream.chunks(7) {
        framer.push(segment);
        while let Some(frame) = framer.next_frame() {
            match dec.decode(frame, 0, &mut batch) {
                Ok(n) => events += n,
                Err(DecodeError::Ignored) => ignored += 1,
                Err(e) => eprintln!("bad frame: {e}"),
            }
        }
        if let Some(e) = framer.error() {
            eprintln!("stream desynchronised: {e}; reconnect and reset()");
            break;
        }
    }
    println!(
        "frames={} events={events} ignored={ignored} symbols={}",
        framer.frames(),
        dec.symbols.len()
    );
    for t in &batch.trades {
        println!(
            "{:?} {} price={} size={} side={:?}",
            t.ts_ns,
            dec.symbols.name(t.symbol).unwrap_or("?"),
            t.price,
            t.size,
            t.side
        );
    }
    assert_eq!(events, 3);
}
