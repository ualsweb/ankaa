//! Columnar event batches produced by decoders and consumed by appenders.

use ankaa_store::{Chunk, Value, chunk::AppendError};

use crate::symbols::SymbolId;

/// Aggressor / book side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Side {
    /// Bid / buyer-initiated.
    Bid = 0,
    /// Ask / seller-initiated.
    Ask = 1,
}

/// One trade print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    /// Exchange event time, ns.
    pub ts_ns: i64,
    /// Host receive time, ns (for latency accounting; not stored in the trades table yet).
    pub recv_ns: i64,
    /// Symbol.
    pub symbol: SymbolId,
    /// Price in ticks (scale 8).
    pub price: i64,
    /// Quantity in ticks (scale 8).
    pub size: i64,
    /// Aggressor side.
    pub side: Side,
    /// Exchange trade id / sequence.
    pub seq: i64,
}

/// One L2 book level update (absolute quantity; 0 removes the level).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookDelta {
    /// Exchange event time, ns.
    pub ts_ns: i64,
    /// Host receive time, ns.
    pub recv_ns: i64,
    /// Symbol.
    pub symbol: SymbolId,
    /// Side of the book.
    pub side: Side,
    /// Price level in ticks (scale 8).
    pub price: i64,
    /// New absolute quantity in ticks (scale 8); 0 = delete level.
    pub size: i64,
    /// First update id of the exchange message that carried this delta (`U` on Binance).
    pub first_update_id: i64,
    /// Last update id of that message (`u` on Binance).
    pub update_id: i64,
}

/// A full book snapshot (REST or exchange snapshot message): absolute levels for both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    /// Snapshot time (exchange time when provided, else host receive time), ns.
    pub ts_ns: i64,
    /// Host receive time, ns.
    pub recv_ns: i64,
    /// Symbol.
    pub symbol: SymbolId,
    /// Exchange update id the snapshot corresponds to (`lastUpdateId` on Binance).
    pub last_update_id: i64,
    /// Bid levels `(price, size)` in ticks, best first not required.
    pub bids: Vec<(i64, i64)>,
    /// Ask levels `(price, size)` in ticks.
    pub asks: Vec<(i64, i64)>,
}

/// A batch of decoded events (struct-of-arrays would be the final form; this is the P1-02
/// shape, sized so one batch never exceeds one 1,024-row vector per table).
#[derive(Debug, Default, Clone)]
pub struct EventBatch {
    /// Trades.
    pub trades: Vec<Trade>,
    /// Book deltas.
    pub deltas: Vec<BookDelta>,
    /// Book snapshots (rare; one per symbol per (re)sync).
    pub snapshots: Vec<BookSnapshot>,
}

impl EventBatch {
    /// Empty batch with reserved capacity.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            trades: Vec::with_capacity(n),
            deltas: Vec::with_capacity(n),
            snapshots: Vec::new(),
        }
    }

    /// Total events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.trades.len() + self.deltas.len() + self.snapshots.len()
    }

    /// True when no events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear without freeing.
    pub fn clear(&mut self) {
        self.trades.clear();
        self.deltas.clear();
        self.snapshots.clear();
    }

    /// Append all trades into a chunk with the canonical `Schema::trades()` layout.
    /// Stops at the first error and returns how many were appended.
    pub fn append_trades(&self, chunk: &mut Chunk) -> (usize, Option<AppendError>) {
        let mut n = 0;
        for t in &self.trades {
            let row = [
                Value::I64(t.ts_ns),
                Value::U32(t.symbol.0),
                Value::I64(t.price),
                Value::I64(t.size),
                Value::U8(t.side as u8),
                Value::I64(t.seq),
            ];
            if let Err(e) = chunk.append_row(&row) {
                return (n, Some(e));
            }
            n += 1;
        }
        (n, None)
    }
}
