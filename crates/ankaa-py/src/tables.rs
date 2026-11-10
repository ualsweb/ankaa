//! `trades`, `book_deltas` and `quotes` tables fed by the Binance decoders.
//!
//! Trimmed copy of `bins/ankaad/src/tables.rs` (binaries are not libraries). Keep the two in
//! sync when the table schemas change.

use std::collections::HashMap;

use ankaa_ingest::{BookDelta, BookSnapshot, EventBatch, Side, SymbolId};
use ankaa_store::{ColumnType, Field, Schema, Table, Value};
use ankaa_ts::{BookSide, BookSync, L2Book, Level};

/// `book_deltas`: `ts, symbol, side(u8), price, size, first_update_id, update_id`.
pub fn book_delta_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("symbol", ColumnType::DictU32),
        Field::new("side", ColumnType::U8),
        Field::new("price", ColumnType::FixedPoint { scale: 8 }),
        Field::new("size", ColumnType::FixedPoint { scale: 8 }),
        Field::new("first_update_id", ColumnType::Int64),
        Field::new("update_id", ColumnType::Int64),
    ])
    .expect("static schema")
}

/// `quotes`: `ts, symbol, bid, bid_size, ask, ask_size, update_id` (top of book after each change).
pub fn quotes_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("symbol", ColumnType::DictU32),
        Field::new("bid", ColumnType::FixedPoint { scale: 8 }),
        Field::new("bid_size", ColumnType::FixedPoint { scale: 8 }),
        Field::new("ask", ColumnType::FixedPoint { scale: 8 }),
        Field::new("ask_size", ColumnType::FixedPoint { scale: 8 }),
        Field::new("update_id", ColumnType::Int64),
    ])
    .expect("static schema")
}

type PendingSnapshot = (i64, Vec<(Side, i64, i64)>);

#[derive(Debug, Clone)]
struct DeltaMsg {
    ts_ns: i64,
    first_id: i64,
    last_id: i64,
    levels: Vec<(Side, i64, i64)>,
}

/// Result of ingesting one batch.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    /// Events seen (trades + deltas + snapshots).
    pub events: usize,
    /// Rows dropped by appenders.
    pub dropped: usize,
}

/// In-memory tables + per-symbol books. Single writer.
pub struct Tables {
    pub trades: Table,
    pub deltas: Table,
    pub quotes: Table,
    books: HashMap<u32, L2Book>,
    sync: HashMap<u32, BookSync<DeltaMsg>>,
    expect_snapshots: bool,
    pub crossed_messages: u64,
    pending_snapshot_rows: HashMap<u32, PendingSnapshot>,
    rows7: Vec<[Value; 7]>,
    rows6: Vec<[Value; 6]>,
}

impl Tables {
    /// `expect_snapshots = true` buffers depth diffs until a snapshot frame arrives; `false`
    /// applies diffs from a cold start (captures recorded without snapshots).
    pub fn new(expect_snapshots: bool) -> Self {
        Self {
            trades: Table::new("trades", Schema::trades(), 1),
            deltas: Table::new("book_deltas", book_delta_schema(), 1),
            quotes: Table::new("quotes", quotes_schema(), 1),
            books: HashMap::new(),
            sync: HashMap::new(),
            expect_snapshots,
            crossed_messages: 0,
            pending_snapshot_rows: HashMap::new(),
            rows7: Vec::new(),
            rows6: Vec::new(),
        }
    }

    pub fn resyncs(&self) -> u64 {
        self.sync.values().map(|s| s.resyncs).sum()
    }

    /// Ingest one decoded batch.
    pub fn ingest(&mut self, batch: &EventBatch) -> IngestOutcome {
        let mut out = IngestOutcome {
            events: batch.len(),
            ..IngestOutcome::default()
        };
        let mut i = 0;
        while i < batch.trades.len() {
            let sym = batch.trades[i].symbol;
            let mut j = i;
            while j < batch.trades.len() && batch.trades[j].symbol == sym {
                j += 1;
            }
            self.rows6.clear();
            self.rows6.extend(batch.trades[i..j].iter().map(|t| {
                [
                    Value::I64(t.ts_ns),
                    Value::U32(t.symbol.0),
                    Value::I64(t.price),
                    Value::I64(t.size),
                    Value::U8(t.side as u8),
                    Value::I64(t.seq),
                ]
            }));
            let part = self.trades.partition_for(sym.0);
            let (ok, _) = self.trades.append_row_arrays(part, &self.rows6);
            out.dropped += (j - i) - ok;
            i = j;
        }
        for sn in &batch.snapshots {
            self.on_snapshot(sn, &mut out);
        }
        let mut i = 0;
        while i < batch.deltas.len() {
            let d0 = &batch.deltas[i];
            let mut msg = DeltaMsg {
                ts_ns: d0.ts_ns,
                first_id: d0.first_update_id,
                last_id: d0.update_id,
                levels: Vec::new(),
            };
            let mut j = i;
            while j < batch.deltas.len()
                && batch.deltas[j].symbol == d0.symbol
                && batch.deltas[j].update_id == d0.update_id
            {
                let d: &BookDelta = &batch.deltas[j];
                msg.levels.push((d.side, d.price, d.size));
                j += 1;
            }
            self.on_diff(d0.symbol, msg, &mut out);
            i = j;
        }
        out
    }

    fn sync_for(&mut self, symbol: SymbolId) -> &mut BookSync<DeltaMsg> {
        let expect = self.expect_snapshots;
        self.sync
            .entry(symbol.0)
            .or_insert_with(|| BookSync::new(expect))
    }

    fn on_snapshot(&mut self, sn: &BookSnapshot, out: &mut IngestOutcome) {
        let decision = self.sync_for(sn.symbol).on_snapshot(sn.last_update_id);
        if decision.load_snapshot {
            let bids: Vec<Level> = sn
                .bids
                .iter()
                .map(|&(price, size)| Level { price, size })
                .collect();
            let asks: Vec<Level> = sn
                .asks
                .iter()
                .map(|&(price, size)| Level { price, size })
                .collect();
            let book = self.books.entry(sn.symbol.0).or_default();
            book.load_snapshot(&bids, &asks, sn.last_update_id);
            let mut rows: Vec<(Side, i64, i64)> = Vec::with_capacity(sn.bids.len() + sn.asks.len());
            rows.extend(sn.bids.iter().map(|&(p, q)| (Side::Bid, p, q)));
            rows.extend(sn.asks.iter().map(|&(p, q)| (Side::Ask, p, q)));
            self.pending_snapshot_rows
                .insert(sn.symbol.0, (sn.last_update_id, rows));
        }
        for msg in decision.apply {
            self.apply_msg(sn.symbol, &msg, out);
        }
    }

    fn flush_snapshot_rows(&mut self, symbol: SymbolId, ts_ns: i64, out: &mut IngestOutcome) {
        let Some((snapshot_id, rows)) = self.pending_snapshot_rows.remove(&symbol.0) else {
            return;
        };
        for (side, price, size) in rows {
            let row = [
                Value::I64(ts_ns),
                Value::U32(symbol.0),
                Value::U8(side as u8),
                Value::I64(price),
                Value::I64(size),
                Value::I64(0),
                Value::I64(snapshot_id),
            ];
            let (ok, _) = self
                .deltas
                .append_rows(self.deltas.partition_for(symbol.0), &[&row]);
            out.dropped += 1 - ok;
        }
        self.emit_quote(symbol, ts_ns, snapshot_id, out);
    }

    fn on_diff(&mut self, symbol: SymbolId, msg: DeltaMsg, out: &mut IngestOutcome) {
        let decision = self
            .sync_for(symbol)
            .on_diff(msg.first_id, msg.last_id, msg);
        for m in decision.apply {
            self.apply_msg(symbol, &m, out);
        }
    }

    fn apply_msg(&mut self, symbol: SymbolId, msg: &DeltaMsg, out: &mut IngestOutcome) {
        self.flush_snapshot_rows(symbol, msg.ts_ns.saturating_sub(1), out);
        let book = self.books.entry(symbol.0).or_default();
        let top_before = book.top();
        for &(side, price, size) in &msg.levels {
            let bs = match side {
                Side::Bid => BookSide::Bid,
                Side::Ask => BookSide::Ask,
            };
            book.apply(bs, price, size, msg.last_id);
        }
        self.rows7.clear();
        self.rows7
            .extend(msg.levels.iter().map(|&(side, price, size)| {
                [
                    Value::I64(msg.ts_ns),
                    Value::U32(symbol.0),
                    Value::U8(side as u8),
                    Value::I64(price),
                    Value::I64(size),
                    Value::I64(msg.first_id),
                    Value::I64(msg.last_id),
                ]
            }));
        let part = self.deltas.partition_for(symbol.0);
        let (ok, _) = self.deltas.append_row_arrays(part, &self.rows7);
        out.dropped += msg.levels.len() - ok;
        let book = &self.books[&symbol.0];
        if book.is_crossed() {
            self.crossed_messages += 1;
        }
        if book.top() != top_before {
            self.emit_quote(symbol, msg.ts_ns, msg.last_id, out);
        }
    }

    fn emit_quote(
        &mut self,
        symbol: SymbolId,
        ts_ns: i64,
        update_id: i64,
        out: &mut IngestOutcome,
    ) {
        let Some(top) = self.books.get(&symbol.0).map(L2Book::top) else {
            return;
        };
        if let (Some(b), Some(a)) = (top.bid, top.ask) {
            let q = [
                Value::I64(ts_ns),
                Value::U32(symbol.0),
                Value::I64(b.price),
                Value::I64(b.size),
                Value::I64(a.price),
                Value::I64(a.size),
                Value::I64(update_id),
            ];
            let (ok, _) = self
                .quotes
                .append_rows(self.quotes.partition_for(symbol.0), &[&q]);
            out.dropped += 1 - ok;
        }
    }

    /// Seal all open chunks.
    pub fn seal_all(&self) {
        self.trades.seal_all();
        self.deltas.seal_all();
        self.quotes.seal_all();
    }
}
