//! The three live tables fed by the Binance decoder, the per-symbol books and their
//! snapshot/diff synchronisation.

use std::collections::HashMap;

use ankaa_ingest::generic::{MapError, RowMapper};
use ankaa_ingest::{BookDelta, BookSnapshot, EventBatch, Side, SymbolId};
use ankaa_store::{ColumnType, Field, Schema, Table, Value};
use ankaa_ts::{BookSide, BookSync, L2Book, Level};

use crate::continuous::Continuous;

/// A user-defined table (`[[tables]]`).
pub struct UserTable {
    /// Name.
    pub name: String,
    /// Column that chooses the partition, if any.
    pub partition_by: Option<String>,
    /// Store.
    pub table: Table,
    /// One dictionary per `dict` column (shared by every source feeding the table).
    pub dicts: Vec<Option<ankaa_ingest::SymbolDict>>,
}

/// A message source bound to a user table (`[[sources]]`).
pub struct SourceBinding {
    /// Source name (metrics label).
    pub name: String,
    /// Index into `Tables::user`.
    pub table: usize,
    /// Message → row.
    pub mapper: RowMapper,
    /// CSV (true) or JSON (false) messages.
    pub csv: bool,
    /// Messages mapped.
    pub mapped: u64,
    /// Messages rejected.
    pub rejected: u64,
}

/// Why a user row older than its partition's newest row is refused.
const STALE: &str = "row rejected by the table (older than the newest row of its partition)";

/// A user row may be stamped this far ahead of its receive time; beyond it the row is rejected.
/// One far-future timestamp would otherwise raise the partition watermark and make every later
/// row "out of order" (the usual cause is a wrong `ts_unit`).
pub const MAX_FUTURE_NS: i64 = 24 * 3600 * 1_000_000_000;

/// `book_deltas`: `ts, symbol, side(u8), price, size, first_update_id, update_id`.
/// Snapshot levels are written with `first_update_id = 0` and `update_id = lastUpdateId`.
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

/// Snapshot levels waiting for an exchange timestamp: `(last_update_id, levels)`.
type PendingSnapshot = (i64, Vec<(Side, i64, i64)>);

/// One exchange message worth of deltas (same symbol and id range).
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
    /// Symbol ids that need a (new) snapshot fetched.
    pub need_snapshot: Vec<SymbolId>,
}

/// Live tables + books. Single writer (the ingest task).
pub struct Tables {
    /// Trades.
    pub trades: Table,
    /// L2 deltas as applied (buffered diffs are written when they are applied).
    pub deltas: Table,
    /// Top-of-book after each change.
    pub quotes: Table,
    books: HashMap<u32, L2Book>,
    sync: HashMap<u32, BookSync<DeltaMsg>>,
    expect_snapshots: bool,
    /// Exchange messages after which a book was crossed (data-quality signal; expected 0).
    pub crossed_messages: u64,
    /// Snapshot rows whose timestamp is not known yet: a REST snapshot has no exchange time, so
    /// its levels are written just before the first diff applied on top of it (exchange time
    /// domain, keeps partitions ordered).
    pending_snapshot_rows: HashMap<u32, PendingSnapshot>,
    /// Reusable row buffers (no per-message allocation).
    rows7: Vec<[Value; 7]>,
    rows6: Vec<[Value; 6]>,
    /// User-defined tables.
    pub user: Vec<UserTable>,
    /// Sources by id (index = source id; `None` = an id whose source is no longer configured).
    pub sources: Vec<Option<SourceBinding>>,
    /// Continuous queries fed from every append.
    pub continuous: Continuous,
    /// True while the WAL is replayed at start: rows that are already in a segment are skipped
    /// by their partition, quietly (they are not "rejected").
    pub replaying: bool,
}

impl Tables {
    /// `expect_snapshots = true` buffers depth diffs per symbol until a snapshot arrives (live
    /// mode); `false` applies diffs from a cold start (replay of captures without snapshots).
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
            user: Vec::new(),
            sources: Vec::new(),
            continuous: Continuous::default(),
            replaying: false,
        }
    }

    /// Add a user-defined table.
    pub fn add_user_table(
        &mut self,
        name: &str,
        schema: Schema,
        partition_by: Option<&str>,
        max_lateness_ns: i64,
    ) -> usize {
        self.user.push(UserTable {
            name: name.to_owned(),
            partition_by: partition_by.map(str::to_owned),
            dicts: RowMapper::dictionaries_for(&schema),
            table: Table::new(name, schema, 1).with_max_lateness(max_lateness_ns),
        });
        self.user.len() - 1
    }

    /// Bind the source with the given (stable) id to a user table.
    pub fn add_source(&mut self, id: u16, name: &str, table: usize, mapper: RowMapper, csv: bool) {
        let idx = usize::from(id);
        if self.sources.len() <= idx {
            self.sources.resize_with(idx + 1, || None);
        }
        self.sources[idx] = Some(SourceBinding {
            name: name.to_owned(),
            table,
            mapper,
            csv,
            mapped: 0,
            rejected: 0,
        });
    }

    /// Distinct values across the dictionaries of user tables.
    pub fn dictionary_values(&self) -> usize {
        self.user
            .iter()
            .flat_map(|u| u.dicts.iter().flatten())
            .map(ankaa_ingest::SymbolDict::len)
            .sum()
    }

    /// Column each table is partitioned by (table → column): `symbol` for the market tables,
    /// `partition_by` for user tables that have one.
    pub fn partition_columns(&self) -> HashMap<String, String> {
        let mut m: HashMap<String, String> = ["trades", "book_deltas", "quotes"]
            .into_iter()
            .map(|t| (t.to_owned(), "symbol".to_owned()))
            .collect();
        for u in &self.user {
            if let Some(c) = &u.partition_by {
                m.insert(u.name.clone(), c.clone());
            }
        }
        m
    }

    /// Allowed lateness of every table that has one (table → ns).
    pub fn lateness(&self) -> HashMap<String, i64> {
        self.user
            .iter()
            .filter(|u| u.table.max_lateness() > 0)
            .map(|u| (u.name.clone(), u.table.max_lateness()))
            .collect()
    }

    /// Schemas of every table by name (for continuous-query binding).
    pub fn schemas(&self) -> HashMap<String, Schema> {
        let mut m = HashMap::new();
        for (n, t) in self.all() {
            m.insert(n.to_owned(), t.schema().clone());
        }
        m
    }

    /// Map one message of a user source and store its rows (one, or one per element of the
    /// source's `rows` array). Rows are checked one by one: a refused row (bad value, out of its
    /// partition's lateness window, more than a day in the future) is counted and skipped, the
    /// others are stored. A message that is not valid at all stores nothing.
    pub fn ingest_user(&mut self, source_id: u16, msg: &[u8], recv_ns: i64) -> UserIngest {
        let mut out = UserIngest::default();
        let Some(src) = self
            .sources
            .get_mut(usize::from(source_id))
            .and_then(Option::as_mut)
        else {
            out.rejected = 1;
            out.error = Some(MapError::Malformed("unknown source id"));
            return out;
        };
        let csv = src.csv;
        let t = &mut self.user[src.table];
        // Parse first, decide, then intern: a refused row must not grow a dictionary (and with it
        // the partition list), or bad input could fill it up.
        let n = match src.mapper.parse(msg, recv_ns, csv, &t.dicts) {
            Ok(n) => n,
            Err(e) => {
                src.rejected += 1;
                out.rejected = 1;
                out.error = Some(e);
                return out;
            }
        };
        let listens = self.continuous.listens(&t.name);
        if n == 0 {
            return out;
        }
        // one lock for the whole message: the stale check, partition creation and the append
        // of every row happen under it (the dictionaries live outside the table)
        let mut w = t.table.writer();
        let lateness = t.table.max_lateness();
        for r in 0..n {
            let parsed = match src.mapper.parsed(r) {
                Ok(p) => p,
                Err(e) => {
                    out.reject(e);
                    continue;
                }
            };
            if parsed.ts > recv_ns.saturating_add(MAX_FUTURE_NS) {
                out.reject(MapError::Malformed(
                    "timestamp is more than a day ahead of the receive time (check ts_unit)",
                ));
                continue;
            }
            if !self.replaying
                && let Some(k) = parsed.key
                && w.partition_watermark(k as usize)
                    .is_some_and(|wm| parsed.ts < wm.saturating_sub(lateness))
            {
                out.reject(MapError::Malformed(STALE));
                continue;
            }
            let (key, row) = src.mapper.finish_row(r, &mut t.dicts);
            match w.append_row_stored(key, row) {
                Ok(stored) => {
                    out.rows += 1;
                    // rows skipped during recovery are already in a segment: they must not
                    // feed a continuous query a second time
                    if stored && listens {
                        self.continuous.on_row(&t.name, row);
                    }
                }
                Err(_) => out.reject(MapError::Malformed(STALE)),
            }
        }
        src.mapped += out.rows as u64;
        src.rejected += out.rejected as u64;
        out
    }

    /// Books (read-only view for metrics/tests).
    pub fn books(&self) -> &HashMap<u32, L2Book> {
        &self.books
    }

    /// True when order books are kept in step with exchange snapshots. Their rows can then
    /// only be regenerated from the log by replaying from the connection they belong to.
    pub fn book_sync_active(&self) -> bool {
        self.expect_snapshots && !self.sync.is_empty()
    }

    /// Exchange the bindings of two source ids (the log names sources differently than this
    /// node's configuration order did).
    pub fn swap_sources(&mut self, a: u16, b: u16) {
        let (a, b) = (usize::from(a), usize::from(b));
        let need = a.max(b) + 1;
        if self.sources.len() < need {
            self.sources.resize_with(need, || None);
        }
        self.sources.swap(a, b);
    }

    /// A new stream connection: every synced book must be re-based on a fresh snapshot.
    pub fn on_reconnect(&mut self) {
        for s in self.sync.values_mut() {
            s.expect_new_snapshot();
        }
    }

    /// WAL LSN of the frame being ingested next (0 = none). Rows from LSNs already covered by
    /// persisted segments are skipped by the partitions (recovery replay).
    pub fn set_lsn(&self, lsn: u64) {
        self.trades.set_current_lsn(lsn);
        self.deltas.set_current_lsn(lsn);
        self.quotes.set_current_lsn(lsn);
        for u in &self.user {
            u.table.set_current_lsn(lsn);
        }
    }

    /// The tables that hold ingested data (tiering, checkpoints, recovery): the market tables
    /// and the user tables. Continuous-query outputs are derived from these and rebuilt at
    /// start, so they are never written to segments.
    pub fn persistent(&self) -> Vec<(&str, &Table)> {
        let mut v: Vec<(&str, &Table)> = vec![
            ("trades", &self.trades),
            ("book_deltas", &self.deltas),
            ("quotes", &self.quotes),
        ];
        v.extend(self.user.iter().map(|u| (u.name.as_str(), &u.table)));
        v
    }

    /// Every queryable table with its name: [`Self::persistent`] plus continuous-query outputs.
    pub fn all(&self) -> Vec<(&str, &Table)> {
        let mut v = self.persistent();
        v.extend(
            self.continuous
                .queries()
                .iter()
                .map(|q| (q.out.name(), &q.out)),
        );
        v
    }

    /// Depth messages ignored as stale across symbols.
    pub fn stale_messages(&self) -> u64 {
        self.sync.values().map(|s| s.dropped_stale).sum()
    }

    /// Resynchronisations requested across symbols.
    pub fn resyncs(&self) -> u64 {
        self.sync.values().map(|s| s.resyncs).sum()
    }

    /// Ingest one decoded batch.
    pub fn ingest(&mut self, batch: &EventBatch) -> IngestOutcome {
        let mut out = IngestOutcome {
            events: batch.len(),
            ..IngestOutcome::default()
        };
        // trades: one lock per run of same-symbol rows, rows built in a reusable buffer
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
            let ok = if self.continuous.listens("trades") {
                // only rows the table accepted reach the continuous queries
                let cq = &mut self.continuous;
                self.trades
                    .append_row_arrays_with(part, &self.rows6, |r| cq.on_row("trades", r))
                    .0
            } else {
                self.trades.append_row_arrays(part, &self.rows6).0
            };
            out.dropped += (j - i) - ok;
            i = j;
        }
        for sn in &batch.snapshots {
            self.on_snapshot(sn, &mut out);
        }
        // group deltas into exchange messages (same symbol + id range, contiguous)
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
            // written with the exchange time of the first diff applied on top (see field doc)
            self.pending_snapshot_rows
                .insert(sn.symbol.0, (sn.last_update_id, rows));
        }
        for msg in decision.apply {
            self.apply_msg(sn.symbol, &msg, out);
        }
        if decision.need_snapshot {
            out.need_snapshot.push(sn.symbol);
        }
    }

    /// Write pending snapshot levels (and the resulting top of book) at `ts_ns`.
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
        if decision.need_snapshot {
            out.need_snapshot.push(symbol);
        }
    }

    /// Apply one exchange message atomically, then sample the top of book once.
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
        // one lock for all levels of the message; rows built in a reusable buffer
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
            let part = self.quotes.partition_for(symbol.0);
            let ok = if self.continuous.listens("quotes") {
                let cq = &mut self.continuous;
                self.quotes
                    .append_row_arrays_with(part, &[q], |r| cq.on_row("quotes", r))
                    .0
            } else {
                self.quotes.append_rows(part, &[&q]).0
            };
            out.dropped += 1 - ok;
        }
    }

    /// Seal the open chunks of the persistent tables (shutdown, before the final flush).
    pub fn seal_all(&self) {
        for (_, t) in self.persistent() {
            t.seal_all();
        }
    }
}

/// What one message of a user source did.
#[derive(Debug, Default)]
pub struct UserIngest {
    /// Rows stored.
    pub rows: usize,
    /// Rows (or the whole message) refused.
    pub rejected: usize,
    /// The first reason a row or the message was refused.
    pub error: Option<MapError>,
}

impl UserIngest {
    fn reject(&mut self, e: MapError) {
        self.rejected += 1;
        if self.error.is_none() {
            self.error = Some(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankaa_ingest::BookDelta;

    fn delta(sym: u32, first: i64, last: i64, side: Side, price: i64, qty: i64) -> BookDelta {
        BookDelta {
            ts_ns: last,
            recv_ns: last,
            symbol: SymbolId(sym),
            side,
            price,
            size: qty,
            first_update_id: first,
            update_id: last,
        }
    }

    #[test]
    fn live_mode_buffers_until_snapshot_then_aligns() {
        let mut tables = Tables::new(true);
        let mut diffs = EventBatch::default();
        diffs.deltas.push(delta(0, 100, 105, Side::Bid, 10, 1));
        diffs.deltas.push(delta(0, 106, 110, Side::Ask, 12, 1));
        let out = tables.ingest(&diffs);
        assert_eq!(out.events, 2);
        assert!(
            tables.books().is_empty(),
            "nothing applied before the snapshot"
        );
        let mut snap = EventBatch::default();
        snap.snapshots.push(BookSnapshot {
            ts_ns: 107,
            recv_ns: 107,
            symbol: SymbolId(0),
            last_update_id: 107,
            bids: vec![(9, 5)],
            asks: vec![(13, 5)],
        });
        let out = tables.ingest(&snap);
        assert_eq!(out.need_snapshot, Vec::<SymbolId>::new());
        let top = tables.books()[&0].top();
        assert_eq!(top.bid.unwrap().price, 9); // bid 10 from the stale message was dropped
        assert_eq!(top.ask.unwrap().price, 12); // ask 12 from message 106..110 applied on top
        assert_eq!(tables.quotes.stats().rows, 2); // after snapshot, after the aligned diff
        // a gap requests a new snapshot
        let mut gap = EventBatch::default();
        gap.deltas.push(delta(0, 120, 121, Side::Bid, 11, 1));
        let out = tables.ingest(&gap);
        assert_eq!(out.need_snapshot, vec![SymbolId(0)]);
        assert_eq!(tables.resyncs(), 1);
    }

    #[test]
    fn cold_mode_applies_immediately_and_samples_once_per_message() {
        let mut t = Tables::new(false);
        let mut b = EventBatch::default();
        // one message: new best bid above the old ask, and the old ask deleted
        b.deltas.push(delta(0, 1, 1, Side::Ask, 100, 1));
        b.deltas.push(delta(0, 2, 2, Side::Bid, 99, 1));
        b.deltas.push(delta(0, 3, 3, Side::Bid, 101, 1));
        b.deltas.push(delta(0, 3, 3, Side::Ask, 100, 0));
        b.deltas.push(delta(0, 3, 3, Side::Ask, 102, 1));
        t.ingest(&b);
        let top = t.books()[&0].top();
        assert_eq!((top.bid.unwrap().price, top.ask.unwrap().price), (101, 102));
        assert_eq!(
            t.crossed_messages, 0,
            "no crossed state after a whole message"
        );
        assert_eq!(
            t.quotes.stats().rows,
            2,
            "one quote per message that changed the top"
        );
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
mod snapshot_consistency {
    //! F-09: books rebuilt from snapshot + diffs equal the next exchange snapshot (checksum),
    //! including when a second snapshot arrives while diffs keep flowing.
    use super::*;
    use ankaa_ingest::{BookDelta, BookSnapshot};

    fn delta(sym: u32, first: i64, last: i64, side: Side, price: i64, qty: i64) -> BookDelta {
        BookDelta {
            ts_ns: last,
            recv_ns: last,
            symbol: SymbolId(sym),
            side,
            price,
            size: qty,
            first_update_id: first,
            update_id: last,
        }
    }

    /// Reference book: apply the same absolute-level semantics with plain maps.
    #[derive(Default)]
    struct RefBook {
        bids: std::collections::BTreeMap<i64, i64>,
        asks: std::collections::BTreeMap<i64, i64>,
    }

    impl RefBook {
        fn load(&mut self, s: &BookSnapshot) {
            self.bids = s.bids.iter().copied().collect();
            self.asks = s.asks.iter().copied().collect();
        }
        fn apply(&mut self, side: Side, price: i64, qty: i64) {
            let m = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if qty == 0 {
                m.remove(&price);
            } else {
                m.insert(price, qty);
            }
        }
        fn checksum(&self) -> u64 {
            let mut h = 0xcbf2_9ce4_8422_2325u64;
            for (p, q) in self.bids.iter().rev().chain(self.asks.iter()) {
                for b in p.to_le_bytes().into_iter().chain(q.to_le_bytes()) {
                    h ^= u64::from(b);
                    h = h.wrapping_mul(0x0100_0000_01b3);
                }
            }
            h
        }
    }

    fn book_checksum(book: &L2Book) -> u64 {
        let mut r = RefBook::default();
        for l in book.levels(BookSide::Bid, usize::MAX) {
            r.bids.insert(l.price, l.size);
        }
        for l in book.levels(BookSide::Ask, usize::MAX) {
            r.asks.insert(l.price, l.size);
        }
        r.checksum()
    }

    #[test]
    fn successive_snapshots_match_reference_checksums() {
        let mut tables = Tables::new(true);
        let mut reference = RefBook::default();
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let mut id = 1_000i64;
        // snapshot 1
        let snap1 = BookSnapshot {
            ts_ns: 1,
            recv_ns: 1,
            symbol: SymbolId(0),
            last_update_id: id,
            bids: (0..20).map(|i| (100_000 - i * 10, 5 + i)).collect(),
            asks: (0..20).map(|i| (100_010 + i * 10, 7 + i)).collect(),
        };
        let mut b = EventBatch::with_capacity(64);
        b.snapshots.push(snap1.clone());
        tables.ingest(&b);
        reference.load(&snap1);
        // 300 diff messages straddling / after the snapshot, each 1..4 levels
        for _ in 0..300 {
            let first = id + 1;
            let n = 1 + (next() % 4) as i64;
            id += n;
            b.clear();
            for k in 0..n {
                let side = if next() % 2 == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                };
                let price = match side {
                    Side::Bid => 100_000 - (next() % 25) as i64 * 10,
                    Side::Ask => 100_010 + (next() % 25) as i64 * 10,
                };
                let qty = if next() % 5 == 0 {
                    0
                } else {
                    1 + (next() % 50) as i64
                };
                b.deltas.push(delta(0, first, id, side, price, qty));
                let _ = k;
                reference.apply(side, price, qty);
            }
            tables.ingest(&b);
            assert_eq!(book_checksum(&tables.books()[&0]), reference.checksum());
        }
        // snapshot 2 arrives (exchange re-snapshot), followed by more diffs
        let snap2 = BookSnapshot {
            ts_ns: 2,
            recv_ns: 2,
            symbol: SymbolId(0),
            last_update_id: id,
            bids: reference.bids.iter().map(|(&p, &q)| (p, q)).collect(),
            asks: reference.asks.iter().map(|(&p, &q)| (p, q)).collect(),
        };
        tables.on_reconnect();
        b.clear();
        b.snapshots.push(snap2.clone());
        tables.ingest(&b);
        reference.load(&snap2);
        assert_eq!(book_checksum(&tables.books()[&0]), reference.checksum());
        for _ in 0..100 {
            let first = id + 1;
            id += 1;
            b.clear();
            let price = 100_000 - (next() % 25) as i64 * 10;
            let qty = (next() % 30) as i64;
            b.deltas.push(delta(0, first, id, Side::Bid, price, qty));
            reference.apply(Side::Bid, price, qty);
            tables.ingest(&b);
            assert_eq!(book_checksum(&tables.books()[&0]), reference.checksum());
        }
        assert_eq!(tables.resyncs(), 0);
        assert_eq!(tables.crossed_messages, 0);
    }
}
