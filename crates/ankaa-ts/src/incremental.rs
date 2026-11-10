//! Incrementally maintained operators for continuous queries: state is updated per arriving
//! row and the result is materialised as rows, so dashboards read a table instead of polling a
//! query over all history.
//!
//! Late data policy (documented, no retraction yet): a row older than the current open bucket
//! of its key is counted in `late_rows` and dropped. Out-of-order rows inside the open bucket are
//! accepted (they cannot change a closed bar).

use std::collections::BTreeMap;

use crate::bucket::Ohlcv;
use crate::sketch::UddSketch;
use crate::sliding::{Sliding, join_bars};

/// Start of the `width`-wide bucket holding `ts`; `None` when it is not representable (a
/// timestamp within one bucket of `i64::MIN`), which callers count as a late row.
fn bucket_start(ts: i64, width: i64) -> Option<i64> {
    ts.div_euclid(width).checked_mul(width)
}

/// A bar being filled, with the timestamps its open and close prices came from: with late
/// rows a bar's first and last rows by time need not be the first and last to arrive.
#[derive(Debug, Clone, Copy)]
struct OpenBar {
    bar: Ohlcv,
    open_ts: i64,
    close_ts: i64,
}

/// One key's open buckets (by start) and the newest timestamp seen.
#[derive(Debug, Default)]
struct KeyBars {
    newest: Option<i64>,
    open: BTreeMap<i64, OpenBar>,
}

/// Per-key streaming OHLCV in event time. A bucket closes when the newest timestamp of its key
/// minus the allowed lateness has passed its end; until then a row of it may still arrive, out
/// of order, and is placed by its timestamp (`open` is the earliest row's price, `close` the
/// latest's; equal timestamps in arrival order). A row whose bucket has closed is late:
/// counted and dropped. With no lateness a bucket closes when a row of a later bucket
/// arrives. The bars are the same whatever order rows within the lateness arrive in.
#[derive(Debug)]
pub struct IncrementalOhlcv {
    width: i64,
    lateness: i64,
    keys: BTreeMap<u32, KeyBars>,
    /// Rows whose bucket had closed.
    pub late_rows: u64,
}

impl IncrementalOhlcv {
    /// Bars of `width_ns` aligned to the epoch, rows in time order.
    #[must_use]
    pub fn new(width_ns: i64) -> Self {
        Self {
            width: width_ns.max(1),
            lateness: 0,
            keys: BTreeMap::new(),
            late_rows: 0,
        }
    }

    /// Accept rows up to `ns` older than the newest of their key (buckets close that much
    /// later).
    pub fn set_lateness(&mut self, ns: i64) {
        self.lateness = ns.max(0);
    }

    /// Add one trade; appends to `out` every `(key, bar)` that closed because of it, oldest
    /// first.
    pub fn push(&mut self, key: u32, ts: i64, price: i64, size: i64, out: &mut Vec<(u32, Ohlcv)>) {
        let Some(start) = bucket_start(ts, self.width) else {
            self.late_rows += 1;
            return;
        };
        let (width, lateness) = (self.width, self.lateness);
        let k = self.keys.entry(key).or_default();
        if let Some(newest) = k.newest
            && start.saturating_add(width) <= newest.saturating_sub(lateness)
        {
            self.late_rows += 1;
            return;
        }
        let notional = i128::from(price) * i128::from(size);
        match k.open.get_mut(&start) {
            Some(cur) => {
                let b = &mut cur.bar;
                b.high = b.high.max(price);
                b.low = b.low.min(price);
                b.volume = b.volume.saturating_add(size);
                b.notional = b.notional.saturating_add(notional);
                b.count = b.count.saturating_add(1);
                if ts < cur.open_ts {
                    cur.open_ts = ts;
                    b.open = price;
                }
                if ts >= cur.close_ts {
                    cur.close_ts = ts;
                    b.close = price;
                }
            }
            None => {
                k.open.insert(
                    start,
                    OpenBar {
                        bar: Ohlcv {
                            start_ns: start,
                            open: price,
                            high: price,
                            low: price,
                            close: price,
                            volume: size,
                            notional,
                            count: 1,
                        },
                        open_ts: ts,
                        close_ts: ts,
                    },
                );
            }
        }
        let newest = k.newest.map_or(ts, |n| n.max(ts));
        k.newest = Some(newest);
        let limit = newest.saturating_sub(lateness);
        while let Some(entry) = k.open.first_entry() {
            if entry.key().saturating_add(width) > limit {
                break;
            }
            out.push((key, entry.remove().bar));
        }
    }

    /// Open bars (the live rows of the output table), by key, oldest first.
    pub fn open_bars(&self) -> impl Iterator<Item = (u32, &Ohlcv)> {
        self.keys
            .iter()
            .flat_map(|(k, kb)| kb.open.values().map(move |o| (*k, &o.bar)))
    }

    /// Open bars of `key`, oldest first.
    pub fn open_bars_of(&self, key: u32) -> impl Iterator<Item = &Ohlcv> {
        self.keys
            .get(&key)
            .into_iter()
            .flat_map(|kb| kb.open.values().map(|o| &o.bar))
    }
}

type Panes = Sliding<Ohlcv, fn(&Ohlcv, &Ohlcv) -> Ohlcv>;

/// One key's trailing window: closed step bars ("panes") and the next window end to emit.
#[derive(Debug)]
struct KeyWindow {
    panes: Panes,
    next_end: i64,
}

/// Per-key OHLCV over a trailing window, emitted every step: the streaming form of
/// [`crate::rolling_bars`], with the same rows. Rows are bucketed into panes of one step; when a
/// pane closes it joins a sliding queue, and every step end whose window holds rows yields one
/// bar (`start_ns` = the window's exclusive end).
#[derive(Debug)]
pub struct IncrementalRolling {
    bars: IncrementalOhlcv,
    window: i64,
    step: i64,
    keys: BTreeMap<u32, KeyWindow>,
}

impl IncrementalRolling {
    /// Windows of `window_ns` every `step_ns` (`window_ns` a positive multiple of `step_ns`).
    #[must_use]
    pub fn new(window_ns: i64, step_ns: i64) -> Self {
        let step = step_ns.max(1);
        Self {
            bars: IncrementalOhlcv::new(step),
            window: window_ns.max(step),
            step,
            keys: BTreeMap::new(),
        }
    }

    /// Rows older than the open panes of their key allow, dropped.
    #[must_use]
    pub fn late_rows(&self) -> u64 {
        self.bars.late_rows
    }

    /// Accept rows up to `ns` older than the newest of their key (panes close that much later).
    pub fn set_lateness(&mut self, ns: i64) {
        self.bars.set_lateness(ns);
    }

    /// Add one trade; appends to `out` the `(key, bar)` of every window that is complete now.
    pub fn push(&mut self, key: u32, ts: i64, price: i64, size: i64, out: &mut Vec<(u32, Ohlcv)>) {
        let mut closed = Vec::new();
        self.bars.push(key, ts, price, size, &mut closed);
        if closed.is_empty() {
            return;
        }
        let first_open = self.bars.open_bars_of(key).next().map(|o| o.start_ns);
        let (window, step) = (self.window, self.step);
        let kw = self.keys.entry(key).or_insert_with(|| KeyWindow {
            panes: Sliding::new(join_bars as fn(&Ohlcv, &Ohlcv) -> Ohlcv),
            next_end: closed[0].1.start_ns.saturating_add(step),
        });
        let emit = |kw: &mut KeyWindow, end: i64, out: &mut Vec<(u32, Ohlcv)>| -> bool {
            while kw
                .panes
                .oldest()
                .is_some_and(|p| p.start_ns < end.saturating_sub(window))
            {
                kw.panes.pop();
            }
            match kw.panes.total() {
                Some(mut bar) => {
                    bar.start_ns = end;
                    out.push((key, bar));
                    true
                }
                None => false,
            }
        };
        for (i, (_, pane)) in closed.iter().enumerate() {
            // windows that end before this pane (steps without rows, nothing new in them)
            while kw.next_end <= pane.start_ns {
                if !emit(kw, kw.next_end, out) {
                    break;
                }
                kw.next_end = kw.next_end.saturating_add(step);
            }
            kw.panes.push(*pane);
            kw.next_end = pane.start_ns.saturating_add(step);
            emit(kw, kw.next_end, out);
            kw.next_end = kw.next_end.saturating_add(step);
            // windows that end between this pane and the next one (closed or still open),
            // while they hold rows
            let until = closed
                .get(i + 1)
                .map(|(_, p)| p.start_ns)
                .or(first_open)
                .unwrap_or(i64::MAX);
            while kw.next_end <= until {
                if !emit(kw, kw.next_end, out) {
                    break;
                }
                kw.next_end = kw.next_end.saturating_add(step);
            }
        }
        if let Some(open) = first_open {
            kw.next_end = open.saturating_add(step);
            let from = kw.next_end.saturating_sub(window);
            while kw.panes.oldest().is_some_and(|p| p.start_ns < from) {
                kw.panes.pop();
            }
        }
    }

    /// Per key, the window that ends with its newest open pane, rows so far (`start_ns` = its
    /// end).
    #[must_use]
    pub fn open_rows(&self) -> Vec<(u32, Ohlcv)> {
        let mut out = Vec::new();
        for (key, kb) in &self.bars.keys {
            let Some(newest) = kb.open.values().next_back() else {
                continue;
            };
            let end = newest.bar.start_ns.saturating_add(self.step);
            let from = end.saturating_sub(self.window);
            let mut bar: Option<Ohlcv> = self.keys.get(key).and_then(|kw| {
                let mut panes = kw.panes.clone();
                while panes.oldest().is_some_and(|p| p.start_ns < from) {
                    panes.pop();
                }
                panes.total()
            });
            for open in kb.open.values().filter(|o| o.bar.start_ns >= from) {
                bar = Some(match bar {
                    Some(b) => join_bars(&b, &open.bar),
                    None => open.bar,
                });
            }
            if let Some(mut b) = bar {
                b.start_ns = end;
                out.push((*key, b));
            }
        }
        out
    }
}

/// One closed window of quantiles.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantileRow {
    /// Key.
    pub key: u32,
    /// Window start, ns.
    pub start_ns: i64,
    /// Sample count.
    pub count: u64,
    /// Requested quantiles, in the order given at construction.
    pub values: Vec<f64>,
    /// Minimum and maximum observed.
    pub min: f64,
    /// Maximum observed.
    pub max: f64,
}

/// One key's open windows (by start) and the newest timestamp seen.
#[derive(Debug, Default)]
struct KeyWindows {
    newest: Option<i64>,
    open: BTreeMap<i64, (UddSketch, f64, f64)>,
}

/// Per-key tumbling-window quantiles with a `UddSketch` per open window, in event time: a
/// window closes when the newest timestamp of its key minus the allowed lateness has passed its
/// end (see [`IncrementalOhlcv`]).
#[derive(Debug)]
pub struct IncrementalPercentiles {
    width: i64,
    lateness: i64,
    quantiles: Vec<f64>,
    alpha: f64,
    keys: BTreeMap<u32, KeyWindows>,
    /// Rows whose window had closed.
    pub late_rows: u64,
}

impl IncrementalPercentiles {
    /// Windows of `width_ns`; `quantiles` in `[0, 1]`; `alpha` relative error (e.g. 0.01).
    #[must_use]
    pub fn new(width_ns: i64, quantiles: Vec<f64>, alpha: f64) -> Self {
        Self {
            width: width_ns.max(1),
            lateness: 0,
            quantiles,
            alpha,
            keys: BTreeMap::new(),
            late_rows: 0,
        }
    }

    /// Accept rows up to `ns` older than the newest of their key.
    pub fn set_lateness(&mut self, ns: i64) {
        self.lateness = ns.max(0);
    }

    fn row_of(
        quantiles: &[f64],
        key: u32,
        start: i64,
        s: &UddSketch,
        min: f64,
        max: f64,
    ) -> QuantileRow {
        QuantileRow {
            key,
            start_ns: start,
            count: s.count(),
            values: quantiles
                .iter()
                .map(|&q| s.quantile(q).map_or(f64::NAN, |v| v.clamp(min, max)))
                .collect(),
            min,
            max,
        }
    }

    /// Add one sample; appends to `out` every window that closed because of it, oldest first.
    pub fn push(&mut self, key: u32, ts: i64, value: f64, out: &mut Vec<QuantileRow>) {
        if !value.is_finite() {
            return;
        }
        let Some(start) = bucket_start(ts, self.width) else {
            self.late_rows += 1;
            return;
        };
        let (width, lateness, alpha) = (self.width, self.lateness, self.alpha);
        let k = self.keys.entry(key).or_default();
        if let Some(newest) = k.newest
            && start.saturating_add(width) <= newest.saturating_sub(lateness)
        {
            self.late_rows += 1;
            return;
        }
        let (s, min, max) = k
            .open
            .entry(start)
            .or_insert_with(|| (UddSketch::new(alpha, 2048), value, value));
        s.add(value);
        *min = min.min(value);
        *max = max.max(value);
        let newest = k.newest.map_or(ts, |n| n.max(ts));
        k.newest = Some(newest);
        let limit = newest.saturating_sub(lateness);
        while let Some(entry) = k.open.first_entry() {
            if entry.key().saturating_add(width) > limit {
                break;
            }
            let start = *entry.key();
            let (s, min, max) = entry.remove();
            out.push(Self::row_of(&self.quantiles, key, start, &s, min, max));
        }
    }

    /// Live (open) windows per key, oldest first.
    pub fn open_rows(&self) -> Vec<QuantileRow> {
        self.keys
            .iter()
            .flat_map(|(k, kw)| {
                kw.open.iter().map(|(start, (s, min, max))| {
                    Self::row_of(&self.quantiles, *k, *start, s, *min, *max)
                })
            })
            .collect()
    }
}

/// Streaming ASOF: keeps the latest right row per key; `join(left)` returns the right values in
/// force at the left row's time (backward, no tolerance), or `None` before the first right row.
#[derive(Debug, Default)]
pub struct IncrementalAsof {
    latest: BTreeMap<u32, (i64, Vec<i64>)>,
    /// Right rows that arrived older than the latest for their key (ignored).
    pub late_rows: u64,
}

impl IncrementalAsof {
    /// Empty state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a right-side row (`values` = the columns to carry into the join).
    pub fn push_right(&mut self, key: u32, ts: i64, values: Vec<i64>) {
        match self.latest.get(&key) {
            Some((t, _)) if *t > ts => self.late_rows += 1,
            _ => {
                self.latest.insert(key, (ts, values));
            }
        }
    }

    /// Right values in force for a left row at `ts` (only if the right row is not newer).
    /// A left row that arrives after a newer right row of its key cannot be joined any more
    /// (only the latest right row is kept): it yields `None` and is counted in `late_rows`.
    pub fn join(&mut self, key: u32, ts: i64) -> Option<&[i64]> {
        if self.latest.get(&key).is_some_and(|(t, _)| *t > ts) {
            self.late_rows += 1;
            return None;
        }
        self.latest.get(&key).map(|(_, v)| v.as_slice())
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig { cases: 200, ..Default::default() })]

        /// Rows that arrive out of order within the allowed lateness give the same bars and
        /// windows as the same rows in time order.
        #[test]
        fn arrival_order_within_lateness_does_not_matter(
            rows in proptest::collection::vec((0u32..3, 1i64..40, 1i64..400, 1i64..20, 0i64..1000), 1..300),
            width in 1i64..60,
            lateness in 0i64..120,
        ) {
            // strictly increasing timestamps per key, then an arrival order in which every row
            // is at most `lateness` older than the newest row of its key that arrived before it
            let mut clock = [0i64; 3];
            let mut timed: Vec<(u32, i64, i64, i64, i64)> = rows
                .iter()
                .map(|&(k, gap, px, sz, jitter)| {
                    clock[k as usize] += gap;
                    (k, clock[k as usize], px, sz, jitter % (lateness + 1))
                })
                .collect();
            let in_order = timed.clone();
            timed.sort_by_key(|&(_, ts, _, _, j)| ts + j);
            let run = |rows: &[(u32, i64, i64, i64, i64)]| {
                let mut bars = IncrementalOhlcv::new(width);
                bars.set_lateness(lateness);
                let mut pct = IncrementalPercentiles::new(width, vec![0.5], 0.01);
                pct.set_lateness(lateness);
                let (mut b_out, mut p_out) = (Vec::new(), Vec::new());
                for &(k, ts, px, sz, _) in rows {
                    bars.push(k, ts, px, sz, &mut b_out);
                    #[allow(clippy::cast_precision_loss)]
                    pct.push(k, ts, px as f64, &mut p_out);
                }
                let mut all: Vec<(u32, Ohlcv)> = b_out;
                all.extend(bars.open_bars().map(|(k, b)| (k, *b)));
                all.sort_by_key(|(k, b)| (*k, b.start_ns));
                let mut q: Vec<QuantileRow> = p_out;
                q.extend(pct.open_rows());
                q.sort_by_key(|r| (r.key, r.start_ns));
                (all, q, bars.late_rows, pct.late_rows)
            };
            let (a, aq, al, apl) = run(&in_order);
            let (b, bq, bl, bpl) = run(&timed);
            proptest::prop_assert_eq!(al, 0);
            proptest::prop_assert_eq!(bl, 0);
            proptest::prop_assert_eq!((apl, bpl), (0, 0));
            proptest::prop_assert_eq!(a, b);
            proptest::prop_assert_eq!(aq, bq);
        }

        /// The streaming rolling windows are the query-time ones (`rolling_bars`) for every
        /// window that has ended, and the live row is the window ending with the open pane.
        #[test]
        fn rolling_streams_what_the_query_computes(
            rows in proptest::collection::vec((0u32..3, 0i64..30, 1i64..400, 1i64..20), 1..400),
            step in 1i64..9,
            steps_per_window in 1i64..7,
        ) {
            let window = step * steps_per_window;
            let mut op = IncrementalRolling::new(window, step);
            let mut out = Vec::new();
            let mut clock = [0i64; 3];
            // per key: timestamps, prices, sizes
            #[allow(clippy::type_complexity)]
            let mut per_key: BTreeMap<u32, (Vec<i64>, Vec<i64>, Vec<i64>)> = BTreeMap::new();
            for (k, gap, price, size) in rows {
                clock[k as usize] += gap;
                let ts = clock[k as usize];
                op.push(k, ts, price, size, &mut out);
                let e = per_key.entry(k).or_default();
                e.0.push(ts);
                e.1.push(price);
                e.2.push(size);
            }
            let open: BTreeMap<u32, Ohlcv> = op.open_rows().into_iter().collect();
            for (k, (ts, px, sz)) in &per_key {
                let mut panes = Vec::new();
                crate::time_buckets(ts, px, sz, step, &mut panes);
                let mut want = Vec::new();
                crate::rolling_bars(&panes, window, step, &mut want);
                let live_end = open[k].start_ns;
                let closed: Vec<Ohlcv> = want.iter().copied().filter(|b| b.start_ns < live_end).collect();
                let got: Vec<Ohlcv> = out.iter().filter(|(key, _)| key == k).map(|(_, b)| *b).collect();
                proptest::prop_assert_eq!(&got, &closed, "key {}", k);
                let live = want.iter().find(|b| b.start_ns == live_end).copied();
                proptest::prop_assert_eq!(Some(open[k]), live, "live row of key {}", k);
            }
        }
    }
    use crate::bucket::time_buckets;

    #[test]
    fn extreme_timestamps_do_not_wrap() {
        let mut o = IncrementalOhlcv::new(60_000_000_000);
        let mut out = Vec::new();
        o.push(1, i64::MIN + 1, 10, 1, &mut out);
        assert_eq!(out.len(), 0);
        assert_eq!(o.late_rows, 1);
        assert_eq!(o.open_bars().count(), 0);
        // the key is still usable afterwards
        o.push(1, 0, 10, 1, &mut out);
        assert_eq!(out.len(), 0);
        assert_eq!(o.open_bars().count(), 1);
        let mut p = IncrementalPercentiles::new(60_000_000_000, vec![0.5], 0.01);
        let mut rows = Vec::new();
        p.push(1, i64::MIN + 1, 1.0, &mut rows);
        assert_eq!(rows.len(), 0);
        assert_eq!(p.late_rows, 1);
        assert_eq!(p.open_rows().len(), 0);
    }

    #[test]
    fn incremental_ohlcv_matches_batch() {
        let mut inc = IncrementalOhlcv::new(1_000);
        let mut ts = Vec::new();
        let mut px = Vec::new();
        let mut sz = Vec::new();
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        let mut t = 0i64;
        for _ in 0..5_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            t += (s % 300) as i64;
            ts.push(t);
            px.push(1_000 + (s % 50) as i64);
            sz.push(1 + (s % 7) as i64);
        }
        let mut out = Vec::new();
        for i in 0..ts.len() {
            inc.push(0, ts[i], px[i], sz[i], &mut out);
        }
        let mut closed: Vec<Ohlcv> = out.iter().map(|(_, b)| *b).collect();
        let open: Vec<Ohlcv> = inc.open_bars().map(|(_, b)| *b).collect();
        closed.extend(open);
        let mut expect = Vec::new();
        time_buckets(&ts, &px, &sz, 1_000, &mut expect);
        assert_eq!(closed.len(), expect.len());
        for (a, b) in closed.iter().zip(&expect) {
            assert_eq!(a.start_ns, b.start_ns);
            assert_eq!(
                (a.open, a.high, a.low, a.close, a.volume, a.count),
                (b.open, b.high, b.low, b.close, b.volume, b.count)
            );
            assert_eq!(a.notional, b.notional);
        }
        assert_eq!(inc.late_rows, 0);
        // a late row is counted, not applied
        inc.push(0, 10, 1, 1, &mut out);
        assert_eq!(inc.late_rows, 1);
    }

    #[test]
    fn incremental_percentiles_close_windows() {
        let mut p = IncrementalPercentiles::new(100, vec![0.5, 0.99], 0.01);
        let mut out = Vec::new();
        for i in 0..100i32 {
            p.push(1, i64::from(i), f64::from(i), &mut out);
            assert_eq!(out.len(), 0);
        }
        p.push(1, 150, 7.0, &mut out);
        let closed = out.pop().expect("window closes");
        assert_eq!(closed.count, 100);
        assert_eq!(closed.start_ns, 0);
        assert!(
            (closed.values[0] - 49.0).abs() <= 1.0,
            "{:?}",
            closed.values
        );
        assert!(
            (closed.values[1] - 98.0).abs() <= 1.5,
            "{:?}",
            closed.values
        );
        assert_eq!(p.open_rows().len(), 1);
        p.push(1, 20, 1.0, &mut out);
        assert_eq!(p.late_rows, 1);
    }

    #[test]
    fn incremental_asof_latest_not_newer() {
        let mut a = IncrementalAsof::new();
        assert!(a.join(1, 10).is_none());
        a.push_right(1, 5, vec![100, 101]);
        assert_eq!(a.join(1, 10), Some(&[100, 101][..]));
        assert!(a.join(1, 4).is_none());
        a.push_right(1, 20, vec![200, 201]);
        assert!(
            a.join(1, 10).is_none(),
            "right row newer than the left row is not in force"
        );
        assert_eq!(a.join(1, 25), Some(&[200, 201][..]));
        // two left rows came after a newer right row; now a right row arrives out of order
        assert_eq!(a.late_rows, 2);
        a.push_right(1, 15, vec![0, 0]);
        assert_eq!(a.late_rows, 3);
    }
}
