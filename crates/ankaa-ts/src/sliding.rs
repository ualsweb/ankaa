//! Sliding-window aggregation in amortised constant time per element.
//!
//! [`Sliding`] is a first-in first-out queue that answers "the combination of everything in
//! the queue" at any moment, for any **associative** operation: it needs no inverse (so it
//! works for minimum, maximum, first/last as well as for sums) and no commutativity (the
//! combination respects arrival order). It is the classic two-stack scheme: elements arrive
//! on a back stack with a running total; when the front stack is empty the back stack is
//! turned over once, storing for every element the combination of itself and everything
//! behind it. Each element is combined at most three times in its life.
//!
//! [`rolling_bars`] uses it to turn per-step bars ("panes") into bars over a trailing window:
//! a row costs one bucket update when it arrives, and each window costs a constant number of
//! pane combinations, whatever the ratio of window to step.

use crate::Ohlcv;

/// FIFO queue with the combination of its contents available in O(1).
#[derive(Debug, Clone)]
pub struct Sliding<T, F> {
    /// `(element, combination of it and every element after it in this stack)`, oldest on top.
    front: Vec<(T, T)>,
    back: Vec<T>,
    /// Combination of the back stack, in arrival order.
    back_total: Option<T>,
    combine: F,
}

impl<T: Clone, F: Fn(&T, &T) -> T> Sliding<T, F> {
    /// Empty queue; `combine(earlier, later)` must be associative.
    pub fn new(combine: F) -> Self {
        Self {
            front: Vec::new(),
            back: Vec::new(),
            back_total: None,
            combine,
        }
    }

    /// Elements in the queue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.front.len() + self.back.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add the newest element.
    pub fn push(&mut self, value: T) {
        self.back_total = Some(match &self.back_total {
            Some(total) => (self.combine)(total, &value),
            None => value.clone(),
        });
        self.back.push(value);
    }

    /// The oldest element, if any.
    #[must_use]
    pub fn oldest(&self) -> Option<&T> {
        self.front
            .last()
            .map(|e| &e.0)
            .or_else(|| self.back.first())
    }

    /// Remove the oldest element.
    pub fn pop(&mut self) -> Option<T> {
        if self.front.is_empty() {
            // turn the back stack over: newest goes in first, so the oldest ends on top, each
            // with the combination of itself and everything newer
            let mut behind: Option<T> = None;
            for value in self.back.drain(..).rev() {
                let total = match &behind {
                    Some(b) => (self.combine)(&value, b),
                    None => value.clone(),
                };
                behind = Some(total.clone());
                self.front.push((value, total));
            }
            self.back_total = None;
        }
        self.front.pop().map(|e| e.0)
    }

    /// Combination of everything in the queue, oldest first (`None` when empty).
    #[must_use]
    pub fn total(&self) -> Option<T> {
        match (self.front.last(), &self.back_total) {
            (Some((_, front)), Some(back)) => Some((self.combine)(front, back)),
            (Some((_, front)), None) => Some(front.clone()),
            (None, back) => back.clone(),
        }
    }
}

/// `earlier` followed by `later` as one bar (starts where `earlier` starts).
#[must_use]
pub fn join_bars(earlier: &Ohlcv, later: &Ohlcv) -> Ohlcv {
    Ohlcv {
        start_ns: earlier.start_ns,
        open: earlier.open,
        high: earlier.high.max(later.high),
        low: earlier.low.min(later.low),
        close: later.close,
        volume: earlier.volume.wrapping_add(later.volume),
        notional: earlier.notional + later.notional,
        count: earlier.count.saturating_add(later.count),
    }
}

/// Bars over a trailing window from per-step bars.
///
/// `panes` are the non-empty bars of consecutive steps of `step_ns` (as [`crate::time_buckets`]
/// returns them, in time order). For every step end `e` whose window `[e - window_ns, e)` holds
/// at least one row, `out` gets one bar of that window with `start_ns = e` (the window's
/// exclusive end). `window_ns` must be a positive multiple of `step_ns`.
///
/// # Panics
/// When `step_ns <= 0` or `window_ns` is not a positive multiple of it.
pub fn rolling_bars(panes: &[Ohlcv], window_ns: i64, step_ns: i64, out: &mut Vec<Ohlcv>) {
    assert!(step_ns > 0, "step must be positive");
    assert!(
        window_ns >= step_ns && window_ns % step_ns == 0,
        "window must be a positive multiple of the step"
    );
    out.clear();
    let mut queue = Sliding::new(join_bars);
    let mut next = 0usize;
    let Some(first) = panes.first() else { return };
    let mut end = first.start_ns.saturating_add(step_ns);
    loop {
        // panes that started before this window's end enter; panes before its start leave
        while next < panes.len() && panes[next].start_ns < end {
            queue.push(panes[next]);
            next += 1;
        }
        let from = end.saturating_sub(window_ns);
        while queue.oldest().is_some_and(|p| p.start_ns < from) {
            queue.pop();
        }
        match queue.total() {
            Some(mut bar) => {
                bar.start_ns = end;
                out.push(bar);
                end = end.saturating_add(step_ns);
            }
            // nothing in the window: jump to the first window the next pane is in
            None => match panes.get(next) {
                Some(p) => end = p.start_ns.saturating_add(step_ns),
                None => return,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time_buckets;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 300, ..ProptestConfig::default() })]

        /// The queue's total is the left-to-right combination of its contents, for an
        /// operation that is associative but neither commutative nor invertible.
        #[test]
        fn total_is_the_ordered_combination(ops in prop::collection::vec(prop_oneof![
            3 => (0u32..1000).prop_map(Some),
            2 => Just(None),
        ], 0..400)) {
            // (first, last, max, concatenated length): order matters, nothing can be undone
            type E = (u32, u32, u32, u32);
            let join = |a: &E, b: &E| (a.0, b.1, a.2.max(b.2), a.3 + b.3);
            let mut queue = Sliding::new(join);
            let mut model: std::collections::VecDeque<E> = std::collections::VecDeque::new();
            for op in ops {
                match op {
                    Some(v) => {
                        queue.push((v, v, v, 1));
                        model.push_back((v, v, v, 1));
                    }
                    None => {
                        prop_assert_eq!(queue.pop(), model.pop_front());
                    }
                }
                let want = model.iter().copied().reduce(|a, b| join(&a, &b));
                prop_assert_eq!(queue.total(), want);
                prop_assert_eq!(queue.len(), model.len());
                prop_assert_eq!(queue.oldest().copied(), model.front().copied());
            }
        }

        /// Rolling bars equal bars computed from the rows of each window, one window at a time.
        #[test]
        fn rolling_equals_recomputing_every_window(
            rows in prop::collection::vec((0i64..40, 1i64..500, 1i64..50), 1..300),
            step in 1i64..7,
            steps_per_window in 1i64..9,
        ) {
            let mut ts = Vec::new();
            let (mut t, mut price, mut size) = (0i64, Vec::new(), Vec::new());
            for (gap, p, s) in rows {
                t += gap;
                ts.push(t);
                price.push(p);
                size.push(s);
            }
            let window = step * steps_per_window;
            let mut panes = Vec::new();
            time_buckets(&ts, &price, &size, step, &mut panes);
            let mut got = Vec::new();
            rolling_bars(&panes, window, step, &mut got);
            // reference: for every step end, the bar of the rows inside the window
            let mut want = Vec::new();
            let first_end = ts[0].div_euclid(step) * step + step;
            let last_end = ts[ts.len() - 1].div_euclid(step) * step + window;
            let mut end = first_end;
            while end <= last_end {
                let (a, b) = (
                    ts.partition_point(|&x| x < end - window),
                    ts.partition_point(|&x| x < end),
                );
                if a < b {
                    let mut one = Vec::new();
                    time_buckets(&ts[a..b], &price[a..b], &size[a..b], i64::MAX / 4, &mut one);
                    let mut bar = one[0];
                    bar.start_ns = end;
                    want.push(bar);
                }
                end += step;
            }
            prop_assert_eq!(got, want);
        }
    }

    #[test]
    fn a_window_of_one_step_is_the_step_bars() {
        let ts = [1, 2, 11, 35, 36, 90];
        let px = [5, 7, 6, 9, 4, 8];
        let sz = [1, 1, 2, 3, 1, 1];
        let mut panes = Vec::new();
        time_buckets(&ts, &px, &sz, 10, &mut panes);
        let mut rolled = Vec::new();
        rolling_bars(&panes, 10, 10, &mut rolled);
        assert_eq!(rolled.len(), panes.len());
        for (r, p) in rolled.iter().zip(&panes) {
            assert_eq!(r.start_ns, p.start_ns + 10);
            assert_eq!(
                (r.open, r.high, r.low, r.close, r.volume, r.count),
                (p.open, p.high, p.low, p.close, p.volume, p.count)
            );
        }
        rolling_bars(&[], 10, 10, &mut rolled);
        assert_eq!(rolled.len(), 0);
    }
}
