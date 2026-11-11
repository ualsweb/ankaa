//! Gap filling: a regular series of time buckets per key, with the buckets that hold no reading
//! filled in (`NULL`, the last value carried forward, or linear interpolation between the
//! neighbouring measured buckets).
//!
//! Work is linear in rows + buckets. A [`Series`] keeps only the buckets that hold readings:
//! rows of a key in time order (the stored order) fold into its last bucket with one compare;
//! a row that arrives out of order opens a new entry, and only then are that key's *buckets*
//! (not its rows) sorted and merged at the end. The dense output is written in one pass per key
//! into struct-of-arrays buffers ([`Filled`]): a run of missing buckets is a tight loop over
//! consecutive bucket indexes, and an interpolated value is computed from the run's two ends
//! (not accumulated step by step), so it carries no drift however long the gap.

/// What one bucket's readings become.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    /// Mean.
    Avg,
    /// Smallest.
    Min,
    /// Largest.
    Max,
    /// Sum.
    Sum,
    /// Number of readings (a missing bucket is 0, whatever the fill).
    Count,
    /// Earliest reading (by timestamp).
    First,
    /// Latest reading (by timestamp).
    Last,
}

impl Agg {
    /// `avg`, `min`, `max`, `sum`, `count`, `first`, `last`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "sum" => Self::Sum,
            "count" => Self::Count,
            "first" => Self::First,
            "last" => Self::Last,
            _ => return None,
        })
    }
}

/// How a bucket without readings is filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fill {
    /// `NULL`.
    Null,
    /// The previous measured bucket's value (last observation carried forward); `NULL` before
    /// the first one.
    Locf,
    /// On the straight line between the previous and the next measured buckets; `NULL` where
    /// one of them is missing (before the first, after the last).
    Linear,
}

impl Fill {
    /// `null`, `locf`, `linear`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "null" | "none" => Self::Null,
            "locf" | "previous" => Self::Locf,
            "linear" | "interpolate" => Self::Linear,
            _ => return None,
        })
    }
}

/// Readings of one bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Acc {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    first: (i64, f64),
    last: (i64, f64),
}

impl Acc {
    #[inline]
    fn new(ts: i64, v: f64) -> Self {
        Self {
            sum: v,
            count: 1,
            min: v,
            max: v,
            first: (ts, v),
            last: (ts, v),
        }
    }

    #[inline]
    fn add(&mut self, ts: i64, v: f64) {
        self.sum += v;
        self.count += 1;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        // ties keep arrival order: the first stays first, the latest arrival is last
        if ts < self.first.0 {
            self.first = (ts, v);
        }
        if ts >= self.last.0 {
            self.last = (ts, v);
        }
    }

    fn merge(&mut self, o: &Self) {
        self.sum += o.sum;
        self.count += o.count;
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
        if o.first.0 < self.first.0 {
            self.first = o.first;
        }
        if o.last.0 >= self.last.0 {
            self.last = o.last;
        }
    }

    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn value(&self, agg: Agg) -> f64 {
        match agg {
            Agg::Avg => self.sum / self.count as f64,
            Agg::Min => self.min,
            Agg::Max => self.max,
            Agg::Sum => self.sum,
            Agg::Count => self.count as f64,
            Agg::First => self.first.1,
            Agg::Last => self.last.1,
        }
    }
}

/// Start of the bucket of `ts` (floor division: negative timestamps align too). Within one
/// width of `i64::MIN` (year 1677) the start is clamped to `i64::MIN` instead of overflowing.
#[inline]
#[must_use]
pub fn bucket_of(ts: i64, width: i64) -> i64 {
    ts.saturating_sub(ts.rem_euclid(width))
}

/// Number of buckets `lo, lo + width, …, hi` (0 when `hi < lo`), without overflow.
#[must_use]
pub fn bucket_count(lo: i64, hi: i64, width: i64) -> u64 {
    if hi < lo || width <= 0 {
        return 0;
    }
    u64::try_from((i128::from(hi) - i128::from(lo)) / i128::from(width) + 1).unwrap_or(u64::MAX)
}

/// The measured buckets of one key.
#[derive(Debug, Clone)]
pub struct Series {
    buckets: Vec<(i64, Acc)>,
    ordered: bool,
}

impl Default for Series {
    fn default() -> Self {
        Self {
            buckets: Vec::new(),
            ordered: true,
        }
    }
}

impl Series {
    /// Add a reading (`value` finite or not: the caller skips `NULL`s) at `ts` to `bucket`
    /// (from [`bucket_of`]).
    #[inline]
    pub fn push(&mut self, bucket: i64, ts: i64, value: f64) {
        match self.buckets.last_mut() {
            Some((b, acc)) if *b == bucket => acc.add(ts, value),
            Some((b, _)) if *b > bucket => {
                self.ordered = false;
                self.buckets.push((bucket, Acc::new(ts, value)));
            }
            _ => self.buckets.push((bucket, Acc::new(ts, value))),
        }
    }

    /// Number of measured buckets so far (before merging out-of-order ones).
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// No readings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Sort and merge the buckets when readings came out of order.
    fn normalize(&mut self) {
        if self.ordered {
            return;
        }
        // stable: readings of one bucket merge in arrival order
        self.buckets.sort_by_key(|(b, _)| *b);
        let mut out: Vec<(i64, Acc)> = Vec::with_capacity(self.buckets.len());
        for (b, acc) in self.buckets.drain(..) {
            match out.last_mut() {
                Some((ob, oacc)) if *ob == b => oacc.merge(&acc),
                _ => out.push((b, acc)),
            }
        }
        self.buckets = out;
        self.ordered = true;
    }

    /// First and last measured bucket.
    pub fn span(&mut self) -> Option<(i64, i64)> {
        self.normalize();
        Some((self.buckets.first()?.0, self.buckets.last()?.0))
    }
}

/// Dense gap-filled output, struct of arrays (one entry per bucket).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Filled {
    /// Bucket start, ns.
    pub bucket: Vec<i64>,
    /// Value (meaningless where `valid` is false).
    pub value: Vec<f64>,
    /// The bucket has a value.
    pub valid: Vec<bool>,
    /// The value was filled in (the bucket held no reading).
    pub filled: Vec<bool>,
}

impl Filled {
    /// Buckets written.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bucket.len()
    }

    /// Nothing written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bucket.is_empty()
    }

    /// Buckets `lo, lo + width, …, hi` of `series` (both multiples of `width`), filled by
    /// `fill`, appended. Measured buckets outside `[lo, hi]` only serve as interpolation ends.
    /// Returns the number of buckets appended.
    ///
    /// # Panics
    /// `width` must be positive and `lo <= hi + width` (an empty range appends nothing).
    pub fn extend(
        &mut self,
        series: &mut Series,
        lo: i64,
        hi: i64,
        width: i64,
        agg: Agg,
        fill: Fill,
    ) -> usize {
        assert!(width > 0, "bucket width must be positive");
        series.normalize();
        let before = self.len();
        if hi < lo {
            return 0;
        }
        let m = &series.buckets;
        // the last measured bucket before the range, as a fill or interpolation start
        let start = m.partition_point(|(b, _)| *b < lo);
        let mut prev: Option<(i64, f64)> =
            start.checked_sub(1).map(|i| (m[i].0, m[i].1.value(agg)));
        let mut cursor = lo;
        for &(b, ref acc) in &m[start..] {
            if b > hi {
                // the next measured bucket after the range: the right end of the last gap
                self.gap(
                    cursor,
                    hi,
                    width,
                    agg,
                    fill,
                    prev,
                    Some((b, acc.value(agg))),
                );
                return self.len() - before;
            }
            let v = acc.value(agg);
            if let Some(gap_end) = b.checked_sub(width) {
                self.gap(cursor, gap_end, width, agg, fill, prev, Some((b, v)));
            }
            self.bucket.push(b);
            self.value.push(v);
            self.valid.push(true);
            self.filled.push(false);
            prev = Some((b, v));
            let Some(c) = b.checked_add(width) else {
                // the last representable bucket
                return self.len() - before;
            };
            cursor = c;
        }
        self.gap(cursor, hi, width, agg, fill, prev, None);
        self.len() - before
    }

    /// Missing buckets `from, from + width, …, to` (none when `to < from`), between the
    /// measured buckets `prev` and `next`.
    #[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
    fn gap(
        &mut self,
        from: i64,
        to: i64,
        width: i64,
        agg: Agg,
        fill: Fill,
        prev: Option<(i64, f64)>,
        next: Option<(i64, f64)>,
    ) {
        let count = bucket_count(from, to, width);
        if count == 0 {
            return;
        }
        // `from + i * width` stays within `[from, to]` for every step: no overflow
        let steps = i64::try_from(count).unwrap_or(i64::MAX);
        let n = usize::try_from(count).unwrap_or(usize::MAX);
        self.bucket.extend((0..steps).map(|i| from + i * width));
        self.filled.extend(std::iter::repeat_n(true, n));
        match (agg, fill, prev, next) {
            (Agg::Count, ..) => {
                self.value.extend(std::iter::repeat_n(0.0, n));
                self.valid.extend(std::iter::repeat_n(true, n));
            }
            (_, Fill::Locf, Some((_, pv)), _) => {
                self.value.extend(std::iter::repeat_n(pv, n));
                self.valid.extend(std::iter::repeat_n(true, n));
            }
            (_, Fill::Linear, Some((pb, pv)), Some((nb, nv))) => {
                // from the two ends, per bucket: no accumulated rounding over a long gap
                let span = (nb - pb) as f64;
                let slope = nv - pv;
                self.value.extend((0..steps).map(|i| {
                    let b = from + i * width;
                    pv + slope * ((b - pb) as f64 / span)
                }));
                self.valid.extend(std::iter::repeat_n(true, n));
            }
            _ => {
                self.value.extend(std::iter::repeat_n(0.0, n));
                self.valid.extend(std::iter::repeat_n(false, n));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(rows: &[(i64, f64)], width: i64) -> Series {
        let mut s = Series::default();
        for &(ts, v) in rows {
            s.push(bucket_of(ts, width), ts, v);
        }
        s
    }

    fn values(f: &Filled) -> Vec<Option<f64>> {
        f.value
            .iter()
            .zip(&f.valid)
            .map(|(v, ok)| ok.then_some(*v))
            .collect()
    }

    #[test]
    fn fills_by_method() {
        // buckets of 10: readings in 0, 10, 40 (two in 40), none in 20, 30
        let rows = [(1, 1.0), (12, 3.0), (41, 6.0), (45, 10.0)];
        let mut s = series(&rows, 10);
        let mut f = Filled::default();
        assert_eq!(f.extend(&mut s, 0, 50, 10, Agg::Avg, Fill::Linear), 6);
        assert_eq!(f.bucket, vec![0, 10, 20, 30, 40, 50]);
        let want = [
            Some(1.0),
            Some(3.0),
            Some(3.0 + 5.0 / 3.0),
            Some(3.0 + 10.0 / 3.0),
            Some(8.0),
            None,
        ];
        for (got, want) in values(&f).iter().zip(want) {
            match (got, want) {
                (Some(g), Some(w)) => assert!((g - w).abs() < 1e-12, "{g} vs {w}"),
                (g, w) => assert_eq!(*g, w),
            }
        }
        assert_eq!(f.filled, vec![false, false, true, true, false, true]);
        let mut f = Filled::default();
        f.extend(&mut s, 0, 50, 10, Agg::Max, Fill::Locf);
        assert_eq!(
            values(&f),
            vec![
                Some(1.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(10.0),
                Some(10.0)
            ]
        );
        let mut f = Filled::default();
        f.extend(&mut s, 0, 50, 10, Agg::Count, Fill::Null);
        assert_eq!(
            values(&f),
            vec![
                Some(1.0),
                Some(1.0),
                Some(0.0),
                Some(0.0),
                Some(2.0),
                Some(0.0)
            ]
        );
        let mut f = Filled::default();
        f.extend(&mut s, -20, 10, 10, Agg::Last, Fill::Locf);
        assert_eq!(values(&f), vec![None, None, Some(1.0), Some(3.0)]);
    }

    #[test]
    fn interpolation_reaches_past_the_range() {
        // range [20, 30] between measured buckets 0 and 50: interpolated from both
        let mut s = series(&[(0, 0.0), (50, 100.0)], 10);
        let mut f = Filled::default();
        f.extend(&mut s, 20, 30, 10, Agg::Avg, Fill::Linear);
        assert_eq!(values(&f), vec![Some(40.0), Some(60.0)]);
    }

    #[test]
    fn out_of_order_readings_merge_into_their_buckets() {
        let ordered = [(1, 1.0), (5, 2.0), (12, 3.0), (31, 4.0), (35, 5.0)];
        let shuffled = [(31, 4.0), (5, 2.0), (12, 3.0), (35, 5.0), (1, 1.0)];
        for agg in [
            Agg::Avg,
            Agg::Min,
            Agg::Max,
            Agg::Sum,
            Agg::Count,
            Agg::First,
            Agg::Last,
        ] {
            let (mut a, mut b) = (Filled::default(), Filled::default());
            a.extend(&mut series(&ordered, 10), 0, 40, 10, agg, Fill::Linear);
            b.extend(&mut series(&shuffled, 10), 0, 40, 10, agg, Fill::Linear);
            assert_eq!(a, b, "{agg:?}");
        }
    }

    #[test]
    fn buckets_align_below_zero() {
        assert_eq!(bucket_of(-1, 10), -10);
        assert_eq!(bucket_of(-10, 10), -10);
        assert_eq!(bucket_of(9, 10), 0);
        // clamped, not overflowing, at the bottom of the range
        assert_eq!(bucket_of(i64::MIN, 7), i64::MIN);
        assert_eq!(bucket_count(i64::MIN, i64::MAX, 1), u64::MAX);
        assert_eq!(bucket_count(5, 4, 1), 0);
    }

    #[test]
    fn extremes_do_not_overflow() {
        let w = 1_000_000_007;
        let top = bucket_of(i64::MAX, w);
        let mut s = series(&[(i64::MAX, 1.0), (top - 3 * w, 2.0)], w);
        let mut f = Filled::default();
        assert_eq!(
            f.extend(&mut s, top - 3 * w, top, w, Agg::Avg, Fill::Linear),
            4
        );
        assert_eq!(f.bucket.last(), Some(&top));
    }
}
