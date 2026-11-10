//! Quantile sketches.
#![allow(clippy::doc_markdown, clippy::cast_possible_wrap, clippy::float_cmp)]
//!
//! [`UddSketch`]: a `DDSketch` with **uniform collapse** (`UDDSketch`, Epicoco et al. 2020). Relative
//! error guarantee `α` on every quantile; buckets are logarithmic (`γ = (1+α)/(1−α)`); when the
//! bucket budget is exceeded the sketch halves its resolution uniformly (`γ ← γ²`), which keeps
//! a (weaker) guarantee instead of losing it like plain DDSketch collapsing. Negative values use a
//! mirrored store; zeros are counted separately. Storage is flat arrays (no `BTreeMap`), sized by
//! the budget; the quantile estimate is `2γ^i/(γ+1)`, whose worst-case relative error is exactly `α`.
//!
//! `add` costs one `ln` (≈ 20–40 cycles); for per-event percentiles in the hot path use the
//! fixed-point [`LatencyHistogram`] instead.

/// UDDSketch over `f64` values with relative accuracy `alpha`.
#[derive(Debug, Clone)]
pub struct UddSketch {
    alpha: f64,
    gamma: f64,
    inv_log_gamma: f64,
    max_buckets: usize,
    /// index offset: bucket `i` is stored at `i - min_index`
    pos: Store,
    neg: Store,
    zero_count: u64,
    count: u64,
    min: f64,
    max: f64,
    sum: f64,
    collapses: u32,
}

#[derive(Debug, Clone, Default)]
struct Store {
    /// first bucket index present (valid when `counts` non-empty)
    base: i32,
    counts: Vec<u64>,
    total: u64,
    /// buckets with a non-zero count, kept current so the budget check on every `add` is O(1)
    nonzero: usize,
}

impl Store {
    /// One more value in bucket `idx` when that bucket already holds values: the common case
    /// by far, a single bounds-checked increment. `false`: the bucket is new, use [`Self::add`].
    #[inline]
    fn bump(&mut self, idx: i32) -> bool {
        let at = i64::from(idx) - i64::from(self.base);
        match usize::try_from(at)
            .ok()
            .and_then(|i| self.counts.get_mut(i))
        {
            Some(count) if *count != 0 => {
                *count += 1;
                self.total += 1;
                true
            }
            _ => false,
        }
    }

    fn add(&mut self, idx: i32, n: u64) {
        if n == 0 {
            return;
        }
        if self.counts.is_empty() {
            self.base = idx;
            self.counts.push(0);
        } else if idx < self.base {
            let grow = (self.base - idx) as usize;
            let mut v = vec![0u64; grow];
            v.extend_from_slice(&self.counts);
            self.counts = v;
            self.base = idx;
        }
        let i = (idx - self.base) as usize;
        if i >= self.counts.len() {
            self.counts.resize(i + 1, 0);
        }
        if self.counts[i] == 0 {
            self.nonzero += 1;
        }
        self.counts[i] += n;
        self.total += n;
    }

    /// Halve resolution: bucket `i` → `floor(i/2)` (γ² grid).
    fn collapse(&mut self) {
        if self.counts.is_empty() {
            return;
        }
        let new_base = self.base.div_euclid(2);
        let last = self.base + self.counts.len() as i32 - 1;
        let new_last = last.div_euclid(2);
        let mut v = vec![0u64; (new_last - new_base + 1) as usize];
        for (k, &c) in self.counts.iter().enumerate() {
            let idx = self.base + k as i32;
            v[(idx.div_euclid(2) - new_base) as usize] += c;
        }
        self.base = new_base;
        self.nonzero = v.iter().filter(|&&c| c > 0).count();
        self.counts = v;
    }

    /// Non-empty buckets.
    fn len(&self) -> usize {
        self.nonzero
    }
}

impl UddSketch {
    /// New sketch. `alpha` in (0, 1) (e.g. 0.01 = 1 % relative error); `max_buckets` ≥ 16 total
    /// non-empty buckets before a uniform collapse (2 × 1,024 ≈ 32 KiB matches v3.2's budget).
    #[must_use]
    pub fn new(alpha: f64, max_buckets: usize) -> Self {
        let alpha = alpha.clamp(1e-6, 0.5);
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        Self {
            alpha,
            gamma,
            inv_log_gamma: 1.0 / gamma.ln(),
            max_buckets: max_buckets.max(16),
            pos: Store::default(),
            neg: Store::default(),
            zero_count: 0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            collapses: 0,
        }
    }

    /// Current relative accuracy (grows after collapses).
    #[must_use]
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Values added (NaN is ignored).
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Uniform collapses performed.
    #[must_use]
    pub fn collapses(&self) -> u32 {
        self.collapses
    }

    /// Approximate memory in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        (self.pos.counts.len() + self.neg.counts.len()) * 8 + 96
    }

    #[inline]
    fn index(&self, v: f64) -> i32 {
        // ceil(log_gamma(v)); v > 0
        (v.ln() * self.inv_log_gamma).ceil() as i32
    }

    /// Add one value.
    pub fn add(&mut self, v: f64) {
        self.add_n(v, 1);
    }

    /// Add every finite value of `values` (others are skipped): the same sketch as calling
    /// [`UddSketch::add`] for each of them in order, but the logarithm that places a value
    /// in its bucket is computed once per distinct value seen recently. Tick data repeats
    /// prices and sizes constantly, so most values find their bucket in a small table.
    pub fn add_slice(&mut self, values: &[f64]) {
        const SLOTS: usize = 256;
        // keyed by the bit pattern of |v|; `u64::MAX` is a NaN pattern, never a key
        const EMPTY: (u64, i32) = (u64::MAX, 0);
        let mut recent = [EMPTY; SLOTS];
        for &v in values {
            if !v.is_finite() {
                continue;
            }
            self.count += 1;
            self.sum += v;
            self.min = self.min.min(v);
            self.max = self.max.max(v);
            if v == 0.0 {
                self.zero_count += 1;
                continue;
            }
            let magnitude = v.abs();
            let bits = magnitude.to_bits();
            let slot = (bits.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize;
            let index = if recent[slot].0 == bits {
                recent[slot].1
            } else {
                let i = self.index(magnitude);
                recent[slot] = (bits, i);
                i
            };
            let store = if v > 0.0 {
                &mut self.pos
            } else {
                &mut self.neg
            };
            if store.bump(index) {
                continue; // no new bucket: the budget cannot be exceeded
            }
            store.add(index, 1);
            if self.pos.len() + self.neg.len() > self.max_buckets {
                while self.pos.len() + self.neg.len() > self.max_buckets {
                    self.collapse();
                }
                // a collapse changes which bucket a value belongs to
                recent = [EMPTY; SLOTS];
            }
        }
    }

    /// Smallest value added (`+inf` when empty).
    #[must_use]
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Largest value added (`-inf` when empty).
    #[must_use]
    pub fn max(&self) -> f64 {
        self.max
    }

    /// Add a value `n` times.
    pub fn add_n(&mut self, v: f64, n: u64) {
        if v.is_nan() || n == 0 {
            return;
        }
        self.count += n;
        self.sum += v * n as f64;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        if v == 0.0 {
            self.zero_count += n;
        } else if v > 0.0 {
            let i = self.index(v);
            self.pos.add(i, n);
        } else {
            let i = self.index(-v);
            self.neg.add(i, n);
        }
        // `while`: one collapse merges neighbours only, so sparse buckets can still be over
        // the budget after it; halving ends with at most two buckets per sign
        while self.pos.len() + self.neg.len() > self.max_buckets {
            self.collapse();
        }
    }

    fn collapse(&mut self) {
        self.pos.collapse();
        self.neg.collapse();
        self.gamma *= self.gamma;
        self.alpha = (self.gamma - 1.0) / (self.gamma + 1.0);
        self.inv_log_gamma = 1.0 / self.gamma.ln();
        self.collapses += 1;
    }

    /// Merge another sketch (same initial parameters; the coarser resolution wins).
    pub fn merge(&mut self, other: &Self) {
        let mut other = other.clone();
        while self.collapses < other.collapses {
            self.collapse();
        }
        while other.collapses < self.collapses {
            other.collapse();
        }
        for (k, &c) in other.pos.counts.iter().enumerate() {
            if c > 0 {
                self.pos.add(other.pos.base + k as i32, c);
            }
        }
        for (k, &c) in other.neg.counts.iter().enumerate() {
            if c > 0 {
                self.neg.add(other.neg.base + k as i32, c);
            }
        }
        self.zero_count += other.zero_count;
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        while self.pos.len() + self.neg.len() > self.max_buckets {
            self.collapse();
        }
    }

    /// Representative value of bucket `i`: `2·γ^i / (γ+1)`, the estimator whose worst-case
    /// relative error over the bucket `(γ^(i−1), γ^i]` is exactly `α` on both sides (the
    /// log-midpoint `γ^(i−½)` overshoots by `α²/2`).
    #[inline]
    fn bucket_value(&self, i: i32) -> f64 {
        2.0 * self.gamma.powi(i) / (self.gamma + 1.0)
    }

    /// Quantile `q ∈ [0, 1]`; `None` when empty. Exact at the extremes (min/max are tracked).
    #[must_use]
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        if q == 0.0 {
            return Some(self.min);
        }
        if q == 1.0 {
            return Some(self.max);
        }
        // rank in [1, count]
        let rank = (q * (self.count as f64 - 1.0)).floor() as u64 + 1;
        let mut seen = 0u64;
        // negatives first, from most negative (highest index in the mirrored store) to least
        for k in (0..self.neg.counts.len()).rev() {
            let c = self.neg.counts[k];
            if c == 0 {
                continue;
            }
            seen += c;
            if seen >= rank {
                return Some(-self.bucket_value(self.neg.base + k as i32));
            }
        }
        seen += self.zero_count;
        if seen >= rank {
            return Some(0.0);
        }
        for (k, &c) in self.pos.counts.iter().enumerate() {
            if c == 0 {
                continue;
            }
            seen += c;
            if seen >= rank {
                return Some(self.bucket_value(self.pos.base + k as i32));
            }
        }
        Some(self.max)
    }

    /// Sum of values.
    #[must_use]
    pub fn sum(&self) -> f64 {
        self.sum
    }
}

/// Fixed-point latency histogram for the measurement path (no `ln`, no allocation after
/// construction): one bucket per `quantum_ns` up to `max_ns`, values above go to an overflow
/// bucket. Exact rank queries within the quantum.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    buckets: Vec<u64>,
    quantum_ns: u64,
    overflow: u64,
    count: u64,
}

impl LatencyHistogram {
    /// `max_ns` range at `quantum_ns` resolution (e.g. 131,072 ns at 1 ns = 2^17 × 8 B = 1 MiB).
    #[must_use]
    pub fn new(max_ns: u64, quantum_ns: u64) -> Self {
        let quantum_ns = quantum_ns.max(1);
        Self {
            buckets: vec![0; (max_ns / quantum_ns + 1) as usize],
            quantum_ns,
            overflow: 0,
            count: 0,
        }
    }

    /// Record one sample.
    #[inline]
    pub fn record(&mut self, ns: u64) {
        let i = (ns / self.quantum_ns) as usize;
        if i < self.buckets.len() {
            self.buckets[i] += 1;
        } else {
            self.overflow += 1;
        }
        self.count += 1;
    }

    /// Samples recorded.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Quantile in ns (lower edge of the bucket); `None` when empty; `u64::MAX` when it falls in
    /// the overflow bucket.
    #[must_use]
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = (q.clamp(0.0, 1.0) * (self.count as f64 - 1.0)).floor() as u64 + 1;
        let mut seen = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(i as u64 * self.quantum_ns);
            }
        }
        Some(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn exact_quantile(sorted: &[f64], q: f64) -> f64 {
        let rank = (q * (sorted.len() as f64 - 1.0)).floor() as usize;
        sorted[rank]
    }

    /// The bucket budget is checked on every `add` against a running count; it must stay
    /// equal to the real number of non-empty buckets through growth, collapses and merges.
    #[test]
    fn bucket_count_is_tracked_exactly() {
        fn real(s: &Store) -> usize {
            s.counts.iter().filter(|&&c| c > 0).count()
        }
        let mut a = UddSketch::new(0.01, 64);
        let mut b = UddSketch::new(0.01, 64);
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for i in 0..20_000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // twelve decades, both signs, descending and ascending arrivals
            let v = 10f64.powf((x % 12_000) as f64 / 1_000.0 - 6.0)
                * if x & 1 == 0 { 1.0 } else { -1.0 };
            if i % 3 == 0 {
                b.add(v);
            } else {
                a.add(v);
            }
            assert_eq!(a.pos.len(), real(&a.pos));
            assert_eq!(a.neg.len(), real(&a.neg));
            assert!(a.pos.len() + a.neg.len() <= 64);
        }
        assert!(a.collapses() > 0, "the budget forces collapses");
        a.merge(&b);
        assert_eq!(a.pos.len(), real(&a.pos));
        assert_eq!(a.neg.len(), real(&a.neg));
        assert!(a.pos.len() + a.neg.len() <= 64);
        assert_eq!(a.count(), 20_000);
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

        /// The batch path is the one-by-one path: same buckets, counts, collapses, extremes.
        #[test]
        fn add_slice_equals_add(
            values in prop::collection::vec(prop_oneof![
                -1e6f64..1e6,
                // few distinct values, as prices and sizes are
                (0u8..40).prop_map(|k| f64::from(k) * 0.25 - 3.0),
                Just(f64::NAN), Just(f64::INFINITY), Just(0.0), Just(-0.0),
            ], 0..3000),
            budget in 16usize..200,
        ) {
            let mut one = UddSketch::new(0.01, budget);
            for &v in &values {
                if v.is_finite() {
                    one.add(v);
                }
            }
            let mut many = UddSketch::new(0.01, budget);
            // in pieces, so the table is rebuilt mid-way too
            for piece in values.chunks(700) {
                many.add_slice(piece);
            }
            prop_assert_eq!(one.count(), many.count());
            prop_assert_eq!(one.collapses(), many.collapses());
            prop_assert_eq!(one.pos.base, many.pos.base);
            prop_assert_eq!(&one.pos.counts, &many.pos.counts);
            prop_assert_eq!(&one.neg.counts, &many.neg.counts);
            prop_assert_eq!(one.zero_count, many.zero_count);
            prop_assert_eq!(one.min().to_bits(), many.min().to_bits());
            prop_assert_eq!(one.max().to_bits(), many.max().to_bits());
            prop_assert_eq!(one.sum().to_bits(), many.sum().to_bits());
            for q in [0.0, 0.5, 0.99, 1.0] {
                prop_assert_eq!(one.quantile(q).map(f64::to_bits), many.quantile(q).map(f64::to_bits));
            }
        }

        #[test]
        fn relative_error_within_alpha(values in prop::collection::vec(-1e6f64..1e6, 1..2000), q in 0.0f64..1.0) {
            let mut s = UddSketch::new(0.01, 4096);
            for &v in &values { s.add(v); }
            let mut sorted = values.clone();
            sorted.sort_by(f64::total_cmp);
            let want = exact_quantile(&sorted, q);
            let got = s.quantile(q).unwrap();
            // relative error guarantee (ties and the zero bucket are exact)
            let tol = s.alpha() * want.abs() + 1e-9;
            prop_assert!((got - want).abs() <= tol * 1.0001 || (got.signum() == want.signum() && ((got - want) / want).abs() <= s.alpha() * 1.0001),
                "q={q} want={want} got={got} alpha={}", s.alpha());
            prop_assert_eq!(s.count(), values.len() as u64);
        }

        #[test]
        fn merge_equals_single(a in prop::collection::vec(0.001f64..1e4, 0..500), b in prop::collection::vec(0.001f64..1e4, 0..500), q in 0.0f64..1.0) {
            let mut s1 = UddSketch::new(0.02, 2048);
            let mut s2 = UddSketch::new(0.02, 2048);
            let mut all = UddSketch::new(0.02, 2048);
            for &v in &a { s1.add(v); all.add(v); }
            for &v in &b { s2.add(v); all.add(v); }
            s1.merge(&s2);
            prop_assert_eq!(s1.count(), all.count());
            if all.count() > 0 {
                let x = s1.quantile(q).unwrap(); let y = all.quantile(q).unwrap();
                prop_assert!(((x - y) / y.max(1e-9)).abs() <= 2.0 * all.alpha() + 1e-9, "{x} vs {y}");
            }
        }
    }

    #[test]
    fn collapse_keeps_guarantee_but_widens_alpha() {
        let mut s = UddSketch::new(0.001, 64); // tiny budget forces collapses
        for i in 1..=100_000u64 {
            s.add(i as f64);
        }
        assert!(s.collapses() > 0);
        assert!(s.alpha() > 0.001);
        let got = s.quantile(0.5).unwrap();
        assert!(
            ((got - 50_000.0) / 50_000.0).abs() <= s.alpha(),
            "{got} alpha={}",
            s.alpha()
        );
        assert!(s.memory_bytes() < 16 * 1024);
    }

    #[test]
    fn negatives_and_zeros() {
        let mut s = UddSketch::new(0.01, 1024);
        for v in [-100.0, -10.0, -1.0, 0.0, 0.0, 1.0, 10.0, 100.0] {
            s.add(v);
        }
        assert_eq!(s.quantile(0.0), Some(-100.0));
        assert_eq!(s.quantile(1.0), Some(100.0));
        let mid = s.quantile(0.5).unwrap();
        assert!(
            (-1.0..=0.0).contains(&mid) || (mid - 0.0).abs() < 1e-9,
            "{mid}"
        );
        assert!(((s.quantile(0.9).unwrap() - 10.0) / 10.0).abs() <= 0.01);
    }

    #[test]
    fn latency_histogram() {
        let mut h = LatencyHistogram::new(1_000, 10);
        for ns in [5u64, 15, 25, 25, 999, 5_000] {
            h.record(ns);
        }
        assert_eq!(h.count(), 6);
        assert_eq!(h.quantile(0.0), Some(0));
        assert_eq!(h.quantile(0.5), Some(20));
        assert_eq!(h.quantile(1.0), Some(u64::MAX));
    }
}
