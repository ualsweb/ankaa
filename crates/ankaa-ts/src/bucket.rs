//! Time-bucket aggregation over sorted timestamps: OHLCV + VWAP numerator.

/// One bucket of trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ohlcv {
    /// Bucket start (inclusive), ns, aligned to `width`.
    pub start_ns: i64,
    /// First price.
    pub open: i64,
    /// Max price.
    pub high: i64,
    /// Min price.
    pub low: i64,
    /// Last price.
    pub close: i64,
    /// Sum of sizes (ticks).
    pub volume: i64,
    /// Σ price·size as i128 (VWAP = notional / volume).
    pub notional: i128,
    /// Trade count.
    pub count: u32,
}

/// Aggregate `(ts, price, size)` rows (sorted by `ts`) into fixed-width buckets aligned to
/// `width_ns` (floor division, so negative timestamps also align). Empty buckets are not
/// emitted. Returns the number of buckets written to `out` (which is cleared first).
///
/// # Panics
/// `width_ns` must be positive.
pub fn time_buckets(
    ts: &[i64],
    price: &[i64],
    size: &[i64],
    width_ns: i64,
    out: &mut Vec<Ohlcv>,
) -> usize {
    assert!(width_ns > 0, "bucket width must be positive");
    assert_eq!(ts.len(), price.len());
    assert_eq!(ts.len(), size.len());
    debug_assert!(ts.windows(2).all(|w| w[0] <= w[1]), "ts not sorted");
    out.clear();
    let n = ts.len();
    let mut i = 0;
    while i < n {
        // one division per bucket, not per row: the rows are in time order, so a bucket is a
        // run of rows below its end, found by a plain comparison
        let start = ts[i].div_euclid(width_ns) * width_ns;
        let end = start.saturating_add(width_ns);
        let mut j = i + 1;
        while j < n && ts[j] < end {
            j += 1;
        }
        let (px, qty) = (&price[i..j], &size[i..j]);
        let mut high = px[0];
        let mut low = px[0];
        let mut volume = 0i64;
        // `price * size` fits in i64 for every row of a fixed-point tick table (scale 8:
        // prices and sizes far below 3·10^9 units); summed in i128 so the total cannot overflow
        let mut notional = 0i128;
        for (&p, &q) in px.iter().zip(qty) {
            high = high.max(p);
            low = low.min(p);
            volume = volume.wrapping_add(q);
            notional += i128::from(p) * i128::from(q);
        }
        out.push(Ohlcv {
            start_ns: start,
            open: px[0],
            high,
            low,
            close: px[px.len() - 1],
            volume,
            notional,
            count: u32::try_from(j - i).unwrap_or(u32::MAX),
        });
        i = j;
    }
    out.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    #[test]
    fn two_buckets() {
        let ts = [0, 1, 9, 10, 15];
        let px = [5, 7, 3, 4, 4];
        let sz = [1, 1, 1, 2, 2];
        let mut out = Vec::new();
        assert_eq!(time_buckets(&ts, &px, &sz, 10, &mut out), 2);
        assert_eq!(
            out[0],
            Ohlcv {
                start_ns: 0,
                open: 5,
                high: 7,
                low: 3,
                close: 3,
                volume: 3,
                notional: 15,
                count: 3
            }
        );
        assert_eq!(
            out[1],
            Ohlcv {
                start_ns: 10,
                open: 4,
                high: 4,
                low: 4,
                close: 4,
                volume: 4,
                notional: 16,
                count: 2
            }
        );
    }

    proptest! {
        #[test]
        fn matches_naive(rows in prop::collection::vec((-1_000i64..1_000, -50i64..50, 0i64..10), 0..300), width in 1i64..40) {
            let mut rows = rows; rows.sort_by_key(|r| r.0);
            let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
            let px: Vec<i64> = rows.iter().map(|r| r.1).collect();
            let sz: Vec<i64> = rows.iter().map(|r| r.2).collect();
            let mut out = Vec::new();
            time_buckets(&ts, &px, &sz, width, &mut out);
            let mut naive: BTreeMap<i64, Ohlcv> = BTreeMap::new();
            for &(t, p, s) in &rows {
                let start = t.div_euclid(width) * width;
                let e = naive.entry(start).or_insert(Ohlcv { start_ns: start, open: p, high: p, low: p, close: p, volume: 0, notional: 0, count: 0 });
                e.high = e.high.max(p); e.low = e.low.min(p); e.close = p; e.volume += s; e.notional += i128::from(p) * i128::from(s); e.count += 1;
            }
            let want: Vec<Ohlcv> = naive.into_values().collect();
            prop_assert_eq!(out, want);
        }
    }
}
