use ankaa_store::{Chunk, ChunkId, ChunkState, ColumnStats, Schema, Value};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: if cfg!(miri) { 8 } else { 256 }, ..ProptestConfig::default() })]

    #[test]
    #[cfg_attr(miri, ignore)]
    fn invariants_hold(deltas in prop::collection::vec(0i64..1_000, 0..3_000), prices in prop::collection::vec(any::<i64>(), 3_000)) {
        let mut c = Chunk::new(ChunkId(0), Schema::trades());
        let mut ts = 0i64;
        let mut min_px = i64::MAX; let mut max_px = i64::MIN;
        for (i, d) in deltas.iter().enumerate() {
            ts += d;
            let px = prices[i];
            c.append_row(&[Value::I64(ts), Value::U32(1), Value::I64(px), Value::I64(1), Value::U8(0), Value::I64(i64::try_from(i).unwrap())]).unwrap();
            min_px = min_px.min(px); max_px = max_px.max(px);
            prop_assert_eq!(c.watermark(), ts);
            prop_assert_eq!(c.num_rows(), i + 1);
        }
        // timestamps non-decreasing
        prop_assert!(c.timestamps().windows(2).all(|w| w[0] <= w[1]));
        // every column has the same length
        for i in 0..6 { prop_assert_eq!(c.column(i).unwrap().len(), c.num_rows()); }
        // time range by binary search equals the linear scan
        if !deltas.is_empty() {
            let lo = ts / 3; let hi = ts / 2;
            let r = c.time_range_rows(lo, hi);
            let lin: Vec<usize> = c.timestamps().iter().enumerate().filter(|(_, t)| **t >= lo && **t <= hi).map(|(i, _)| i).collect();
            prop_assert_eq!(r.len(), lin.len());
            if let Some(&f) = lin.first() { prop_assert_eq!(r.start, f); }
        }
        let zm = c.seal().clone();
        prop_assert_eq!(c.state(), ChunkState::Sealed);
        if deltas.is_empty() {
            prop_assert_eq!(zm.per_column[2], ColumnStats::I64 { min: i64::MAX, max: i64::MIN });
        } else {
            prop_assert_eq!(zm.per_column[2], ColumnStats::I64 { min: min_px, max: max_px });
        }
    }
}
