#![allow(clippy::float_cmp)]
//! Differential tests: every available backend must agree with the scalar reference on
//! random data *and* on IEEE special values (NaN, ±inf, ±0, denormals).

use ankaa_simd::scalar::ScalarBackend;
use ankaa_simd::{BackendKind, CmpOp, SelVec, SimdBackend, VECTOR_LEN};
use proptest::prelude::*;

const REL_TOL: f64 = 1e-6;

fn backends() -> Vec<Box<dyn SimdBackend>> {
    let mut v: Vec<Box<dyn SimdBackend>> = vec![Box::new(ScalarBackend)];
    for k in [BackendKind::Neon, BackendKind::Sve2] {
        if let Ok(b) = ankaa_simd::dispatch::make(k) {
            v.push(b);
        }
    }
    // the per-kernel mix the engine runs with
    v.push(ankaa_simd::dispatch::best());
    v
}

fn special_f32() -> Vec<f32> {
    vec![
        0.0,
        -0.0,
        1.0,
        -1.0,
        f32::NAN,
        -f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::MIN_POSITIVE,
        -f32::MIN_POSITIVE,
        1.0e-40, // denormal
        -1.0e-40,
        f32::MAX,
        f32::MIN,
        f32::EPSILON,
        123.456,
    ]
}

fn arb_f32() -> impl Strategy<Value = f32> {
    prop_oneof![
        8 => any::<f32>(),
        2 => prop::sample::select(special_f32()),
        2 => -1000.0f32..1000.0,
    ]
}

fn arb_i64() -> impl Strategy<Value = i64> {
    prop_oneof![
        6 => any::<i64>(),
        2 => prop::sample::select(vec![0i64, 1, -1, i64::MAX, i64::MIN, 1_000_000_000_000]),
        2 => -1_000i64..1_000,
    ]
}

/// Float reductions are compared relative to the **sum of magnitudes** (the condition number of
/// summation), not to the result: with mixed-sign inputs near `f32::MAX` the true sum can cancel to
/// almost zero while legitimately different summation orders differ by many ulps of the partials.
#[allow(clippy::float_cmp)]
fn approx_eq_sum(a: f64, b: f64, inputs: &[f32]) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a.is_infinite() || b.is_infinite() {
        return a == b;
    }
    let sum_abs: f64 = inputs.iter().map(|x| f64::from(x.abs())).sum();
    let scale = sum_abs.max(a.abs()).max(b.abs()).max(1.0);
    (a - b).abs() <= REL_TOL * scale
}

#[allow(clippy::float_cmp)]
fn f32_eq_or_both_nan(a: f32, b: f32) -> bool {
    (a.is_nan() && b.is_nan()) || a == b
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn filter_range_f32_matches_scalar(
        data in prop::collection::vec(arb_f32(), 0..=VECTOR_LEN),
        lo in arb_f32(), hi in arb_f32(),
    ) {
        let (min, max) = if lo <= hi { (lo, hi) } else { (hi, lo) };
        let mut want = SelVec::new();
        ScalarBackend.filter_range_f32(&data, min, max, &mut want);
        for b in backends() {
            let mut got = SelVec::new();
            let n = b.filter_range_f32(&data, min, max, &mut got);
            prop_assert_eq!(n, want.len(), "{} count", b.kind());
            prop_assert_eq!(got.as_slice(), want.as_slice(), "{} indices", b.kind());
        }
    }

    #[test]
    fn filter_range_i64_matches_scalar(
        data in prop::collection::vec(arb_i64(), 0..=VECTOR_LEN),
        lo in arb_i64(), hi in arb_i64(),
    ) {
        let (min, max) = if lo <= hi { (lo, hi) } else { (hi, lo) };
        let mut want = SelVec::new();
        ScalarBackend.filter_range_i64(&data, min, max, &mut want);
        for b in backends() {
            let mut got = SelVec::new();
            b.filter_range_i64(&data, min, max, &mut got);
            prop_assert_eq!(got.as_slice(), want.as_slice(), "{}", b.kind());
        }
    }

    #[test]
    fn filter_range_unsorted_bounds_match_scalar(
        f in prop::collection::vec(arb_f32(), 0..=VECTOR_LEN),
        i in prop::collection::vec(arb_i64(), 0..=VECTOR_LEN),
        a in arb_f32(), b in arb_f32(), c in arb_i64(), d in arb_i64(),
    ) {
        let mut want = SelVec::new();
        ScalarBackend.filter_range_f32(&f, a, b, &mut want);
        for bk in backends() {
            let mut got = SelVec::new();
            bk.filter_range_f32(&f, a, b, &mut got);
            prop_assert_eq!(got.as_slice(), want.as_slice(), "{} f32", bk.kind());
        }
        ScalarBackend.filter_range_i64(&i, c, d, &mut want);
        for bk in backends() {
            let mut got = SelVec::new();
            bk.filter_range_i64(&i, c, d, &mut got);
            prop_assert_eq!(got.as_slice(), want.as_slice(), "{} i64", bk.kind());
        }
    }

    #[test]
    fn gather_matches_scalar(data in prop::collection::vec(arb_f32(), 1..=VECTOR_LEN), seed in any::<u64>()) {
        // Build a selection of roughly half the rows.
        let mut sel = SelVec::new();
        let mut s = seed;
        for i in 0..data.len() {
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            if s & 1 == 1 { sel.push(i as u16); }
        }
        let mut want = vec![0f32; sel.len()];
        ScalarBackend.gather_f32(&data, &sel, &mut want);
        for b in backends() {
            let mut got = vec![0f32; sel.len()];
            b.gather_f32(&data, &sel, &mut got);
            prop_assert!(want.iter().zip(&got).all(|(a, c)| f32_eq_or_both_nan(*a, *c)), "{}", b.kind());
        }
    }

    #[test]
    fn sum_f32_within_tolerance(data in prop::collection::vec(arb_f32(), 0..=VECTOR_LEN)) {
        let want = ScalarBackend.sum_f32(&data);
        for b in backends() {
            let got = b.sum_f32(&data);
            prop_assert!(approx_eq_sum(want, got, &data), "{}: want {want} got {got}", b.kind());
        }
    }

    #[test]
    fn sum_i64_exact(data in prop::collection::vec(arb_i64(), 0..=VECTOR_LEN)) {
        let want = ScalarBackend.sum_i64(&data);
        for b in backends() {
            prop_assert_eq!(b.sum_i64(&data), want, "{}", b.kind());
        }
    }

    #[test]
    fn min_max_f32_matches(data in prop::collection::vec(arb_f32(), 0..=VECTOR_LEN)) {
        let (wmn, wmx) = ScalarBackend.min_max_f32(&data);
        for b in backends() {
            let (mn, mx) = b.min_max_f32(&data);
            prop_assert!(f32_eq_or_both_nan(mn, wmn), "{} min want {wmn} got {mn}", b.kind());
            prop_assert!(f32_eq_or_both_nan(mx, wmx), "{} max want {wmx} got {mx}", b.kind());
        }
    }

    #[test]
    fn min_max_i64_matches(data in prop::collection::vec(arb_i64(), 0..=VECTOR_LEN)) {
        let want = ScalarBackend.min_max_i64(&data);
        for b in backends() {
            prop_assert_eq!(b.min_max_i64(&data), want, "{}", b.kind());
        }
    }

    #[test]
    fn count_f32_matches(data in prop::collection::vec(arb_f32(), 0..=VECTOR_LEN), v in arb_f32(), w in arb_f32(), which in 0u8..8) {
        let op = match which {
            0 => CmpOp::Lt(v), 1 => CmpOp::Le(v), 2 => CmpOp::Gt(v), 3 => CmpOp::Ge(v),
            4 => CmpOp::Eq(v), 5 => CmpOp::Ne(v), 6 => CmpOp::Range(v.min(w), v.max(w)),
            // unsorted bounds: an empty range unless v <= w
            _ => CmpOp::Range(v, w),
        };
        let want = ScalarBackend.count_f32(&data, op);
        for b in backends() {
            prop_assert_eq!(b.count_f32(&data, op), want, "{} {:?}", b.kind(), op);
        }
    }

    #[test]
    fn count_i64_matches(data in prop::collection::vec(arb_i64(), 0..=VECTOR_LEN), v in arb_i64(), w in arb_i64(), which in 0u8..8) {
        let op = match which {
            0 => CmpOp::Lt(v), 1 => CmpOp::Le(v), 2 => CmpOp::Gt(v), 3 => CmpOp::Ge(v),
            4 => CmpOp::Eq(v), 5 => CmpOp::Ne(v), 6 => CmpOp::Range(v.min(w), v.max(w)),
            // unsorted bounds: an empty range unless v <= w
            _ => CmpOp::Range(v, w),
        };
        let want = ScalarBackend.count_i64(&data, op);
        for b in backends() {
            prop_assert_eq!(b.count_i64(&data, op), want, "{} {:?}", b.kind(), op);
        }
    }
}

#[test]
fn special_values_filter() {
    let data = special_f32();
    let mut want = SelVec::new();
    ScalarBackend.filter_range_f32(&data, -1.0, 1.0, &mut want);
    for b in backends() {
        let mut got = SelVec::new();
        b.filter_range_f32(&data, -1.0, 1.0, &mut got);
        assert_eq!(got, want, "{}", b.kind());
    }
}

#[test]
fn backends_report_kind() {
    for b in backends() {
        let k = b.kind();
        assert!(ankaa_simd::dispatch::is_available(k), "{k}");
    }
    let detected = ankaa_simd::detect();
    assert!(ankaa_simd::dispatch::is_available(detected));
}

/// When `ANKAA_EXPECT_SVE2=<vl_bytes>` is set (CI QEMU matrix), the SVE2 backend must be
/// selected and must report that vector length; this guards against the differential suite
/// silently passing with SVE2 skipped.
#[test]
fn expected_backend_is_present() {
    if let Ok(v) = std::env::var("ANKAA_EXPECT_SVE2") {
        let want_vl: usize = v.parse().expect("ANKAA_EXPECT_SVE2 is the VL in bytes");
        assert!(
            ankaa_simd::dispatch::is_available(BackendKind::Sve2),
            "SVE2 expected but not available"
        );
        // at VL 128 NEON is preferred (measured faster on Neoverse V2); wider vectors pick SVE2
        let want_kind = if want_vl > 16 {
            BackendKind::Sve2
        } else {
            BackendKind::Neon
        };
        assert_eq!(ankaa_simd::detect(), want_kind);
        #[cfg(all(feature = "sve2", target_arch = "aarch64", target_os = "linux"))]
        {
            let b = ankaa_simd::sve2::Sve2Backend::new();
            assert_eq!(b.vl_bytes, want_vl, "rdvl reported a different VL");
            eprintln!(
                "SVE2 backend active, VL = {} bits, ported kernels: {:?}",
                b.vl_bytes * 8,
                ankaa_simd::sve2::PORTED
            );
        }
        #[cfg(not(all(feature = "sve2", target_arch = "aarch64", target_os = "linux")))]
        let _ = want_vl;
    } else {
        eprintln!(
            "backends under test: {:?}",
            backends().iter().map(|b| b.kind()).collect::<Vec<_>>()
        );
    }
}

#[test]
#[should_panic(expected = "slice longer than VECTOR_LEN")]
fn over_long_slice_is_rejected() {
    let data = vec![0.0f32; VECTOR_LEN + 1];
    let mut sel = SelVec::new();
    ScalarBackend.filter_range_f32(&data, -1.0, 1.0, &mut sel);
}

#[test]
fn full_selection_fits_in_slack() {
    let data = vec![0.5f32; VECTOR_LEN];
    for b in backends() {
        let mut sel = SelVec::new();
        assert_eq!(
            b.filter_range_f32(&data, 0.0, 1.0, &mut sel),
            VECTOR_LEN,
            "{}",
            b.kind()
        );
        assert_eq!(
            sel.as_slice().last().copied(),
            Some((VECTOR_LEN - 1) as u16)
        );
    }
}

/// Every length from 0 to 300: crosses the four-vector block and the single-vector tail at
/// VL 128, 256 and 512 (block = 16/32/64 f32 lanes, 8/16/32 i64 lanes) for every kernel.
#[test]
fn every_length_up_to_300_matches_scalar() {
    let f: Vec<f32> = (0..300)
        .map(|i| match i % 37 {
            5 => f32::NAN,
            11 => f32::INFINITY,
            17 => -0.0,
            _ => ((i * 7919) % 201) as f32 - 100.0,
        })
        .collect();
    let v: Vec<i64> = (0..300)
        .map(|i| match i % 41 {
            3 => i64::MIN,
            7 => i64::MAX,
            _ => (i64::from(i) * 7919) % 201 - 100,
        })
        .collect();
    let s = ScalarBackend;
    for n in 0..=300 {
        let (f, v) = (&f[..n], &v[..n]);
        for b in backends() {
            let k = b.kind();
            let (mut want, mut got) = (SelVec::new(), SelVec::new());
            s.filter_range_f32(f, -50.0, 50.0, &mut want);
            b.filter_range_f32(f, -50.0, 50.0, &mut got);
            assert_eq!(got, want, "{k} filter_f32 n={n}");
            s.filter_range_i64(v, -50, 50, &mut want);
            b.filter_range_i64(v, -50, 50, &mut got);
            assert_eq!(got, want, "{k} filter_i64 n={n}");
            assert!(
                approx_eq_sum(s.sum_f32(f), b.sum_f32(f), f),
                "{k} sum_f32 n={n}"
            );
            assert_eq!(b.sum_i64(v), s.sum_i64(v), "{k} sum_i64 n={n}");
            let (wmn, wmx) = s.min_max_f32(f);
            let (mn, mx) = b.min_max_f32(f);
            assert!(
                f32_eq_or_both_nan(mn, wmn) && f32_eq_or_both_nan(mx, wmx),
                "{k} min_max_f32 n={n}"
            );
            assert_eq!(b.min_max_i64(v), s.min_max_i64(v), "{k} min_max_i64 n={n}");
            for op in [
                CmpOp::Gt(0.0),
                CmpOp::Ne(0.0),
                CmpOp::Range(-50.0, 50.0),
                CmpOp::Range(1.0, -1.0),
            ] {
                assert_eq!(
                    b.count_f32(f, op),
                    s.count_f32(f, op),
                    "{k} count_f32 {op:?} n={n}"
                );
            }
            for op in [
                CmpOp::Gt(0),
                CmpOp::Eq(-100),
                CmpOp::Range(-50, 50),
                CmpOp::Range(1, -1),
            ] {
                assert_eq!(
                    b.count_i64(v, op),
                    s.count_i64(v, op),
                    "{k} count_i64 {op:?} n={n}"
                );
            }
        }
    }
}

/// The unsigned-difference range test of the i64 kernels at the edges of the type: a span of
/// the whole range (`max - min` = 2^64 - 1), single-value ranges at both ends, and bounds that
/// straddle zero.
#[test]
fn i64_range_extremes_match_scalar() {
    let v: Vec<i64> = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX]
        .iter()
        .copied()
        .cycle()
        .take(VECTOR_LEN)
        .collect();
    let ranges = [
        (i64::MIN, i64::MAX),
        (i64::MIN, i64::MIN),
        (i64::MAX, i64::MAX),
        (i64::MIN, -1),
        (0, i64::MAX),
        (-1, 1),
        (i64::MAX, i64::MIN),
    ];
    for b in backends() {
        for (lo, hi) in ranges {
            let (mut want, mut got) = (SelVec::new(), SelVec::new());
            ScalarBackend.filter_range_i64(&v, lo, hi, &mut want);
            b.filter_range_i64(&v, lo, hi, &mut got);
            assert_eq!(got, want, "{} filter [{lo}, {hi}]", b.kind());
            let op = CmpOp::Range(lo, hi);
            assert_eq!(
                b.count_i64(&v, op),
                ScalarBackend.count_i64(&v, op),
                "{} count [{lo}, {hi}]",
                b.kind()
            );
        }
    }
}
