//! `gapfill` against `DataFusion`'s own engine: every measured bucket must equal
//! `date_bin` + `GROUP BY` over the same rows, and every filled bucket must be what its method
//! defines (`NULL`, the previous measured value, or the point on the line between the measured
//! neighbours). Random sparse series, several keys, `NULL` readings, keys interleaved row by row
//! (short runs) or stored one chunk per key (long runs).
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]

use std::collections::BTreeMap;

use ankaa_sql::{register_chunks, register_functions};
use ankaa_store::{Chunk, ChunkId, ColumnType, Field, Schema, Value};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{Float64Type, Int64Type, TimestampNanosecondType, UInt32Type};
use datafusion::prelude::*;
use proptest::prelude::*;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("dev", ColumnType::DictU32),
        Field::nullable("v", ColumnType::Float64),
        Field::nullable("p", ColumnType::FixedPoint { scale: 2 }),
    ])
    .unwrap()
}

/// `(ts, dev, v)` rows, in time order.
fn ctx(rows: &[(i64, u32, Option<f64>)], one_chunk_per_key: bool) -> SessionContext {
    let ctx = SessionContext::new();
    let mut chunks: BTreeMap<u32, Chunk> = BTreeMap::new();
    for &(ts, dev, v) in rows {
        let slot = if one_chunk_per_key { dev } else { 0 };
        let c = chunks
            .entry(slot)
            .or_insert_with(|| Chunk::new(ChunkId(u64::from(slot)), schema()));
        let p = v.map_or(Value::Null, |x| Value::I64((x * 100.0).round() as i64));
        c.append_row(&[
            Value::I64(ts),
            Value::U32(dev),
            v.map_or(Value::Null, Value::F64),
            p,
        ])
        .unwrap();
    }
    register_chunks(&ctx, "t", schema(), chunks.into_values().collect()).unwrap();
    register_functions(&ctx);
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

/// `(dev, bucket) → (value, filled)` from a gapfill result (key column first).
fn gapfilled(batches: &[RecordBatch]) -> BTreeMap<(u32, i64), (Option<f64>, bool)> {
    let mut out = BTreeMap::new();
    for b in batches {
        let dev = b.column(0).as_primitive::<UInt32Type>();
        let bucket = b.column(1).as_primitive::<TimestampNanosecondType>();
        let value = b.column(2).as_primitive::<Float64Type>();
        let filled = b.column(3).as_boolean();
        for i in 0..b.num_rows() {
            let v = value.is_valid(i).then(|| value.value(i));
            out.insert((dev.value(i), bucket.value(i)), (v, filled.value(i)));
        }
    }
    out
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 * (1.0 + a.abs().max(b.abs()))
}

/// Readings: up to 4 devices, sparse in time (gaps of up to 40 buckets), some without value.
fn rows_strategy() -> impl Strategy<Value = Vec<(i64, u32, Option<f64>)>> {
    proptest::collection::vec(
        (
            0i64..40_000,
            0u32..4,
            proptest::option::weighted(0.85, -1000.0f64..1000.0),
        ),
        1..300,
    )
    .prop_map(|mut v| {
        // whole hundredths, so the fixed-point copy is exact
        for r in &mut v {
            r.2 = r.2.map(|x| (x * 100.0).round() / 100.0);
        }
        v.sort_by_key(|r| r.0);
        v
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn measured_buckets_match_datafusion_and_fills_follow_their_method(
        rows in rows_strategy(),
        width_us in prop_oneof![Just(1i64), Just(7), Just(1000)],
        agg in prop_oneof![Just("avg"), Just("min"), Just("max"), Just("sum"), Just("count")],
        method in prop_oneof![Just("null"), Just("locf"), Just("linear")],
        per_key in any::<bool>(),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(async {
            let ctx = ctx(&rows, per_key);
            let width = width_us * 1000;
            let got = gapfilled(&run(&ctx, &format!(
                "SELECT * FROM gapfill('t', 'v', {width}, 'dev', '{agg}', '{method}')"
            )).await);
            // the oracle: DataFusion's own grouping over the non-NULL readings
            let oracle = run(&ctx, &format!(
                "SELECT dev, CAST(date_bin(INTERVAL '{width_us} microseconds', ts) AS BIGINT) AS b, \
                 CAST({agg}(v) AS DOUBLE) AS x FROM t WHERE v IS NOT NULL GROUP BY dev, b"
            )).await;
            let mut want: BTreeMap<(u32, i64), f64> = BTreeMap::new();
            for b in &oracle {
                let (d, k, x) = (
                    b.column(0).as_primitive::<UInt32Type>(),
                    b.column(1).as_primitive::<Int64Type>(),
                    b.column(2).as_primitive::<Float64Type>(),
                );
                for i in 0..b.num_rows() {
                    want.insert((d.value(i), k.value(i)), x.value(i));
                }
            }
            // every measured bucket is the oracle's, and nothing the oracle has is missing
            let measured: BTreeMap<_, _> = got.iter().filter(|(_, (_, f))| !*f).collect();
            assert_eq!(measured.len(), want.len());
            for ((d, b), x) in &want {
                let (v, filled) = got[&(*d, *b)];
                assert!(!filled);
                assert!(close(v.unwrap(), *x), "{d} {b}: {v:?} vs {x}");
            }
            // per key: contiguous buckets from the first to the last measured one
            for d in 0..4u32 {
                let keys: Vec<i64> = got.keys().filter(|(k, _)| *k == d).map(|(_, b)| *b).collect();
                assert!(keys.windows(2).all(|w| w[1] - w[0] == width));
                let ms: Vec<(i64, f64)> = want.iter().filter(|((k, _), _)| *k == d).map(|((_, b), x)| (*b, *x)).collect();
                for &b in &keys {
                    let (v, filled) = got[&(d, b)];
                    if !filled {
                        continue;
                    }
                    let prev = ms.iter().rev().find(|(mb, _)| *mb < b);
                    let next = ms.iter().find(|(mb, _)| *mb > b);
                    let expect = match (agg, method) {
                        ("count", _) => Some(0.0),
                        (_, "null") => None,
                        (_, "locf") => prev.map(|p| p.1),
                        _ => match (prev, next) {
                            (Some(p), Some(n)) => Some(p.1 + (n.1 - p.1) * ((b - p.0) as f64 / (n.0 - p.0) as f64)),
                            _ => None,
                        },
                    };
                    match (v, expect) {
                        (Some(a), Some(e)) => assert!(close(a, e), "{d} {b}: {a} vs {e}"),
                        (a, e) => assert_eq!(a, e, "{d} {b}"),
                    }
                }
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_fixed_point_values_and_errors() {
    // one device: readings in buckets 0, 3 (two) and 5 of 1 µs; nothing elsewhere
    let rows = [
        (100, 0, Some(1.5)),
        (3_100, 0, Some(2.0)),
        (3_900, 0, Some(4.0)),
        (5_000, 0, Some(10.0)),
    ];
    let ctx = ctx(&rows, false);
    // a range wider than the data: leading and trailing buckets, every key present
    let got = gapfilled(
        &run(
            &ctx,
            "SELECT * FROM gapfill('t', 'p', 1000, 'dev', 'avg', 'locf', -2000, 8000)",
        )
        .await,
    );
    let values: Vec<Option<f64>> = got.values().map(|(v, _)| *v).collect();
    // fixed-point column `p` (scale 2) comes out as decimals
    assert_eq!(
        values,
        vec![
            None,
            None,
            Some(1.5),
            Some(1.5),
            Some(1.5),
            Some(3.0),
            Some(3.0),
            Some(10.0),
            Some(10.0),
            Some(10.0)
        ]
    );
    assert_eq!(got.keys().next(), Some(&(0, -2000)));
    assert_eq!(got.keys().last(), Some(&(0, 7000)));
    // without a key: one series, three columns
    let b = run(&ctx, "SELECT * FROM gapfill('t', 'v', 1000)").await;
    assert_eq!(b[0].num_columns(), 3);
    assert_eq!(b.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    // SQL around it: filter and aggregate the output
    let b = run(
        &ctx,
        "SELECT count(*) FROM gapfill('t', 'v', 1000, 'dev', 'avg', 'linear') WHERE filled",
    )
    .await;
    assert_eq!(b[0].column(0).as_primitive::<Int64Type>().value(0), 3);
    for (sql, err) in [
        ("SELECT * FROM gapfill('t', 'v', 0)", "positive"),
        (
            "SELECT * FROM gapfill('t', 'v', 1000, 'dev', 'median')",
            "not one of",
        ),
        (
            "SELECT * FROM gapfill('t', 'v', 1000, 'dev', 'avg', 'spline')",
            "not one of",
        ),
        (
            "SELECT * FROM gapfill('t', 'v', 1000, 'dev', 'avg', 'null', 5)",
            "together",
        ),
        (
            "SELECT * FROM gapfill('t', 'v', 1, 'dev', 'avg', 'null', 0, 100000000000)",
            "buckets",
        ),
        ("SELECT * FROM gapfill('t', 'nope', 1000)", "nope"),
    ] {
        let e = match ctx.sql(sql).await {
            Ok(df) => df.collect().await.unwrap_err().to_string(),
            Err(e) => e.to_string(),
        };
        assert!(e.contains(err), "{sql}: {e}");
    }
}
