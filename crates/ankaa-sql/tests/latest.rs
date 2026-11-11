//! `latest(table, key)`: the newest row of every key value, checked against a plain pass over
//! the rows, for chunks in every tier, chunks that hold one key value and chunks that mix them.
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use std::collections::BTreeMap;
use std::sync::Arc;

use ankaa_sql::{ChunkTable, Names};
use ankaa_store::{Chunk, ChunkId, ChunkRef, ColumnType, Field, Schema, Value};
use ankaa_tier::Encoder;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int64Type, TimestampNanosecondType};
use datafusion::prelude::SessionContext;

const DEVICES: [&str; 7] = [
    "press01", "press02", "robot01", "robot02", "oven01", "agv07", "idle",
];
const LINES: [&str; 3] = ["line1", "line2", "line3"];

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("device", ColumnType::DictU32),
        Field::new("line", ColumnType::DictU32),
        Field::nullable("kw", ColumnType::Float64),
        Field::new("n", ColumnType::Int64),
    ])
    .unwrap()
}

/// Which tier chunk `i` is put in (0 hot, 1 warm, 2 cold).
type Tier = fn(usize) -> u8;

/// `(ts, device, line, kw, n)`; `n` is the order the rows were stored in.
type Row = (i64, u32, u32, Option<f64>, i64);

/// Rows per device, in time order. Device 3 ends with three rows at the same timestamp, device
/// 4 reports last long before the others, the last device never reports.
fn rows() -> Vec<Vec<Row>> {
    let mut n = 0i64;
    (0..DEVICES.len() as u32 - 1)
        .map(|d| {
            let count = if d == 4 {
                150
            } else {
                2_500 + 37 * i64::from(d)
            };
            (0..count)
                .map(|i| {
                    n += 1;
                    let mut ts = i * 1_000 + i64::from(d);
                    if d == 3 && i >= count - 3 {
                        ts = (count - 3) * 1_000 + 3;
                    }
                    let kw = (n % 13 != 0).then(|| n as f64 / 8.0);
                    (ts, d, (i as u32 / 700 + d) % 3, kw, n)
                })
                .collect()
        })
        .collect()
}

fn chunk(id: u64, rows: &[Row]) -> Chunk {
    let mut c = Chunk::new(ChunkId(id), schema());
    for r in rows {
        c.append_row(&[
            Value::I64(r.0),
            Value::U32(r.1),
            Value::U32(r.2),
            r.3.map_or(Value::Null, Value::F64),
            Value::I64(r.4),
        ])
        .unwrap();
    }
    c.seal();
    c
}

/// Chunks of the rows: per device (as a table partitioned by device stores them, several chunks
/// each) or everything in time order (chunks that mix devices), each chunk in the tier `tier`
/// picks for it.
fn table(by_device: bool, tier: impl Fn(usize) -> u8, dir: &std::path::Path) -> ChunkTable {
    let data = rows();
    let mut chunks: Vec<Chunk> = Vec::new();
    if by_device {
        for device in &data {
            for part in device.chunks(1_024) {
                chunks.push(chunk(chunks.len() as u64 + 1, part));
            }
        }
    } else {
        let mut all: Vec<Row> = data.iter().flatten().copied().collect();
        all.sort_by_key(|r| (r.0, r.4));
        for part in all.chunks(3_000) {
            chunks.push(chunk(chunks.len() as u64 + 1, part));
        }
    }
    let enc = Arc::new(Encoder::new());
    let refs: Vec<ChunkRef> = chunks
        .into_iter()
        .enumerate()
        .map(|(i, c)| match tier(i) {
            0 => ChunkRef::Hot(Arc::new(c)),
            1 => ChunkRef::Encoded(enc.encode_warm("t", 0, &c).unwrap()),
            _ => ChunkRef::Encoded(
                enc.write_cold(dir, &enc.encode_warm("t", i as u32, &c).unwrap())
                    .unwrap(),
            ),
        })
        .collect();
    ChunkTable::from_shared(schema(), refs).with_names(vec![
        None,
        Some(Names::of(&DEVICES)),
        Some(Names::of(&LINES)),
    ])
}

/// The newest row of each value of column `key` (1 = device, 2 = line) by a plain pass: the
/// greatest timestamp, the last stored among equals.
fn expected(key: usize, by_device: bool) -> BTreeMap<String, (i64, Option<f64>, i64)> {
    let mut all: Vec<Row> = rows().into_iter().flatten().collect();
    if !by_device {
        all.sort_by_key(|r| (r.0, r.4)); // the order the mixed chunks store them in
    }
    let mut best: BTreeMap<String, (i64, Option<f64>, i64)> = BTreeMap::new();
    for r in all {
        let name = if key == 1 {
            DEVICES[r.1 as usize]
        } else {
            LINES[r.2 as usize]
        };
        let e = best.entry(name.to_owned()).or_insert((i64::MIN, None, 0));
        if r.0 >= e.0 {
            *e = (r.0, r.3, r.4);
        }
    }
    best
}

async fn latest(ctx: &SessionContext, sql: &str) -> BTreeMap<String, (i64, Option<f64>, i64)> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut got = BTreeMap::new();
    for b in &batches {
        let key = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
        let key = key.as_string::<i32>();
        let ts = b.column(1).as_primitive::<TimestampNanosecondType>();
        let kw = b.column(2).as_primitive::<Float64Type>();
        let n = b.column(3).as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            let kw = kw.is_valid(i).then(|| kw.value(i));
            let old = got.insert(key.value(i).to_owned(), (ts.value(i), kw, n.value(i)));
            assert!(old.is_none(), "{} twice", key.value(i));
        }
    }
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn latest_is_the_newest_row_of_every_key_in_every_tier() {
    let dir = tempfile::tempdir().unwrap();
    let tiers: [(&str, Tier); 5] = [
        ("hot", |_| 0),
        ("warm", |_| 1),
        ("cold", |_| 2),
        ("mixed tiers", |i| (i % 3) as u8),
        // as a running server has it: old chunks cold, the newest of each device hot
        ("cold then hot", |i| if i % 3 == 2 { 0 } else { 2 }),
    ];
    for by_device in [true, false] {
        for (name, tier) in tiers {
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(table(by_device, tier, dir.path())))
                .unwrap();
            ankaa_sql::register_functions(&ctx);
            let what = format!("{name}, chunks per device: {by_device}");
            assert_eq!(
                latest(&ctx, "SELECT device, ts, kw, n FROM latest('t', 'device')").await,
                expected(1, by_device),
                "{what}"
            );
            // a key the table is not stored by: its values are mixed in the chunks
            assert_eq!(
                latest(&ctx, "SELECT line, ts, kw, n FROM latest('t', 'line')").await,
                expected(2, by_device),
                "{what}"
            );
            // every column, a filter and an aggregate on top
            let all = ctx
                .sql("SELECT * FROM latest('t', 'device') WHERE device <> 'agv07' ORDER BY device")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                all.iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                5,
                "{what}"
            );
            assert_eq!(all[0].num_columns(), 5);
            let n = ctx
                .sql("SELECT count(*) FROM latest('t', 'device')")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                n[0].column(0).as_primitive::<Int64Type>().value(0),
                6,
                "{what}"
            );
        }
    }
}

#[tokio::test]
async fn latest_refuses_what_it_cannot_answer() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table(true, |_| 0, dir.path())))
        .unwrap();
    ankaa_sql::register_functions(&ctx);
    for (sql, why) in [
        ("SELECT * FROM latest('t', 'kw')", "not a dict column"),
        ("SELECT * FROM latest('t', 'nope')", "no column nope"),
        ("SELECT * FROM latest('t')", "no column symbol"),
        ("SELECT * FROM latest('missing', 'device')", "missing"),
        ("SELECT * FROM latest('t', 'device', 3)", "at most two"),
    ] {
        let err = match ctx.sql(sql).await {
            Ok(df) => df.collect().await.map(|_| ()),
            Err(e) => Err(e),
        }
        .expect_err(sql)
        .to_string();
        assert!(err.contains(why), "{sql}: {err}");
    }
    // an empty table has no newest rows
    let empty = ChunkTable::from_shared(schema(), Vec::new());
    ctx.register_table("e", Arc::new(empty)).unwrap();
    let rows = ctx
        .sql("SELECT * FROM latest('e', 'device')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
}
