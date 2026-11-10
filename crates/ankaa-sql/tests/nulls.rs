//! Readings with missing values (`NULL`): a chunk table with nullable columns, its chunks in
//! every tier, must answer exactly like `DataFusion`'s own in-memory table built from the same
//! rows, in a session with every ANKAA rule.

use std::sync::Arc;

use ankaa_sql::{ChunkTable, default_session};
use ankaa_store::{Chunk, ChunkId, ChunkRef, ColumnType, Field, Schema, Value};
use ankaa_tier::Encoder;
use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, TimestampNanosecondArray, UInt8Array,
    UInt32Array,
};
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("device", ColumnType::DictU32),
        Field::nullable("temp", ColumnType::Float64),
        Field::nullable("energy", ColumnType::Int64),
        Field::nullable("state", ColumnType::U8),
        Field::new("seq", ColumnType::Int64),
    ])
    .unwrap()
}

/// Row `i`: some columns missing on a pattern, one chunk (the third) with no missing value.
fn row(i: i64, dense: bool) -> [Value; 6] {
    let gap = |m: i64| !dense && i % m == 0;
    #[allow(clippy::cast_precision_loss)]
    let temp = if gap(5) {
        Value::Null
    } else {
        Value::F64((i % 300) as f64 / 10.0)
    };
    [
        Value::I64(i * 1_000),
        Value::U32(u32::try_from(i % 4).unwrap()),
        temp,
        if gap(7) {
            Value::Null
        } else {
            Value::I64(i % 1_000)
        },
        if gap(3) {
            Value::Null
        } else {
            Value::U8(u8::try_from(i % 2).unwrap())
        },
        Value::I64(i),
    ]
}

fn chunk(id: u64, from: i64, rows: i64, dense: bool) -> Chunk {
    let mut c = Chunk::new(ChunkId(id), schema());
    for i in from..from + rows {
        c.append_row(&row(i, dense)).unwrap();
    }
    c.seal();
    c
}

/// The same rows as one Arrow batch with null bitmaps.
fn reference(ranges: &[(i64, i64, bool)]) -> RecordBatch {
    let (mut ts, mut dev, mut temp, mut energy, mut state, mut seq) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for &(from, n, dense) in ranges {
        for i in from..from + n {
            let cells = row(i, dense);
            let (Value::I64(stamp), Value::U32(device), Value::I64(n)) =
                (&cells[0], &cells[1], &cells[5])
            else {
                unreachable!()
            };
            ts.push(*stamp);
            dev.push(*device);
            seq.push(*n);
            temp.push(match cells[2] {
                Value::F64(x) => Some(x),
                _ => None,
            });
            energy.push(match cells[3] {
                Value::I64(x) => Some(x),
                _ => None,
            });
            state.push(match cells[4] {
                Value::U8(x) => Some(x),
                _ => None,
            });
        }
    }
    let arrow_schema = ankaa_sql::arrow_conv::arrow_schema(&schema());
    let cols: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")),
        Arc::new(UInt32Array::from(dev)),
        Arc::new(Float64Array::from(temp)),
        Arc::new(Int64Array::from(energy)),
        Arc::new(UInt8Array::from(state)),
        Arc::new(Int64Array::from(seq)),
    ];
    RecordBatch::try_new(arrow_schema, cols).unwrap()
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let out = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut lines = Vec::new();
    for b in &out {
        for r in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let col = b.column(c);
                    if col.is_null(r) {
                        "NULL".to_owned()
                    } else if col.data_type() == &DataType::Float64 {
                        format!("{:.6e}", col.as_primitive::<Float64Type>().value(r))
                    } else {
                        arrow::util::display::array_value_to_string(col, r).unwrap()
                    }
                })
                .collect();
            lines.push(cells.join("|"));
        }
    }
    lines.sort();
    lines
}

#[tokio::test]
async fn nullable_columns_answer_like_datafusion() {
    let enc = Arc::new(Encoder::new());
    let dir = tempfile::tempdir().unwrap();
    let ranges = [
        (0, 6_000, false),
        (6_000, 6_000, false),
        (12_000, 6_000, true),
        (18_000, 6_000, false),
    ];
    let chunks: Vec<Chunk> = ranges
        .iter()
        .zip(1..)
        .map(|(&(from, n, dense), id)| chunk(id, from, n, dense))
        .collect();
    let mut refs = vec![ChunkRef::Hot(Arc::new(chunks[0].clone()))];
    refs.push(ChunkRef::Encoded(
        enc.encode_warm("t", 0, &chunks[1]).unwrap(),
    ));
    refs.push(ChunkRef::Encoded(
        enc.write_cold(dir.path(), &enc.encode_warm("t", 0, &chunks[2]).unwrap())
            .unwrap(),
    ));
    refs.push(ChunkRef::Encoded(
        enc.write_cold(dir.path(), &enc.encode_warm("t", 0, &chunks[3]).unwrap())
            .unwrap(),
    ));

    let ankaa = default_session();
    ankaa
        .register_table("t", Arc::new(ChunkTable::from_shared(schema(), refs)))
        .unwrap();
    let plain = SessionContext::new();
    let batch = reference(&ranges);
    plain
        .register_table(
            "t",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();

    for q in [
        "SELECT count(*), count(temp), count(energy), count(state) FROM t",
        "SELECT sum(energy), min(energy), max(energy), avg(temp), min(temp), max(temp) FROM t",
        "SELECT device, count(temp), sum(energy), max(state) FROM t GROUP BY device",
        "SELECT state, count(*), sum(energy) FROM t GROUP BY state",
        "SELECT device, state, count(*) FROM t GROUP BY device, state",
        "SELECT date_bin(interval '1 millisecond', ts) AS b, count(temp), sum(energy), avg(temp) FROM t GROUP BY b",
        "SELECT count(*) FROM t WHERE temp IS NULL",
        "SELECT count(*) FROM t WHERE energy IS NOT NULL AND ts >= to_timestamp_nanos(5000000) AND ts < to_timestamp_nanos(19000000)",
        "SELECT sum(energy), count(energy) FROM t WHERE ts >= to_timestamp_nanos(12000000) AND ts < to_timestamp_nanos(18000000)",
        "SELECT sum(energy), max(energy) FROM t WHERE ts BETWEEN to_timestamp_nanos(3000000) AND to_timestamp_nanos(20000000)",
        "SELECT seq, temp, energy, state FROM t WHERE seq % 997 = 0",
        "SELECT seq, temp FROM t ORDER BY ts DESC LIMIT 7",
        "SELECT device, last_value(temp ORDER BY ts) FROM t GROUP BY device",
    ] {
        assert_eq!(rows(&ankaa, q).await, rows(&plain, q).await, "{q}");
    }
}
