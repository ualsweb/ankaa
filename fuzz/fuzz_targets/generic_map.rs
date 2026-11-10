#![no_main]
//! The generic row mapper (JSON and CSV messages from NATS, Kafka and files → rows) must never
//! panic on arbitrary bytes, must respect the dictionary cap, and a row it accepts must have the
//! width and the value types of the table schema.
use std::collections::HashMap;

use ankaa_ingest::generic::{RowMapper, TsUnit, parse_rfc3339_ns};
use ankaa_store::{ColumnType, Field, Schema, Value};
use libfuzzer_sys::fuzz_target;

const DICT_CAP: usize = 8;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("key", ColumnType::DictU32),
        Field::new("px", ColumnType::FixedPoint { scale: 8 }),
        Field::new("n", ColumnType::Int64),
        Field::new("flag", ColumnType::U8),
        Field::new("load", ColumnType::Float64),
        Field::new("tag", ColumnType::DictU32),
    ])
    .expect("static schema")
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, msg)) = data.split_first() else {
        return;
    };
    let csv = mode & 1 == 1;
    let unit = match (mode >> 1) % 5 {
        0 => TsUnit::Seconds,
        1 => TsUnit::Millis,
        2 => TsUnit::Micros,
        3 => TsUnit::Nanos,
        _ => TsUnit::Rfc3339,
    };
    let from_receive = mode & 0x20 != 0;
    let cols: &[(&str, &str, &str)] = &[
        ("ts", "t", "0"),
        ("key", "k.id", "1"),
        ("px", "v.0", "2"),
        ("n", "n", "3"),
        ("flag", "f", "4"),
        ("load", "l", "5"),
        ("tag", "g", "6"),
    ];
    let map: HashMap<String, String> = cols
        .iter()
        .filter(|(c, _, _)| !(from_receive && *c == "ts"))
        .map(|(c, json, idx)| ((*c).to_owned(), (if csv { *idx } else { *json }).to_owned()))
        .collect();
    let schema = schema();
    let Ok(mapper) = RowMapper::new(schema.clone(), &map, csv, unit, from_receive, Some("key"))
    else {
        return;
    };
    let mut mapper = mapper.with_max_dict_entries(DICT_CAP);
    let mut dicts = RowMapper::dictionaries_for(&schema);
    // the same message twice, then split into lines: dictionary state carries over
    let lines = msg.split(|&b| b == b'\n');
    for m in [msg, msg].into_iter().chain(lines) {
        if let Ok((key, row)) = mapper.map(m, 1_700_000_000_000_000_000, csv, &mut dicts) {
            assert_eq!(row.len(), schema.fields().len());
            for (v, f) in row.iter().zip(schema.fields()) {
                let ok = match f.ty {
                    ColumnType::TimestampNs | ColumnType::FixedPoint { .. } | ColumnType::Int64 => {
                        matches!(v, Value::I64(_))
                    }
                    ColumnType::DictU32 => matches!(v, Value::U32(_)),
                    ColumnType::U8 => matches!(v, Value::U8(_)),
                    ColumnType::Float64 => matches!(v, Value::F64(x) if x.is_finite()),
                };
                assert!(ok, "{v:?} in a {:?} column", f.ty);
            }
            assert_eq!(Value::U32(key), row[1]);
            assert!((key as usize) < DICT_CAP);
        }
        for d in dicts.iter().flatten() {
            assert!(d.len() <= DICT_CAP);
        }
    }
    if let Ok(text) = std::str::from_utf8(msg) {
        let _ = parse_rfc3339_ns(text);
    }
});
