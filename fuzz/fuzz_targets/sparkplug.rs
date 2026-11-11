#![no_main]
//! Sparkplug B (MQTT payloads of industrial gateways → rows) must never panic on arbitrary bytes:
//! the decoder, the definitions a BIRTH leaves behind (their encoding must read back equal) and
//! the mapper, whose accepted rows must have the width and the value types of the schema and
//! respect the dictionary cap. The first byte picks the message type and whether the next
//! message reuses the definitions (alias resolution across messages).
use std::collections::HashMap;

use ankaa_ingest::generic::{RowMapper, TsUnit};
use ankaa_ingest::sparkplug::{self, Definitions};
use ankaa_store::{ColumnType, Field, Schema, Value};
use libfuzzer_sys::fuzz_target;

const DICT_CAP: usize = 8;
const KINDS: [&str; 6] = ["NBIRTH", "DBIRTH", "NDATA", "DDATA", "NDEATH", "NCMD"];

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("metric", ColumnType::DictU32),
        Field::new("device", ColumnType::DictU32),
        Field::nullable("value", ColumnType::Float64),
        Field::nullable("n", ColumnType::Int64),
        Field::nullable("px", ColumnType::FixedPoint { scale: 4 }),
        Field::new("hist", ColumnType::U8),
    ])
    .expect("static schema")
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    let _ = sparkplug::header(rest);
    let _ = sparkplug::for_each_metric(rest, |_| {});
    let map: HashMap<String, String> = [
        ("ts", "$timestamp"),
        ("metric", "$metric"),
        ("device", "$device"),
        ("value", "$value"),
        ("n", "$value"),
        ("px", "$value"),
        ("hist", "$historical"),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    let mut m = RowMapper::new(schema(), &map, false, TsUnit::Millis, true, Some("metric"))
        .expect("static map")
        .with_max_dict_entries(DICT_CAP)
        .with_topic_prefix(true)
        .with_sparkplug(true)
        .expect("sparkplug selectors");
    let mut dicts = RowMapper::dictionaries_for(&schema());
    // the same bytes as two messages: a BIRTH-like one, then a DATA one that may use its aliases
    let first = KINDS[usize::from(mode) % KINDS.len()];
    let second = KINDS[usize::from(mode >> 4) % KINDS.len()];
    for kind in [first, second] {
        let mut msg = format!("spBv1.0/g/{kind}/n/d").into_bytes();
        msg.push(0);
        msg.extend_from_slice(rest);
        let Ok(n) = m.parse(&msg, 1_000_000_000, false, &dicts) else {
            continue;
        };
        for r in 0..n {
            if m.parsed(r).is_ok() {
                let (_, row) = m.finish_row(r, &mut dicts);
                assert_eq!(row.len(), 7);
                assert!(matches!(row[0], Value::I64(_)));
                assert!(matches!(row[1], Value::U32(k) if (k as usize) < DICT_CAP));
                assert!(matches!(row[3], Value::F64(x) if x.is_finite()) || row[3] == Value::Null);
                assert!(matches!(row[6], Value::U8(0 | 1)));
            }
        }
        for d in dicts.iter().flatten() {
            assert!(d.len() <= DICT_CAP);
        }
    }
    if let Some(defs) = m.sparkplug_definitions() {
        let mut buf = Vec::new();
        defs.encode(&mut buf);
        let back = Definitions::decode(&buf).expect("own encoding reads back");
        let mut again = Vec::new();
        back.encode(&mut again);
        assert_eq!(buf, again);
    }
    let _ = Definitions::decode(rest);
});
