#![no_main]
//! `InfluxDB` line protocol (what Telegraf and the `InfluxDB` client libraries write → rows) must
//! never panic on arbitrary bytes: the line splitter, the parser (whose spans must lie inside
//! the line they came from, and decode) and the mapper, whose accepted rows must have the width
//! and the value types of the schema and respect the dictionary cap. The first byte picks the
//! timestamp unit, the receive-time fallback, the measurement filter and the MQTT topic prefix.
use std::collections::HashMap;

use ankaa_ingest::generic::{LineProtocol, RowMapper, TsUnit};
use ankaa_ingest::line_protocol::{self, Escapes, Kind, Line, Lines};
use ankaa_store::{ColumnType, Field, Schema, Value};
use libfuzzer_sys::fuzz_target;

const DICT_CAP: usize = 8;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("host", ColumnType::DictU32),
        Field::new("m", ColumnType::DictU32),
        Field::nullable("v", ColumnType::Float64),
        Field::nullable("n", ColumnType::Int64),
        Field::nullable("px", ColumnType::FixedPoint { scale: 8 }),
        Field::nullable("up", ColumnType::U8),
        Field::nullable("at", ColumnType::TimestampNs),
    ])
    .expect("static schema")
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, msg)) = data.split_first() else {
        return;
    };
    // the parser on its own
    let mut line = Line::default();
    let mut text = String::new();
    let mut last_end = 0usize;
    for chunk in Lines::new(msg) {
        let bytes = chunk.bytes(msg);
        assert!(!bytes.is_empty());
        let _ = line_protocol::measurement(msg, chunk);
        if line_protocol::parse(msg, chunk, &mut line).is_err() {
            continue;
        }
        assert!(std::str::from_utf8(bytes).is_ok());
        assert!(!line.fields.is_empty());
        assert!(!line.measurement.raw(msg).is_empty());
        line_protocol::unescape(
            line.measurement.as_str(msg),
            Escapes::Measurement,
            &mut text,
        );
        // spans follow each other inside the message
        let mut at = line.measurement.end();
        for (k, v) in &line.tags {
            assert!(at < k.start() && k.start() < k.end() && k.end() < v.start());
            assert!(v.start() < v.end());
            at = v.end();
            line_protocol::unescape(k.as_str(msg), Escapes::Key, &mut text);
            line_protocol::unescape(v.as_str(msg), Escapes::Key, &mut text);
        }
        for (k, v) in &line.fields {
            assert!(at < k.start() && k.start() < k.end() && k.end() < v.start());
            assert!(v.start() <= v.end());
            at = v.end();
            let raw = v.as_str(msg);
            match v.kind {
                Kind::Float => assert!(raw.parse::<f64>().is_ok()),
                Kind::Int => assert!(raw.parse::<i64>().is_ok()),
                Kind::UInt => assert!(raw.parse::<u64>().is_ok()),
                Kind::True | Kind::False => assert!(!raw.is_empty()),
                Kind::Str => assert!(!raw.contains('"')),
                Kind::StrEscaped => line_protocol::unescape(raw, Escapes::String, &mut text),
            }
        }
        assert!(at as usize <= msg.len());
        last_end = at as usize;
    }
    assert!(last_end <= msg.len());

    // the mapper
    let unit = match mode & 3 {
        0 => TsUnit::Seconds,
        1 => TsUnit::Millis,
        2 => TsUnit::Micros,
        _ => TsUnit::Nanos,
    };
    let from_receive = mode & 4 != 0;
    let with_topic = mode & 8 != 0;
    let settings = LineProtocol {
        measurement: (mode & 16 != 0).then(|| "m".to_owned()),
    };
    let mut map: HashMap<String, String> = [("m", "$measurement"), ("v", "value"), ("n", "a b")]
        .into_iter()
        .map(|(a, b)| (a.to_owned(), b.to_owned()))
        .collect();
    if with_topic {
        map.insert("host".to_owned(), "$topic.1".to_owned());
    }
    let schema = schema();
    let mut m = RowMapper::new(
        schema.clone(),
        &map,
        false,
        unit,
        from_receive,
        Some("host"),
    )
    .expect("static map")
    .with_max_dict_entries(DICT_CAP)
    .with_topic_prefix(with_topic)
    .with_line_protocol(Some(&settings))
    .expect("line protocol mapping");
    let mut dicts = RowMapper::dictionaries_for(&schema);
    let mut framed = b"plant/a\0".to_vec();
    framed.extend_from_slice(msg);
    let input: &[u8] = if with_topic { &framed } else { msg };
    // the same message twice: dictionary state carries over, nothing else does
    let mut rows = [0usize; 2];
    for n_rows in &mut rows {
        let Ok(n) = m.parse(input, 1_700_000_000_000_000_000, false, &dicts) else {
            continue;
        };
        *n_rows = n;
        for r in 0..n {
            if m.parsed(r).is_err() {
                continue;
            }
            let (key, row) = m.finish_row(r, &mut dicts);
            assert_eq!(row.len(), schema.fields().len());
            for (v, f) in row.iter().zip(schema.fields()) {
                let ok = match (f.ty, v) {
                    (_, Value::Null) => f.nullable,
                    (
                        ColumnType::TimestampNs | ColumnType::FixedPoint { .. } | ColumnType::Int64,
                        Value::I64(_),
                    )
                    | (ColumnType::DictU32, Value::U32(_))
                    | (ColumnType::U8, Value::U8(_)) => true,
                    (ColumnType::Float64, Value::F64(x)) => x.is_finite(),
                    _ => false,
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
    assert_eq!(
        rows[0], rows[1],
        "the number of rows depends on the message alone"
    );
});
