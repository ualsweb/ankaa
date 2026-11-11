//! Sparkplug B: the zero-copy decoder against a reference protobuf codec (`prost`, with the
//! message types of `sparkplug_b.proto` written out below), robustness on arbitrary bytes, and
//! the mapping of metrics onto table rows (aliases, births, NULLs, rejections).
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use std::collections::HashMap;

use ankaa_ingest::generic::{MapError, RowMapper, TsUnit};
use ankaa_ingest::sparkplug::{self, Definitions, datatype as dt};
use ankaa_ingest::{SymbolDict, SymbolId};
use ankaa_store::{ColumnType, Field, Schema, Value};
use proptest::prelude::*;
use prost::Message as _;

/// `org.eclipse.tahu.protobuf.Payload` (the fields a host reads).
#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(uint64, optional, tag = "1")]
    timestamp: Option<u64>,
    #[prost(message, repeated, tag = "2")]
    metrics: Vec<Metric>,
    #[prost(uint64, optional, tag = "3")]
    seq: Option<u64>,
    #[prost(string, optional, tag = "4")]
    uuid: Option<String>,
    #[prost(bytes = "vec", optional, tag = "5")]
    body: Option<Vec<u8>>,
}

/// `Payload.Metric`.
#[derive(Clone, PartialEq, prost::Message)]
struct Metric {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(uint64, optional, tag = "2")]
    alias: Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    timestamp: Option<u64>,
    #[prost(uint32, optional, tag = "4")]
    datatype: Option<u32>,
    #[prost(bool, optional, tag = "5")]
    is_historical: Option<bool>,
    #[prost(bool, optional, tag = "6")]
    is_transient: Option<bool>,
    #[prost(bool, optional, tag = "7")]
    is_null: Option<bool>,
    #[prost(oneof = "Val", tags = "10, 11, 12, 13, 14, 15, 16")]
    value: Option<Val>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum Val {
    #[prost(uint32, tag = "10")]
    Int(u32),
    #[prost(uint64, tag = "11")]
    Long(u64),
    #[prost(float, tag = "12")]
    Float(f32),
    #[prost(double, tag = "13")]
    Double(f64),
    #[prost(bool, tag = "14")]
    Bool(bool),
    #[prost(string, tag = "15")]
    Str(String),
    #[prost(bytes = "vec", tag = "16")]
    Bytes(Vec<u8>),
}

/// What the decoder must report for a metric built by `metric_strategy`.
#[derive(Debug, Clone, PartialEq)]
enum Want {
    Int(i64),
    UInt(u64),
    /// bits, so that NaN compares equal
    Float(u64),
    Bool(bool),
    Str(String),
    Null,
    Unsupported,
}

fn value_strategy() -> impl Strategy<Value = (u32, Val, Want)> {
    prop_oneof![
        any::<i8>().prop_map(|v| (dt::INT8, Val::Int(u32::from(v as u8)), Want::Int(v.into()))),
        any::<i16>().prop_map(|v| (
            dt::INT16,
            Val::Int(u32::from(v as u16)),
            Want::Int(v.into())
        )),
        any::<i32>().prop_map(|v| (dt::INT32, Val::Int(v as u32), Want::Int(v.into()))),
        any::<i64>().prop_map(|v| (dt::INT64, Val::Long(v as u64), Want::Int(v))),
        any::<u8>().prop_map(|v| (dt::UINT8, Val::Int(v.into()), Want::UInt(v.into()))),
        any::<u16>().prop_map(|v| (dt::UINT16, Val::Int(v.into()), Want::UInt(v.into()))),
        any::<u32>().prop_map(|v| (dt::UINT32, Val::Int(v), Want::UInt(v.into()))),
        any::<u64>().prop_map(|v| (dt::UINT64, Val::Long(v), Want::UInt(v))),
        any::<u64>().prop_map(|v| (dt::DATETIME, Val::Long(v), Want::Int(v as i64))),
        any::<f32>().prop_map(|v| (
            dt::FLOAT,
            Val::Float(v),
            Want::Float(f64::from(v).to_bits())
        )),
        any::<f64>().prop_map(|v| (dt::DOUBLE, Val::Double(v), Want::Float(v.to_bits()))),
        any::<bool>().prop_map(|v| (dt::BOOLEAN, Val::Bool(v), Want::Bool(v))),
        ".{0,12}".prop_map(|v| (dt::STRING, Val::Str(v.clone()), Want::Str(v))),
        ".{0,12}".prop_map(|v| (dt::TEXT, Val::Str(v.clone()), Want::Str(v))),
        proptest::collection::vec(any::<u8>(), 0..8).prop_map(|v| (
            17,
            Val::Bytes(v),
            Want::Unsupported
        )),
    ]
}

fn metric_strategy() -> impl Strategy<Value = (Metric, Want)> {
    (
        value_strategy(),
        proptest::option::of("[a-zA-Z/ ]{1,16}"),
        proptest::option::of(any::<u64>()),
        proptest::option::of(any::<u64>()),
        any::<(bool, bool, bool)>(),
    )
        .prop_map(|((ty, val, want), name, alias, ts, (hist, trans, null))| {
            let m = Metric {
                name,
                alias,
                timestamp: ts,
                datatype: Some(ty),
                is_historical: hist.then_some(true),
                is_transient: trans.then_some(true),
                is_null: null.then_some(true),
                value: Some(val),
            };
            (m, if null { Want::Null } else { want })
        })
}

fn got_value(v: sparkplug::Value<'_>) -> Want {
    match v {
        sparkplug::Value::Int(x) => Want::Int(x),
        sparkplug::Value::UInt(x) => Want::UInt(x),
        sparkplug::Value::Float(x) => Want::Float(x.to_bits()),
        sparkplug::Value::Bool(b) => Want::Bool(b),
        sparkplug::Value::Str(s) => Want::Str(String::from_utf8(s.to_vec()).unwrap()),
        sparkplug::Value::Null => Want::Null,
        sparkplug::Value::Unsupported => Want::Unsupported,
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: ProptestConfig::default().cases.max(2000),
        ..ProptestConfig::default()
    })]

    /// Every payload the reference codec encodes decodes to the same fields and values.
    #[test]
    fn decoder_matches_the_reference_codec(
        metrics in proptest::collection::vec(metric_strategy(), 0..12),
        ts in proptest::option::of(any::<u64>()),
        seq in proptest::option::of(0u64..256),
        uuid in proptest::option::of(".{0,8}"),
    ) {
        let p = Payload {
            timestamp: ts,
            metrics: metrics.iter().map(|(m, _)| m.clone()).collect(),
            seq,
            uuid,
            body: None,
        };
        let bytes = p.encode_to_vec();
        let mut got = Vec::new();
        let h = sparkplug::for_each_metric(&bytes, |m| got.push(m)).unwrap();
        prop_assert_eq!(h, sparkplug::header(&bytes).unwrap());
        prop_assert_eq!((h.timestamp, h.seq, h.metrics), (ts, seq, metrics.len()));
        for (g, (m, want)) in got.iter().zip(&metrics) {
            prop_assert_eq!(g.name.map(<[u8]>::to_vec), m.name.clone().map(String::into_bytes));
            prop_assert_eq!(g.alias, m.alias);
            prop_assert_eq!(g.timestamp, m.timestamp);
            prop_assert_eq!(Some(g.datatype), m.datatype);
            prop_assert_eq!(g.is_historical, m.is_historical.is_some());
            prop_assert_eq!(g.is_transient, m.is_transient.is_some());
            prop_assert_eq!(&got_value(g.value), want);
        }
    }

    /// Arbitrary bytes, and valid payloads with bytes flipped, never panic, and the two walks
    /// agree on what they accept.
    #[test]
    fn arbitrary_bytes_never_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..256),
        metrics in proptest::collection::vec(metric_strategy(), 1..4),
        flips in proptest::collection::vec((any::<usize>(), any::<u8>()), 1..4),
    ) {
        let check = |b: &[u8]| {
            let walked = sparkplug::for_each_metric(b, |_| {});
            let head = sparkplug::header(b);
            if let (Ok(w), Ok(h)) = (walked, head) {
                assert_eq!(w, h);
            }
        };
        check(&bytes);
        let mut p = Payload {
            metrics: metrics.into_iter().map(|(m, _)| m).collect(),
            ..Payload::default()
        }
        .encode_to_vec();
        for (i, b) in flips {
            let n = p.len();
            p[i % n] ^= b;
        }
        check(&p);
    }

    /// Definitions survive their encoding, whatever the ids and names.
    #[test]
    fn definitions_round_trip(
        entries in proptest::collection::vec(
            (
                ("[a-z]{1,4}", "[a-z0-9]{1,4}", "[a-z]{0,3}"),
                proptest::option::of(prop_oneof![0u64..100, any::<u64>()]),
                ".{0,20}",
                (0u32..20, any::<bool>()),
            ),
            0..40,
        ),
    ) {
        let mut t = Definitions::default();
        for ((g, n, dev), a, name, (ty, tr)) in &entries {
            prop_assert!(t.define(g, n, dev, *a, name, *ty, *tr));
        }
        let mut buf = Vec::new();
        t.encode(&mut buf);
        let back = Definitions::decode(&buf).unwrap();
        for ((g, n, dev), a, name, _) in &entries {
            if let Some(a) = a {
                prop_assert_eq!(back.by_alias(g, n, *a), t.by_alias(g, n, *a));
            }
            prop_assert_eq!(back.by_name(g, n, dev, name), t.by_name(g, n, dev, name));
        }
        prop_assert_eq!(back.len(), t.len());
        let mut again = Vec::new();
        back.encode(&mut again);
        prop_assert_eq!(again, buf.clone());
        // truncated encodings are refused, never misread
        for cut in (0..buf.len()).step_by(buf.len() / 64 + 1) {
            prop_assert!(Definitions::decode(&buf[..cut]).is_err() || cut == 0 && t.is_empty());
        }
    }
}

// --- mapping ---------------------------------------------------------------------------------

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("node", ColumnType::DictU32),
        Field::new("device", ColumnType::DictU32),
        Field::new("metric", ColumnType::DictU32),
        Field::nullable("value", ColumnType::Float64),
        Field::new("text", ColumnType::DictU32),
        Field::new("hist", ColumnType::U8),
    ])
    .unwrap()
}

fn mapper() -> RowMapper {
    let map: HashMap<String, String> = [
        ("ts", "$timestamp"),
        ("node", "$node"),
        ("device", "$device"),
        ("metric", "$metric"),
        ("value", "$value"),
        ("text", "$string"),
        ("hist", "$historical"),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    RowMapper::new(schema(), &map, false, TsUnit::Millis, false, Some("metric"))
        .unwrap()
        .with_topic_prefix(true)
        .with_sparkplug(true)
        .unwrap()
}

fn msg(topic: &str, p: &Payload) -> Vec<u8> {
    let mut m = topic.as_bytes().to_vec();
    m.push(0);
    m.extend_from_slice(&p.encode_to_vec());
    m
}

fn named(name: &str, alias: u64, ty: u32, v: Val) -> Metric {
    Metric {
        name: Some(name.into()),
        alias: Some(alias),
        datatype: Some(ty),
        value: Some(v),
        ..Metric::default()
    }
}

fn by_alias(alias: u64, ts: u64, v: Val) -> Metric {
    Metric {
        alias: Some(alias),
        timestamp: Some(ts),
        value: Some(v),
        ..Metric::default()
    }
}

/// (metric name, value column, text column, timestamp) of one row.
type Row = (String, Value, Option<String>, i64);

/// Rows of one message, each as a [`Row`] or why it was refused.
fn rows(
    m: &mut RowMapper,
    message: &[u8],
    dicts: &mut [Option<SymbolDict>],
) -> Vec<Result<Row, MapError>> {
    let n = m.parse(message, 7, false, dicts).unwrap();
    (0..n)
        .map(|r| {
            let p = m.parsed(r)?;
            let (_, row) = m.finish_row(r, dicts);
            let row = row.to_vec();
            let name = |c: usize, v: &Value| match v {
                Value::U32(id) => dicts[c]
                    .as_ref()
                    .unwrap()
                    .name(SymbolId(*id))
                    .filter(|n| !n.is_empty())
                    .map(str::to_owned),
                _ => None,
            };
            Ok((name(3, &row[3]).unwrap(), row[4], name(5, &row[5]), p.ts))
        })
        .collect()
}

#[test]
fn births_define_aliases_that_data_messages_use() {
    let mut m = mapper();
    let mut d = RowMapper::dictionaries_for(&schema());
    let birth = Payload {
        timestamp: Some(1_000),
        seq: Some(0),
        metrics: vec![
            named("Temp", 0, dt::DOUBLE, Val::Double(20.5)),
            named("State", 1, dt::STRING, Val::Str("IDLE".into())),
            named("Count", 2, dt::INT32, Val::Int((-3i32) as u32)),
            // a Template: no scalar value
            named("Recipe", 3, 19, Val::Bytes(vec![1, 2])),
            Metric {
                is_transient: Some(true),
                ..named("Scratch", 4, dt::DOUBLE, Val::Double(1.0))
            },
        ],
        ..Payload::default()
    };
    let got = rows(
        &mut m,
        &msg("spBv1.0/plant/DBIRTH/gw1/press3", &birth),
        &mut d,
    );
    // the transient metric is not stored; the template is refused, its neighbours are not
    assert_eq!(got.len(), 4);
    let ts = 1_000_000_000;
    assert_eq!(got[0], Ok(("Temp".into(), Value::F64(20.5), None, ts)));
    assert_eq!(
        got[1],
        Ok(("State".into(), Value::Null, Some("IDLE".into()), ts))
    );
    assert_eq!(got[2], Ok(("Count".into(), Value::F64(-3.0), None, ts)));
    assert!(got[3].is_err());
    // five names and five aliases
    assert_eq!(m.sparkplug_definitions().unwrap().len(), 10);

    // a DATA message names its metrics by alias only; an unknown alias refuses its row only
    let data = Payload {
        timestamp: Some(2_000),
        seq: Some(1),
        metrics: vec![
            by_alias(0, 2_001, Val::Double(21.0)),
            by_alias(9, 2_002, Val::Double(1.0)),
            Metric {
                is_null: Some(true),
                ..by_alias(1, 2_003, Val::Str(String::new()))
            },
            // no datatype in a DATA message: the Int32 of the BIRTH decides the sign
            by_alias(2, 2_004, Val::Int((-7i32) as u32)),
            // transient by its BIRTH, although this message does not say so: not stored
            by_alias(4, 2_005, Val::Double(9.0)),
        ],
        ..Payload::default()
    };
    let got = rows(
        &mut m,
        &msg("spBv1.0/plant/DDATA/gw1/press3", &data),
        &mut d,
    );
    assert_eq!(
        got[0],
        Ok(("Temp".into(), Value::F64(21.0), None, 2_001_000_000))
    );
    assert!(matches!(got[1], Err(MapError::Malformed(_))));
    assert_eq!(
        got[2],
        Ok(("State".into(), Value::Null, None, 2_003_000_000))
    );
    assert_eq!(
        got[3],
        Ok(("Count".into(), Value::F64(-7.0), None, 2_004_000_000))
    );
    assert_eq!(got.len(), 4);

    // aliases are per edge node: the same alias on another node is unknown
    let other = rows(
        &mut m,
        &msg("spBv1.0/plant/DDATA/gw2/press3", &data),
        &mut d,
    );
    assert!(other.iter().all(Result::is_err));

    // an NBIRTH starts a new session: the node's old aliases are gone
    let nbirth = Payload {
        metrics: vec![named("Uptime", 0, dt::INT64, Val::Long(5))],
        ..Payload::default()
    };
    rows(&mut m, &msg("spBv1.0/plant/NBIRTH/gw1", &nbirth), &mut d);
    let got = rows(
        &mut m,
        &msg("spBv1.0/plant/DDATA/gw1/press3", &data),
        &mut d,
    );
    assert_eq!(got[0].as_ref().unwrap().0, "Uptime");
    assert!(got[2].is_err());
}

#[test]
fn other_messages_give_no_rows() {
    let mut m = mapper();
    let mut d = RowMapper::dictionaries_for(&schema());
    // after a message with rows, one without must not leave them in the result (they would be
    // stored again: an NCMD the server publishes comes back through its own subscription)
    let birth = Payload {
        metrics: vec![named("Temp", 0, dt::DOUBLE, Val::Double(1.0))],
        ..Payload::default()
    };
    assert_eq!(
        rows(&mut m, &msg("spBv1.0/plant/NBIRTH/gw1", &birth), &mut d).len(),
        1
    );
    assert_eq!(
        m.parse(b"spBv1.0/plant/NCMD/gw1\0", 0, false, &d).unwrap(),
        0
    );
    assert_eq!(m.mapped().n_rows(), 0);
    assert!(
        m.parse(b"spBv1.0/plant/NDATA/gw1\0\xff", 0, false, &d)
            .is_err()
    );
    assert_eq!(m.mapped().n_rows(), 0);
    let p = Payload {
        metrics: vec![named("bdSeq", 0, dt::INT64, Val::Long(1))],
        ..Payload::default()
    }
    .encode_to_vec();
    for topic in [
        "spBv1.0/plant/NDEATH/gw1",
        "spBv1.0/plant/NCMD/gw1",
        "spBv1.0/STATE/host",
        "factory/line1/temp",
    ] {
        let mut message = topic.as_bytes().to_vec();
        message.push(0);
        message.extend_from_slice(&p);
        assert_eq!(m.parse(&message, 0, false, &d).unwrap(), 0, "{topic}");
    }
    // a payload that is not protobuf refuses the message
    assert!(
        m.parse(b"spBv1.0/plant/NDATA/gw1\0\xff\xff", 0, false, &d)
            .is_err()
    );
}

#[test]
fn aliases_restored_from_a_snapshot_resolve_without_a_birth() {
    let mut first = mapper();
    let mut d = RowMapper::dictionaries_for(&schema());
    let birth = Payload {
        metrics: vec![named("Temp", 5, dt::FLOAT, Val::Float(1.5))],
        ..Payload::default()
    };
    rows(&mut first, &msg("spBv1.0/plant/NBIRTH/gw1", &birth), &mut d);
    let mut snap = Vec::new();
    first.sparkplug_definitions().unwrap().encode(&mut snap);

    let mut restarted = mapper();
    restarted.set_sparkplug_definitions(Definitions::decode(&snap).unwrap());
    let data = Payload {
        timestamp: Some(3),
        metrics: vec![by_alias(5, 4, Val::Float(2.5))],
        ..Payload::default()
    };
    let got = rows(
        &mut restarted,
        &msg("spBv1.0/plant/NDATA/gw1", &data),
        &mut d,
    );
    assert_eq!(
        got[0],
        Ok(("Temp".into(), Value::F64(2.5), None, 4_000_000))
    );
}

#[test]
fn selectors_need_the_sparkplug_format_and_it_needs_them() {
    let pairs = |p: &[(&str, &str)]| -> HashMap<String, String> {
        p.iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect()
    };
    let sp = pairs(&[("ts", "$timestamp"), ("metric", "$metric")]);
    let json = pairs(&[("ts", "t")]);
    let new = |map: &HashMap<String, String>| {
        RowMapper::new(schema(), map, false, TsUnit::Millis, false, None).unwrap()
    };
    assert!(new(&sp).with_sparkplug(false).is_err());
    assert!(new(&json).with_sparkplug(true).is_err());
    assert!(new(&sp).with_sparkplug(true).is_ok());
}

#[test]
fn rebirth_request_is_a_valid_ncmd_payload() {
    let p = Payload::decode(&sparkplug::rebirth_request(1_762_700_000_000)[..]).unwrap();
    assert_eq!(p.timestamp, Some(1_762_700_000_000));
    assert_eq!(p.metrics.len(), 1);
    assert_eq!(p.metrics[0].name.as_deref(), Some("Node Control/Rebirth"));
    assert_eq!(p.metrics[0].datatype, Some(dt::BOOLEAN));
    assert_eq!(p.metrics[0].value, Some(Val::Bool(true)));
}
