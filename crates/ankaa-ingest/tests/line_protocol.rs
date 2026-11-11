//! `InfluxDB` line protocol: the in-place parser against the parser of the `InfluxDB` project
//! (`influxdb-line-protocol`, which `InfluxDB` 3 parses its writes with) on generated, damaged and
//! random input; robustness on arbitrary bytes; and the mapping of lines onto table rows (keys,
//! types, NULLs, refusals, limits).

use std::collections::HashMap;

use ankaa_ingest::generic::{LineProtocol, MAX_ROWS_PER_MESSAGE, MapError, RowMapper, TsUnit};
use ankaa_ingest::line_protocol::{self as lp, Escapes, Kind, Line, Lines};
use ankaa_ingest::{SymbolDict, SymbolId};
use ankaa_store::{ColumnType, Field, Schema, Value};
use influxdb_line_protocol as reference;
use proptest::prelude::*;

// ---------------------------------------------------------------------------------------------
// The parser against the reference
// ---------------------------------------------------------------------------------------------

/// A field value, decoded.
#[derive(Debug, Clone, PartialEq)]
enum Val {
    /// The bits of the float (so that the comparison is exact).
    F64(u64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Str(String),
}

/// One line, decoded.
#[derive(Debug, Clone, PartialEq)]
struct Reading {
    measurement: String,
    tags: Vec<(String, String)>,
    fields: Vec<(String, Val)>,
    timestamp: Option<i64>,
}

fn decoded(raw: &str, escapes: Escapes) -> String {
    let mut out = String::new();
    lp::unescape(raw, escapes, &mut out);
    out
}

/// What this crate reads from `msg`: per line that holds data, the reading or `None` (refused).
fn ours(msg: &[u8]) -> Vec<Option<Reading>> {
    let mut line = Line::default();
    Lines::new(msg)
        .map(|chunk| {
            lp::parse(msg, chunk, &mut line).ok()?;
            Some(Reading {
                measurement: decoded(line.measurement.as_str(msg), Escapes::Measurement),
                tags: line
                    .tags
                    .iter()
                    .map(|(k, v)| {
                        (
                            decoded(k.as_str(msg), Escapes::Key),
                            decoded(v.as_str(msg), Escapes::Key),
                        )
                    })
                    .collect(),
                fields: line
                    .fields
                    .iter()
                    .map(|(k, v)| {
                        let text = v.as_str(msg);
                        let value = match v.kind {
                            Kind::Float => Val::F64(text.parse::<f64>().unwrap().to_bits()),
                            Kind::Int => Val::I64(text.parse().unwrap()),
                            Kind::UInt => Val::U64(text.parse().unwrap()),
                            Kind::True => Val::Bool(true),
                            Kind::False => Val::Bool(false),
                            Kind::Str => {
                                assert!(!text.contains("\\\\") && !text.contains("\\\""));
                                Val::Str(text.to_owned())
                            }
                            Kind::StrEscaped => Val::Str(decoded(text, Escapes::String)),
                        };
                        (decoded(k.as_str(msg), Escapes::Key), value)
                    })
                    .collect(),
                timestamp: line.timestamp,
            })
        })
        .collect()
}

/// What the reference parser reads from `msg`: per line that holds data, the line and its
/// reading. The lines are given to it without the two things this crate accepts and it does
/// not: a carriage return before the newline, and spaces after a line without a timestamp.
fn theirs(msg: &str) -> Vec<(&str, Option<Reading>)> {
    let mut out = Vec::new();
    for raw in reference::split_lines(msg) {
        let line = raw.strip_suffix('\r').unwrap_or(raw).trim_end_matches(' ');
        let mut parsed = reference::parse_lines(line);
        match parsed.next() {
            None => {} // blank or a comment
            Some(Err(_)) => out.push((line, None)),
            Some(Ok(p)) => out.push((
                line,
                Some(Reading {
                    measurement: p.series.measurement.to_string(),
                    tags: p
                        .series
                        .tag_set
                        .iter()
                        .flatten()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    fields: p
                        .field_set
                        .iter()
                        .map(|(k, v)| {
                            let value = match v {
                                reference::FieldValue::F64(x) => Val::F64(x.to_bits()),
                                reference::FieldValue::I64(x) => Val::I64(*x),
                                reference::FieldValue::U64(x) => Val::U64(*x),
                                reference::FieldValue::Boolean(x) => Val::Bool(*x),
                                reference::FieldValue::String(x) => Val::Str(x.to_string()),
                            };
                            (k.to_string(), value)
                        })
                        .collect(),
                    timestamp: p.timestamp,
                }),
            )),
        }
        assert!(parsed.next().is_none(), "one line gave two: {line:?}");
    }
    out
}

/// The third difference: the reference accepts a comma after the **first** field of a line
/// when no field follows it (`m v=1,` and `m v=1, 5`, but not `m v=1,w=2,`), which neither the
/// Go parser nor the specification do; this crate refuses it. Recognised here as: the reference
/// read one field, and the line without one of its commas reads the same here.
fn dangling_comma(line: &str, theirs: &Reading) -> bool {
    theirs.fields.len() == 1
        && line.match_indices(',').any(|(at, _)| {
            let mut repaired = line.to_owned();
            repaired.remove(at);
            ours(repaired.as_bytes()) == [Some(theirs.clone())]
        })
}

/// Where this crate and the reference read `msg` differently (`None`: they agree on every
/// line, on its verdict and on its values).
fn disagreement(msg: &str) -> Option<String> {
    let ours = ours(msg.as_bytes());
    let theirs = theirs(msg);
    if ours.len() != theirs.len() {
        return Some(format!(
            "{} lines here, {} in the reference",
            ours.len(),
            theirs.len()
        ));
    }
    for (a, (line, b)) in ours.iter().zip(&theirs) {
        let same = a == b || (a.is_none() && b.as_ref().is_some_and(|b| dangling_comma(line, b)));
        if !same {
            return Some(format!("{line:?}: {a:?} here, {b:?} in the reference"));
        }
    }
    None
}

/// Characters that mean something to the grammar, and a few that do not.
fn grammar_char() -> impl Strategy<Value = char> {
    prop_oneof![
        4 => proptest::sample::select(vec![
            '\\', '"', ',', '=', ' ', '\n', '#', '-', '.', 'e', 'i', 'u', 't', 'f', '1', '0',
            'm', 'a',
        ]),
        1 => proptest::sample::select(vec![
            '\t', '\r', '\0', '+', 'E', 'T', 'F', '9', 'x', 'é', '日', '\u{1F600}',
        ]),
    ]
}

/// A measurement, a key or a tag value: plain text with escapes, sometimes with a character
/// that ends it or makes it invalid.
fn name() -> impl Strategy<Value = String> {
    let piece = prop_oneof![
        40 => "[a-z0-9_.-]{1,6}",
        8 => proptest::sample::select(vec!["\\,", "\\=", "\\ ", "\\\\", "\\x", "\\\"", "é", "日本"])
            .prop_map(str::to_owned),
        1 => proptest::sample::select(vec!["=", ",", "\"", "\\", "#", "\0"]).prop_map(str::to_owned),
    ];
    proptest::collection::vec(piece, 1..4).prop_map(|p| p.concat())
}

/// A field value of every type, mostly well formed.
fn field_value() -> impl Strategy<Value = String> {
    let string_piece = prop_oneof![
        6 => "[a-z0-9 ,=]{0,6}",
        2 => proptest::sample::select(vec!["\\\"", "\\\\", "\\", "\\n", "\n", "é", "#"])
            .prop_map(str::to_owned),
    ];
    prop_oneof![
        9 => any::<i32>().prop_map(|v| v.to_string()),
        9 => (any::<i32>(), 0u32..1000).prop_map(|(a, b)| format!("{a}.{b}")),
        6 => any::<f64>().prop_filter("finite", |x| x.is_finite()).prop_map(|x| format!("{x:e}")),
        3 => (any::<i16>(), any::<i8>(), any::<bool>()).prop_map(|(m, e, up)| {
            format!("{m}{}{e}", if up { "E+" } else { "e" }).replace("+-", "-")
        }),
        9 => any::<i64>().prop_map(|v| format!("{v}i")),
        6 => any::<u64>().prop_map(|v| format!("{v}u")),
        1 => any::<u128>().prop_map(|v| format!("{v}i")),
        1 => any::<u128>().prop_map(|v| format!("{v}u")),
        6 => proptest::sample::select(vec![
            "t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE",
        ])
        .prop_map(str::to_owned),
        12 => proptest::collection::vec(string_piece, 0..4).prop_map(|p| format!("\"{}\"", p.concat())),
        2 => proptest::sample::select(vec![
            "", "1.", ".5", "1e", "-", "tRue", "nan", "1x", "\"open", "-1u", "1.5.5", "0x1",
        ])
        .prop_map(str::to_owned),
    ]
}

/// One line, mostly well formed.
fn line() -> impl Strategy<Value = String> {
    (
        proptest::sample::select(vec!["", "", "", " ", "\t ", "  "]),
        name(),
        proptest::collection::vec((name(), name()), 0..4),
        prop_oneof![12 => Just(" "), 2 => Just("  "), 1 => Just("\t"), 1 => Just("")],
        proptest::collection::vec((name(), field_value()), 1..5),
        proptest::option::weighted(
            0.7,
            prop_oneof![
                20 => any::<i64>().prop_map(|v| v.to_string()),
                1 => any::<u128>().prop_map(|v| v.to_string()),
                1 => Just("1.5".to_owned()),
            ],
        ),
        prop_oneof![
            8 => Just(""),
            2 => Just(" "),
            1 => Just("  "),
            2 => Just("\r"),
            1 => Just(" \r"),
            1 => Just("\t"),
        ],
    )
        .prop_map(|(lead, m, tags, sep, fields, ts, tail)| {
            use std::fmt::Write as _;
            let mut s = format!("{lead}{m}");
            for (k, v) in tags {
                let _ = write!(s, ",{k}={v}");
            }
            s.push_str(sep);
            let fields: Vec<String> = fields.iter().map(|(k, v)| format!("{k}={v}")).collect();
            s.push_str(&fields.join(","));
            if let Some(ts) = ts {
                let _ = write!(s, " {ts}");
            }
            s.push_str(tail);
            s
        })
}

/// A message: lines, comments and blank lines.
fn message() -> impl Strategy<Value = String> {
    let part = prop_oneof![
        8 => line(),
        1 => Just(String::new()),
        1 => "# [a-z =,\"]{0,12}",
        1 => Just("   ".to_owned()),
    ];
    (proptest::collection::vec(part, 1..5), any::<bool>()).prop_map(|(parts, newline)| {
        let mut s = parts.join("\n");
        if newline {
            s.push('\n');
        }
        s
    })
}

/// `msg` with a few characters replaced, removed or added.
fn damaged() -> impl Strategy<Value = String> {
    (
        message(),
        proptest::collection::vec((any::<prop::sample::Index>(), 0u8..3, grammar_char()), 1..4),
    )
        .prop_map(|(msg, edits)| {
            let mut chars: Vec<char> = msg.chars().collect();
            for (at, op, c) in edits {
                let i = at.index(chars.len() + 1);
                match op {
                    0 if i < chars.len() => chars[i] = c,
                    1 if i < chars.len() => {
                        chars.remove(i);
                    }
                    _ => chars.insert(i.min(chars.len()), c),
                }
            }
            chars.into_iter().collect()
        })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: ProptestConfig::default().cases.max(3000), ..ProptestConfig::default() })]

    /// Generated messages: the same lines are accepted and refused, with equal values.
    #[test]
    fn reads_what_the_reference_reads(msg in message()) {
        prop_assert_eq!(disagreement(&msg), None);
    }

    /// Damaged messages: still the same verdict per line, and equal values.
    #[test]
    fn refuses_what_the_reference_refuses(msg in damaged()) {
        prop_assert_eq!(disagreement(&msg), None);
    }

    /// Text made of the characters of the grammar in any order.
    #[test]
    fn agrees_on_grammar_soup(chars in proptest::collection::vec(grammar_char(), 0..60)) {
        let msg: String = chars.into_iter().collect();
        prop_assert_eq!(disagreement(&msg), None);
    }

    /// The spans of an accepted line follow each other inside the line, none of them empty
    /// except an empty string value, and a number's span is a number (what the fuzz target
    /// asserts, here on lines that are mostly accepted).
    #[test]
    fn spans_follow_each_other_inside_the_line(msg in prop_oneof![message(), damaged()]) {
        let msg = msg.as_bytes();
        let mut line = Line::default();
        for chunk in Lines::new(msg) {
            if lp::parse(msg, chunk, &mut line).is_err() {
                continue;
            }
            let bytes = chunk.bytes(msg);
            let first = bytes.as_ptr() as usize - msg.as_ptr() as usize;
            let m = line.measurement;
            prop_assert!(first == m.start() as usize && m.start() < m.end());
            let mut at = m.end();
            for (k, v) in &line.tags {
                prop_assert!(at < k.start() && k.start() < k.end() && k.end() < v.start());
                prop_assert!(v.start() < v.end());
                at = v.end();
            }
            for (k, v) in &line.fields {
                prop_assert!(at < k.start() && k.start() < k.end() && k.end() < v.start());
                prop_assert!(v.start() <= v.end());
                at = v.end();
                let raw = v.as_str(msg);
                let ok = match v.kind {
                    Kind::Float => raw.parse::<f64>().is_ok(),
                    Kind::Int => raw.parse::<i64>().is_ok(),
                    Kind::UInt => raw.parse::<u64>().is_ok(),
                    Kind::True | Kind::False => !raw.is_empty(),
                    Kind::Str => !raw.contains('"'),
                    Kind::StrEscaped => raw.contains('\\'),
                };
                prop_assert!(ok, "{:?} {raw:?}", v.kind);
            }
            prop_assert!(at as usize <= first + bytes.len());
        }
    }

    /// Arbitrary bytes never panic, in the parser or in the mapper, and a row that is accepted
    /// has the width and the value types of the schema.
    #[test]
    fn arbitrary_bytes_never_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..200),
        prefix in proptest::sample::select(vec!["", "m ", "m,host=a v=", "m v=\"", "m,host=a v=1,n=2i 5\n"]),
    ) {
        let mut msg = prefix.as_bytes().to_vec();
        msg.extend_from_slice(&bytes);
        let mut line = Line::default();
        for chunk in Lines::new(&msg) {
            let _ = lp::measurement(&msg, chunk);
            if lp::parse(&msg, chunk, &mut line).is_ok() {
                prop_assert!(std::str::from_utf8(chunk.bytes(&msg)).is_ok());
                prop_assert!(!line.fields.is_empty());
            }
        }
        let schema = schema();
        let mut m = mapper(&[], TsUnit::Nanos, true, &LineProtocol::default()).with_max_dict_entries(4);
        let mut dicts = RowMapper::dictionaries_for(&schema);
        for _ in 0..2 {
            let n = m.parse(&msg, 7, false, &dicts).unwrap();
            for r in 0..n {
                if m.parsed(r).is_ok() {
                    let (_, row) = m.finish_row(r, &mut dicts);
                    prop_assert_eq!(row.len(), schema.fields().len());
                    prop_assert!(matches!(row[0], Value::I64(_)));
                    prop_assert!(matches!(row[1], Value::U32(k) if k < 4));
                    prop_assert!(matches!(row[3], Value::F64(x) if x.is_finite()) || row[3] == Value::Null);
                }
            }
            for d in dicts.iter().flatten() {
                prop_assert!(d.len() <= 4);
            }
        }
    }
}

/// The comparison above is only worth something if the generators give both kinds of lines:
/// many that are accepted (with tags, every value type, escapes, timestamps) and many that are
/// refused.
#[test]
fn the_generators_give_accepted_and_refused_lines() {
    use proptest::strategy::ValueTree as _;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let (mut accepted, mut refused, mut escaped, mut strings, mut quoted_newlines) =
        (0, 0, 0, 0, 0);
    for i in 0..2000 {
        let msg = if i % 2 == 0 {
            message().new_tree(&mut runner).unwrap().current()
        } else {
            damaged().new_tree(&mut runner).unwrap().current()
        };
        for (line, reading) in theirs(&msg) {
            match reading {
                None => refused += 1,
                Some(r) => {
                    accepted += 1;
                    escaped += usize::from(line.contains('\\'));
                    strings += usize::from(r.fields.iter().any(|(_, v)| matches!(v, Val::Str(_))));
                    quoted_newlines += usize::from(line.contains('\n'));
                }
            }
        }
    }
    assert!(accepted > 1000, "accepted: {accepted}");
    assert!(refused > 1000, "refused: {refused}");
    assert!(escaped > 200, "accepted with escapes: {escaped}");
    assert!(strings > 200, "accepted with a string value: {strings}");
    assert!(
        quoted_newlines > 20,
        "accepted with a newline inside: {quoted_newlines}"
    );
}

/// Lines a human would write to check a parser, each against the reference.
#[test]
fn hand_written_lines_agree_with_the_reference() {
    for msg in [
        "cpu,host=a,region=west usage_system=64.2,n=3i 1590488773254420000",
        "weather temperature=82",
        "  measurement  a=1i  123  ",
        "m,a,b=1 c,d=2",
        "tag1=1,tag2=2 value=1 123",
        "field=1 1234",
        ",tag1=1 value=1 123",
        "m, v=1",
        "m,foo= bar=1i",
        "foo 1234",
        "m v=1\r\nm v=2\r\n",
        "m v=1 \n",
        "m v=\"a\nb\" 1\nm v=2",
        "m v=\"unterminated\nm v=2\nm v=3",
        "# comment with a \"quote\nm v=1",
        "# a=b \"swallows\nm v=1\n\" m v=2",
        "m\\ x,k\\,1=v\\=1 f\\ 1=\"s\\\"q\\\\\" -5",
        "m v=1e5,w=1.5E-3,x=-0,y=007 1",
        "m v=9223372036854775808i",
        "m v=18446744073709551615u",
        "m v=1 9223372036854775808",
        "m v=t,w=F,x=TRUE,y=tRUE",
        "m\\\\ v=1",
        "m,k=v\\\\ v=1",
        "m v=1,,w=2",
        "m v=1,w=2,",
        "m v=1 2 3",
        "m\tv=1",
        "\tm v=1",
        "m v=\"\"",
        "m \\=1",
        "m\0 v=1",
        "m,k\0=v\0 f\0=\"\0\"",
    ] {
        let theirs: Vec<Option<Reading>> = theirs(msg).into_iter().map(|(_, r)| r).collect();
        assert_eq!(ours(msg.as_bytes()), theirs, "{msg:?}");
    }
    // the differences, one by one: what this crate accepts and the reference does not…
    for (msg, timestamp) in [
        ("m v=1\r\n", None),
        ("m v=1  \n", None),
        ("m v=1 5 \r\n", Some(5)),
    ] {
        let got = ours(msg.as_bytes());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].as_ref().unwrap().timestamp, timestamp, "{msg:?}");
    }
    assert!(reference::parse_lines("m v=1\r\n").next().unwrap().is_err());
    assert!(reference::parse_lines("m v=1  \n").next().unwrap().is_err());
    // … and what the reference accepts and this crate does not
    for msg in ["m v=1,", "m v=1, 5"] {
        assert_eq!(ours(msg.as_bytes()), [None], "{msg:?}");
        assert!(
            reference::parse_lines(msg).next().unwrap().is_ok(),
            "{msg:?}"
        );
        assert_eq!(disagreement(msg), None);
    }
}

// ---------------------------------------------------------------------------------------------
// Lines onto rows
// ---------------------------------------------------------------------------------------------

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("host", ColumnType::DictU32),
        Field::new("kind", ColumnType::DictU32),
        Field::nullable("usage", ColumnType::Float64),
        Field::nullable("n", ColumnType::Int64),
        Field::nullable("px", ColumnType::FixedPoint { scale: 8 }),
        Field::nullable("up", ColumnType::U8),
    ])
    .unwrap()
}

fn map_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect()
}

fn try_mapper(
    schema: &Schema,
    map: &[(&str, &str)],
    unit: TsUnit,
    from_receive: bool,
    settings: Option<&LineProtocol>,
) -> Result<RowMapper, MapError> {
    RowMapper::new(
        schema.clone(),
        &map_of(map),
        false,
        unit,
        from_receive,
        Some("host"),
    )?
    .with_line_protocol(settings)
}

/// A mapper for [`schema`], `kind` taking the measurement unless `map` says otherwise.
fn mapper(
    map: &[(&str, &str)],
    unit: TsUnit,
    from_receive: bool,
    settings: &LineProtocol,
) -> RowMapper {
    let mut map = map.to_vec();
    if !map.iter().any(|(c, _)| *c == "kind") {
        map.push(("kind", "$measurement"));
    }
    try_mapper(&schema(), &map, unit, from_receive, Some(settings)).unwrap()
}

/// Map `msg` and take every row: its values, or why it was refused.
fn rows(
    m: &mut RowMapper,
    dicts: &mut [Option<SymbolDict>],
    msg: &[u8],
) -> Vec<Result<Vec<Value>, MapError>> {
    let n = m.parse(msg, 999, false, dicts).unwrap();
    (0..n)
        .map(|r| {
            m.parsed(r)?;
            Ok(m.finish_row(r, dicts).1.to_vec())
        })
        .collect()
}

fn name_of(dicts: &[Option<SymbolDict>], col: usize, v: Value) -> String {
    let Value::U32(id) = v else {
        panic!("{v:?} is not a dictionary id")
    };
    dicts[col]
        .as_ref()
        .unwrap()
        .name(SymbolId(id))
        .unwrap()
        .to_owned()
}

#[test]
fn columns_are_filled_by_key() {
    let mut m = mapper(&[], TsUnit::Nanos, false, &LineProtocol::default());
    let mut d = RowMapper::dictionaries_for(&schema());
    // a Telegraf line carries more than the table wants: the other keys are ignored
    let msg = "cpu,cpu=cpu-total,host=web-01,rack=r7 usage=97.5,n=12i,px=21.5,up=t,usage_iowait=0.1,note=\"x\" 1762700000000000001";
    let got = rows(&mut m, &mut d, msg.as_bytes());
    let row = got[0].as_ref().unwrap();
    assert_eq!(row[0], Value::I64(1_762_700_000_000_000_001));
    assert_eq!(name_of(&d, 1, row[1]), "web-01");
    assert_eq!(name_of(&d, 2, row[2]), "cpu");
    assert_eq!(row[3], Value::F64(97.5));
    assert_eq!(row[4], Value::I64(12));
    assert_eq!(row[5], Value::I64(2_150_000_000));
    assert_eq!(row[6], Value::U8(1));
    // a tag and a field are looked up the same way: a string field fills a dict column, and the
    // order of the keys does not matter
    let got = rows(&mut m, &mut d, b"mem up=F,host=\"db-02\",usage=1 5");
    let row = got[0].as_ref().unwrap();
    assert_eq!(name_of(&d, 1, row[1]), "db-02");
    assert_eq!(name_of(&d, 2, row[2]), "mem");
    assert_eq!(row[6], Value::U8(0));
    // a key that comes twice: the last one counts
    let got = rows(&mut m, &mut d, b"cpu,host=a,host=b usage=1,usage=2 6");
    let row = got[0].as_ref().unwrap();
    assert_eq!(name_of(&d, 1, row[1]), "b");
    assert_eq!(row[3], Value::F64(2.0));
    let got = rows(&mut m, &mut d, b"cpu,host=a usage=1,host=\"field\" 7");
    assert_eq!(name_of(&d, 1, got[0].as_ref().unwrap()[1]), "field");
}

#[test]
fn map_reads_other_keys_the_measurement_and_the_topic() {
    let schema = Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("host", ColumnType::DictU32),
        Field::new("kind", ColumnType::DictU32),
        Field::new("site", ColumnType::DictU32),
        Field::nullable("idle", ColumnType::Float64),
        Field::nullable("idle_again", ColumnType::Float64),
        Field::nullable("spaced", ColumnType::Int64),
    ])
    .unwrap();
    let mut m = try_mapper(
        &schema,
        &[
            ("host", "tags.host"),
            ("kind", "$measurement"),
            ("site", "$topic.1"),
            ("idle", "usage_idle"),
            ("idle_again", " usage_idle "),
            ("spaced", "my key,=x"),
        ],
        TsUnit::Seconds,
        false,
        Some(&LineProtocol::default()),
    )
    .unwrap()
    .with_topic_prefix(true);
    let mut d = RowMapper::dictionaries_for(&schema);
    // an escaped key is compared decoded; an escaped measurement and tag value are stored decoded
    let msg = b"telegraf/plant-1/cpu\0c\\ p\\,u,tags.host=web\\ 01 usage_idle=90,my\\ key\\,\\=x=4i,idle=1 1762700000";
    let got = rows(&mut m, &mut d, msg);
    let row = got[0].as_ref().unwrap();
    assert_eq!(row[0], Value::I64(1_762_700_000_000_000_000));
    assert_eq!(name_of(&d, 1, row[1]), "web 01");
    assert_eq!(name_of(&d, 2, row[2]), "c p,u");
    assert_eq!(name_of(&d, 3, row[3]), "plant-1");
    assert_eq!(
        row[4],
        Value::F64(90.0),
        "the column's own name is not read"
    );
    assert_eq!(row[5], Value::F64(90.0), "two columns may read one key");
    assert_eq!(row[6], Value::I64(4));
    // a string value with escapes is stored decoded
    let got = rows(
        &mut m,
        &mut d,
        b"t/s/x\0m tags.host=\"say \\\"hi\\\" \\\\ there\" 1",
    );
    assert_eq!(
        name_of(&d, 1, got[0].as_ref().unwrap()[1]),
        "say \"hi\" \\ there"
    );
    // a topic without the segment leaves the column without a value
    let got = rows(&mut m, &mut d, b"t\0m,tags.host=a usage_idle=1 1");
    assert_eq!(got[0], Err(MapError::Missing("site".into())));
    // a message without its topic is refused whole
    assert!(
        m.parse(b"m,tags.host=a usage_idle=1 1", 0, false, &d)
            .is_err()
    );
}

#[test]
fn timestamps_take_the_declared_unit_or_the_receive_time() {
    let mut d = RowMapper::dictionaries_for(&schema());
    for (unit, text, ns) in [
        (
            TsUnit::Nanos,
            "1762700000123456789",
            1_762_700_000_123_456_789,
        ),
        (
            TsUnit::Micros,
            "1762700000123456",
            1_762_700_000_123_456_000,
        ),
        (TsUnit::Millis, "1762700000123", 1_762_700_000_123_000_000),
        (TsUnit::Seconds, "1762700000", 1_762_700_000_000_000_000),
        (TsUnit::Seconds, "-5", -5_000_000_000),
    ] {
        let mut m = mapper(&[], unit, false, &LineProtocol::default());
        let got = rows(
            &mut m,
            &mut d,
            format!("m,host=a usage=1 {text}").as_bytes(),
        );
        assert_eq!(got[0].as_ref().unwrap()[0], Value::I64(ns), "{unit:?}");
    }
    // a timestamp that does not fit in nanoseconds (a wrong unit, usually) refuses the row
    let mut m = mapper(&[], TsUnit::Seconds, false, &LineProtocol::default());
    let got = rows(&mut m, &mut d, b"m,host=a usage=1 1762700000123456789");
    assert_eq!(
        got[0],
        Err(MapError::Convert {
            column: "ts".into(),
            reason: "bad timestamp".into()
        })
    );
    // no timestamp: refused, or the receive time when the source says so
    let got = rows(&mut m, &mut d, b"m,host=a usage=1");
    assert_eq!(got[0], Err(MapError::Missing("ts".into())));
    let mut m = mapper(&[], TsUnit::Seconds, true, &LineProtocol::default());
    let got = rows(&mut m, &mut d, b"m,host=a usage=1\nm,host=a usage=1 3");
    assert_eq!(got[0].as_ref().unwrap()[0], Value::I64(999));
    assert_eq!(got[1].as_ref().unwrap()[0], Value::I64(3_000_000_000));
}

#[test]
fn a_missing_key_is_null_or_refuses_the_row() {
    let mut m = mapper(&[], TsUnit::Nanos, false, &LineProtocol::default());
    let mut d = RowMapper::dictionaries_for(&schema());
    let got = rows(&mut m, &mut d, b"m,host=a other=1 1");
    assert_eq!(
        got[0].as_ref().unwrap()[3..],
        [Value::Null, Value::Null, Value::Null, Value::Null]
    );
    // a column that is not nullable needs its key
    let got = rows(&mut m, &mut d, b"m usage=1 2");
    assert_eq!(got[0], Err(MapError::Missing("host".into())));
    // a value that is present but wrong is an error, also for a nullable column
    let got = rows(&mut m, &mut d, b"m,host=a usage=\"high\" 3");
    assert!(matches!(&got[0], Err(MapError::Convert { column, .. }) if column == "usage"));
    // the refused rows left nothing in the dictionaries
    assert_eq!(d[1].as_ref().unwrap().len(), 1);
}

/// Every value converts as the same value does in JSON: numbers as numbers (with or without the
/// `i`/`u` suffix), strings and tags as strings, booleans as booleans.
#[test]
fn values_convert_like_json() {
    let column_types = [
        ColumnType::Float64,
        ColumnType::Int64,
        ColumnType::U8,
        ColumnType::FixedPoint { scale: 8 },
        ColumnType::DictU32,
        ColumnType::TimestampNs,
    ];
    // (line protocol field value, the same value in JSON)
    let values = [
        ("1.5", "1.5"),
        ("42", "42"),
        ("-7", "-7"),
        ("42i", "42"),
        ("-7i", "-7"),
        ("42u", "42"),
        ("300i", "300"),
        ("0.5", "0.5"),
        ("1e3", "1e3"),
        ("1.5E-3", "1.5E-3"),
        ("21.700000000000003", "21.700000000000003"),
        ("0.123456789", "0.123456789"),
        ("9223372036854775807i", "9223372036854775807"),
        ("18446744073709551615u", "18446744073709551615"),
        ("99999999999999999999", "99999999999999999999"),
        ("1e400", "1e400"),
        ("t", "true"),
        ("TRUE", "true"),
        ("f", "false"),
        ("False", "false"),
        ("\"abc\"", "\"abc\""),
        ("\"12.5\"", "\"12.5\""),
        ("\"7\"", "\"7\""),
        ("\"\"", "\"\""),
        ("\"q\\\"b\\\\\"", "\"q\\\"b\\\\\""),
    ];
    for ty in column_types {
        for nullable in [false, true] {
            if nullable && ty == ColumnType::DictU32 {
                continue;
            }
            let field = if nullable {
                Field::nullable("v", ty)
            } else {
                Field::new("v", ty)
            };
            let schema =
                Schema::new(vec![Field::new("ts", ColumnType::TimestampNs), field]).unwrap();
            let new = |map: &[(&str, &str)]| {
                RowMapper::new(
                    schema.clone(),
                    &map_of(map),
                    false,
                    TsUnit::Millis,
                    true,
                    None,
                )
                .unwrap()
            };
            let mut json = new(&[("v", "v")]);
            let mut lines = new(&[])
                .with_line_protocol(Some(&LineProtocol::default()))
                .unwrap();
            let mut tags = new(&[])
                .with_line_protocol(Some(&LineProtocol::default()))
                .unwrap();
            let take = |m: &mut RowMapper, msg: String| {
                let mut d = RowMapper::dictionaries_for(&schema);
                m.map(msg.as_bytes(), 5, false, &mut d).map(|(_, row)| {
                    let v = row[1];
                    match (v, &d[1]) {
                        (Value::U32(id), Some(d)) => d.name(SymbolId(id)).unwrap().to_owned(),
                        _ => format!("{v:?}"),
                    }
                })
            };
            for (line_value, json_value) in values {
                let want = take(&mut json, format!("{{\"v\":{json_value}}}"));
                let got = take(&mut lines, format!("m v={line_value}"));
                assert_eq!(got, want, "{line_value} into {ty:?} (nullable: {nullable})");
                // a tag is a string
                if let Some(text) = json_value.strip_prefix('"')
                    && !text.contains('\\')
                    && text.len() > 1
                {
                    let tag = text.trim_end_matches('"');
                    let got = take(&mut tags, format!("m,v={tag} x=1"));
                    assert_eq!(got, want, "tag {tag} into {ty:?}");
                }
            }
        }
    }
}

#[test]
fn a_malformed_line_refuses_only_its_row() {
    let mut m = mapper(&[], TsUnit::Nanos, false, &LineProtocol::default());
    let mut d = RowMapper::dictionaries_for(&schema());
    let msg = b"# written by a collector\n\
        m,host=a usage=1 1\n\
        \n\
        m,host=b usage= 2\n\
        m,host=c usage=3 3\r\n\
        not a line\n\
        m,host=\xff usage=4 4\n\
        m,host=d usage=5 5";
    let got = rows(&mut m, &mut d, msg);
    assert_eq!(got.len(), 6, "comments and blank lines are not rows");
    assert!(got[0].is_ok() && got[2].is_ok() && got[5].is_ok());
    assert_eq!(got[1], Err(MapError::Malformed("malformed field value")));
    assert_eq!(got[3], Err(MapError::Malformed("field without a value")));
    assert_eq!(got[4], Err(MapError::Malformed("line is not utf-8")));
    assert_eq!(d[1].as_ref().unwrap().names(), ["a", "c", "d"]);
    // a message without a line gives no row and is not an error; nothing is left of the
    // message before it
    assert_eq!(m.parse(b"# nothing\n\n", 0, false, &d).unwrap(), 0);
    assert_eq!(m.mapped().n_rows(), 0);
    assert_eq!(m.parse(b"", 0, false, &d).unwrap(), 0);
}

#[test]
fn measurement_keeps_the_lines_of_one_measurement() {
    let only = |name: &str| LineProtocol {
        measurement: Some(name.to_owned()),
    };
    let mut m = mapper(&[], TsUnit::Nanos, false, &only("cpu"));
    let mut d = RowMapper::dictionaries_for(&schema());
    let msg = b"cpu,host=a usage=1 1\n\
        mem,host=a used=5i 1\n\
        cpux,host=a usage=1 1\n\
        mem this line is not even valid\n\
        cpu,host=a usage= 2\n\
        ,host=a usage=1 2\n\
        cpu,host=b usage=2 3";
    let got = rows(&mut m, &mut d, msg);
    // other measurements are skipped, not refused; a broken line of this measurement, or one
    // whose measurement cannot be read, is refused
    assert_eq!(got.len(), 4);
    assert!(got[0].is_ok() && got[3].is_ok());
    assert_eq!(got[1], Err(MapError::Malformed("malformed field value")));
    assert_eq!(got[2], Err(MapError::Malformed("no measurement")));
    // the name is compared decoded
    let mut m = mapper(&[], TsUnit::Nanos, false, &only("my cpu"));
    let got = rows(
        &mut m,
        &mut d,
        b"my\\ cpu,host=a usage=1 1\nmy,host=a usage=1 1",
    );
    assert_eq!(got.len(), 1);
    assert_eq!(name_of(&d, 2, got[0].as_ref().unwrap()[2]), "my cpu");
}

#[test]
fn limits() {
    let mut d = RowMapper::dictionaries_for(&schema());
    // rows per message
    let mut m = mapper(&[], TsUnit::Nanos, true, &LineProtocol::default());
    let mut msg = "m,host=a usage=1\n".repeat(MAX_ROWS_PER_MESSAGE);
    assert_eq!(
        m.parse(msg.as_bytes(), 0, false, &d).unwrap(),
        MAX_ROWS_PER_MESSAGE
    );
    assert!(m.parsed(MAX_ROWS_PER_MESSAGE - 1).is_ok());
    msg.push_str("# a comment does not count\n\n");
    assert!(m.parse(msg.as_bytes(), 0, false, &d).is_ok());
    msg.push_str("m,host=a usage=1");
    assert_eq!(
        m.parse(msg.as_bytes(), 0, false, &d),
        Err(MapError::Malformed("too many rows in one message"))
    );
    assert_eq!(m.mapped().n_rows(), 0);
    // the mapper is as good as new after it
    assert_eq!(rows(&mut m, &mut d, b"m,host=a usage=1").len(), 1);
    // dictionary values: at most 256 bytes
    let long = "h".repeat(257);
    let got = rows(&mut m, &mut d, format!("m,host={long} usage=1").as_bytes());
    assert!(matches!(&got[0], Err(MapError::Convert { column, .. }) if column == "host"));
    let got = rows(
        &mut m,
        &mut d,
        format!("m,host={} usage=1", &long[1..]).as_bytes(),
    );
    assert!(got[0].is_ok());
    // new dictionary values of one message count against the cap together
    let mut m = mapper(&[], TsUnit::Nanos, true, &LineProtocol::default()).with_max_dict_entries(3);
    let mut d = RowMapper::dictionaries_for(&schema());
    let lines: Vec<String> = (0..8)
        .map(|i| format!("m,host=h{} usage=1", if i == 1 { 0 } else { i }))
        .collect();
    let got = rows(&mut m, &mut d, lines.join("\n").as_bytes());
    assert_eq!(
        got.iter().filter(|r| r.is_ok()).count(),
        4,
        "h0 twice, h2, h3"
    );
    assert_eq!(d[1].as_ref().unwrap().len(), 3);
}

#[test]
fn a_mapping_that_cannot_work_is_refused_when_the_mapper_is_built() {
    let on = LineProtocol::default();
    let err = |map: &[(&str, &str)], unit: TsUnit, settings: &LineProtocol| {
        try_mapper(&schema(), map, unit, true, Some(settings))
            .unwrap_err()
            .to_string()
    };
    assert!(err(&[("ts", "time")], TsUnit::Nanos, &on).contains("leave it out of map"));
    assert!(err(&[], TsUnit::Rfc3339, &on).contains("seconds, millis, micros or nanos"));
    assert!(err(&[("usage", "$measurement")], TsUnit::Nanos, &on).contains("dict column"));
    assert!(err(&[("host", "$value")], TsUnit::Nanos, &on).contains("Sparkplug"));
    assert!(err(&[("host", "$nope")], TsUnit::Nanos, &on).contains("expected a tag or field key"));
    let empty = LineProtocol {
        measurement: Some(String::new()),
    };
    assert!(err(&[], TsUnit::Nanos, &empty).contains("must not be empty"));
    // `rows` and CSV indexes belong to other formats
    let json = RowMapper::new(schema(), &HashMap::new(), false, TsUnit::Nanos, true, None)
        .unwrap()
        .with_rows(Some("samples"))
        .unwrap();
    assert!(json.with_line_protocol(Some(&on)).is_err());
    let csv = RowMapper::new(
        schema(),
        &map_of(&[("host", "1")]),
        true,
        TsUnit::Nanos,
        true,
        None,
    )
    .unwrap();
    assert!(csv.with_line_protocol(Some(&on)).is_err());
    // without the settings nothing changes: the mapper still reads JSON
    let mut json = try_mapper(
        &schema(),
        &[
            ("host", "h"),
            ("kind", "k"),
            ("usage", "u"),
            ("n", "n"),
            ("px", "p"),
            ("up", "up"),
        ],
        TsUnit::Nanos,
        true,
        None,
    )
    .unwrap();
    let mut d = RowMapper::dictionaries_for(&schema());
    assert!(json.map(br#"{"h":"a","k":"b"}"#, 1, false, &mut d).is_ok());
}
