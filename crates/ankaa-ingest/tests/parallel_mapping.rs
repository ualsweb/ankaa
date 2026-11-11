//! A message mapped by a worker that has no access to the dictionaries, then resolved and taken
//! by their owner (`Mapped::resolve`, `parsed`, `finish_row`), must give exactly what mapping with
//! the dictionaries at hand gives: the same rows, the same refusals (also at the dictionary cap,
//! counted per message in row order), the same dictionary values and ids. This is what lets
//! `ankaad` map messages on many cores and still apply them as one thread would (RFC-0001).
//! Checked for every format a worker maps: one JSON object per message, `rows`, and `InfluxDB`
//! line protocol (several lines per message).

use std::collections::HashMap;

use ankaa_ingest::SymbolDict;
use ankaa_ingest::generic::{LineProtocol, Mapped, RowMapper, TsUnit};
use ankaa_store::{ColumnType, Field, Schema, Value};
use proptest::prelude::*;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("dev", ColumnType::DictU32),
        Field::new("tag", ColumnType::DictU32),
        Field::nullable("v", ColumnType::Float64),
    ])
    .unwrap()
}

/// What the messages of a stream look like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    /// One JSON object, one row.
    Json,
    /// One JSON object with an array of readings (`rows`).
    Rows,
    /// `InfluxDB` line protocol, one line per reading.
    Lines,
}

fn mapper(format: Format, cap: usize) -> RowMapper {
    let map: HashMap<String, String> = match format {
        Format::Rows => vec![("ts", "t"), ("dev", "$msg.dev"), ("tag", "g"), ("v", "v")],
        Format::Json => vec![("ts", "t"), ("dev", "dev"), ("tag", "g"), ("v", "v")],
        // `dev` and `v` by the name of their column
        Format::Lines => vec![("tag", "g")],
    }
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    let lines = LineProtocol {
        measurement: Some("r".into()),
    };
    RowMapper::new(schema(), &map, false, TsUnit::Millis, false, Some("dev"))
        .unwrap()
        .with_max_dict_entries(cap)
        .with_rows((format == Format::Rows).then_some("s"))
        .unwrap()
        .with_line_protocol((format == Format::Lines).then_some(&lines))
        .unwrap()
}

/// One reading: `(t, tag, v)`; a `None` tag leaves it out (a refused row).
type Reading = (u16, Option<u8>, Option<i16>);

fn reading_json(r: Reading, with_dev: Option<u8>) -> String {
    use std::fmt::Write as _;
    let mut s = format!("{{\"t\":{}", r.0);
    if let Some(d) = with_dev {
        let _ = write!(s, ",\"dev\":\"d{d}\"");
    }
    if let Some(g) = r.1 {
        let _ = write!(s, ",\"g\":\"g{g}\"");
    }
    match r.2 {
        Some(v) => {
            let _ = write!(s, ",\"v\":{v}.5}}");
        }
        None => s.push_str(",\"v\":null}"),
    }
    s
}

/// The same reading as a line: the tag `g` and the field `v` may be missing, another field
/// always follows (a line needs one), and every third line is of a measurement the source skips.
fn reading_line(r: Reading, dev: u8) -> String {
    use std::fmt::Write as _;
    let measurement = if r.0.is_multiple_of(3) { "other" } else { "r" };
    let mut s = format!("{measurement},dev=d{dev}");
    if let Some(g) = r.1 {
        let _ = write!(s, ",g=g{g}");
    }
    s.push(' ');
    if let Some(v) = r.2 {
        let _ = write!(s, "v={v}.5,");
    }
    let _ = write!(s, "n=1i {}", r.0);
    s
}

/// What a stream produced: per message, per row, the stored values or a refusal.
type Outcome = Vec<Vec<Option<Vec<Value>>>>;

fn sequential(msgs: &[Vec<u8>], format: Format, cap: usize) -> (Outcome, Vec<Option<SymbolDict>>) {
    let mut m = mapper(format, cap);
    let mut dicts = RowMapper::dictionaries_for(&schema());
    let mut out = Vec::new();
    for msg in msgs {
        let mut per = Vec::new();
        if let Ok(n) = m.parse(msg, 0, false, &dicts) {
            for r in 0..n {
                if m.parsed(r).is_ok() {
                    per.push(Some(m.finish_row(r, &mut dicts).1.to_vec()));
                } else {
                    per.push(None);
                }
            }
        }
        out.push(per);
    }
    (out, dicts)
}

fn via_workers(
    msgs: &[Vec<u8>],
    format: Format,
    cap: usize,
    workers: usize,
) -> (Outcome, Vec<Option<SymbolDict>>) {
    // each worker has its own mapper; messages are dealt round-robin and mapped `lag` messages
    // ahead of the applier (as jobs in flight are); each applied result goes back to its worker,
    // which learns the ids in it and resolves those values itself afterwards
    let mut pool: Vec<RowMapper> = (0..workers).map(|_| mapper(format, cap)).collect();
    let lag = workers * 2;
    let mut ready: std::collections::VecDeque<(usize, Option<Mapped>)> =
        std::collections::VecDeque::new();
    let mut dicts = RowMapper::dictionaries_for(&schema());
    let mut out = Vec::new();
    let mut apply = |(w, result): (usize, Option<Mapped>),
                     pool: &mut Vec<RowMapper>,
                     dicts: &mut Vec<Option<SymbolDict>>| {
        let mut per = Vec::new();
        if let Some(mut m) = result {
            m.resolve(dicts, cap);
            for r in 0..m.n_rows() {
                if m.parsed(r).is_ok() {
                    per.push(Some(m.finish_row(r, dicts).1.to_vec()));
                } else {
                    per.push(None);
                }
            }
            pool[w].learn(&m);
        }
        out.push(per);
    };
    for (i, msg) in msgs.iter().enumerate() {
        let w = i % workers;
        let mapper = &mut pool[w];
        let result = mapper.parse(msg, 0, false, &[]).ok().map(|_| {
            let mut taken = mapper.empty_mapped();
            mapper.swap_mapped(&mut taken);
            taken
        });
        ready.push_back((w, result));
        if ready.len() > lag {
            let next = ready.pop_front().unwrap();
            apply(next, &mut pool, &mut dicts);
        }
    }
    while let Some(next) = ready.pop_front() {
        apply(next, &mut pool, &mut dicts);
    }
    (out, dicts)
}

fn names(d: &[Option<SymbolDict>]) -> Vec<Vec<String>> {
    d.iter()
        .map(|x| x.as_ref().map(|d| d.names().to_vec()).unwrap_or_default())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: ProptestConfig::default().cases.max(1000), ..ProptestConfig::default() })]

    #[test]
    fn workers_map_like_one_thread(
        msgs in proptest::collection::vec(
            (0u8..12, proptest::collection::vec((any::<u16>(), proptest::option::weighted(0.9, 0u8..9), any::<Option<i16>>()), 1..6), any::<bool>()),
            1..40,
        ),
        format in proptest::sample::select(vec![Format::Json, Format::Rows, Format::Lines]),
        cap in 2usize..12,
        workers in 1usize..8,
    ) {
        let encoded: Vec<Vec<u8>> = msgs
            .iter()
            .map(|(dev, readings, broken)| {
                let body = match format {
                    Format::Rows => {
                        let elems: Vec<String> = readings.iter().map(|r| reading_json(*r, None)).collect();
                        format!("{{\"dev\":\"d{dev}\",\"s\":[{}]}}", elems.join(","))
                    }
                    Format::Json => reading_json(readings[0], Some(*dev)),
                    Format::Lines => {
                        let lines: Vec<String> = readings.iter().map(|r| reading_line(*r, *dev)).collect();
                        // now and then a line that is not line protocol among the others
                        let sep = if *broken && dev % 3 == 0 { "\nr,dev= v=1\n" } else { "\n" };
                        lines.join(sep)
                    }
                };
                // now and then a message that is not JSON at all (a refused line, as line
                // protocol)
                if *broken && dev % 5 == 0 { b"{not json".to_vec() } else { body.into_bytes() }
            })
            .collect();
        let (a, da) = sequential(&encoded, format, cap);
        let (b, db) = via_workers(&encoded, format, cap, workers);
        prop_assert_eq!(a, b);
        prop_assert_eq!(names(&da), names(&db));
    }
}

/// The property above would hold for a mapper that refuses everything: line protocol streams do
/// store rows, skip the other measurement, leave `NULL`s and refuse lines.
#[test]
fn line_protocol_streams_store_skip_and_refuse() {
    let readings: [Reading; 5] = [
        (1, Some(1), Some(3)),
        (2, Some(2), None),
        (3, Some(1), Some(4)), // t % 3 == 0: another measurement
        (4, None, Some(5)),    // no tag: refused
        (5, Some(1), Some(6)),
    ];
    let lines: Vec<String> = readings.iter().map(|r| reading_line(*r, 7)).collect();
    let msgs = vec![lines.join("\n").into_bytes(), b"r,dev= v=1 9".to_vec()];
    let (a, da) = sequential(&msgs, Format::Lines, 8);
    let (b, db) = via_workers(&msgs, Format::Lines, 8, 3);
    assert_eq!(a, b);
    assert_eq!(names(&da), names(&db));
    let stored: Vec<bool> = a[0].iter().map(Option::is_some).collect();
    assert_eq!(stored, [true, true, false, true]);
    assert_eq!(a[0][1].as_ref().unwrap()[3], Value::Null);
    assert_eq!(a[0][3].as_ref().unwrap()[3], Value::F64(6.5));
    assert_eq!(a[1], [None]);
    assert_eq!(names(&da)[1], ["d7"]);
    assert_eq!(names(&da)[2], ["g1", "g2"]);
}
