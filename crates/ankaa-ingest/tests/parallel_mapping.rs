//! A message mapped by a worker that has no access to the dictionaries, then resolved and taken
//! by their owner (`Mapped::resolve`, `parsed`, `finish_row`), must give exactly what mapping with
//! the dictionaries at hand gives: the same rows, the same refusals (also at the dictionary cap,
//! counted per message in row order), the same dictionary values and ids. This is what lets
//! `ankaad` map messages on many cores and still apply them as one thread would (RFC-0001).

use std::collections::HashMap;

use ankaa_ingest::SymbolDict;
use ankaa_ingest::generic::{Mapped, RowMapper, TsUnit};
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

fn mapper(rows: bool, cap: usize) -> RowMapper {
    let map: HashMap<String, String> = if rows {
        [("ts", "t"), ("dev", "$msg.dev"), ("tag", "g"), ("v", "v")]
    } else {
        [("ts", "t"), ("dev", "dev"), ("tag", "g"), ("v", "v")]
    }
    .into_iter()
    .map(|(a, b)| (a.to_owned(), b.to_owned()))
    .collect();
    RowMapper::new(schema(), &map, false, TsUnit::Millis, false, Some("dev"))
        .unwrap()
        .with_max_dict_entries(cap)
        .with_rows(rows.then_some("s"))
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

/// What a stream produced: per message, per row, the stored values or a refusal.
type Outcome = Vec<Vec<Option<Vec<Value>>>>;

fn sequential(msgs: &[Vec<u8>], rows: bool, cap: usize) -> (Outcome, Vec<Option<SymbolDict>>) {
    let mut m = mapper(rows, cap);
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
    rows: bool,
    cap: usize,
    workers: usize,
) -> (Outcome, Vec<Option<SymbolDict>>) {
    // each worker has its own mapper; messages are dealt round-robin and mapped `lag` messages
    // ahead of the applier (as jobs in flight are); each applied result goes back to its worker,
    // which learns the ids in it and resolves those values itself afterwards
    let mut pool: Vec<RowMapper> = (0..workers).map(|_| mapper(rows, cap)).collect();
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
        rows in any::<bool>(),
        cap in 2usize..12,
        workers in 1usize..8,
    ) {
        let encoded: Vec<Vec<u8>> = msgs
            .iter()
            .map(|(dev, readings, broken)| {
                let body = if rows {
                    let elems: Vec<String> = readings.iter().map(|r| reading_json(*r, None)).collect();
                    format!("{{\"dev\":\"d{dev}\",\"s\":[{}]}}", elems.join(","))
                } else {
                    reading_json(readings[0], Some(*dev))
                };
                // now and then a message that is not JSON at all
                if *broken && dev % 5 == 0 { b"{not json".to_vec() } else { body.into_bytes() }
            })
            .collect();
        let (a, da) = sequential(&encoded, rows, cap);
        let (b, db) = via_workers(&encoded, rows, cap, workers);
        prop_assert_eq!(a, b);
        prop_assert_eq!(names(&da), names(&db));
    }
}
