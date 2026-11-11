//! `dict` columns shown as strings (RFC-0002): a chunk table with published names must answer
//! every query exactly as `DataFusion` answers it over the same rows held as plain strings.
//! Covered: `=`, `<>`, `IN`, `NOT IN` and `OR` on names (answered on the ids, per chunk or row
//! by row), names that were never stored, grouping and ordering by name, the dense aggregation
//! path, joins on names, limits under a name filter, and the table functions' key columns.
//! The chunks come in two layouts: one id per chunk (a table partitioned by the column) and ids
//! mixed inside chunks (a tag that varies within a partition).
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use std::sync::Arc;

use ankaa_sql::{ChunkTable, Names};
use ankaa_store::{Chunk, ChunkId, ChunkRef, ColumnType, Field, Schema, Value};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field as AField, Schema as ASchema, TimeUnit};
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

const HOSTS: [&str; 6] = ["web-1", "web-2", "db-1", "db-2", "cache-1", "edge-9"];
const REGIONS: [&str; 3] = ["eu-west", "us-east", "ap-south"];

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", ColumnType::TimestampNs),
        Field::new("host", ColumnType::DictU32),
        Field::new("region", ColumnType::DictU32),
        Field::nullable("load", ColumnType::Float64),
        Field::new("n", ColumnType::Int64),
    ])
    .unwrap()
}

/// `(ts, host id, region id, load, n)`.
type Row = (i64, u32, u32, Option<f64>, i64);

/// Rows of every host: 400 readings 1 ms apart; the region depends on the host when `mixed` is
/// false (so a chunk holds one region) and on the row otherwise.
fn rows(mixed: bool) -> Vec<Vec<Row>> {
    (0..HOSTS.len() as u32 - 1) // the last host never reports: a name without rows
        .map(|h| {
            (0..400i64)
                .map(|i| {
                    let region = if mixed { (i as u32 + h) % 3 } else { h % 3 };
                    let load = (i % 11 != 0).then(|| f64::from(h) * 10.0 + (i % 7) as f64);
                    (i * 1_000_000 + i64::from(h), h, region, load, i)
                })
                .collect()
        })
        .collect()
}

fn contexts(mixed: bool, one_chunk: bool) -> (SessionContext, SessionContext) {
    let data = rows(mixed);
    // the chunk table: a chunk per host (one host id per chunk), or one chunk for everything
    let mut chunks: Vec<Chunk> = Vec::new();
    if one_chunk {
        let mut all: Vec<Row> = data.iter().flatten().copied().collect();
        all.sort_by_key(|r| r.0);
        let mut c = Chunk::new(ChunkId(1), schema());
        for r in all {
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
        chunks.push(c);
    } else {
        for (h, host_rows) in data.iter().enumerate() {
            let mut c = Chunk::new(ChunkId(h as u64 + 1), schema());
            for r in host_rows {
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
            chunks.push(c);
        }
    }
    let config = || SessionConfig::new().with_target_partitions(3);
    let runtime = || Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default());
    let ankaa = ankaa_sql::session(config(), runtime());
    let table = ChunkTable::from_shared(
        schema(),
        chunks
            .into_iter()
            .map(|c| ChunkRef::Hot(Arc::new(c)))
            .collect(),
    )
    .with_names(vec![
        None,
        Some(Names::of(&HOSTS)),
        Some(Names::of(&REGIONS)),
    ]);
    ankaa.register_table("cpu", Arc::new(table)).unwrap();
    ankaa_sql::register_functions(&ankaa);

    // the reference: the same rows as plain strings in a DataFusion table
    let all: Vec<Row> = data.iter().flatten().copied().collect();
    let aschema = Arc::new(ASchema::new(vec![
        AField::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        AField::new("host", DataType::Utf8, false),
        AField::new("region", DataType::Utf8, false),
        AField::new("load", DataType::Float64, true),
        AField::new("n", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampNanosecondArray::from(all.iter().map(|r| r.0).collect::<Vec<_>>())
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(
            all.iter().map(|r| HOSTS[r.1 as usize]),
        )),
        Arc::new(StringArray::from_iter_values(
            all.iter().map(|r| REGIONS[r.2 as usize]),
        )),
        Arc::new(Float64Array::from(
            all.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            all.iter().map(|r| r.4).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(aschema.clone(), columns).unwrap();
    let reference = SessionContext::new_with_config(config());
    reference
        .register_table(
            "cpu",
            Arc::new(MemTable::try_new(aschema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    (ankaa, reference)
}

/// The result as text, with every column cast to a plain type so that a dictionary-encoded
/// string and a plain string print alike; rows sorted unless the query orders them.
async fn text(ctx: &SessionContext, sql: &str) -> String {
    let df = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let batches = df.collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let plain: Vec<RecordBatch> = batches
        .iter()
        .map(|b| {
            let fields: Vec<AField> = b
                .schema()
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Dictionary(_, v) => {
                        AField::new(f.name(), v.as_ref().clone(), f.is_nullable())
                    }
                    _ => f.as_ref().clone(),
                })
                .collect();
            let cols: Vec<ArrayRef> = b
                .columns()
                .iter()
                .zip(&fields)
                .map(|(c, f)| arrow::compute::cast(c, f.data_type()).unwrap())
                .collect();
            RecordBatch::try_new(Arc::new(ASchema::new(fields)), cols).unwrap()
        })
        .collect();
    let rendered = pretty_format_batches(&plain).unwrap().to_string();
    if sql.to_ascii_lowercase().contains("order by") {
        return rendered;
    }
    let mut lines: Vec<&str> = rendered.lines().collect();
    lines.sort_unstable();
    lines.join("\n")
}

const QUERIES: &[&str] = &[
    "SELECT count(*) FROM cpu WHERE host = 'web-1'",
    "SELECT count(*), sum(n) FROM cpu WHERE host <> 'web-1'",
    "SELECT count(*) FROM cpu WHERE host IN ('db-1', 'cache-1', 'nobody')",
    "SELECT count(*) FROM cpu WHERE host NOT IN ('db-1', 'cache-1')",
    "SELECT count(*) FROM cpu WHERE host = 'db-1' OR host = 'db-2'",
    "SELECT count(*) FROM cpu WHERE host = 'nobody'",
    "SELECT count(*) FROM cpu WHERE host <> 'nobody'",
    "SELECT count(*) FROM cpu WHERE host = 'edge-9'",
    "SELECT count(*) FROM cpu WHERE region = 'us-east'",
    "SELECT count(*), avg(load) FROM cpu WHERE region <> 'us-east' AND host IN ('web-1', 'db-2')",
    "SELECT count(*) FROM cpu WHERE region IN ('eu-west', 'ap-south') AND n < 50",
    "SELECT count(*) FROM cpu WHERE 'web-2' = host AND ts >= '1970-01-01T00:00:00.1Z'",
    "SELECT host, count(*), sum(n), min(load), max(load) FROM cpu GROUP BY host",
    "SELECT region, host, count(load) FROM cpu GROUP BY region, host",
    "SELECT host, date_bin(INTERVAL '100 milliseconds', ts) AS b, count(*), sum(n) FROM cpu GROUP BY host, b",
    "SELECT region, count(*) FROM cpu WHERE host = 'web-1' GROUP BY region",
    "SELECT DISTINCT region FROM cpu WHERE host IN ('web-1', 'db-1')",
    "SELECT host, region, n FROM cpu WHERE host = 'cache-1' AND n >= 395 ORDER BY ts",
    "SELECT ts, host, load FROM cpu WHERE region = 'ap-south' ORDER BY ts, host LIMIT 7",
    "SELECT host, max(n) FROM cpu WHERE host LIKE 'web%' GROUP BY host",
    "SELECT host, count(*) FROM cpu WHERE host > 'db-2' GROUP BY host",
    "SELECT upper(host) AS h, count(*) FROM cpu GROUP BY upper(host)",
    "SELECT a.host, count(*) FROM cpu a JOIN cpu b ON a.host = b.host AND a.ts = b.ts WHERE b.region = 'eu-west' GROUP BY a.host",
    "SELECT host, region FROM cpu WHERE n = 0 ORDER BY host DESC",
    "SELECT count(DISTINCT host), count(DISTINCT region) FROM cpu",
    "SELECT host FROM cpu WHERE host = 'db-1' LIMIT 3",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn names_answer_like_plain_strings() {
    for (mixed, one_chunk) in [(false, false), (true, false), (true, true)] {
        let (ankaa, reference) = contexts(mixed, one_chunk);
        for sql in QUERIES {
            assert_eq!(
                text(&ankaa, sql).await,
                text(&reference, sql).await,
                "{sql}\n(regions mixed in chunks: {mixed}, one chunk: {one_chunk})"
            );
        }
    }
}

/// The plan: a name predicate on a column with one id per chunk is answered by the scan alone
/// (no filter above it, matching chunks only); on mixed chunks the scan is wrapped in a filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn name_predicates_are_answered_by_the_scan() {
    let (ankaa, _) = contexts(true, false);
    let plan = |sql: &'static str| {
        let ankaa = ankaa.clone();
        async move {
            let b = ankaa
                .sql(&format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            pretty_format_batches(&b).unwrap().to_string()
        }
    };
    let by_host = plan("SELECT n FROM cpu WHERE host = 'web-1'").await;
    assert!(!by_host.contains("FilterExec"), "{by_host}");
    assert!(by_host.contains("ChunkScanExec"), "{by_host}");
    let by_region = plan("SELECT n FROM cpu WHERE region = 'us-east'").await;
    assert!(by_region.contains("FilterExec"), "{by_region}");
}

/// The table functions return their key column as names when the input has names.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn table_functions_return_names() {
    let (ankaa, reference) = contexts(false, false);
    let got = text(
        &ankaa,
        "SELECT host, count(*) AS buckets, sum(CASE WHEN filled THEN 1 ELSE 0 END) AS filled \
         FROM gapfill('cpu', 'load', 50000000, 'host', 'avg', 'locf') GROUP BY host",
    )
    .await;
    assert!(got.contains("web-1") && got.contains("cache-1"), "{got}");
    // percentiles per host: the same keys as a GROUP BY over the reference
    let keys = text(
        &ankaa,
        "SELECT host FROM percentiles('cpu', 'load', '0.5', 'host')",
    )
    .await;
    let want = text(&reference, "SELECT DISTINCT host FROM cpu").await;
    assert_eq!(keys, want);
}

/// A query written for numeric ids (the surface before RFC-0002) is refused with an error that
/// names the problem; it never returns the answer to a different question.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn id_queries_fail_loudly() {
    let (ankaa, _) = contexts(false, false);
    ankaa
        .sql(
            "CREATE TABLE d AS SELECT * FROM (VALUES (arrow_cast(0, 'UInt32'), 'web-1'), \
             (arrow_cast(1, 'UInt32'), 'web-2')) AS t(id, name)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for sql in [
        "SELECT count(*) FROM cpu WHERE host = 0",
        "SELECT d.name, count(*) FROM cpu t JOIN d ON d.id = t.host GROUP BY d.name",
        "SELECT count(*) FROM cpu WHERE host IN (SELECT id FROM d)",
    ] {
        let err = match ankaa.sql(sql).await {
            Ok(df) => df.collect().await.map(|_| ()),
            Err(e) => Err(e),
        }
        .expect_err(sql)
        .to_string();
        assert!(err.contains("Cannot cast string"), "{sql}: {err}");
    }
}
