//! Flight SQL against an in-process server, through `FlightSqlServiceClient`: statements,
//! prepared statements, catalog metadata, the handshake, and what is refused (missing token,
//! writes, parameters, malformed commands). The limits of plain Flight are checked on the
//! Flight SQL path as well.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use ankaa_flight::{AnkaaFlight, QueryMetrics};
use ankaa_sql::register_chunks;
use ankaa_store::{Chunk, ChunkId, Schema, Value};
use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, Any, CommandGetDbSchemas, CommandGetSqlInfo,
    CommandGetTables, CommandStatementIngest, CommandStatementQuery, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, Ticket};
use base64::Engine;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use prost::Message;
use tonic::Code;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const TOKEN: &str = "s3cret-token-0123456789";
const SQL_URL: &str = "type.googleapis.com/arrow.flight.protocol.sql.";

fn trades_ctx() -> Arc<SessionContext> {
    let ctx = Arc::new(SessionContext::new());
    let mut c = Chunk::new(ChunkId(1), Schema::trades());
    for i in 0..100i64 {
        c.append_row(&[
            Value::I64(1_000 + i),
            Value::U32(1),
            Value::I64(50 + i),
            Value::I64(1),
            Value::U8(0),
            Value::I64(i),
        ])
        .unwrap();
    }
    register_chunks(&ctx, "trades", Schema::trades(), vec![c]).unwrap();
    ctx
}

/// Serve `flight` on a loopback port the way `ankaad` does and return a channel to it.
async fn start(flight: AnkaaFlight) -> (Channel, Arc<QueryMetrics>) {
    let metrics = flight.metrics();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        ankaa_flight::serve(flight, listener, None, std::future::pending())
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, metrics)
}

async fn sql_client(flight: AnkaaFlight, token: Option<&str>) -> FlightSqlServiceClient<Channel> {
    let (channel, _) = start(flight).await;
    let mut client = FlightSqlServiceClient::new(channel);
    if let Some(t) = token {
        client.set_token(t.to_owned());
    }
    client
}

/// The gRPC code inside a client error.
fn code(e: &FlightError) -> Code {
    match e {
        FlightError::Tonic(status) => status.code(),
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}

fn message(e: &FlightError) -> String {
    match e {
        FlightError::Tonic(status) => status.message().to_owned(),
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}

async fn fetch(client: &mut FlightSqlServiceClient<Channel>, info: FlightInfo) -> Vec<RecordBatch> {
    assert_eq!(info.endpoint.len(), 1, "one endpoint, on this server");
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn strings(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(column)
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn with_token<T>(msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {TOKEN}")).unwrap(),
    );
    req
}

const AGG: &str =
    "SELECT count(*) AS n, max(price) AS hi FROM trades WHERE ts >= to_timestamp_nanos(1050)";

#[tokio::test]
async fn statement_query_with_bearer_token() {
    let mut client = sql_client(
        AnkaaFlight::new(trades_ctx(), Some(TOKEN.into())),
        Some(TOKEN),
    )
    .await;
    let info = client.execute(AGG.to_owned(), None).await.unwrap();
    let schema = info.clone().try_decode_schema().unwrap();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["n", "hi"]
    );
    let batches = fetch(&mut client, info).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        50
    );
    assert_eq!(
        batches[0].column(1).as_primitive::<Int64Type>().value(0),
        149
    );

    // GetSchema with the same command
    let cmd = CommandStatementQuery {
        query: AGG.to_owned(),
        transaction_id: None,
    };
    let schema_result = client
        .inner_mut()
        .get_schema(with_token(FlightDescriptor::new_cmd(
            cmd.as_any().encode_to_vec(),
        )))
        .await
        .unwrap()
        .into_inner();
    let got = arrow::datatypes::Schema::try_from(&schema_result).unwrap();
    assert_eq!(got.fields().len(), 2);

    // the plain dialect answers on the same connection
    let info = client
        .inner_mut()
        .get_flight_info(with_token(FlightDescriptor::new_cmd(AGG)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        &info.endpoint[0].ticket.as_ref().unwrap().ticket[..],
        AGG.as_bytes()
    );
}

#[tokio::test]
async fn prepared_statement_without_parameters() {
    let mut client = sql_client(
        AnkaaFlight::new(trades_ctx(), Some(TOKEN.into())),
        Some(TOKEN),
    )
    .await;
    let mut stmt = client.prepare(AGG.to_owned(), None).await.unwrap();
    assert_eq!(stmt.dataset_schema().unwrap().fields().len(), 2);
    assert_eq!(stmt.dataset_schema().unwrap().field(1).name(), "hi");
    assert!(stmt.parameter_schema().unwrap().fields().is_empty());
    // a prepared statement can be executed more than once
    for _ in 0..2 {
        let info = stmt.execute().await.unwrap();
        let batches = fetch(&mut client, info).await;
        assert_eq!(
            batches[0].column(0).as_primitive::<Int64Type>().value(0),
            50
        );
    }
    stmt.close().await.unwrap();

    let actions: Vec<_> = client
        .inner_mut()
        .list_actions(with_token(arrow_flight::Empty {}))
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    let names: Vec<_> = actions.iter().map(|a| a.r#type.as_str()).collect();
    assert_eq!(names, ["CreatePreparedStatement", "ClosePreparedStatement"]);
}

/// What ADBC and the JDBC driver do with a user name and a password: `Handshake` with a Basic
/// header, then the bearer token from the response headers on every call.
#[tokio::test]
async fn handshake_takes_the_token_as_basic_password_or_bearer() {
    let (channel, metrics) = start(AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))).await;

    let mut client = FlightSqlServiceClient::new(channel.clone());
    client.handshake("anyone", TOKEN).await.unwrap();
    assert_eq!(client.token().map(String::as_str), Some(TOKEN));
    let info = client.execute(AGG.to_owned(), None).await.unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 1);

    let mut wrong = FlightSqlServiceClient::new(channel.clone());
    assert!(wrong.handshake("anyone", "not-the-token").await.is_err());
    assert!(wrong.token().is_none());

    // raw handshakes: header value -> expected outcome
    let unpadded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(format!("u:{TOKEN}"));
    let padded = base64::engine::general_purpose::STANDARD.encode(format!("user:{TOKEN}"));
    assert!(padded.ends_with('='), "this case exercises padding");
    let no_colon = base64::engine::general_purpose::STANDARD.encode(TOKEN);
    let not_utf8 = base64::engine::general_purpose::STANDARD.encode([0xff, b':', 0xfe]);
    let cases: Vec<(Option<String>, bool)> = vec![
        (Some(format!("Basic {unpadded}")), true), // the Go client (ADBC) sends no padding
        (Some(format!("basic {padded}")), true),
        (Some(format!("Bearer {TOKEN}")), true), // JDBC with `token=`
        (Some(format!("Basic {no_colon}")), false),
        (Some(format!("Basic {not_utf8}")), false),
        (Some("Basic !!!not base64!!!".to_owned()), false),
        (Some("Basic".to_owned()), false),
        (Some("Bearer nope".to_owned()), false),
        (Some(format!("Digest {TOKEN}")), false),
        (None, false),
    ];
    let mut raw = FlightServiceClient::new(channel);
    let before = metrics.unauthenticated.load(Relaxed);
    let mut refused = 0;
    for (header, ok) in cases {
        let mut req = tonic::Request::new(futures::stream::iter([HandshakeRequest::default()]));
        if let Some(h) = &header {
            req.metadata_mut().insert(
                "authorization",
                MetadataValue::try_from(h.as_str()).unwrap(),
            );
        }
        match raw.handshake(req).await {
            Ok(resp) => {
                assert!(ok, "{header:?} must be refused");
                assert_eq!(
                    resp.metadata().get("authorization").unwrap(),
                    format!("Bearer {TOKEN}").as_str()
                );
                let replies: Vec<_> = resp.into_inner().try_collect().await.unwrap();
                assert_eq!(replies.len(), 1);
            }
            Err(status) => {
                assert!(!ok, "{header:?} must be accepted: {status}");
                assert_eq!(status.code(), Code::Unauthenticated, "{header:?}");
                refused += 1;
            }
        }
    }
    assert_eq!(metrics.unauthenticated.load(Relaxed) - before, refused);
}

#[tokio::test]
async fn handshake_without_a_server_token_accepts_anything() {
    let mut client = sql_client(AnkaaFlight::new(trades_ctx(), None), None).await;
    client.handshake("", "").await.unwrap();
    let info = client.execute(AGG.to_owned(), None).await.unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 1);
}

#[tokio::test]
async fn every_call_needs_the_token() {
    let (channel, metrics) = start(AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))).await;
    for token in [None, Some("wrong-token")] {
        let mut client = FlightSqlServiceClient::new(channel.clone());
        if let Some(t) = token {
            client.set_token(t.to_owned());
        }
        let e = client.execute(AGG.to_owned(), None).await.unwrap_err();
        assert_eq!(code(&e), Code::Unauthenticated);
        let e = client.prepare(AGG.to_owned(), None).await.unwrap_err();
        assert_eq!(code(&e), Code::Unauthenticated);
        let e = client.get_catalogs().await.unwrap_err();
        assert_eq!(code(&e), Code::Unauthenticated);
        let e = client.get_sql_info(vec![]).await.unwrap_err();
        assert_eq!(code(&e), Code::Unauthenticated);
        let e = client
            .execute_update("DELETE FROM trades".to_owned(), None)
            .await
            .unwrap_err();
        assert_eq!(code(&e), Code::Unauthenticated);
        // a ticket built by hand does not get past the check either
        let ticket = TicketStatementQuery {
            statement_handle: AGG.as_bytes().to_vec().into(),
        };
        let e = client
            .do_get(Ticket::new(ticket.as_any().encode_to_vec()))
            .await
            .err()
            .unwrap();
        assert_eq!(code(&e), Code::Unauthenticated);
        let status = client
            .inner_mut()
            .list_actions(arrow_flight::Empty {})
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);
    }
    assert!(metrics.unauthenticated.load(Relaxed) >= 12);
    assert_eq!(metrics.queries.load(Relaxed), 0, "nothing was planned");
}

/// The value of one `SqlInfo` row (the value column is a dense union).
fn info_value(batches: &[RecordBatch], id: SqlInfo) -> Option<arrow::array::ArrayRef> {
    for b in batches {
        let names = b.column(0).as_primitive::<arrow::datatypes::UInt32Type>();
        let values = b.column(1).as_union();
        for i in 0..b.num_rows() {
            if names.value(i) == id as u32 {
                return Some(values.value(i));
            }
        }
    }
    None
}

#[tokio::test]
async fn sql_info_describes_a_read_only_server() {
    let mut client = sql_client(
        AnkaaFlight::new(trades_ctx(), Some(TOKEN.into())),
        Some(TOKEN),
    )
    .await;
    let info = client.get_sql_info(vec![]).await.unwrap();
    let all = fetch(&mut client, info).await;
    let name = info_value(&all, SqlInfo::FlightSqlServerName).unwrap();
    assert_eq!(name.as_string::<i32>().value(0), "ANKAA");
    let version = info_value(&all, SqlInfo::FlightSqlServerVersion).unwrap();
    assert_eq!(version.as_string::<i32>().value(0), ankaa_flight::VERSION);
    let read_only = info_value(&all, SqlInfo::FlightSqlServerReadOnly).unwrap();
    assert!(read_only.as_boolean().value(0));
    let transactions = info_value(&all, SqlInfo::FlightSqlServerTransaction).unwrap();
    assert_eq!(
        transactions
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(0),
        0
    );
    for off in [
        SqlInfo::FlightSqlServerSubstrait,
        SqlInfo::FlightSqlServerCancel,
        SqlInfo::FlightSqlServerBulkIngestion,
        SqlInfo::SqlDdlTable,
        SqlInfo::SqlTransactionsSupported,
    ] {
        let v = info_value(&all, off).unwrap();
        assert!(!v.as_boolean().value(0), "{off:?}");
    }

    // a selection: known ids are answered once, unknown ones are left out
    let info = client
        .get_sql_info(vec![
            SqlInfo::FlightSqlServerReadOnly,
            SqlInfo::FlightSqlServerName,
            SqlInfo::FlightSqlServerReadOnly,
            SqlInfo::SqlKeywords,
        ])
        .await
        .unwrap();
    let some = fetch(&mut client, info).await;
    assert_eq!(rows(&some), 2);
    assert!(info_value(&some, SqlInfo::FlightSqlServerVersion).is_none());

    // only ids the server has nothing for (and a great many of them): an empty answer
    let cmd = CommandGetSqlInfo {
        info: (1_000_000..1_100_000).collect(),
    };
    let info = client
        .inner_mut()
        .get_flight_info(with_token(FlightDescriptor::new_cmd(
            cmd.as_any().encode_to_vec(),
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&fetch(&mut client, info).await), 0);
}

#[tokio::test]
async fn catalog_metadata() {
    let mut client = sql_client(
        AnkaaFlight::new(trades_ctx(), Some(TOKEN.into())),
        Some(TOKEN),
    )
    .await;

    let info = client.get_catalogs().await.unwrap();
    assert_eq!(strings(&fetch(&mut client, info).await, 0), ["datafusion"]);

    let info = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        })
        .await
        .unwrap();
    let schemas = fetch(&mut client, info).await;
    assert_eq!(strings(&schemas, 0), ["datafusion"]);
    assert_eq!(strings(&schemas, 1), ["public"]);
    let info = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: Some("elsewhere".to_owned()),
            db_schema_filter_pattern: Some("pub%".to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 0);

    let info = client.get_table_types().await.unwrap();
    let types = strings(&fetch(&mut client, info).await, 0);
    assert!(types.contains(&"TABLE".to_owned()) && types.contains(&"VIEW".to_owned()));

    let tables = |pattern: Option<&str>, types: &[&str], include_schema| CommandGetTables {
        catalog: None,
        db_schema_filter_pattern: None,
        table_name_filter_pattern: pattern.map(str::to_owned),
        table_types: types.iter().map(|t| (*t).to_owned()).collect(),
        include_schema,
    };
    // without the schema column
    let info = client.get_tables(tables(None, &[], false)).await.unwrap();
    assert_eq!(info.clone().try_decode_schema().unwrap().fields().len(), 4);
    let listed = fetch(&mut client, info).await;
    assert_eq!(listed[0].num_columns(), 4);
    assert_eq!(strings(&listed, 0), ["datafusion"]);
    assert_eq!(strings(&listed, 1), ["public"]);
    assert_eq!(strings(&listed, 2), ["trades"]);
    assert_eq!(strings(&listed, 3), ["TABLE"]);
    // with it: the Arrow schema of the table, IPC-encoded
    let info = client
        .get_tables(tables(Some("tra%"), &["TABLE"], true))
        .await
        .unwrap();
    assert_eq!(info.clone().try_decode_schema().unwrap().fields().len(), 5);
    let listed = fetch(&mut client, info).await;
    assert_eq!(strings(&listed, 2), ["trades"]);
    let encoded = listed[0].column(4).as_binary::<i32>().value(0).to_vec();
    let table_schema =
        arrow::datatypes::Schema::try_from(arrow_flight::IpcMessage(encoded.into())).unwrap();
    // six columns went into the chunk
    assert_eq!(table_schema.fields().len(), 6);
    assert!(table_schema.field_with_name("price").is_ok());
    // filters that match nothing
    let info = client
        .get_tables(tables(Some("nope%"), &[], false))
        .await
        .unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 0);
    let info = client
        .get_tables(tables(None, &["VIEW"], true))
        .await
        .unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 0);

    // hostile filter patterns are answered (a result or a refusal), never a broken call
    let long = "%_".repeat(50_000);
    for pattern in ["\\", "%\\", "[(", "_%_%", "\u{0}", long.as_str()] {
        let info = client
            .get_tables(tables(Some(pattern), &[pattern], true))
            .await
            .unwrap();
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        match client.do_get(ticket).await {
            Ok(stream) => match stream.try_collect::<Vec<_>>().await {
                Ok(batches) => assert_eq!(rows(&batches), 0),
                Err(e) => assert_eq!(code(&e), Code::InvalidArgument),
            },
            Err(e) => assert_eq!(code(&e), Code::InvalidArgument),
        }
    }
    // unsupported metadata is refused by name
    let e = client
        .get_primary_keys(arrow_flight::sql::CommandGetPrimaryKeys {
            catalog: None,
            db_schema: None,
            table: "trades".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented);
    assert!(message(&e).contains("CommandGetPrimaryKeys"), "{e}");
}

#[tokio::test]
async fn writes_are_refused() {
    let (channel, metrics) = start(AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))).await;
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_token(TOKEN.to_owned());

    // the update and ingest commands
    let e = client
        .execute_update("DELETE FROM trades".to_owned(), None)
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented);
    assert!(message(&e).contains("read-only"), "{e}");
    let ingest = CommandStatementIngest {
        table: "trades".to_owned(),
        ..Default::default()
    };
    let batch = RecordBatch::try_from_iter([(
        "x",
        Arc::new(arrow::array::Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let e = client
        .execute_ingest(ingest, futures::stream::iter([Ok(batch)]))
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented);
    assert!(message(&e).contains("read-only"), "{e}");
    let mut stmt = client
        .prepare("SELECT 1 AS x".to_owned(), None)
        .await
        .unwrap();
    let e = stmt.execute_update().await.unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented);
    assert!(message(&e).contains("read-only"), "{e}");

    // writes dressed as queries: the read-only check of plain Flight
    for bad in [
        "DROP TABLE trades",
        "CREATE TABLE t AS SELECT 1",
        "INSERT INTO trades SELECT * FROM trades",
        "CREATE EXTERNAL TABLE x STORED AS ARROW LOCATION '/etc/hostname'",
        "COPY (SELECT 1) TO '/tmp/ankaa-flight-sql-should-not-exist.csv'",
        "SET datafusion.execution.batch_size = 1",
    ] {
        let e = client.execute(bad.to_owned(), None).await.expect_err(bad);
        assert_eq!(code(&e), Code::InvalidArgument, "{bad}: {e}");
        let e = client.prepare(bad.to_owned(), None).await.expect_err(bad);
        assert_eq!(code(&e), Code::InvalidArgument, "{bad}: {e}");
        // a handle or a ticket written by hand is the same text: refused when executed
        let ticket = TicketStatementQuery {
            statement_handle: bad.as_bytes().to_vec().into(),
        };
        let e = client
            .do_get(Ticket::new(ticket.as_any().encode_to_vec()))
            .await
            .expect_err(bad);
        assert_eq!(code(&e), Code::InvalidArgument, "{bad}: {e}");
    }
    assert!(!std::path::Path::new("/tmp/ankaa-flight-sql-should-not-exist.csv").exists());

    // no transactions
    let e = client.begin_transaction().await.unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented);
    let e = client
        .execute(AGG.to_owned(), Some(b"tx-1".to_vec().into()))
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented);

    // the table is as it was
    let info = client
        .execute("SELECT count(*) FROM trades".to_owned(), None)
        .await
        .unwrap();
    let batches = fetch(&mut client, info).await;
    assert_eq!(
        batches[0].column(0).as_primitive::<Int64Type>().value(0),
        100
    );
    assert!(metrics.errors.load(Relaxed) >= 3 + 6 * 3);
}

#[tokio::test]
async fn statement_parameters_are_refused() {
    let mut client = sql_client(
        AnkaaFlight::new(trades_ctx(), Some(TOKEN.into())),
        Some(TOKEN),
    )
    .await;
    for parameterised in [
        "SELECT count(*) FROM trades WHERE price > $1",
        "SELECT count(*) FROM trades WHERE price > ?",
    ] {
        // "invalid argument", so that a client does not take it for "no prepared statements"
        let e = client
            .prepare(parameterised.to_owned(), None)
            .await
            .unwrap_err();
        assert_eq!(code(&e), Code::InvalidArgument, "{parameterised}: {e}");
        assert!(message(&e).contains("parameters"), "{e}");
        let e = client
            .execute(parameterised.to_owned(), None)
            .await
            .unwrap_err();
        assert_eq!(code(&e), Code::InvalidArgument, "{parameterised}: {e}");
        assert!(message(&e).contains("parameters"), "{e}");
    }

    // binding values to a prepared statement (a DoPut) is refused as well
    let mut stmt = client
        .prepare("SELECT 1 AS x".to_owned(), None)
        .await
        .unwrap();
    let params = RecordBatch::try_from_iter([(
        "$1",
        Arc::new(arrow::array::Int64Array::from(vec![7])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    stmt.set_parameters(params).unwrap();
    let e = stmt.execute().await.unwrap_err();
    assert_eq!(code(&e), Code::InvalidArgument);
    assert!(message(&e).contains("parameters"), "{e}");
}

fn any(name: &str, value: &[u8]) -> Vec<u8> {
    Any {
        type_url: format!("{SQL_URL}{name}"),
        value: value.to_vec().into(),
    }
    .encode_to_vec()
}

/// Deterministic noise (xorshift), so a failure can be reproduced.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// What a refused request looks like. A handler that panicked would show up as another code or
/// as a broken connection.
fn refusal(c: Code) -> bool {
    matches!(c, Code::InvalidArgument | Code::Unimplemented)
}

/// The server still answers a query.
async fn assert_alive(channel: Channel) {
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_token(TOKEN.to_owned());
    let info = client.execute(AGG.to_owned(), None).await.unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 1);
}

/// A statement handle is SQL text and nothing else: the WAL ticket of plain Flight cannot be
/// reached through Flight SQL.
#[tokio::test]
async fn a_statement_handle_is_not_a_wal_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let flight = AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))
        .with_wal_dir(Some(dir.path().to_path_buf()));
    let (channel, metrics) = start(flight).await;
    let mut raw = FlightServiceClient::new(channel);
    // plain Flight opens the stream with this ticket (no separate replication token is set)
    let plain = raw.do_get(with_token(Ticket::new("__wal__:1"))).await;
    assert!(plain.is_ok());
    assert_eq!(metrics.wal_streams.load(Relaxed), 1);
    drop(plain);
    // as a statement handle the same text is a statement, and not a valid one
    for ticket in [
        TicketStatementQuery {
            statement_handle: b"__wal__:1".to_vec().into(),
        }
        .as_any(),
        arrow_flight::sql::CommandPreparedStatementQuery {
            prepared_statement_handle: b"__wal__:1".to_vec().into(),
        }
        .as_any(),
    ] {
        let status = raw
            .do_get(with_token(Ticket::new(ticket.encode_to_vec())))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument, "{status}");
    }
    assert!(metrics.wal_streams.load(Relaxed) <= 1);
    assert_eq!(metrics.wal_refused.load(Relaxed), 0);
}

/// Malformed and unknown commands in descriptors and tickets get a status.
#[tokio::test]
async fn malformed_commands_get_a_status() {
    let (channel, _) = start(AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))).await;
    let mut raw = FlightServiceClient::new(channel.clone());

    // a Flight SQL type whose body is not that message
    let garbage = any("CommandStatementQuery", &[0xff, 0xff, 0xff, 0xff]);
    let status = raw
        .get_flight_info(with_token(FlightDescriptor::new_cmd(garbage.clone())))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status}");
    let status = raw
        .get_schema(with_token(FlightDescriptor::new_cmd(garbage.clone())))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status}");
    let status = raw
        .do_get(with_token(Ticket::new(any(
            "TicketStatementQuery",
            &[0xff, 0xff, 0xff],
        ))))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status}");

    // a handle that is not text
    let ticket = TicketStatementQuery {
        statement_handle: vec![0xc3, 0x28, 0xff].into(),
    };
    let status = raw
        .do_get(with_token(Ticket::new(ticket.as_any().encode_to_vec())))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status}");

    // a type the specification does not have; the name is echoed in part only
    let long_name = "X".repeat(100_000);
    for name in ["NoSuchCommand", "", long_name.as_str()] {
        let status = raw
            .get_flight_info(with_token(FlightDescriptor::new_cmd(any(name, b""))))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unimplemented, "{status}");
        assert!(status.message().len() < 200);
        let status = raw
            .do_get(with_token(Ticket::new(any(name, b"\x01\x02"))))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unimplemented, "{status}");
    }
    // a query command is not a ticket
    let cmd = CommandStatementQuery {
        query: AGG.to_owned(),
        transaction_id: None,
    };
    let status = raw
        .do_get(with_token(Ticket::new(cmd.as_any().encode_to_vec())))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented, "{status}");
    assert_alive(channel).await;
}

/// Malformed action bodies and `DoPut` streams get a status.
#[tokio::test]
async fn malformed_actions_and_puts_get_a_status() {
    let (channel, _) = start(AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))).await;
    let mut raw = FlightServiceClient::new(channel.clone());
    let cmd = CommandStatementQuery {
        query: AGG.to_owned(),
        transaction_id: None,
    };

    // actions: a body that is no `Any`, an `Any` of another type, an empty body, unknown names
    let prepare = ActionCreatePreparedStatementRequest {
        query: AGG.to_owned(),
        transaction_id: None,
    };
    let bodies: [(&str, Vec<u8>); 7] = [
        ("CreatePreparedStatement", vec![0xff; 16]),
        ("CreatePreparedStatement", Vec::new()),
        ("CreatePreparedStatement", cmd.as_any().encode_to_vec()),
        (
            "CreatePreparedStatement",
            any("ActionCreatePreparedStatementRequest", &[0x0a, 0xff]),
        ),
        ("ClosePreparedStatement", prepare.as_any().encode_to_vec()),
        ("ClosePreparedStatement", vec![0x0a]),
        ("CancelQuery", Vec::new()),
    ];
    for (name, body) in bodies {
        let status = raw
            .do_action(with_token(Action::new(name, body)))
            .await
            .unwrap_err();
        assert!(refusal(status.code()), "{name}: {status}");
    }
    for name in ["", "NoSuchAction", "BeginTransaction", "EndSavepoint"] {
        let status = raw
            .do_action(with_token(Action::new(
                name,
                prepare.as_any().encode_to_vec(),
            )))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unimplemented, "{name}: {status}");
    }

    // DoPut: no message, no descriptor, a descriptor that is no command, a broken command
    let puts: Vec<Vec<FlightData>> = vec![
        vec![],
        vec![FlightData::default()],
        vec![FlightData::default().with_descriptor(FlightDescriptor::new_cmd("SELECT 1"))],
        vec![
            FlightData::default()
                .with_descriptor(FlightDescriptor::new_path(vec!["trades".to_owned()])),
        ],
        vec![
            FlightData::default().with_descriptor(FlightDescriptor::new_cmd(any(
                "CommandStatementUpdate",
                &[0xff, 0xff],
            ))),
        ],
        vec![
            FlightData::default()
                .with_descriptor(FlightDescriptor::new_cmd(any("NoSuchCommand", b""))),
        ],
    ];
    for messages in puts {
        let status = raw
            .do_put(with_token(futures::stream::iter(messages)))
            .await
            .unwrap_err();
        assert!(refusal(status.code()), "{status}");
    }
    assert_alive(channel).await;
}

/// Noise, bare and inside the envelope of each kind of message, on every call that decodes.
#[tokio::test]
async fn noise_gets_a_status() {
    let (channel, _) = start(AnkaaFlight::new(trades_ctx(), Some(TOKEN.into()))).await;
    let mut raw = FlightServiceClient::new(channel.clone());
    for seed in 1..=200u64 {
        let bytes = noise(seed, (seed as usize * 7) % 300);
        let wrapped = [
            bytes.clone(),
            any("CommandStatementQuery", &bytes),
            any("CommandPreparedStatementQuery", &bytes),
            any("CommandGetTables", &bytes),
            any("CommandGetDbSchemas", &bytes),
            any("CommandGetSqlInfo", &bytes),
            any("TicketStatementQuery", &bytes),
            any("ActionCreatePreparedStatementRequest", &bytes),
        ];
        for payload in wrapped {
            // any of these may be a valid message by chance: then the answer is a result or a
            // refusal of its content, which are the same codes
            if let Err(status) = raw
                .get_flight_info(with_token(FlightDescriptor::new_cmd(payload.clone())))
                .await
            {
                assert!(refusal(status.code()), "seed {seed}: {status}");
            }
            match raw.do_get(with_token(Ticket::new(payload.clone()))).await {
                Err(status) => assert!(refusal(status.code()), "seed {seed}: {status}"),
                Ok(resp) => {
                    if let Err(status) = resp.into_inner().try_collect::<Vec<_>>().await {
                        assert!(refusal(status.code()), "seed {seed}: {status}");
                    }
                }
            }
            if let Err(status) = raw
                .do_action(with_token(Action::new(
                    "CreatePreparedStatement",
                    payload.clone(),
                )))
                .await
            {
                assert!(refusal(status.code()), "seed {seed}: {status}");
            }
            let put = FlightData::default().with_descriptor(FlightDescriptor::new_cmd(payload));
            let status = raw
                .do_put(with_token(futures::stream::iter([put])))
                .await
                .unwrap_err();
            assert!(refusal(status.code()), "seed {seed}: {status}");
        }
    }
    assert_alive(channel).await;
}

/// More rows than the HTTP/2 window holds: the server blocks on a client that does not read.
const ENDLESS: &str = "SELECT a.value AS x, b.value AS y FROM generate_series(1, 3000) a CROSS JOIN generate_series(1, 3000) b";

/// The query deadline covers a Flight SQL statement whose client stops reading, and the cut
/// stream ends with an error.
#[tokio::test]
async fn query_timeout_applies() {
    let flight = AnkaaFlight::new(Arc::new(SessionContext::new()), None)
        .with_query_timeout(Duration::from_millis(300));
    let (channel, metrics) = start(flight).await;
    let mut client = FlightSqlServiceClient::new(channel);
    let mut stmt = client.prepare(ENDLESS.to_owned(), None).await.unwrap();
    let info = stmt.execute().await.unwrap();
    let mut stream = client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    // do not read; the server must give up on its own and account the query
    let t0 = Instant::now();
    while metrics.do_get_total.load(Relaxed) == 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "the stalled query was never ended"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut seen = 0usize;
    let end = loop {
        match stream.try_next().await {
            Ok(Some(b)) => seen += b.num_rows(),
            Ok(None) => break None,
            Err(e) => break Some(e),
        }
    };
    let e = end.expect("a stream cut by the deadline must not end as if it were complete");
    assert_eq!(code(&e), Code::DeadlineExceeded);
    assert!(seen < 9_000_000);
}

/// A Flight SQL statement waits for a query slot like any other query.
#[tokio::test]
async fn concurrent_query_cap_applies() {
    let timeout = Duration::from_millis(800);
    let flight = AnkaaFlight::new(Arc::new(SessionContext::new()), None)
        .with_query_timeout(timeout)
        .with_max_concurrent_queries(1);
    let (channel, _) = start(flight).await;
    let mut client = FlightSqlServiceClient::new(channel);
    // the only slot: a query whose client does not read holds it until its deadline
    let info = client.execute(ENDLESS.to_owned(), None).await.unwrap();
    let t0 = Instant::now();
    let _stalled = client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    // planning needs no slot
    let info = client
        .execute("SELECT 1 AS x".to_owned(), None)
        .await
        .unwrap();
    // executing does: the call returns only once the first query has ended, which is at its
    // deadline (without the cap it returns within milliseconds)
    let second = client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await;
    assert!(
        t0.elapsed() >= timeout,
        "the second query ran beside the first ({:?})",
        t0.elapsed()
    );
    // it got the slot with little of its own deadline left: it ran or it was refused in time
    if let Err(e) = second {
        assert!(
            matches!(code(&e), Code::ResourceExhausted | Code::DeadlineExceeded),
            "{e}"
        );
    }
}

/// A Flight SQL statement is charged to the session's memory pool.
#[tokio::test]
async fn memory_pool_applies() {
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_limit(256 * 1024, 1.0)
        .with_disk_manager_builder(
            datafusion::execution::disk_manager::DiskManagerBuilder::default()
                .with_mode(datafusion::execution::disk_manager::DiskManagerMode::Disabled),
        )
        .build_arc()
        .unwrap();
    let ctx = Arc::new(SessionContext::new_with_config_rt(
        datafusion::prelude::SessionConfig::new(),
        runtime,
    ));
    let mut client = sql_client(AnkaaFlight::new(ctx, None), None).await;
    // sorting a million rows does not fit in 256 KiB, and there is no disk to spill to
    let sql = "SELECT value FROM generate_series(1, 1000000) ORDER BY value DESC";
    let mut stmt = client.prepare(sql.to_owned(), None).await.unwrap();
    let info = stmt.execute().await.unwrap();
    let result = match client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
    {
        Ok(stream) => stream.try_collect::<Vec<_>>().await.map(|b| rows(&b)),
        Err(e) => Err(e),
    };
    let e = result.expect_err("the sort must fail for lack of memory");
    assert!(
        message(&e).to_lowercase().contains("resources exhausted"),
        "{e}"
    );
    // a small query still runs
    let info = client
        .execute("SELECT 1 AS x".to_owned(), None)
        .await
        .unwrap();
    assert_eq!(rows(&fetch(&mut client, info).await), 1);
}
