//! Start the Flight server on a loopback port, query through a Flight client, check auth.

use std::sync::Arc;

use ankaa_flight::AnkaaFlight;
use ankaa_sql::register_chunks;
use ankaa_store::{Chunk, ChunkId, Schema, Value};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use tonic::metadata::MetadataValue;

#[tokio::test]
async fn sql_over_flight_with_bearer_token() {
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = AnkaaFlight::new(ctx, Some("s3cret".into())).into_server();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);
    let sql =
        "SELECT count(*) AS n, max(price) AS hi FROM trades WHERE ts >= to_timestamp_nanos(1050)";

    // without a token: unauthenticated
    let err = client
        .get_flight_info(FlightDescriptor::new_cmd(sql))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let auth = MetadataValue::try_from("Bearer s3cret").unwrap();
    let mut req = tonic::Request::new(FlightDescriptor::new_cmd(sql));
    req.metadata_mut().insert("authorization", auth.clone());
    let info = client.get_flight_info(req).await.unwrap().into_inner();
    assert_eq!(info.endpoint.len(), 1);
    let ticket: Ticket = info.endpoint[0].ticket.clone().unwrap();

    let mut req = tonic::Request::new(ticket);
    req.metadata_mut().insert("authorization", auth);
    let stream = client.do_get(req).await.unwrap().into_inner();
    let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
    );
    let batches: Vec<_> = decoder.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 50);
    assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 149);

    // DDL, DML and statements are rejected even with a valid token.
    for bad in [
        "DROP TABLE trades",
        "CREATE TABLE t AS SELECT 1",
        "CREATE EXTERNAL TABLE x STORED AS ARROW LOCATION '/etc/hostname'",
        "COPY (SELECT 1) TO '/tmp/ankaa-should-not-exist.csv'", // never executed: rejected before planning
        "SET datafusion.execution.batch_size = 1",
    ] {
        let mut req = tonic::Request::new(FlightDescriptor::new_cmd(bad));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer s3cret").unwrap(),
        );
        let err = client.get_flight_info(req).await.expect_err(bad);
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{bad}: {err}");
    }
    assert!(!std::path::Path::new("/tmp/ankaa-should-not-exist.csv").exists());
}

/// The WAL ticket: its own token, a bounded number of streams, records up to the durable LSN
/// only, and a distinct status when the log no longer holds what a standby asks for.
#[tokio::test]
async fn wal_stream_is_protected_bounded_and_reports_truncation() {
    use ankaa_wal::{Wal, WalConfig};
    use arrow::datatypes::UInt64Type;
    use std::sync::atomic::{AtomicU64, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let (mut wal, _) = Wal::open(
        dir.path(),
        WalConfig {
            group_commit_bytes: 64,
            segment_bytes: 100,
        },
    )
    .unwrap();
    for i in 0..12u8 {
        wal.append(1, &[i; 20]).unwrap();
        wal.commit().unwrap();
    }
    let durable = Arc::new(AtomicU64::new(8));
    let d2 = durable.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let flight = AnkaaFlight::new(Arc::new(SessionContext::new()), Some("query".into()))
        .with_wal_dir(Some(dir.path().to_path_buf()))
        .with_wal_token(Some("repl".into()))
        .with_max_wal_streams(1)
        .with_wal_durable_lsn(Arc::new(move || d2.load(Ordering::Relaxed)));
    let metrics = flight.metrics();
    let svc = flight.into_server();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = FlightServiceClient::new(channel);
    let open = |token: &'static str, from: u64| {
        let mut client = client.clone();
        async move {
            let mut req = tonic::Request::new(Ticket::new(format!("__wal__:{from}")));
            req.metadata_mut().insert(
                "authorization",
                MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
            );
            client.do_get(req).await.map(|r| {
                arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                    r.into_inner()
                        .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
                )
            })
        }
    };
    // the query token does not open the log
    let err = open("query", 1).await.err().unwrap();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    // the replication token does; only durable records arrive
    let mut stream = open("repl", 1).await.unwrap();
    let mut got: Vec<u64> = Vec::new();
    while got.len() < 8 {
        let b = stream.try_next().await.unwrap().unwrap();
        got.extend(b.column(0).as_primitive::<UInt64Type>().values().iter());
    }
    assert_eq!(got, (1..=8).collect::<Vec<u64>>());
    let more = tokio::time::timeout(std::time::Duration::from_millis(300), stream.try_next()).await;
    assert!(more.is_err(), "records above the durable LSN are held back");
    // a second stream is refused while the first is open
    let err = open("repl", 1).await.err().unwrap();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(metrics.wal_streams.load(Ordering::Relaxed), 1);
    // moving the durable LSN releases the rest
    durable.store(u64::MAX, Ordering::Relaxed);
    while got.len() < 12 {
        let b = stream.try_next().await.unwrap().unwrap();
        got.extend(b.column(0).as_primitive::<UInt64Type>().values().iter());
    }
    assert_eq!(got.len(), 12);
    drop(stream);
    // the slot is released once the client is gone
    let t0 = std::time::Instant::now();
    while metrics.wal_streams.load(Ordering::Relaxed) != 0 {
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(5),
            "slot not released"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // a checkpoint removed the first records: a standby asking for them is told so
    assert!(wal.truncate_before(7).unwrap() >= 1);
    let mut stream = open("repl", 1).await.unwrap();
    let err = stream.try_next().await.unwrap_err();
    let arrow_flight::error::FlightError::Tonic(status) = err else {
        panic!("expected a gRPC status, got {err:?}")
    };
    assert_eq!(status.code(), tonic::Code::OutOfRange, "{status:?}");
    assert!(metrics.wal_refused.load(Ordering::Relaxed) >= 2);
}

/// The query deadline fires even when the client stops reading the stream.
#[tokio::test]
async fn query_deadline_fires_without_a_reading_client() {
    let ctx = Arc::new(SessionContext::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let flight =
        AnkaaFlight::new(ctx, None).with_query_timeout(std::time::Duration::from_millis(300));
    let metrics = flight.metrics();
    let svc = flight.into_server();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);
    // far more rows than the HTTP/2 window holds: the server blocks on the client
    let sql = "SELECT a.value AS x, b.value AS y FROM generate_series(1, 3000) a CROSS JOIN generate_series(1, 3000) b";
    let stream = client
        .do_get(tonic::Request::new(Ticket::new(sql)))
        .await
        .unwrap()
        .into_inner();
    // do not read; the server must give up on its own and account the query
    let t0 = std::time::Instant::now();
    while metrics
        .do_get_total
        .load(std::sync::atomic::Ordering::Relaxed)
        == 0
    {
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(10),
            "the stalled query was never ended"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    drop(stream);
}
