//! A real Postgres client (tokio-postgres) connects with SCRAM, runs simple and extended
//! queries, and is refused for a wrong password and for writes.

use std::sync::Arc;

use ankaa_pg::{PgHandlers, serve};
use ankaa_sql::register_chunks;
use ankaa_store::{Chunk, ChunkId, Schema, Value};
use datafusion::prelude::SessionContext;

async fn start() -> (std::net::SocketAddr, Arc<SessionContext>) {
    let ctx = Arc::new(SessionContext::new());
    let mut c = Chunk::new(ChunkId(1), Schema::trades());
    for i in 0..10i64 {
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
    let handlers = Arc::new(PgHandlers::new(ctx.clone(), "s3cret"));
    tokio::spawn(async move {
        serve(listener, handlers, std::future::pending())
            .await
            .unwrap();
    });
    (addr, ctx)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn psql_client_scram_query_and_read_only() {
    let (addr, ctx) = start().await;
    let conn = format!(
        "host={} port={} user=ankaa password=s3cret dbname=ankaa",
        addr.ip(),
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let rows = client
        .query("SELECT count(*) AS n, max(price) AS hi FROM trades", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let n: i64 = rows[0].get("n");
    let hi: i64 = rows[0].get("hi");
    assert_eq!((n, hi), (10, 59));
    // extended protocol with a parameter
    let rows = client
        .query(
            "SELECT count(*) AS n FROM trades WHERE price > $1",
            &[&55i64],
        )
        .await
        .unwrap();
    let n: i64 = rows[0].get("n");
    assert_eq!(n, 4);
    // writes are refused
    for bad in [
        "DROP TABLE trades",
        "CREATE TABLE t (a int)",
        "INSERT INTO trades VALUES (1)",
    ] {
        let err = client.simple_query(bad).await.expect_err(bad);
        let msg = err.to_string()
            + &err
                .as_db_error()
                .map(|e| e.message().to_owned())
                .unwrap_or_default();
        assert!(
            msg.contains("read-only") || msg.contains("42501"),
            "{bad}: {msg}"
        );
    }
    // EXPLAIN ANALYZE, SELECT INTO and cursors must not execute writes or create tables either
    let target = std::env::temp_dir().join(format!(
        "ankaa-pg-must-not-exist-{}.csv",
        std::process::id()
    ));
    let sql = format!(
        "EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{}'",
        target.display()
    );
    assert!(client.simple_query(&sql).await.is_err());
    assert!(!target.exists(), "EXPLAIN ANALYZE executed a COPY");
    assert!(
        client
            .simple_query("SELECT * INTO pwned FROM trades")
            .await
            .is_err()
    );
    assert!(
        client
            .query("SELECT * INTO pwned2 FROM trades", &[])
            .await
            .is_err()
    );
    assert!(
        client
            .simple_query("SET datafusion.execution.batch_size = 1")
            .await
            .is_err()
    );
    assert!(
        client
            .simple_query("SET application_name = 'qa'")
            .await
            .is_ok()
    );
    assert!(
        client
            .query("SELECT count(*) FROM pwned", &[])
            .await
            .is_err()
    );
    // the table is still there
    let rows = client
        .query("SELECT count(*) FROM trades", &[])
        .await
        .unwrap();
    let n: i64 = rows[0].get(0);
    assert_eq!(n, 10);
    // compatibility SETs are acknowledged but never change the session every client shares
    let tz_before = ctx.state().config().options().execution.time_zone.clone();
    for set in [
        "SET timezone = 'Asia/Tokyo'",
        "SET timezone TO 'Asia/Tokyo'",
        "SET SESSION timezone = 'Asia/Tokyo'",
        "SET TIME ZONE 'Asia/Tokyo'",
        "SET client_encoding = 'UTF8'",
    ] {
        client
            .simple_query(set)
            .await
            .unwrap_or_else(|e| panic!("{set}: {e}"));
        assert_eq!(
            ctx.state().config().options().execution.time_zone,
            tz_before,
            "{set} leaked into the shared session"
        );
    }
    // what drivers and ORMs wrap every query in is accepted (there is nothing to commit)
    for st in [
        "BEGIN",
        "COMMIT",
        "BEGIN",
        "ROLLBACK",
        "DISCARD ALL",
        "SET NAMES 'UTF8'",
    ] {
        client
            .simple_query(st)
            .await
            .unwrap_or_else(|e| panic!("{st}: {e}"));
    }
    client.simple_query("BEGIN").await.unwrap();
    let rows = client
        .query("SELECT count(*) FROM trades", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 10);
    client.simple_query("COMMIT").await.unwrap();
    // a writing body behind a WITH is refused like any other write
    assert!(
        client
            .simple_query("WITH x AS (SELECT 1) INSERT INTO trades SELECT * FROM trades")
            .await
            .is_err()
    );
    // wrong password
    let bad = format!(
        "host={} port={} user=ankaa password=nope dbname=ankaa",
        addr.ip(),
        addr.port()
    );
    let Err(err) = tokio_postgres::connect(&bad, tokio_postgres::NoTls).await else {
        panic!("bad password accepted")
    };
    let code = err.as_db_error().map(|e| e.code().code().to_owned());
    assert_eq!(code.as_deref(), Some("28P01"), "{err}");
    // a second and third connection with the right password still work (per-connection SCRAM)
    for _ in 0..2 {
        let (c2, conn2) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("later connection");
        tokio::spawn(async move {
            let _ = conn2.await;
        });
        let rows = c2.query("SELECT count(*) FROM trades", &[]).await.unwrap();
        let n: i64 = rows[0].get(0);
        assert_eq!(n, 10);
    }
}
