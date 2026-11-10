//! TLS round trip with self-signed certificates generated at test time (rcgen):
//! server-only TLS, then mutual TLS where a client without a certificate is refused.

use std::sync::Arc;

use ankaa_flight::{AnkaaFlight, TlsFiles, client_endpoint, serve};
use ankaa_sql::register_chunks;
use ankaa_store::{Chunk, ChunkId, Schema, Value};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use rcgen::{CertificateParams, Issuer, KeyPair};
use tonic::metadata::MetadataValue;

struct Pki {
    dir: std::path::PathBuf,
    ca_pem: Vec<u8>,
    client_cert: Vec<u8>,
    client_key: Vec<u8>,
}

fn make_pki(name: &str) -> Pki {
    let dir = std::env::temp_dir().join(format!("ankaa-tls-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().unwrap();
    let server_cert = CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .signed_by(&server_key, &ca)
        .unwrap();
    let client_key = KeyPair::generate().unwrap();
    let client_cert = CertificateParams::new(vec!["client".to_owned()])
        .unwrap()
        .signed_by(&client_key, &ca)
        .unwrap();

    std::fs::write(dir.join("ca.pem"), ca_cert.pem()).unwrap();
    std::fs::write(dir.join("server.pem"), server_cert.pem()).unwrap();
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).unwrap();
    Pki {
        dir,
        ca_pem: ca_cert.pem().into_bytes(),
        client_cert: client_cert.pem().into_bytes(),
        client_key: client_key.serialize_pem().into_bytes(),
    }
}

fn ctx() -> Arc<SessionContext> {
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
    ctx
}

async fn start(tls: TlsFiles) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = AnkaaFlight::new(ctx(), Some("s3cret".into()));
    tokio::spawn(async move {
        serve(svc, listener, Some(&tls), std::future::pending())
            .await
            .unwrap();
    });
    addr
}

async fn count_rows(endpoint: tonic::transport::Endpoint) -> Result<i64, tonic::Status> {
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut client = FlightServiceClient::new(channel);
    let auth = MetadataValue::try_from("Bearer s3cret").unwrap();
    let mut req = tonic::Request::new(FlightDescriptor::new_cmd(
        "SELECT count(*) AS n FROM trades",
    ));
    req.metadata_mut().insert("authorization", auth.clone());
    let info = client.get_flight_info(req).await?.into_inner();
    let ticket: Ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut req = tonic::Request::new(ticket);
    req.metadata_mut().insert("authorization", auth);
    let stream = client.do_get(req).await?.into_inner();
    let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
    );
    let batches: Vec<_> = decoder
        .try_collect()
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
    Ok(batches[0].column(0).as_primitive::<Int64Type>().value(0))
}

#[tokio::test]
async fn server_tls_round_trip_and_plaintext_refused() {
    let pki = make_pki("server");
    let addr = start(TlsFiles {
        cert: pki.dir.join("server.pem"),
        key: pki.dir.join("server.key"),
        client_ca: None,
    })
    .await;
    // trusted CA + matching SAN
    let ep = client_endpoint(
        &addr.to_string(),
        Some(&pki.ca_pem),
        Some("localhost"),
        None,
    )
    .unwrap();
    assert_eq!(count_rows(ep).await.unwrap(), 10);
    // wrong name
    let ep = client_endpoint(
        &addr.to_string(),
        Some(&pki.ca_pem),
        Some("evil.example"),
        None,
    )
    .unwrap();
    assert!(count_rows(ep).await.is_err());
    // untrusted CA
    let other = make_pki("other");
    let ep = client_endpoint(
        &addr.to_string(),
        Some(&other.ca_pem),
        Some("localhost"),
        None,
    )
    .unwrap();
    assert!(count_rows(ep).await.is_err());
    // plaintext client against a TLS server
    let ep = client_endpoint(&addr.to_string(), None, None, None).unwrap();
    assert!(count_rows(ep).await.is_err());
}

#[tokio::test]
async fn mutual_tls_requires_client_certificate() {
    let pki = make_pki("mtls");
    let addr = start(TlsFiles {
        cert: pki.dir.join("server.pem"),
        key: pki.dir.join("server.key"),
        client_ca: Some(pki.dir.join("ca.pem")),
    })
    .await;
    let ep = client_endpoint(
        &addr.to_string(),
        Some(&pki.ca_pem),
        Some("localhost"),
        Some((&pki.client_cert, &pki.client_key)),
    )
    .unwrap();
    assert_eq!(count_rows(ep).await.unwrap(), 10);
    let ep = client_endpoint(
        &addr.to_string(),
        Some(&pki.ca_pem),
        Some("localhost"),
        None,
    )
    .unwrap();
    assert!(
        count_rows(ep).await.is_err(),
        "client without certificate must be refused"
    );
}
