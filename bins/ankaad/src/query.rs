//! `ankaad query`: Flight client that runs SQL against a running `ankaad serve`.

use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::TryStreamExt;
use tonic::metadata::MetadataValue;

/// Client TLS material (PEM bytes).
pub struct ClientTls {
    /// CA that signed the server certificate.
    pub ca: Vec<u8>,
    /// Expected server name (defaults to the host in the address).
    pub domain: Option<String>,
    /// Client `(cert, key)` for mutual TLS.
    pub identity: Option<(Vec<u8>, Vec<u8>)>,
}

/// Run `sql` against the Flight server at `addr`, print results.
pub async fn query(
    addr: &str,
    token: Option<&str>,
    tls: Option<&ClientTls>,
    sql: &str,
) -> anyhow::Result<()> {
    let endpoint = ankaa_flight::client_endpoint(
        addr,
        tls.map(|t| t.ca.as_slice()),
        tls.and_then(|t| t.domain.as_deref()),
        tls.and_then(|t| {
            t.identity
                .as_ref()
                .map(|(c, k)| (c.as_slice(), k.as_slice()))
        }),
    )
    .map_err(|e| anyhow::anyhow!("endpoint: {e}"))?;
    let channel = endpoint.connect().await?;
    let mut client = FlightServiceClient::new(channel);
    let auth = token
        .map(|t| MetadataValue::try_from(format!("Bearer {t}")))
        .transpose()?;
    // one call: the SQL text is the ticket (no `GetFlightInfo` round trip, one planning)
    let t0 = std::time::Instant::now();
    let mut req = tonic::Request::new(Ticket::new(sql.to_owned()));
    if let Some(a) = &auth {
        req.metadata_mut().insert("authorization", a.clone());
    }
    let stream = client.do_get(req).await?.into_inner();
    let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
    );
    let batches: Vec<_> = decoder.try_collect().await?;
    // the clock stops when the last batch has arrived: printing a large result is not query time
    let query_ms = t0.elapsed().as_secs_f64() * 1e3;
    let rows: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    println!("-- {sql}");
    if rows > 0 {
        println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    }
    println!("-- {rows} rows in {query_ms:.2} ms");
    Ok(())
}
