//! Standby side of replication: stream the primary's WAL over Arrow Flight (ticket
//! `__wal__:<from_lsn>`) and hand every record, with the primary's LSN, to the server loop,
//! which appends it to the local WAL under the same LSN and applies it to the tables. The two
//! logs therefore stay record-for-record identical, and "the next LSN I need" is simply the
//! end of the local log.
//!
//! Transient failures reconnect with backoff. Failures that retrying cannot fix end the task
//! with an error so the server stops instead of serving a silently diverging copy: the primary
//! no longer has the records this standby needs, or its log ends before this standby's.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ankaa_feeds::live::SourceStats;
use arrow::array::AsArray;
use arrow::datatypes::UInt64Type;
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::TryStreamExt;
use tokio::sync::{mpsc, watch};
use tonic::metadata::MetadataValue;
use tracing::{info, warn};

use crate::metrics::Metrics;

/// How to reach the primary.
#[derive(Clone)]
pub struct Primary {
    /// `host:port` of the primary's Flight endpoint.
    pub addr: String,
    /// Bearer token.
    pub token: Option<String>,
    /// CA certificate (PEM) when the primary uses TLS.
    pub tls_ca: Option<Vec<u8>>,
    /// Expected TLS server name.
    pub tls_domain: Option<String>,
}

/// One WAL record of the primary.
#[derive(Debug, Clone)]
pub struct Replicated {
    /// The primary's LSN.
    pub lsn: u64,
    /// The primary's epoch.
    pub epoch: u64,
    /// The record payload (an encoded frame).
    pub payload: bytes::Bytes,
}

/// True when `host:port` names this machine.
pub fn is_loopback(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Run until shutdown (`Ok`) or until replication cannot continue (`Err`, with the reason).
/// `from_lsn` is the first primary LSN wanted: the end of the local log + 1.
#[allow(clippy::too_many_lines)]
pub async fn run_wal_source(
    primary: Primary,
    mut from_lsn: u64,
    tx: mpsc::Sender<Replicated>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> Result<SourceStats, String> {
    let mut stats = SourceStats::default();
    let mut backoff = Duration::from_millis(500);
    let auth = primary
        .token
        .as_deref()
        .and_then(|t| MetadataValue::try_from(format!("Bearer {t}")).ok());
    let expected = ankaa_flight::wal_stream_schema();
    if primary.tls_ca.is_none() && primary.token.is_some() && !is_loopback(&primary.addr) {
        warn!(addr = %primary.addr, "replication: no TLS to the primary (replication.primary_tls_ca is unset): the token and the WAL cross the network in clear text");
    }
    loop {
        if *shutdown.borrow() {
            return Ok(stats);
        }
        // every retry path sleeps first: a stream that opens and fails at once must not spin
        macro_rules! wait_before_retry {
            () => {{
                metrics.replication_connected.store(0, Ordering::Relaxed);
                tokio::select! {
                    _ = shutdown.changed() => return Ok(stats),
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }};
        }
        let endpoint = ankaa_flight::client_endpoint(
            &primary.addr,
            primary.tls_ca.as_deref(),
            primary.tls_domain.as_deref(),
            None,
        )
        .map_err(|e| format!("bad primary endpoint {:?}: {e}", primary.addr))?;
        let channel = match endpoint.connect().await {
            Ok(c) => c,
            Err(e) => {
                warn!(addr = %primary.addr, error = %e, "replication: connect failed; retrying");
                wait_before_retry!();
                continue;
            }
        };
        let mut client = FlightServiceClient::new(channel)
            .max_decoding_message_size(ankaa_wal::MAX_PAYLOAD + ankaa_wal::TAIL_POLL_BYTES);
        let mut req = tonic::Request::new(Ticket::new(format!(
            "{}{from_lsn}",
            ankaa_flight::WAL_TICKET_PREFIX
        )));
        if let Some(a) = &auth {
            req.metadata_mut().insert("authorization", a.clone());
        }
        let stream = match client.do_get(req).await {
            Ok(s) => s.into_inner(),
            Err(e) => {
                warn!(addr = %primary.addr, error = %e, "replication: WAL stream refused; retrying");
                wait_before_retry!();
                continue;
            }
        };
        stats.connects += 1;
        metrics.replication_connects.fetch_add(1, Ordering::Relaxed);
        metrics.replication_connected.store(1, Ordering::Relaxed);
        info!(addr = %primary.addr, from_lsn, "replication: streaming primary WAL");
        let mut decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );
        loop {
            let next = tokio::select! {
                _ = shutdown.changed() => {
                    metrics.replication_connected.store(0, Ordering::Relaxed);
                    return Ok(stats);
                }
                b = decoder.try_next() => b,
            };
            match next {
                Ok(Some(batch)) => {
                    // anything else than the WAL stream (version skew, a proxy, the wrong
                    // server) is an error, never a panic
                    if batch.schema().fields() != expected.fields() {
                        warn!(schema = ?batch.schema(), "replication: unexpected stream schema; reconnecting");
                        break;
                    }
                    let (Some(lsns), Some(epochs), Some(payloads)) = (
                        batch.column(0).as_primitive_opt::<UInt64Type>(),
                        batch.column(1).as_primitive_opt::<UInt64Type>(),
                        batch.column(2).as_binary_opt::<i32>(),
                    ) else {
                        warn!("replication: unexpected column types; reconnecting");
                        break;
                    };
                    metrics
                        .replication_last_receive_ms
                        .store(now_ms(), Ordering::Relaxed);
                    for i in 0..batch.num_rows() {
                        let lsn = lsns.value(i);
                        if lsn != from_lsn {
                            metrics.replication_connected.store(0, Ordering::Relaxed);
                            return Err(format!(
                                "primary sent WAL record {lsn} where {from_lsn} was expected; this standby must be re-seeded"
                            ));
                        }
                        let rec = Replicated {
                            lsn,
                            epoch: epochs.value(i),
                            payload: bytes::Bytes::copy_from_slice(payloads.value(i)),
                        };
                        // a standby awaits on backpressure: nothing may be dropped
                        if tx.send(rec).await.is_err() {
                            metrics.replication_connected.store(0, Ordering::Relaxed);
                            return Ok(stats);
                        }
                        stats.frames += 1;
                        from_lsn = lsn + 1;
                        metrics.replication_lsn.store(lsn, Ordering::Relaxed);
                    }
                    // progress: the next failure starts from a short wait again
                    backoff = Duration::from_millis(500);
                }
                Ok(None) => {
                    warn!("replication: primary closed the WAL stream; reconnecting");
                    break;
                }
                Err(arrow_flight::error::FlightError::Tonic(refusal))
                    if refusal.code() == tonic::Code::OutOfRange =>
                {
                    metrics.replication_connected.store(0, Ordering::Relaxed);
                    return Err(format!(
                        "the primary cannot serve WAL from lsn {from_lsn}: {}. Re-seed this standby (copy the primary's WAL directory, then its checkpoint directory)",
                        refusal.message()
                    ));
                }
                Err(e) => {
                    warn!(error = %e, "replication: WAL stream error; reconnecting");
                    break;
                }
            }
        }
        wait_before_retry!();
    }
}
