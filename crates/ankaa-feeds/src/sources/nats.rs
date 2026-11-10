//! NATS core subscription (feature `nats`). The client reconnects on its own; each
//! reconnection is counted. Core NATS has no replay: messages published while the server is
//! down, or dropped because the ingest channel was full, are gone (at most once).

use futures::StreamExt;
use tokio::sync::{mpsc, watch};

use super::{SourceOpts, SourceStats, deliver, oversized, redact_url, shutdown_wait};
use crate::capture::Frame;

/// Subscribe to `subject` on `url` and deliver every message until shutdown.
pub async fn run(
    source_id: u16,
    url: &str,
    subject: &str,
    opts: &SourceOpts,
    tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
) -> SourceStats {
    let mut stats = SourceStats::default();
    let mut backoff = std::time::Duration::from_millis(500);
    // credentials may be part of the URL: never log it verbatim
    let shown = redact_url(url);
    loop {
        let client = match async_nats::connect(url).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(source_id, url = %shown, error = %e, "nats connect failed; retrying");
                tokio::select! {
                    () = shutdown_wait(&mut shutdown) => return stats,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                continue;
            }
        };
        stats.connects += 1;
        let mut sub = match client.subscribe(subject.to_owned()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(source_id, %subject, error = %e, "nats subscribe failed");
                tokio::select! {
                    () = shutdown_wait(&mut shutdown) => return stats,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                continue;
            }
        };
        backoff = std::time::Duration::from_millis(500);
        tracing::info!(source_id, url = %shown, %subject, "nats subscribed");
        loop {
            tokio::select! {
                () = shutdown_wait(&mut shutdown) => return stats,
                msg = sub.next() => match msg {
                    Some(m) => {
                        if !oversized(m.payload.len(), opts, &mut stats) {
                            deliver(&tx, source_id, &m.payload, &mut stats);
                        }
                    }
                    None => break, // subscription ended: reconnect
                }
            }
        }
    }
}
