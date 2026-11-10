//! MQTT 3.1.1 subscription (feature `mqtt`): the transport of most plant-floor, building and
//! energy telemetry (devices, PLC gateways, Sparkplug-style brokers).
//!
//! - **`QoS` 1 and a persistent session** (`clean_session = false`, client id
//!   `ankaa-<source name>`): while the server is down the broker queues the messages of the
//!   subscription and delivers them on reconnect, so an outage or a restart loses nothing the
//!   broker kept.
//! - **Acknowledged after hand-off**: a message is acknowledged only once it is in the ingest
//!   channel (manual acks), and the channel is waited on rather than dropped from, so a full
//!   channel slows the broker down instead of losing readings. What remains is the window
//!   between the hand-off and the WAL group commit: a crash there loses those messages (at most
//!   once for that window, at least once otherwise).
//! - Every message is delivered as `topic \0 payload`: `$topic` / `$topic.N` in a source's `map`
//!   read the topic or one of its `/`-separated segments.
//!
//! URL: `mqtt://[user[:password]@]host[:port]` (port 1883 by default). Credentials are never
//! logged. TLS (`mqtts://`) is not built in yet: use a trusted network or a local TLS proxy.

use std::time::Duration;

use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use tokio::sync::{mpsc, watch};

use super::{SourceOpts, SourceStats, deliver_wait, oversized, redact_url, shutdown_wait};
use crate::capture::Frame;

/// Broker address and credentials from an `mqtt://` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerUrl {
    /// Host name or address.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// `(user, password)` when the URL carries them.
    pub credentials: Option<(String, String)>,
}

/// Parse `mqtt://[user[:password]@]host[:port]`.
pub fn parse_url(url: &str) -> Result<BrokerUrl, String> {
    let rest = url.strip_prefix("mqtt://").ok_or_else(|| {
        "MQTT url must start with mqtt:// (mqtts:// is not supported yet)".to_owned()
    })?;
    let rest = rest.trim_end_matches('/');
    let (auth, hostport) = match rest.rsplit_once('@') {
        Some((a, h)) => (Some(a), h),
        None => (None, rest),
    };
    let credentials = auth.map(|a| match a.split_once(':') {
        Some((u, p)) => (u.to_owned(), p.to_owned()),
        None => (a.to_owned(), String::new()),
    });
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (
            h,
            p.parse::<u16>()
                .map_err(|_| format!("MQTT url: bad port {p:?}"))?,
        ),
        _ => (hostport, 1883),
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() || host.contains('/') {
        return Err("MQTT url: missing or invalid host".to_owned());
    }
    Ok(BrokerUrl {
        host: host.to_owned(),
        port,
        credentials,
    })
}

/// `topic \0 payload`, the message the row mapper of an MQTT source expects.
#[must_use]
pub fn topic_message(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(topic.len() + 1 + payload.len());
    m.extend_from_slice(topic.as_bytes());
    m.push(0);
    m.extend_from_slice(payload);
    m
}

fn options(broker: &BrokerUrl, client_id: &str, opts: &SourceOpts) -> MqttOptions {
    let mut o = MqttOptions::new(client_id, broker.host.clone(), broker.port);
    o.set_keep_alive(Duration::from_secs(30));
    o.set_clean_session(false);
    o.set_manual_acks(true);
    // the topic and the MQTT header ride on top of the payload limit
    let max = opts.max_message_bytes.saturating_add(64 * 1024);
    o.set_max_packet_size(max, 64 * 1024);
    if let Some((u, p)) = &broker.credentials {
        o.set_credentials(u.clone(), p.clone());
    }
    o
}

/// Subscribe to `topic` (MQTT filter, `+` and `#` allowed) and deliver every message until
/// shutdown.
pub async fn run(
    source_id: u16,
    url: &str,
    topic: &str,
    opts: &SourceOpts,
    tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
) -> SourceStats {
    let mut stats = SourceStats::default();
    let shown = redact_url(url);
    let broker = match parse_url(url) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(source_id, url = %shown, error = %e, "mqtt source not started");
            return stats;
        }
    };
    let client_id = opts
        .client_id
        .clone()
        .unwrap_or_else(|| format!("ankaa-{source_id}"));
    let (client, mut events): (AsyncClient, EventLoop) =
        AsyncClient::new(options(&broker, &client_id, opts), 64);
    let mut backoff = Duration::from_millis(500);
    loop {
        let event = tokio::select! {
            () = shutdown_wait(&mut shutdown) => break,
            e = events.poll() => e,
        };
        match event {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                stats.connects += 1;
                backoff = Duration::from_millis(500);
                tracing::info!(source_id, url = %shown, %topic, session_present = ack.session_present, "mqtt connected");
                // also with a stored session: subscribing again is harmless and covers a
                // broker that lost it
                if let Err(e) = client.try_subscribe(topic, QoS::AtLeastOnce) {
                    tracing::warn!(source_id, %topic, error = %e, "mqtt subscribe request failed");
                }
            }
            Ok(Event::Incoming(Packet::SubAck(ack))) => {
                if ack
                    .return_codes
                    .iter()
                    .any(|c| matches!(c, rumqttc::SubscribeReasonCode::Failure))
                {
                    tracing::error!(source_id, %topic, "mqtt broker refused the subscription");
                }
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                if !oversized(p.payload.len(), opts, &mut stats) {
                    let msg = topic_message(&p.topic, &p.payload);
                    if !deliver_wait(&tx, source_id, &msg, &mut stats, &mut shutdown).await {
                        break; // engine gone or shutting down: not acknowledged, redelivered
                    }
                }
                if let Err(e) = client.try_ack(&p) {
                    tracing::debug!(source_id, error = %e, "mqtt ack not queued");
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(source_id, url = %shown, error = %e, "mqtt connection lost; reconnecting");
                tokio::select! {
                    () = shutdown_wait(&mut shutdown) => break,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
    let _ = client.try_disconnect();
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        assert_eq!(
            parse_url("mqtt://broker.local").unwrap(),
            BrokerUrl {
                host: "broker.local".into(),
                port: 1883,
                credentials: None
            }
        );
        let b = parse_url("mqtt://plant:s3cret@10.0.0.5:1884/").unwrap();
        assert_eq!((b.host.as_str(), b.port), ("10.0.0.5", 1884));
        assert_eq!(b.credentials, Some(("plant".into(), "s3cret".into())));
        assert_eq!(parse_url("mqtt://[::1]:1883").unwrap().host, "::1");
        for bad in [
            "tcp://h",
            "mqtts://h",
            "mqtt://",
            "mqtt://h:x",
            "mqtt://h/topic",
        ] {
            assert!(parse_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn topic_prefix() {
        assert_eq!(topic_message("a/b", b"{}"), b"a/b\0{}");
    }
}
