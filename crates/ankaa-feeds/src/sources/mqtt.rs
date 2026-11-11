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
//! URL: `mqtt://[user[:password]@]host[:port]` (port 1883), or `mqtts://…` for TLS (port 8883).
//! Credentials are never logged.
//!
//! **TLS** (rustls with the aws-lc-rs provider: AES-GCM through the Arm AES/PMULL instructions,
//! no OpenSSL): TLS 1.3 and 1.2, the broker certificate checked against the public (Mozilla)
//! roots, or, when `tls_ca` is given, against that CA only (a plant's private PKI must not also
//! trust every public CA). `tls_cert` + `tls_key` add a client certificate (AWS `IoT` Core, brokers with
//! mutual TLS). The files are read once, when the source starts; a bad file stops the source with
//! the reason in the log.

use std::time::Duration;

use std::sync::Arc;

use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, TlsConfiguration, Transport,
};
use tokio::sync::{mpsc, watch};

use super::{
    SourceOpts, SourceStats, TlsFiles, deliver_wait, oversized, redact_url, shutdown_wait,
};
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
    /// `mqtts://`: connect with TLS.
    pub tls: bool,
}

/// Parse `mqtt://[user[:password]@]host[:port]` or `mqtts://…`.
pub fn parse_url(url: &str) -> Result<BrokerUrl, String> {
    let (rest, tls) = if let Some(r) = url.strip_prefix("mqtts://") {
        (r, true)
    } else if let Some(r) = url.strip_prefix("mqtt://") {
        (r, false)
    } else {
        return Err("MQTT url must start with mqtt:// or mqtts://".to_owned());
    };
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
        _ => (hostport, if tls { 8883 } else { 1883 }),
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() || host.contains('/') {
        return Err("MQTT url: missing or invalid host".to_owned());
    }
    Ok(BrokerUrl {
        host: host.to_owned(),
        port,
        credentials,
        tls,
    })
}

/// The TLS client configuration of an `mqtts://` source. Reads the PEM files; every error names
/// the file and what is wrong with it.
pub fn tls_config(files: &TlsFiles) -> Result<Arc<rustls::ClientConfig>, String> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let read_certs = |path: &std::path::Path| -> Result<Vec<CertificateDer<'static>>, String> {
        let certs = CertificateDer::pem_file_iter(path)
            .map_err(|e| format!("{}: {e}", path.display()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if certs.is_empty() {
            return Err(format!(
                "{}: no PEM certificate in the file",
                path.display()
            ));
        }
        Ok(certs)
    };
    let mut roots = rustls::RootCertStore::empty();
    match &files.ca {
        Some(path) => {
            for c in read_certs(path)? {
                roots
                    .add(c)
                    .map_err(|e| format!("{}: not a usable CA certificate: {e}", path.display()))?;
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS: {e}"))?
        .with_root_certificates(roots);
    let config = match (&files.cert, &files.key) {
        (Some(cert), Some(key)) => {
            let chain = read_certs(cert)?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|e| format!("{}: no usable PEM private key: {e}", key.display()))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("client certificate and key do not match: {e}"))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => return Err("tls_cert and tls_key must be given together".to_owned()),
    };
    Ok(Arc::new(config))
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

fn options(
    broker: &BrokerUrl,
    client_id: &str,
    opts: &SourceOpts,
    tls: Option<Arc<rustls::ClientConfig>>,
) -> MqttOptions {
    let mut o = MqttOptions::new(client_id, broker.host.clone(), broker.port);
    if let Some(config) = tls {
        o.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(config)));
    }
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
    let tls = if broker.tls {
        match tls_config(&opts.tls) {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::error!(source_id, url = %shown, error = %e, "mqtt source not started: TLS configuration");
                return stats;
            }
        }
    } else {
        None
    };
    let client_id = opts
        .client_id
        .clone()
        .unwrap_or_else(|| format!("ankaa-{source_id}"));
    let (client, mut events): (AsyncClient, EventLoop) =
        AsyncClient::new(options(&broker, &client_id, opts, tls), 64);
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
                tracing::info!(source_id, url = %shown, %topic, tls = broker.tls, session_present = ack.session_present, "mqtt connected");
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
                credentials: None,
                tls: false,
            }
        );
        let b = parse_url("mqtt://plant:s3cret@10.0.0.5:1884/").unwrap();
        assert_eq!((b.host.as_str(), b.port), ("10.0.0.5", 1884));
        assert_eq!(b.credentials, Some(("plant".into(), "s3cret".into())));
        assert_eq!(parse_url("mqtt://[::1]:1883").unwrap().host, "::1");
        let t = parse_url("mqtts://broker.example.com").unwrap();
        assert!(t.tls && t.port == 8883);
        assert!(!parse_url("mqtt://h").unwrap().tls);
        for bad in [
            "tcp://h",
            "ssl://h",
            "mqtt://",
            "mqtt://h:x",
            "mqtt://h/topic",
        ] {
            assert!(parse_url(bad).is_err(), "{bad}");
        }
    }

    /// TLS material is checked when the source starts: every bad input is refused with the file
    /// it came from; good inputs give a configuration.
    #[test]
    fn tls_files_are_checked() {
        let dir = tempfile::tempdir().unwrap();
        let w = |name: &str, body: &str| {
            let p = dir.path().join(name);
            std::fs::write(&p, body).unwrap();
            p
        };
        let ca = rcgen::generate_simple_self_signed(vec!["broker.plant".into()]).unwrap();
        let client = rcgen::generate_simple_self_signed(vec!["ankaa".into()]).unwrap();
        let other = rcgen::generate_simple_self_signed(vec!["other".into()]).unwrap();
        let ca_pem = w("ca.pem", &ca.cert.pem());
        let cert_pem = w("client.pem", &client.cert.pem());
        let key_pem = w("client.key", &client.signing_key.serialize_pem());
        let wrong_key = w("other.key", &other.signing_key.serialize_pem());
        let empty = w("empty.pem", "");
        // public roots, a private CA, a client certificate
        tls_config(&TlsFiles::default()).unwrap();
        tls_config(&TlsFiles {
            ca: Some(ca_pem.clone()),
            ..TlsFiles::default()
        })
        .unwrap();
        tls_config(&TlsFiles {
            ca: Some(ca_pem.clone()),
            cert: Some(cert_pem.clone()),
            key: Some(key_pem.clone()),
        })
        .unwrap();
        // refused, each with a reason that names the problem
        let err = |f: TlsFiles| tls_config(&f).unwrap_err();
        assert!(
            err(TlsFiles {
                ca: Some(dir.path().join("missing.pem")),
                ..TlsFiles::default()
            })
            .contains("missing.pem")
        );
        assert!(
            err(TlsFiles {
                ca: Some(empty.clone()),
                ..TlsFiles::default()
            })
            .contains("no PEM certificate")
        );
        assert!(
            err(TlsFiles {
                cert: Some(cert_pem.clone()),
                ..TlsFiles::default()
            })
            .contains("together")
        );
        assert!(
            err(TlsFiles {
                cert: Some(cert_pem.clone()),
                key: Some(empty),
                ..TlsFiles::default()
            })
            .contains("private key")
        );
        assert!(
            err(TlsFiles {
                cert: Some(cert_pem),
                key: Some(wrong_key),
                ..TlsFiles::default()
            })
            .contains("do not match")
        );
    }

    #[test]
    fn topic_prefix() {
        assert_eq!(topic_message("a/b", b"{}"), b"a/b\0{}");
    }
}
