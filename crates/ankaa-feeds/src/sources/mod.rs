//! Generic message sources for user-defined tables ("bring your own feed").
//!
//! Every source delivers raw messages as [`FrameKind::User`] frames whose payload is
//! `[u16 source id][message]`, so they travel through the same channel, WAL and recovery path as
//! exchange frames; the server maps them onto a table with `ankaa_ingest::generic::RowMapper`.
//!
//! - `file`: a file replayed line by line (NDJSON / CSV), for tests and backfills.
//! - `nats` (feature `nats`): a NATS core subject (at most once: no replay after a restart).
//! - `kafka` (feature `kafka`): a Kafka topic, all partitions.
//! - `mqtt` (feature `mqtt`): an MQTT topic filter, at-least-once with a persistent session.
//!
//! **Positions.** File and Kafka sources report how far they have read with in-band control
//! frames ([`crate::capture::source_state_payload`]). The server persists the newest state it
//! has made durable and hands it back as [`SourceOpts::resume`] on the next start, so a restart
//! continues where the durable data ends: at least once, with a short window of repeated
//! messages after a crash and none after a clean stop.

use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};

use crate::capture::{Frame, FrameKind, source_state_payload, user_payload};

pub mod file;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "nats")]
pub mod nats;

/// Counters returned when a source task ends.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SourceStats {
    /// Messages delivered to the channel.
    pub messages: u64,
    /// Messages dropped because the channel was full.
    pub dropped_full: u64,
    /// (Re)connections.
    pub connects: u64,
    /// Messages dropped because they exceeded [`SourceOpts::max_message_bytes`].
    pub oversized: u64,
}

/// Per-source settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOpts {
    /// Largest message accepted; bigger ones are dropped and counted.
    pub max_message_bytes: usize,
    /// Kafka: start a partition without a saved position at the earliest retained offset
    /// instead of the latest.
    pub start_earliest: bool,
    /// Position saved by a previous run (opaque to the server; produced by the same transport).
    pub resume: Option<String>,
    /// MQTT client id (the broker keeps the session under it); `ankaa-<source id>` when unset.
    pub client_id: Option<String>,
    /// TLS files of an `mqtts://` source.
    pub tls: TlsFiles,
}

/// TLS material of a source, as paths to PEM files (read when the source starts).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsFiles {
    /// CA certificates to trust *instead of* the public (Mozilla) roots: a plant's private PKI.
    pub ca: Option<std::path::PathBuf>,
    /// Client certificate chain (brokers that authenticate clients by certificate).
    pub cert: Option<std::path::PathBuf>,
    /// Private key of the client certificate.
    pub key: Option<std::path::PathBuf>,
}

impl Default for SourceOpts {
    fn default() -> Self {
        Self {
            max_message_bytes: 1024 * 1024,
            start_earliest: false,
            resume: None,
            client_id: None,
            tls: TlsFiles::default(),
        }
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// True (and counted) when `len` exceeds the source's message size limit.
pub(crate) fn oversized(len: usize, opts: &SourceOpts, stats: &mut SourceStats) -> bool {
    if len > opts.max_message_bytes {
        stats.oversized += 1;
        crate::counters::note_oversized();
        true
    } else {
        false
    }
}

/// Wrap a message as a user frame and hand it to the ingest channel (drop-and-count when full).
#[allow(dead_code)] // used by the NATS source only
pub(crate) fn deliver(
    tx: &mpsc::Sender<Frame>,
    source_id: u16,
    msg: &[u8],
    stats: &mut SourceStats,
) {
    let frame = Frame {
        recv_ns: now_ns(),
        kind: FrameKind::User,
        payload: user_payload(source_id, msg),
    };
    if tx.try_send(frame).is_ok() {
        stats.messages += 1;
    } else {
        stats.dropped_full += 1;
        crate::counters::note_dropped_full();
    }
}

/// Like [`deliver`] but waits for channel space instead of dropping: for transports that can
/// be read at the engine's pace (files, Kafka). Returns `false` when the engine is gone or
/// shutdown was requested while waiting.
pub(crate) async fn deliver_wait(
    tx: &mpsc::Sender<Frame>,
    source_id: u16,
    msg: &[u8],
    stats: &mut SourceStats,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let frame = Frame {
        recv_ns: now_ns(),
        kind: FrameKind::User,
        payload: user_payload(source_id, msg),
    };
    tokio::select! {
        r = tx.send(frame) => {
            if r.is_ok() {
                stats.messages += 1;
            }
            r.is_ok()
        }
        () = shutdown_wait(shutdown) => false,
    }
}

/// Report the source's position, in band (after the messages it covers).
pub(crate) async fn send_state(
    tx: &mpsc::Sender<Frame>,
    source_id: u16,
    state: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let frame = Frame {
        recv_ns: now_ns(),
        kind: FrameKind::Control,
        payload: source_state_payload(source_id, state),
    };
    tokio::select! {
        r = tx.send(frame) => r.is_ok(),
        () = shutdown_wait(shutdown) => false,
    }
}

/// Report a final position on the way out: shutdown has been requested, so this does not race
/// against it; it only gives up when the engine does not take the frame within a second.
pub(crate) async fn send_state_bounded(tx: &mpsc::Sender<Frame>, source_id: u16, state: &str) {
    let frame = Frame {
        recv_ns: now_ns(),
        kind: FrameKind::Control,
        payload: source_state_payload(source_id, state),
    };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), tx.send(frame)).await;
}

/// Wait on the shutdown signal (helper for `select!`).
pub(crate) async fn shutdown_wait(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

pub use ankaa_core::redact_url;

/// Which transport a source uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    /// File path, replayed line by line.
    File(std::path::PathBuf),
    /// NATS URL + subject.
    Nats {
        /// `nats://host:4222`.
        url: String,
        /// Subject (wildcards allowed).
        subject: String,
    },
    /// Kafka bootstrap servers + topic.
    Kafka {
        /// Comma-separated `host:port` list.
        bootstrap: String,
        /// Topic.
        topic: String,
    },
    /// MQTT broker URL + topic filter.
    Mqtt {
        /// `mqtt://[user:password@]host[:port]` or `mqtts://…` (TLS, port 8883 by default).
        url: String,
        /// Topic filter (`+` / `#` wildcards allowed).
        topic: String,
    },
}

/// Run a source until shutdown. Unavailable transports (feature not compiled) return at once
/// with zero stats after logging.
pub async fn run_source(
    source_id: u16,
    transport: Transport,
    opts: SourceOpts,
    tx: mpsc::Sender<Frame>,
    shutdown: watch::Receiver<bool>,
) -> SourceStats {
    match transport {
        Transport::File(path) => file::run(source_id, &path, &opts, tx, shutdown).await,
        #[cfg(feature = "nats")]
        Transport::Nats { url, subject } => {
            nats::run(source_id, &url, &subject, &opts, tx, shutdown).await
        }
        #[cfg(not(feature = "nats"))]
        Transport::Nats { .. } => {
            tracing::error!(
                source_id,
                "NATS source requested but ankaa-feeds was built without the `nats` feature"
            );
            SourceStats::default()
        }
        #[cfg(feature = "kafka")]
        Transport::Kafka { bootstrap, topic } => {
            kafka::run(source_id, &bootstrap, &topic, &opts, tx, shutdown).await
        }
        #[cfg(not(feature = "kafka"))]
        Transport::Kafka { .. } => {
            tracing::error!(
                source_id,
                "Kafka source requested but ankaa-feeds was built without the `kafka` feature"
            );
            SourceStats::default()
        }
        #[cfg(feature = "mqtt")]
        Transport::Mqtt { url, topic } => {
            mqtt::run(source_id, &url, &topic, &opts, tx, shutdown).await
        }
        #[cfg(not(feature = "mqtt"))]
        Transport::Mqtt { .. } => {
            tracing::error!(
                source_id,
                "MQTT source requested but ankaa-feeds was built without the `mqtt` feature"
            );
            SourceStats::default()
        }
    }
}
