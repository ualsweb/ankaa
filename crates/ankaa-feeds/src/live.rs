//! Live WebSocket source: connects, reconnects with backoff, answers pings, and delivers raw
//! frames (with host receive timestamps) to a bounded channel. Used by `ankaa-capture` and by
//! `ankaad serve`. Decoding happens on the consumer side so this task stays trivial.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::capture::{Frame, FrameKind};

/// Binance keyless market-data endpoint (combined streams).
pub const BINANCE_DATA_STREAM: &str = "wss://data-stream.binance.vision/stream";

/// Build a Binance combined-stream URL from stream names.
#[must_use]
pub fn binance_url(base: &str, streams: &[&str]) -> String {
    format!("{base}?streams={}", streams.join("/"))
}

/// Host clock, ns since the Unix epoch.
#[must_use]
pub fn now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

pub use crate::counters::dropped_full_total;
use crate::counters::note_dropped_full;

/// Largest REST snapshot body accepted.
const MAX_SNAPSHOT_BODY: usize = 16 * 1024 * 1024;

/// Counters updated by [`run_ws_source`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceStats {
    /// Successful connections.
    pub connects: u64,
    /// Frames delivered.
    pub frames: u64,
    /// Frames dropped because the channel was full (backpressure, counted not blocked).
    pub dropped_full: u64,
}

/// Run until `shutdown` resolves. Frames go to `tx`; when the channel is full the frame is
/// dropped and counted (RX never blocks, PRD v4.1 §4.4). Returns final stats.
pub async fn run_ws_source(
    url: String,
    tx: mpsc::Sender<Frame>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> SourceStats {
    run_ws_source_with_key(url, None, tx, shutdown).await
}

/// Binance SBE market-data endpoint (requires an Ed25519 API key in `X-MBX-APIKEY`).
pub const BINANCE_SBE_STREAM: &str = "wss://stream-sbe.binance.com/stream";

/// Like [`run_ws_source`], adding the `X-MBX-APIKEY` header when `api_key` is set (needed for the
/// Binance SBE endpoint; the key is never logged).
pub async fn run_ws_source_with_key(
    url: String,
    api_key: Option<String>,
    tx: mpsc::Sender<Frame>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> SourceStats {
    // rustls needs exactly one process-level crypto provider; dependencies may enable both.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut stats = SourceStats::default();
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            return stats;
        }
        info!(url = %ankaa_core::redact_url(&url), "connecting");
        let request = match build_request(&url, api_key.as_deref()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "invalid websocket request");
                return stats;
            }
        };
        let conn = tokio::select! {
            r = tokio_tungstenite::connect_async(request) => r,
            _ = shutdown.changed() => return stats,
        };
        let (mut ws, _) = match conn {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, backoff_s = backoff.as_secs(), "connect failed");
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    _ = shutdown.changed() => return stats,
                }
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        stats.connects += 1;
        let _ = tx.try_send(Frame {
            recv_ns: now_ns(),
            kind: FrameKind::Control,
            payload: Bytes::from_static(b"connect"),
        });
        loop {
            let msg = tokio::select! {
                m = ws.next() => m,
                _ = shutdown.changed() => { let _ = ws.close(None).await; return stats; }
            };
            let Some(msg) = msg else {
                warn!("stream ended");
                break;
            };
            let frame = match msg {
                Ok(Message::Text(t)) => Frame {
                    recv_ns: now_ns(),
                    kind: FrameKind::Text,
                    payload: Bytes::from(t.as_bytes().to_vec()),
                },
                Ok(Message::Binary(b)) => Frame {
                    recv_ns: now_ns(),
                    kind: FrameKind::Binary,
                    payload: Bytes::from(b.to_vec()),
                },
                Ok(Message::Ping(p)) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Ok(Message::Close(c)) => {
                    warn!(?c, "server closed");
                    break;
                }
                Ok(_) => continue,
                Err(e) => {
                    warn!(error = %e, "ws error");
                    break;
                }
            };
            match tx.try_send(frame) {
                Ok(()) => stats.frames += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    stats.dropped_full += 1;
                    note_dropped_full();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return stats,
            }
        }
        let _ = tx.try_send(Frame {
            recv_ns: now_ns(),
            kind: FrameKind::Control,
            payload: Bytes::from_static(b"disconnect"),
        });
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => return stats,
        }
    }
}

/// Binance spot REST base (public, keyless; weight-limited).
pub const BINANCE_REST: &str = "https://api.binance.com";

/// Fetch a depth snapshot for `symbol` (upper-case, e.g. `BTCUSDT`) and wrap it as a
/// [`FrameKind::Snapshot`] frame: `{"s":"<symbol>","snapshot":<REST body>}`. `limit` ≤ 5000.
pub async fn fetch_binance_depth_snapshot(
    rest_base: &str,
    symbol: &str,
    limit: u32,
) -> Result<Frame, String> {
    let url = format!(
        "{rest_base}/api/v3/depth?symbol={symbol}&limit={}",
        limit.min(5000)
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("{url}: HTTP {}", resp.status()));
    }
    // bound the body before and while reading it (a snapshot is a few hundred kilobytes)
    if resp
        .content_length()
        .is_some_and(|n| n > MAX_SNAPSHOT_BODY as u64)
    {
        return Err("snapshot body too large".to_owned());
    }
    let mut resp = resp;
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
        if body.len() + chunk.len() > MAX_SNAPSHOT_BODY {
            return Err("snapshot body too large".to_owned());
        }
        body.extend_from_slice(&chunk);
    }
    let mut payload = Vec::with_capacity(body.len() + symbol.len() + 24);
    payload.extend_from_slice(br#"{"s":""#);
    payload.extend_from_slice(symbol.as_bytes());
    payload.extend_from_slice(br#"","snapshot":"#);
    payload.extend_from_slice(&body);
    payload.push(b'}');
    Ok(Frame {
        recv_ns: now_ns(),
        kind: FrameKind::Snapshot,
        payload: Bytes::from(payload),
    })
}

/// Symbols (upper-case) that have a depth stream in a Binance stream list.
#[must_use]
pub fn depth_symbols(streams: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = streams
        .iter()
        .filter_map(|s| {
            let (sym, rest) = s.split_once('@')?;
            rest.starts_with("depth").then(|| sym.to_ascii_uppercase())
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

/// Build the WebSocket handshake request, adding `X-MBX-APIKEY` when a key is given.
pub fn build_request(
    url: &str,
    api_key: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().map_err(|e| e.to_string())?;
    if let Some(k) = api_key {
        let v = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(k)
            .map_err(|_| "api key contains invalid header characters".to_owned())?;
        req.headers_mut().insert("X-MBX-APIKEY", v);
    }
    Ok(req)
}
