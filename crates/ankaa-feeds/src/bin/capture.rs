//! `ankaa-capture`: record raw WebSocket frames from a market-data feed into `.ankc` files.
//!
//! Default target is Binance's keyless market-data endpoint. Reconnects with backoff, rotates
//! files hourly, notes connect/disconnect events as control frames, and logs statistics.
//!
//! Example:
//! `ankaa-capture --out captures --streams btcusdt@trade,btcusdt@depth@100ms,ethusdt@trade`
#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ankaa_feeds::capture::{Frame, FrameKind, encode_frame, encode_header};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Capture raw market-data WebSocket frames into .ankc files"
)]
struct Args {
    /// WebSocket base URL (Binance market-data-only endpoint by default).
    #[arg(long, default_value = "wss://data-stream.binance.vision/stream")]
    url: String,
    /// Comma-separated stream names (Binance combined-stream syntax).
    #[arg(
        long,
        default_value = "btcusdt@trade,btcusdt@depth@100ms,ethusdt@trade,ethusdt@depth@100ms"
    )]
    streams: String,
    /// Output directory.
    #[arg(long, default_value = "captures")]
    out: PathBuf,
    /// Rotate files every N seconds.
    #[arg(long, default_value_t = 3600)]
    rotate_secs: u64,
    /// Stop after N seconds (0 = run forever).
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    /// After each connect, fetch a REST depth snapshot for every depth stream symbol and store it
    /// as a snapshot frame, so replays can rebuild exact books.
    #[arg(long, default_value_t = false)]
    snapshots: bool,
    /// REST base URL for snapshots.
    #[arg(long, default_value = ankaa_feeds::live::BINANCE_REST)]
    rest_url: String,
    /// Binance API key (or env `BINANCE_API_KEY`) for the SBE endpoint. Never logged.
    #[arg(long, env = "BINANCE_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
}

fn now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

struct Writer {
    dir: PathBuf,
    source: String,
    file: Option<tokio::fs::File>,
    opened_at: u64,
    rotate_secs: u64,
    buf: BytesMut,
    frames: u64,
    bytes: u64,
}

impl Writer {
    async fn ensure_open(&mut self) -> std::io::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        if self.file.is_some() && now - self.opened_at < self.rotate_secs {
            return Ok(());
        }
        self.flush().await?;
        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.dir.join(format!("{now}.ankc"));
        let mut f = tokio::fs::File::create(&path).await?;
        let mut hdr = BytesMut::new();
        encode_header(&self.source, &mut hdr);
        f.write_all(&hdr).await?;
        info!(path = %path.display(), "opened capture file");
        self.file = Some(f);
        self.opened_at = now;
        Ok(())
    }

    async fn write(&mut self, frame: &Frame) -> std::io::Result<()> {
        self.ensure_open().await?;
        encode_frame(frame, &mut self.buf);
        self.frames += 1;
        self.bytes += frame.payload.len() as u64;
        if self.buf.len() >= 1 << 20 {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        if let Some(f) = self.file.as_mut() {
            if !self.buf.is_empty() {
                f.write_all(&self.buf).await?;
                self.buf.clear();
            }
            f.flush().await?;
        }
        Ok(())
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    // rustls needs exactly one process-level crypto provider; dependencies may enable both.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();
    let args = Args::parse();
    let streams = args
        .streams
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{}?streams={}", args.url, streams);
    // A stream list that cannot make a URL never will: stop now instead of retrying it forever
    // (a list separated by spaces instead of commas did exactly that for 20 minutes).
    if let Some(bad) = args.streams.split(',').map(str::trim).find(|s| {
        !s.bytes()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'/' | b'?' | b'&' | b'#'))
    }) {
        anyhow::bail!("invalid stream name {bad:?}: names are separated by commas, without spaces");
    }
    if streams.is_empty() {
        anyhow::bail!("--streams is empty");
    }
    ankaa_feeds::live::build_request(&url, args.api_key.as_deref())
        .map_err(|e| anyhow::anyhow!("cannot build a request for {url}: {e}"))?;
    let mut w = Writer {
        dir: args.out.clone(),
        source: url.clone(),
        file: None,
        opened_at: 0,
        rotate_secs: args.rotate_secs.max(60),
        buf: BytesMut::with_capacity(2 << 20),
        frames: 0,
        bytes: 0,
    };
    let started = tokio::time::Instant::now();
    let mut backoff = Duration::from_secs(1);
    let mut reconnects = 0u64;
    let mut stats_tick = tokio::time::interval(Duration::from_secs(60));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    'outer: loop {
        if args.duration_secs > 0 && started.elapsed().as_secs() >= args.duration_secs {
            break;
        }
        info!(%url, "connecting");
        let conn = tokio::select! {
            r = async {
                let req = ankaa_feeds::live::build_request(&url, args.api_key.as_deref()).map_err(|e| tokio_tungstenite::tungstenite::Error::Url(tokio_tungstenite::tungstenite::error::UrlError::UnableToConnect(e)))?;
                tokio_tungstenite::connect_async(req).await
            } => r,
            _ = &mut shutdown => break 'outer,
        };
        let (mut ws, _) = match conn {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, backoff_s = backoff.as_secs(), "connect failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        w.write(&Frame {
            recv_ns: now_ns(),
            kind: FrameKind::Control,
            payload: Bytes::from("connect"),
        })
        .await?;
        if args.snapshots {
            let names: Vec<&str> = args.streams.split(',').map(str::trim).collect();
            for sym in ankaa_feeds::live::depth_symbols(&names) {
                match ankaa_feeds::live::fetch_binance_depth_snapshot(&args.rest_url, &sym, 1000)
                    .await
                {
                    Ok(frame) => w.write(&frame).await?,
                    Err(e) => warn!(%sym, error = %e, "snapshot fetch failed"),
                }
            }
        }
        loop {
            tokio::select! {
                msg = ws.next() => {
                    let Some(msg) = msg else {
                        warn!("stream ended");
                        break;
                    };
                    match msg {
                        Ok(Message::Text(t)) => w.write(&Frame { recv_ns: now_ns(), kind: FrameKind::Text, payload: Bytes::from(t.as_bytes().to_vec()) }).await?,
                        Ok(Message::Binary(b)) => w.write(&Frame { recv_ns: now_ns(), kind: FrameKind::Binary, payload: Bytes::from(b.to_vec()) }).await?,
                        Ok(Message::Ping(p)) => { let _ = ws.send(Message::Pong(p)).await; }
                        Ok(Message::Close(c)) => { warn!(?c, "server closed"); break; }
                        Ok(_) => {}
                        Err(e) => { warn!(error = %e, "ws error"); break; }
                    }
                }
                _ = stats_tick.tick() => {
                    w.flush().await?;
                    info!(frames = w.frames, payload_bytes = w.bytes, reconnects, uptime_s = started.elapsed().as_secs(), "stats");
                }
                _ = &mut shutdown => { info!("ctrl-c"); break 'outer; }
            }
            if args.duration_secs > 0 && started.elapsed().as_secs() >= args.duration_secs {
                break 'outer;
            }
        }
        reconnects += 1;
        w.write(&Frame {
            recv_ns: now_ns(),
            kind: FrameKind::Control,
            payload: Bytes::from("disconnect"),
        })
        .await?;
        tokio::time::sleep(backoff).await;
    }
    w.write(&Frame {
        recv_ns: now_ns(),
        kind: FrameKind::Control,
        payload: Bytes::from("shutdown"),
    })
    .await?;
    w.flush().await?;
    info!(
        frames = w.frames,
        payload_bytes = w.bytes,
        reconnects,
        "done"
    );
    Ok(())
}
