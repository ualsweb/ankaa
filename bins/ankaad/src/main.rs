//! `ankaad`: ANKAA command line and (later) server.
//!
//! P1-11a: `replay` ingests one or more `.ankc` capture files through the Binance JSON decoder
//! into in-memory tables (`trades`, `book_deltas`, `quotes` = top-of-book after each update)
//! and runs SQL against them. This is the end-to-end path of PRD v4.1 §4.4–4.5 without the
//! network: capture → decoder → partition appenders → chunks → `DataFusion`.
#![forbid(unsafe_code)]

mod continuous;
mod dict_names;
mod map_pool;
mod metrics;
mod query;
mod replication;
mod serve;
mod source_state;
mod sources;
mod tables;
mod tiering;
mod wal_writer;
mod write_pool;

use std::path::PathBuf;
use std::time::Instant;

// Queries allocate and free buffers of megabytes on several threads; the glibc allocator
// returned that memory to the kernel and faulted it in again, which showed as run-to-run
// variation on the server (docs/benchmarks/2026-11-10-axion-first-light.md).
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use ankaa_feeds::binance::BinanceJsonDecoder;
use ankaa_feeds::capture::{FrameKind, decode_all_bytes_tolerant};
use ankaa_ingest::{Decoder, EventBatch};
use ankaa_sql::{register_functions, register_snapshot};
use clap::{Parser, Subcommand};
use tracing::{info, warn};

use crate::tables::Tables;

#[derive(Parser, Debug)]
#[command(version, about = "ANKAA-ENGINE: Arm-native real-time tick-data engine")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
enum Cmd {
    /// Replay capture files into in-memory tables and run SQL.
    Replay {
        /// `.ankc` capture files (in time order).
        #[arg(required = true)]
        captures: Vec<PathBuf>,
        /// SQL to run (repeatable). Tables: `trades`, `book_deltas`, `quotes`, `symbols`.
        /// Inputs: `.ankc` captures, Databento `.dbn`/`.dbn.zst`, or `.csv`/`.parquet` trade files
        /// (columns ts, symbol, price, size[, side, seq]).
        #[arg(long = "sql")]
        sql: Vec<String>,
        /// Treat depth diffs as needing a snapshot frame (captures recorded with `--snapshots`).
        #[arg(long, default_value_t = false)]
        expect_snapshots: bool,
        /// Also write each query result as CSV (`q01.csv`, `q02.csv`, ...) into this directory
        /// (for differential oracles and benchmark loaders).
        #[arg(long)]
        out_dir: Option<PathBuf>,
        /// Print only the timing line of each query, not its rows (for profiling: formatting a
        /// large result costs more than running the query).
        #[arg(long, default_value_t = false)]
        quiet: bool,
    },
    /// Ingest live Binance streams and serve SQL over Arrow Flight (+ /metrics).
    ///
    /// Settings come from `--config <file.toml>` (see `ankaad config print` for the defaults);
    /// every flag below overrides the file.
    Serve {
        /// TOML configuration file.
        #[arg(long, short = 'c')]
        config: Option<PathBuf>,
        /// WebSocket base URL (`feed.url`).
        #[arg(long)]
        url: Option<String>,
        /// REST base URL for depth snapshots (`feed.rest_url`).
        #[arg(long)]
        rest_url: Option<String>,
        /// Comma-separated Binance stream names (`feed.streams`).
        #[arg(long)]
        streams: Option<String>,
        /// Write-ahead log directory (`storage.wal_dir`); enables recovery on restart.
        #[arg(long)]
        wal_dir: Option<PathBuf>,
        /// Disable REST depth snapshots (books start cold from diffs).
        #[arg(long, default_value_t = false)]
        no_depth_snapshots: bool,
        /// Binance API key (or env `BINANCE_API_KEY`) for the SBE endpoint
        /// (`--url wss://stream-sbe.binance.com/stream`). Never logged.
        #[arg(long, env = "BINANCE_API_KEY", hide_env_values = true)]
        api_key: Option<String>,
        /// Flight bind address (`server.flight_addr`).
        #[arg(long)]
        flight_addr: Option<std::net::SocketAddr>,
        /// Metrics bind address (`server.metrics_addr`).
        #[arg(long)]
        metrics_addr: Option<std::net::SocketAddr>,
        /// PostgreSQL wire protocol bind address (`server.pg_addr`), read-only; password = token.
        #[arg(long)]
        pg_addr: Option<std::net::SocketAddr>,
        /// Bearer token (or env `ANKAA_TOKEN`, or `server.token_env`). Required for non-loopback binds.
        #[arg(long, env = "ANKAA_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// TLS certificate chain, PEM (`server.tls_cert`).
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        /// TLS private key, PEM (`server.tls_key`).
        #[arg(long)]
        tls_key: Option<PathBuf>,
        /// Client CA, PEM: require client certificates (`server.tls_client_ca`).
        #[arg(long)]
        tls_client_ca: Option<PathBuf>,
        /// Snapshot (table swap) interval in milliseconds (`storage.snapshot_ms`).
        #[arg(long)]
        snapshot_ms: Option<u64>,
        /// Show `dict` columns as numeric ids instead of names (`server.dict_columns = "ids"`).
        #[arg(long)]
        dict_ids: bool,
        /// Stop after N seconds (0 = until Ctrl-C).
        #[arg(long, default_value_t = 0)]
        duration_secs: u64,
        /// Run as a read-only standby of this primary (`host:port` of its Flight endpoint); the
        /// primary's WAL is streamed and ingested instead of a feed (`replication.standby_of`).
        #[arg(long)]
        standby_of: Option<String>,
    },
    /// Run SQL against a running `ankaad serve` over Arrow Flight.
    Query {
        /// Flight address.
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        /// Bearer token (or env `ANKAA_TOKEN`).
        #[arg(long, env = "ANKAA_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// CA certificate (PEM) that signed the server certificate; enables TLS.
        #[arg(long)]
        tls_ca: Option<PathBuf>,
        /// Expected server name when it differs from the host part of `--addr`.
        #[arg(long)]
        tls_domain: Option<String>,
        /// Client certificate (PEM) for mutual TLS.
        #[arg(long, requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        /// Client private key (PEM) for mutual TLS.
        #[arg(long, requires = "tls_cert")]
        tls_key: Option<PathBuf>,
        /// SQL (repeatable).
        #[arg(long = "sql", required = true)]
        sql: Vec<String>,
    },
    /// Configuration helpers.
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCmd {
    /// Print the default configuration as TOML.
    Print,
    /// Parse, validate and run the start-up resource checks on a file; exit non-zero on failure.
    Check {
        /// TOML configuration file.
        file: PathBuf,
    },
}

/// Load the config file (or defaults) and apply CLI overrides.
#[allow(clippy::too_many_arguments)]
fn build_config(
    file: Option<&PathBuf>,
    url: Option<String>,
    rest_url: Option<String>,
    streams: Option<String>,
    wal_dir: Option<PathBuf>,
    no_depth_snapshots: bool,
    flight_addr: Option<std::net::SocketAddr>,
    metrics_addr: Option<std::net::SocketAddr>,
    pg_addr: Option<std::net::SocketAddr>,
    token: Option<String>,
    tls: (Option<PathBuf>, Option<PathBuf>, Option<PathBuf>),
    snapshot_ms: Option<u64>,
) -> anyhow::Result<ankaa_core::Config> {
    let mut cfg = match file {
        Some(f) => ankaa_core::Config::parse_toml(
            &std::fs::read_to_string(f)
                .map_err(|e| anyhow::anyhow!("config {}: {e}", f.display()))?,
        )?,
        None => ankaa_core::Config::default(),
    };
    if let Some(v) = url {
        cfg.feed.url = v;
    }
    if let Some(v) = rest_url {
        cfg.feed.rest_url = v;
    }
    if let Some(v) = streams {
        cfg.feed.streams = v
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if wal_dir.is_some() {
        cfg.storage.wal_dir = wal_dir;
    }
    if no_depth_snapshots {
        cfg.feed.depth_snapshots = false;
    }
    if let Some(v) = flight_addr {
        cfg.server.flight_addr = v;
    }
    if let Some(v) = metrics_addr {
        cfg.server.metrics_addr = v;
    }
    if pg_addr.is_some() {
        cfg.server.pg_addr = pg_addr;
    }
    if let Some(v) = token {
        cfg.server.token = Some(ankaa_core::Secret::new(v));
    }
    if tls.0.is_some() {
        cfg.server.tls_cert = tls.0;
    }
    if tls.1.is_some() {
        cfg.server.tls_key = tls.1;
    }
    if tls.2.is_some() {
        cfg.server.tls_client_ca = tls.2;
    }
    if let Some(v) = snapshot_ms {
        cfg.storage.snapshot_ms = v;
    }
    cfg.resolve_secrets();
    cfg.validate()?;
    Ok(cfg)
}

#[allow(clippy::too_many_lines)]
async fn replay(
    captures: &[PathBuf],
    sql: &[String],
    expect_snapshots: bool,
    out_dir: Option<&PathBuf>,
    quiet: bool,
) -> anyhow::Result<()> {
    if let Some(d) = out_dir {
        std::fs::create_dir_all(d)?;
    }
    let started = Instant::now();
    let mut tables = Tables::new(expect_snapshots);
    let mut dec = BinanceJsonDecoder::new();
    let mut sbe = ankaa_feeds::binance::BinanceSbeDecoder::new();
    let mut batch = EventBatch::with_capacity(1024);
    let (mut frames_total, mut events_total, mut dropped_total) = (0usize, 0usize, 0usize);
    let (mut decode_ns, mut ingest_ns) = (0u128, 0u128);
    for path in captures {
        match sources::kind_of(path) {
            sources::SourceKind::Dbn => {
                let mut r = ankaa_feeds::databento::DbnReplay::open(path)?;
                info!(path = %path.display(), dataset = r.dataset(), "dbn opened");
                loop {
                    batch.clear();
                    let t0 = Instant::now();
                    let Some(n) = r.next_batch(dec.symbols_mut(), &mut batch, 1024)? else {
                        break;
                    };
                    decode_ns += t0.elapsed().as_nanos();
                    if n == 0 {
                        continue;
                    }
                    frames_total += 1;
                    let t1 = Instant::now();
                    let out = tables.ingest(&batch);
                    ingest_ns += t1.elapsed().as_nanos();
                    events_total += out.events;
                    dropped_total += out.dropped;
                }
                let st = r.stats();
                info!(
                    records = st.records,
                    trades = st.trades,
                    snapshots = st.snapshots,
                    ignored = st.ignored,
                    unmapped = st.unmapped,
                    "dbn done"
                );
                continue;
            }
            kind @ (sources::SourceKind::Csv | sources::SourceKind::Parquet) => {
                info!(path = %path.display(), ?kind, "trade file opened");
                for b in sources::arrow_batches(path, kind)? {
                    let b = b?;
                    batch.clear();
                    let t0 = Instant::now();
                    sources::trades_from_batch(&b, dec.symbols_mut(), &mut batch)?;
                    decode_ns += t0.elapsed().as_nanos();
                    frames_total += 1;
                    let t1 = Instant::now();
                    let out = tables.ingest(&batch);
                    ingest_ns += t1.elapsed().as_nanos();
                    events_total += out.events;
                    dropped_total += out.dropped;
                }
                continue;
            }
            sources::SourceKind::Capture => {}
        }
        let bytes = bytes::Bytes::from(std::fs::read(path)?);
        let (hdr, frames, tail) = decode_all_bytes_tolerant(&bytes)?;
        if let Some(e) = tail {
            warn!(path = %path.display(), error = %e, frames = frames.len(), "capture ends in an invalid frame; replaying the frames before it");
        }
        info!(path = %path.display(), source = %hdr.source, frames = frames.len(), "capture loaded");
        for f in &frames {
            batch.clear();
            let t0 = Instant::now();
            let decoded = match f.kind {
                FrameKind::Text => dec.decode(&f.payload, f.recv_ns, &mut batch),
                FrameKind::Binary => {
                    sbe.decode(&f.payload, f.recv_ns, dec.symbols_mut(), &mut batch)
                }
                FrameKind::Snapshot => dec.decode_snapshot(&f.payload, f.recv_ns, &mut batch),
                // user-source frames need the server's table definitions; `replay` is market data only
                FrameKind::Control | FrameKind::User => continue,
            };
            decode_ns += t0.elapsed().as_nanos();
            frames_total += 1;
            match decoded {
                Ok(_) => {
                    let t1 = Instant::now();
                    let out = tables.ingest(&batch);
                    ingest_ns += t1.elapsed().as_nanos();
                    events_total += out.events;
                    dropped_total += out.dropped;
                }
                Err(ankaa_ingest::DecodeError::Ignored) => {}
                Err(e) => tracing::warn!(error = %e, "frame rejected"),
            }
        }
    }
    tables.seal_all();
    let elapsed = started.elapsed();
    let st = dec.stats();
    info!(
        frames = frames_total,
        events = events_total,
        dropped = dropped_total,
        trades = st.trades,
        deltas = st.deltas,
        gaps = st.gaps,
        book_snapshots = st.snapshots,
        resyncs = tables.resyncs(),
        symbols = dec.symbols().len(),
        books = tables.books().len(),
        ms = elapsed.as_millis(),
        events_per_s = (events_total as f64 / elapsed.as_secs_f64()) as u64,
        decode_ns_per_frame = decode_ns.checked_div(frames_total as u128).unwrap_or(0),
        ingest_ns_per_event = ingest_ns.checked_div(events_total as u128).unwrap_or(0),
        "replay done"
    );
    let ctx = ankaa_sql::default_session();
    register_snapshot(&ctx, tables.trades.snapshot())?;
    register_snapshot(&ctx, tables.deltas.snapshot())?;
    register_snapshot(&ctx, tables.quotes.snapshot())?;
    serve::register_symbols(&ctx, &dec)?;
    register_functions(&ctx);
    let plans = ankaa_sql::PlanCache::default();
    for (qi, q) in sql.iter().enumerate() {
        let t0 = Instant::now();
        // as the server does: a statement seen before is not parsed and optimised again
        let plan = plans
            .physical_plan(&ctx, q, datafusion::execution::context::SQLOptions::new())
            .await?;
        let df_schema = plan.schema();
        let task_ctx = ctx.task_ctx();
        // parse + logical plan + optimisers + physical plan: paid by every query, whatever
        // its size, so it is reported apart from the execution
        let plan_ms = t0.elapsed().as_secs_f64() * 1e3;
        let batches = datafusion::physical_plan::collect(plan, task_ctx).await?;
        // the query's time: plan + execute + collect; writing and printing the result are not
        // part of it (they grow with the number of rows and no engine is charged for them)
        let query_ms = t0.elapsed().as_secs_f64() * 1e3;
        if let Some(d) = out_dir {
            let path = d.join(format!("q{:02}.csv", qi + 1));
            let file = std::fs::File::create(&path)?;
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .with_timestamp_tz_format("%Y-%m-%dT%H:%M:%S%.9fZ".to_owned())
                .build(file);
            if batches.is_empty() {
                // header only, so consumers still see the schema
                w.write(&arrow::record_batch::RecordBatch::new_empty(
                    df_schema.clone(),
                ))?;
            }
            for b in &batches {
                w.write(b)?;
            }
        }
        let rows: usize = batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum();
        println!("-- {q}");
        if !quiet {
            println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
        }
        println!(
            "-- {rows} rows in {query_ms:.2} ms (plan {plan_ms:.2} ms, execute {:.2} ms)\n",
            query_ms - plan_ms
        );
    }
    Ok(())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    ankaa_flight::install_crypto_provider();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Replay {
            captures,
            sql,
            expect_snapshots,
            out_dir,
            quiet,
        } => replay(&captures, &sql, expect_snapshots, out_dir.as_ref(), quiet).await,
        Cmd::Serve {
            config,
            url,
            rest_url,
            streams,
            wal_dir,
            no_depth_snapshots,
            api_key,
            flight_addr,
            metrics_addr,
            pg_addr,
            token,
            tls_cert,
            tls_key,
            tls_client_ca,
            snapshot_ms,
            dict_ids,
            duration_secs,
            standby_of,
        } => {
            let mut config = build_config(
                config.as_ref(),
                url,
                rest_url,
                streams,
                wal_dir,
                no_depth_snapshots,
                flight_addr,
                metrics_addr,
                pg_addr,
                token,
                (tls_cert, tls_key, tls_client_ca),
                snapshot_ms,
            )?;
            if standby_of.is_some() {
                config.replication.standby_of = standby_of;
                config.validate()?;
            }
            if dict_ids {
                config.server.dict_columns = ankaa_core::config::DictColumns::Ids;
            }
            let api_key = api_key.or_else(|| {
                config
                    .feed
                    .api_key_env
                    .as_deref()
                    .and_then(|v| std::env::var(v).ok())
                    .filter(|v| !v.is_empty())
            });
            serve::serve(serve::ServeOpts {
                config,
                duration: (duration_secs > 0)
                    .then(|| std::time::Duration::from_secs(duration_secs)),
                api_key: api_key.map(ankaa_core::Secret::new),
            })
            .await
        }
        Cmd::Query {
            addr,
            token,
            tls_ca,
            tls_domain,
            tls_cert,
            tls_key,
            sql,
        } => {
            let tls = match tls_ca {
                Some(ca) => Some(query::ClientTls {
                    ca: std::fs::read(&ca)?,
                    domain: tls_domain,
                    identity: match (tls_cert, tls_key) {
                        (Some(c), Some(k)) => Some((std::fs::read(c)?, std::fs::read(k)?)),
                        _ => None,
                    },
                }),
                None => None,
            };
            if tls.is_none() && token.is_some() && !replication::is_loopback(&addr) {
                warn!(%addr, "no --tls-ca: the token and the results cross the network in clear text");
            }
            for q in &sql {
                query::query(&addr, token.as_deref(), tls.as_ref(), q).await?;
            }
            Ok(())
        }
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Print => {
                print!("{}", ankaa_core::Config::default().to_toml());
                Ok(())
            }
            ConfigCmd::Check { file } => {
                let text = std::fs::read_to_string(&file)
                    .map_err(|e| anyhow::anyhow!("config {}: {e}", file.display()))?;
                let cfg = ankaa_core::Config::from_toml(&text)?;
                // dry run: validates limits and paths without creating directories
                cfg.check_resources_dry_run()?;
                // TLS files of the sources: read and parsed, as the server does at start
                serve::check_sources(&cfg)?;
                println!("{}: ok", file.display());
                Ok(())
            }
        },
    }
}
