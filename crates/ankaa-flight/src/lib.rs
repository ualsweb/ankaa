//! Arrow Flight server for ANKAA (P1-09a).
//!
//! One service speaks two dialects on the same port.
//!
//! Plain Flight (`ankaad query`, the Python package, the benchmark harness):
//! - `GetFlightInfo` with a `FlightDescriptor::Cmd` whose bytes are a SQL string returns the
//!   result schema and one endpoint whose ticket is the same SQL.
//! - `DoGet(ticket = SQL bytes)` executes the query on the shared `SessionContext` and streams
//!   the record batches as Flight data.
//! - `ListFlights` lists registered tables.
//!
//! Flight SQL (ADBC, the Flight SQL JDBC driver, `FlightSqlServiceClient`): a descriptor,
//! ticket or action body that is a protobuf `Any` with a
//! `type.googleapis.com/arrow.flight.protocol.sql.*` type URL is handled by the `flight_sql`
//! module: statements, prepared statements without parameters and the catalog metadata
//! commands. It ends in the same planning and execution functions as plain Flight, so the
//! read-only check, the query timeout, the cap on concurrent queries and the memory pool are
//! the same ones.
//!
//! Both:
//! - Auth: when a token is configured, every call must carry `authorization: Bearer <token>`.
//!   `Handshake` exists for the clients that insist on one (JDBC, ADBC with a user name and
//!   password): it takes the token as a bearer header or as the password of a Basic header and
//!   answers with the bearer header to send from then on.
//! - TLS: [`TlsFiles`] (PEM cert chain + key, optional client CA for mutual TLS) via [`serve`];
//!   clients use [`client_endpoint`].
//!
//! The `SessionContext` is shared with the ingest side, which re-registers table snapshots
//! periodically; queries therefore see data up to the last snapshot.
#![forbid(unsafe_code)]

mod flight_sql;

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_descriptor::DescriptorType,
};
use datafusion::prelude::{SQLOptions, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// TLS material for the server, as PEM files.
#[derive(Debug, Clone)]
pub struct TlsFiles {
    /// Certificate chain (PEM).
    pub cert: std::path::PathBuf,
    /// Private key (PEM, PKCS#8 or SEC1).
    pub key: std::path::PathBuf,
    /// When set, clients must present a certificate signed by this CA (mutual TLS).
    pub client_ca: Option<std::path::PathBuf>,
}

/// Install the process-wide rustls crypto provider once. Binaries that link both `ring` and
/// `aws-lc-rs` (through different dependencies) otherwise panic at the first TLS handshake.
pub fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

impl TlsFiles {
    fn load(&self) -> std::io::Result<tonic::transport::ServerTlsConfig> {
        install_crypto_provider();
        let cert = std::fs::read(&self.cert)?;
        let key = std::fs::read(&self.key)?;
        let mut cfg = tonic::transport::ServerTlsConfig::new()
            .identity(tonic::transport::Identity::from_pem(cert, key));
        if let Some(ca) = &self.client_ca {
            cfg = cfg.client_ca_root(tonic::transport::Certificate::from_pem(std::fs::read(ca)?));
        }
        Ok(cfg)
    }
}

/// Serve `service` on `listener` until `shutdown` resolves, with optional TLS.
///
/// Without `tls` this is plain HTTP/2 (loopback and development only).
pub async fn serve<F>(
    service: AnkaaFlight,
    listener: tokio::net::TcpListener,
    tls: Option<&TlsFiles>,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: std::future::Future<Output = ()>,
{
    // HTTP/2 keepalive: a client that vanished without closing (host down, NAT timeout) is
    // dropped instead of holding its streams, plans and WAL tails forever
    let mut builder = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(20)))
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        // one connection cannot open an unbounded number of calls
        .max_concurrent_streams(Some(256))
        .concurrency_limit_per_connection(64);
    if let Some(t) = tls {
        builder = builder.tls_config(t.load()?)?;
    }
    builder
        .add_service(service.into_server())
        .serve_with_incoming_shutdown(accept_nodelay(listener), shutdown)
        .await?;
    Ok(())
}

/// The listener's connections with Nagle's algorithm off.
///
/// `Server::tcp_nodelay` only applies to listeners tonic opens itself, so it is set here on
/// every accepted socket. Without it each response waits for the client's delayed ACK: about
/// 40 ms, twice per `GetFlightInfo` + `DoGet` round trip (measured as 83 ms per query on the
/// first server run).
fn accept_nodelay(
    listener: tokio::net::TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    tokio_stream::wrappers::TcpListenerStream::new(listener).map(|s| {
        if let Ok(sock) = &s {
            let _ = sock.set_nodelay(true);
        }
        s
    })
}

#[cfg(test)]
mod accept_tests {
    use futures::StreamExt;

    /// Every socket the Flight server accepts has `TCP_NODELAY` set (a listener hands out
    /// sockets with it off).
    #[tokio::test]
    async fn accepted_sockets_have_nodelay() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let plain = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let plain_addr = plain.local_addr().unwrap();
        let _a = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _b = tokio::net::TcpStream::connect(plain_addr).await.unwrap();
        let mut incoming = Box::pin(super::accept_nodelay(listener));
        let sock = incoming.next().await.unwrap().unwrap();
        assert!(sock.nodelay().unwrap());
        let (untouched, _) = plain.accept().await.unwrap();
        assert!(
            !untouched.nodelay().unwrap(),
            "the default is off: the test means something"
        );
    }
}

/// Client endpoint for `host:port`. With `ca_pem`, TLS is used and the server certificate must
/// chain to that CA and match `domain` (defaults to the host part of `addr`). `identity_pem` is
/// a `(cert, key)` pair for mutual TLS.
pub fn client_endpoint(
    addr: &str,
    ca_pem: Option<&[u8]>,
    domain: Option<&str>,
    identity_pem: Option<(&[u8], &[u8])>,
) -> Result<tonic::transport::Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    if ca_pem.is_some() {
        install_crypto_provider();
    }
    let scheme = if ca_pem.is_some() { "https" } else { "http" };
    // keepalive pings detect a half-open connection (peer host gone) within ~30 s; without
    // them a streaming call would wait forever
    let endpoint = tonic::transport::Endpoint::from_shared(format!("{scheme}://{addr}"))?
        .tcp_nodelay(true)
        .connect_timeout(std::time::Duration::from_secs(10))
        .http2_keep_alive_interval(std::time::Duration::from_secs(10))
        .keep_alive_timeout(std::time::Duration::from_secs(20))
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)));
    let Some(ca) = ca_pem else {
        return Ok(endpoint);
    };
    let host = domain
        .unwrap_or_else(|| addr.rsplit_once(':').map_or(addr, |(h, _)| h))
        .to_owned();
    let mut tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca))
        .domain_name(host);
    if let Some((cert, key)) = identity_pem {
        tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
    }
    Ok(endpoint.tls_config(tls)?)
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Query-side counters, scraped by `/metrics` (relaxed atomics).
#[derive(Debug, Default)]
pub struct QueryMetrics {
    /// Queries planned (`GetFlightInfo`/`GetSchema`/`DoGet`).
    pub queries: std::sync::atomic::AtomicU64,
    /// Queries rejected (bad SQL, not read-only) or failed during execution.
    pub errors: std::sync::atomic::AtomicU64,
    /// Calls refused for a missing or wrong bearer token.
    pub unauthenticated: std::sync::atomic::AtomicU64,
    /// Rows streamed to clients.
    pub rows: std::sync::atomic::AtomicU64,
    /// Nanoseconds spent from `DoGet` start to the last batch, summed.
    pub do_get_ns_total: std::sync::atomic::AtomicU64,
    /// `DoGet` calls completed.
    pub do_get_total: std::sync::atomic::AtomicU64,
    /// Last `DoGet` wall time in nanoseconds.
    pub last_do_get_ns: std::sync::atomic::AtomicU64,
    /// WAL streams (standbys) currently open.
    pub wal_streams: std::sync::atomic::AtomicU64,
    /// WAL stream requests refused (wrong token, too many streams).
    pub wal_refused: std::sync::atomic::AtomicU64,
}

/// Returns the WAL writer's durable LSN.
pub type DurableLsn = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Flight service over a shared `DataFusion` context.
#[derive(Clone)]
pub struct AnkaaFlight {
    ctx: Arc<SessionContext>,
    token: Option<Arc<str>>,
    metrics: Arc<QueryMetrics>,
    /// Wall-clock budget per `DoGet` stream (planning + execution + transfer).
    query_timeout: std::time::Duration,
    /// WAL directory served to standbys through the `__wal__:<from_lsn>` ticket.
    wal_dir: Option<std::path::PathBuf>,
    /// Token for the WAL ticket (falls back to `token` when unset).
    wal_token: Option<Arc<str>>,
    /// Bounds the number of concurrent WAL streams.
    wal_slots: Arc<tokio::sync::Semaphore>,
    /// Durable LSN of the local writer: records above it are not served yet.
    wal_durable: Option<DurableLsn>,
    /// Bounds the number of queries executing at once.
    query_slots: Arc<tokio::sync::Semaphore>,
    /// Logical plans of statements seen before (see [`ankaa_sql::PlanCache`]).
    plans: Arc<ankaa_sql::PlanCache>,
}

/// Default cap on queries executing at once.
pub const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 32;

/// Default cap on concurrent WAL streams (standbys) per server.
pub const DEFAULT_MAX_WAL_STREAMS: usize = 4;

/// Ticket prefix for WAL streaming (replication).
pub const WAL_TICKET_PREFIX: &str = "__wal__:";

/// Arrow schema of the WAL stream: `lsn: UInt64, epoch: UInt64, payload: Binary`.
#[must_use]
pub fn wal_stream_schema() -> SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("lsn", arrow::datatypes::DataType::UInt64, false),
        arrow::datatypes::Field::new("epoch", arrow::datatypes::DataType::UInt64, false),
        arrow::datatypes::Field::new("payload", arrow::datatypes::DataType::Binary, false),
    ]))
}

/// Default per-query wall-clock budget.
pub const DEFAULT_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl std::fmt::Debug for AnkaaFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnkaaFlight")
            .field("auth", &self.token.is_some())
            .finish_non_exhaustive()
    }
}

impl AnkaaFlight {
    /// New service. `token = None` disables authentication (loopback/dev only).
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, token: Option<String>) -> Self {
        Self {
            ctx,
            token: token.map(Into::into),
            metrics: Arc::new(QueryMetrics::default()),
            query_timeout: DEFAULT_QUERY_TIMEOUT,
            wal_dir: None,
            wal_token: None,
            wal_slots: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_WAL_STREAMS)),
            wal_durable: None,
            query_slots: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_CONCURRENT_QUERIES)),
            plans: Arc::new(ankaa_sql::PlanCache::default()),
        }
    }

    /// Serve the WAL in `dir` to standbys (ticket `__wal__:<from_lsn>`, bearer-authenticated).
    #[must_use]
    pub fn with_wal_dir(mut self, dir: Option<std::path::PathBuf>) -> Self {
        self.wal_dir = dir;
        self
    }

    /// Require this token (instead of the query token) for the WAL ticket, so that SQL clients
    /// cannot read the raw log.
    #[must_use]
    pub fn with_wal_token(mut self, token: Option<String>) -> Self {
        self.wal_token = token.map(Into::into);
        self
    }

    /// Cap the number of concurrent WAL streams (default [`DEFAULT_MAX_WAL_STREAMS`]).
    #[must_use]
    pub fn with_max_wal_streams(mut self, n: usize) -> Self {
        self.wal_slots = Arc::new(tokio::sync::Semaphore::new(n.max(1)));
        self
    }

    /// Serve only records at or below the LSN this returns (the local writer's durable LSN).
    #[must_use]
    pub fn with_wal_durable_lsn(mut self, f: DurableLsn) -> Self {
        self.wal_durable = Some(f);
        self
    }

    /// Cap the number of queries executing at once; further ones wait inside their own
    /// deadline and are refused when it passes.
    #[must_use]
    pub fn with_max_concurrent_queries(mut self, n: usize) -> Self {
        self.query_slots = Arc::new(tokio::sync::Semaphore::new(n.max(1)));
        self
    }

    /// Set the per-query wall-clock budget (planning + execution + transfer).
    #[must_use]
    pub fn with_query_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// New service sharing externally owned query counters.
    #[must_use]
    pub fn with_metrics(
        ctx: Arc<SessionContext>,
        token: Option<String>,
        metrics: Arc<QueryMetrics>,
    ) -> Self {
        Self {
            ctx,
            token: token.map(Into::into),
            metrics,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
            wal_dir: None,
            wal_token: None,
            wal_slots: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_WAL_STREAMS)),
            wal_durable: None,
            query_slots: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_CONCURRENT_QUERIES)),
            plans: Arc::new(ankaa_sql::PlanCache::default()),
        }
    }

    /// Stream WAL records from `from_lsn` as Arrow batches until the client goes away. Polls
    /// the directory every 50 ms when it has caught up (blocking reads on the blocking pool; a
    /// poll only reads bytes written since the previous one).
    fn wal_stream(&self, from_lsn: u64) -> Result<BoxStream<FlightData>, Status> {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(dir) = self.wal_dir.clone() else {
            return Err(Status::failed_precondition(
                "this server does not serve its WAL",
            ));
        };
        let Ok(permit) = self.wal_slots.clone().try_acquire_owned() else {
            self.metrics.wal_refused.fetch_add(1, Relaxed);
            return Err(Status::resource_exhausted(
                "too many WAL streams (replication.max_standbys)",
            ));
        };
        let schema = wal_stream_schema();
        let tail = Arc::new(parking_lot::Mutex::new(ankaa_wal::Tail::new(
            &dir, from_lsn,
        )));
        let durable = self.wal_durable.clone();
        let m = self.metrics.clone();
        m.wal_streams.fetch_add(1, Relaxed);
        let batches = futures::stream::unfold((tail, false), move |(tail, failed)| {
            let schema = schema.clone();
            let durable = durable.clone();
            async move {
                if failed {
                    return None; // an error ends the stream
                }
                loop {
                    let t = tail.clone();
                    let limit = durable.as_ref().map_or(u64::MAX, |f| f());
                    let polled = tokio::task::spawn_blocking(move || {
                        let mut t = t.lock();
                        t.set_limit(limit);
                        t.poll(4096)
                    })
                    .await;
                    let records = match polled {
                        Ok(Ok(r)) => r,
                        Ok(Err(e)) => {
                            return Some((
                                Err(arrow_flight::error::FlightError::Tonic(Box::new(
                                    wal_status(&e),
                                ))),
                                (tail, true),
                            ));
                        }
                        Err(e) => {
                            return Some((
                                Err(arrow_flight::error::FlightError::ExternalError(Box::new(e))),
                                (tail, true),
                            ));
                        }
                    };
                    if records.is_empty() {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                    let lsn: Vec<u64> = records.iter().map(|r| r.lsn).collect();
                    let epoch: Vec<u64> = records.iter().map(|r| r.epoch).collect();
                    let payload = arrow::array::BinaryArray::from_iter_values(
                        records.iter().map(|r| r.payload.as_slice()),
                    );
                    let batch = arrow::record_batch::RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(arrow::array::UInt64Array::from(lsn)),
                            Arc::new(arrow::array::UInt64Array::from(epoch)),
                            Arc::new(payload),
                        ],
                    )
                    .map_err(arrow_flight::error::FlightError::Arrow);
                    return Some((batch, (tail, false)));
                }
            }
        });
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(wal_stream_schema())
            .build(batches)
            .map_err(|e| match e {
                arrow_flight::error::FlightError::Tonic(s) => *s,
                e => Status::internal(e.to_string()),
            });
        // release the slot and the gauge when the client goes away
        let flight = Timed {
            inner: flight,
            on_drop: Some(move || {
                m.wal_streams.fetch_sub(1, Relaxed);
                drop(permit);
            }),
        };
        Ok(Box::pin(flight))
    }

    fn check_wal_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let Some(want) = &self.wal_token else {
            return self.check_auth(req);
        };
        let got = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(ankaa_core::bearer_token)
            .unwrap_or("");
        if ankaa_core::constant_time_eq(got.as_bytes(), want.as_bytes()) {
            Ok(())
        } else {
            self.metrics
                .wal_refused
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(Status::permission_denied(
                "the WAL stream needs the replication token",
            ))
        }
    }

    /// Shared query counters.
    #[must_use]
    pub fn metrics(&self) -> Arc<QueryMetrics> {
        self.metrics.clone()
    }

    /// Wrap into a tonic server.
    #[must_use]
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let Some(want) = &self.token else {
            return Ok(());
        };
        let got = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(ankaa_core::bearer_token)
            .unwrap_or("");
        if ankaa_core::constant_time_eq(got.as_bytes(), want.as_bytes()) {
            Ok(())
        } else {
            self.metrics
                .unauthenticated
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(Status::unauthenticated("invalid or missing bearer token"))
        }
    }

    /// Read-only SQL: no DDL, no DML, no `SET`/statements. Anything else is rejected before planning.
    fn sql_options() -> SQLOptions {
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
    }

    /// Plan `sql` (read-only, inside the query timeout) without executing it.
    async fn plan(&self, sql: &str) -> Result<datafusion::prelude::DataFrame, Status> {
        use std::sync::atomic::Ordering::Relaxed;
        self.metrics.queries.fetch_add(1, Relaxed);
        tokio::time::timeout(
            self.query_timeout,
            self.ctx.sql_with_options(sql, Self::sql_options()),
        )
        .await
        .map_err(|_| {
            self.metrics.errors.fetch_add(1, Relaxed);
            Status::deadline_exceeded("query planning exceeded the server's query timeout")
        })?
        .map_err(|e| {
            self.metrics.errors.fetch_add(1, Relaxed);
            Status::invalid_argument(format!("sql: {e}"))
        })
    }

    async fn plan_schema(&self, sql: &str) -> Result<SchemaRef, Status> {
        let df = self.plan(sql).await?;
        Ok(Arc::new(df.schema().as_arrow().clone()))
    }

    /// Execute `sql` and stream the result: a query slot for the whole life of the query, the
    /// read-only check, the plan cache and the deadline. Every query of both dialects runs
    /// through here.
    async fn run_query(&self, sql: &str) -> Result<BoxStream<FlightData>, Status> {
        use std::sync::atomic::Ordering::Relaxed;
        tracing::debug!(%sql, "do_get");
        let m = self.metrics.clone();
        let t0 = std::time::Instant::now();
        m.queries.fetch_add(1, Relaxed);
        let deadline = t0 + self.query_timeout;
        // a slot for the whole life of the query; waiting for one counts against its deadline
        let slot =
            tokio::time::timeout_at(deadline.into(), self.query_slots.clone().acquire_owned())
                .await
                .map_err(|_| {
                    m.errors.fetch_add(1, Relaxed);
                    Status::resource_exhausted(
                        "too many queries are running (server.max_concurrent_queries); try again",
                    )
                })?
                .map_err(|_| Status::internal("query slots closed"))?;
        // a statement seen before is not parsed and optimised again; the physical plan is
        // always built here, on the chunks that exist now
        let plan = tokio::time::timeout_at(
            deadline.into(),
            self.plans
                .physical_plan(&self.ctx, sql, Self::sql_options()),
        )
        .await
        .map_err(|_| {
            m.errors.fetch_add(1, Relaxed);
            Status::deadline_exceeded("query planning exceeded the server's query timeout")
        })?
        .map_err(|e| {
            m.errors.fetch_add(1, Relaxed);
            Status::invalid_argument(format!("sql: {e}"))
        })?;
        let schema: SchemaRef = plan.schema();
        let stream =
            datafusion::physical_plan::execute_stream(plan, self.ctx.task_ctx()).map_err(|e| {
                m.errors.fetch_add(1, Relaxed);
                Status::internal(format!("execute: {e}"))
            })?;
        Ok(Box::pin(stream_with_deadline(
            stream, schema, deadline, t0, m, slot,
        )))
    }

    /// The token a `Handshake` call proved it knows; it is sent back as the bearer token.
    ///
    /// Flight SQL clients authenticate in two ways. Given a token, they send `authorization:
    /// Bearer …` on every call, and the JDBC driver opens with a `Handshake` carrying that
    /// header. Given a user name and a password, the JDBC driver and ADBC send `authorization:
    /// Basic base64(user:password)` on `Handshake` and take the bearer token for the following
    /// calls from the response headers. The user name is free and the password is the server
    /// token, as on the Postgres listener.
    fn handshake_token<T>(&self, req: &Request<T>) -> Result<String, Status> {
        let Some(want) = &self.token else {
            return Ok("anonymous".to_owned()); // no authentication: any credentials do
        };
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let got = ankaa_core::bearer_token(header)
            .map(str::to_owned)
            .or_else(|| basic_password(header));
        if got.is_some_and(|g| ankaa_core::constant_time_eq(g.as_bytes(), want.as_bytes())) {
            Ok(want.to_string())
        } else {
            self.metrics
                .unauthenticated
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(Status::unauthenticated(
                "handshake: send the token as a bearer token or as the password of basic authentication",
            ))
        }
    }
}

/// The password of an `Authorization: Basic base64(user:password)` header value.
///
/// Padding is optional: the Go client behind ADBC encodes without it.
fn basic_password(header_value: &str) -> Option<String> {
    use base64::Engine;
    let (scheme, rest) = header_value.trim().split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(rest.trim().trim_end_matches('='))
        .ok()?;
    let credentials = String::from_utf8(decoded).ok()?;
    let (_user, password) = credentials.split_once(':')?;
    Some(password.to_owned())
}

/// gRPC status for a WAL tail error: a follower that cannot continue from this log gets a
/// distinct code so that it stops instead of retrying.
fn wal_status(e: &ankaa_wal::WalError) -> Status {
    match e {
        ankaa_wal::WalError::Truncated { .. } | ankaa_wal::WalError::Ahead { .. } => {
            Status::out_of_range(e.to_string())
        }
        _ => {
            // the details (file names, offsets) belong in the server log, not on the wire
            tracing::warn!(error = %e, "WAL stream: read failed");
            Status::data_loss("the WAL could not be read (details are in the server log)")
        }
    }
}

/// Run `stream` in its own task and return the Flight response stream that drains it.
///
/// The task owns the plan and enforces the deadline. The response stream only drains a small
/// channel: when the client stops reading (HTTP/2 flow control then stops polling the
/// response), the task still times out and drops the plan with its memory, instead of holding
/// them for as long as the connection lives. A stream cut by the deadline ends with an error,
/// never as if it were complete.
fn stream_with_deadline(
    stream: datafusion::execution::SendableRecordBatchStream,
    schema: SchemaRef,
    deadline: std::time::Instant,
    t0: std::time::Instant,
    m: Arc<QueryMetrics>,
    slot: tokio::sync::OwnedSemaphorePermit,
) -> impl Stream<Item = Result<FlightData, Status>> + Send + 'static {
    use std::sync::atomic::Ordering::Relaxed;
    let (btx, brx) = tokio::sync::mpsc::channel::<
        Result<arrow::record_batch::RecordBatch, arrow_flight::error::FlightError>,
    >(2);
    let timed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = timed_out.clone();
    tokio::spawn(async move {
        let mut stream = stream;
        let sleep = tokio::time::sleep_until(deadline.into());
        tokio::pin!(sleep);
        loop {
            let next = tokio::select! {
                () = &mut sleep => {
                    flag.store(true, Relaxed);
                    break;
                }
                n = stream.next() => n,
            };
            let item = match next {
                None => break,
                Some(Ok(b)) => {
                    m.rows.fetch_add(b.num_rows() as u64, Relaxed);
                    Ok(b)
                }
                Some(Err(e)) => {
                    m.errors.fetch_add(1, Relaxed);
                    Err(arrow_flight::error::FlightError::ExternalError(Box::new(e)))
                }
            };
            let failed = item.is_err();
            tokio::select! {
                () = &mut sleep => {
                    flag.store(true, Relaxed);
                    break;
                }
                sent = btx.send(item) => {
                    if sent.is_err() || failed {
                        break; // client gone, or the query failed
                    }
                }
            }
        }
        if flag.load(Relaxed) {
            m.errors.fetch_add(1, Relaxed);
        }
        // the plan is dropped here; account wall time (finished, failed, timed out or abandoned)
        drop(stream);
        drop(slot);
        let ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        m.do_get_ns_total.fetch_add(ns, Relaxed);
        m.do_get_total.fetch_add(1, Relaxed);
        m.last_do_get_ns.store(ns, Relaxed);
    });
    // a stream that ends because of the deadline ends with an error, never as if complete
    let batches =
        futures::stream::unfold((brx, timed_out, false), |(mut rx, flag, done)| async move {
            if done {
                return None;
            }
            match rx.recv().await {
                Some(item) => Some((item, (rx, flag, false))),
                None if flag.load(std::sync::atomic::Ordering::Relaxed) => Some((
                    Err(arrow_flight::error::FlightError::Tonic(Box::new(
                        Status::deadline_exceeded("query exceeded the server's query timeout"),
                    ))),
                    (rx, flag, true),
                )),
                None => None,
            }
        });
    FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batches)
        .map_err(|e| match e {
            arrow_flight::error::FlightError::Tonic(s) => *s,
            e => Status::internal(e.to_string()),
        })
}

/// Stream wrapper that runs a closure when dropped (end of a `DoGet`).
struct Timed<S, F: FnOnce()> {
    inner: S,
    on_drop: Option<F>,
}

impl<S: Stream + Unpin, F: FnOnce() + Unpin> Stream for Timed<S, F> {
    type Item = S::Item;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<S, F: FnOnce()> Drop for Timed<S, F> {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

fn sql_from_descriptor(d: &FlightDescriptor) -> Result<String, Status> {
    if d.r#type == DescriptorType::Cmd as i32 {
        String::from_utf8(d.cmd.to_vec()).map_err(|_| Status::invalid_argument("cmd is not utf-8"))
    } else if d.r#type == DescriptorType::Path as i32 && d.path.len() == 1 {
        Ok(format!("SELECT * FROM {}", d.path[0]))
    } else {
        Err(Status::invalid_argument(
            "descriptor must be CMD (SQL text) or a single-element PATH (table name)",
        ))
    }
}

#[tonic::async_trait]
impl FlightService for AnkaaFlight {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn handshake(
        &self,
        req: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // the credentials are in the headers; the request stream carries nothing we need
        let token = self.handshake_token(&req)?;
        let bearer =
            tonic::metadata::MetadataValue::try_from(format!("Bearer {token}")).map_err(|_| {
                Status::failed_precondition("the server token cannot be sent as a header value")
            })?;
        let reply = HandshakeResponse {
            protocol_version: 0,
            payload: Vec::new().into(),
        };
        let mut resp: Response<Self::HandshakeStream> =
            Response::new(Box::pin(futures::stream::iter([Ok(reply)])));
        resp.metadata_mut().insert("authorization", bearer);
        Ok(resp)
    }

    async fn list_flights(
        &self,
        req: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.check_auth(&req)?;
        let catalog = self
            .ctx
            .catalog("datafusion")
            .ok_or_else(|| Status::internal("no default catalog"))?;
        let schema = catalog
            .schema("public")
            .ok_or_else(|| Status::internal("no public schema"))?;
        let mut infos = Vec::new();
        for name in schema.table_names() {
            if let Ok(Some(t)) = schema.table(&name).await {
                let sql = format!("SELECT * FROM {name}");
                let info = FlightInfo::new()
                    .try_with_schema(t.schema().as_ref())
                    .map_err(|e| Status::internal(e.to_string()))?
                    .with_descriptor(FlightDescriptor::new_path(vec![name.clone()]))
                    .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(sql)));
                infos.push(Ok(info));
            }
        }
        Ok(Response::new(Box::pin(futures::stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        req: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_auth(&req)?;
        let d = req.into_inner();
        if let Some(any) = flight_sql::command_of(&d) {
            return self.sql_flight_info(any, d).await.map(Response::new);
        }
        let sql = sql_from_descriptor(&d)?;
        let schema = self.plan_schema(&sql).await?;
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(d)
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(sql)));
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        req: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.check_auth(&req)?;
        let schema = if let Some(any) = flight_sql::command_of(req.get_ref()) {
            self.sql_schema(any).await?
        } else {
            let sql = sql_from_descriptor(req.get_ref())?;
            self.plan_schema(&sql).await?
        };
        let opts = arrow::ipc::writer::IpcWriteOptions::default();
        let res: SchemaResult = arrow_flight::SchemaAsIpc::new(&schema, &opts)
            .try_into()
            .map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(res))
    }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        if req
            .get_ref()
            .ticket
            .starts_with(WAL_TICKET_PREFIX.as_bytes())
        {
            self.check_wal_auth(&req)?;
        } else {
            self.check_auth(&req)?;
        }
        let ticket = req.into_inner().ticket;
        // a Flight SQL ticket is a protobuf `Any`: it never starts with the WAL prefix
        if let Some(any) = flight_sql::any_of(&ticket) {
            return self.sql_do_get(any).await.map(Response::new);
        }
        let sql = String::from_utf8(ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket is not utf-8"))?;
        if let Some(rest) = sql.strip_prefix(WAL_TICKET_PREFIX) {
            let from: u64 = rest
                .trim()
                .parse()
                .map_err(|_| Status::invalid_argument("wal ticket: bad lsn"))?;
            return Ok(Response::new(self.wal_stream(from)?));
        }
        Ok(Response::new(self.run_query(&sql).await?))
    }

    async fn do_put(
        &self,
        req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.check_auth(&req)?;
        // Nothing is ever written through Flight. Only the first message is read, to tell a
        // Flight SQL client what exactly was refused.
        let mut stream = req.into_inner();
        let first = tokio::time::timeout(self.query_timeout, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("do_put: no message arrived in time"))??;
        if let Some(any) = first
            .and_then(|m| m.flight_descriptor)
            .as_ref()
            .and_then(flight_sql::command_of)
        {
            self.metrics
                .errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(flight_sql::refuse_put(any));
        }
        Err(Status::unimplemented(
            "do_put (ingest goes through feeds, not Flight)",
        ))
    }

    async fn do_action(
        &self,
        req: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.check_auth(&req)?;
        self.sql_do_action(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn list_actions(
        &self,
        req: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.check_auth(&req)?;
        Ok(Response::new(Box::pin(futures::stream::iter(
            flight_sql::action_types().into_iter().map(Ok),
        ))))
    }

    async fn do_exchange(
        &self,
        _req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}
