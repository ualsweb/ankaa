//! Server configuration (TOML) with validation and start-up resource checks (PRD F-20).
//!
//! Every CLI flag of `ankaad serve` has a counterpart here; the CLI wins when both are given.
//! `validate` rejects contradictory or unsafe settings (non-loopback bind without a token,
//! zero intervals, bad stream names). `check_resources` is the fail-fast gate: on Linux it checks
//! `RLIMIT_MEMLOCK` against the configured locked-memory budget.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level configuration file.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Feed source.
    pub feed: FeedConfig,
    /// Storage and durability.
    pub storage: StorageConfig,
    /// Network endpoints.
    pub server: ServerConfig,
    /// Resource budgets checked at start-up.
    pub resources: ResourceConfig,
    /// User-defined tables (generic ingest, "bring your own feed").
    pub tables: Vec<TableConfig>,
    /// Message sources feeding user-defined tables.
    pub sources: Vec<SourceConfig>,
    /// Continuous (incrementally maintained) queries.
    pub continuous: Vec<ContinuousConfig>,
    /// Replication (this node as a read-only standby of a primary).
    pub replication: ReplicationConfig,
}

/// Column type names accepted in `[[tables]]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnKind {
    /// Nanosecond timestamp (the first column must be one).
    TimestampNs,
    /// Fixed-point decimal stored as `i64` with 8 decimals.
    Fixed8,
    /// Plain `i64`.
    Int64,
    /// Dictionary-encoded string (`u32` id, names in `<table>_symbols`-style lookups).
    Dict,
    /// Small enum / flags byte.
    U8,
    /// `f64`.
    Float64,
}

/// One user-defined column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    /// Column name.
    pub name: String,
    /// Type.
    #[serde(rename = "type")]
    pub kind: ColumnKind,
    /// A row may come without this value (stored as `NULL`; aggregates skip it). Not allowed
    /// for the timestamp and for `dict` columns (keys). Default false: a row without the value
    /// is rejected.
    #[serde(default)]
    pub nullable: bool,
}

/// A user-defined table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// Table name (SQL identifier).
    pub name: String,
    /// Columns; the first must be `timestamp_ns`.
    pub columns: Vec<ColumnConfig>,
    /// Column whose value chooses the partition (ordering and watermark are per partition);
    /// must be a `dict` column. Unset = one partition.
    #[serde(default)]
    pub partition_by: Option<String>,
    /// Maximum distinct values per `dict` column (every value of the `partition_by` column is a
    /// partition, a few hundred bytes of memory each before any data). Rows that would add a
    /// value beyond it are rejected and counted. Default 100,000.
    #[serde(default = "default_max_dict_entries")]
    pub max_dict_entries: usize,
    /// How much older than the newest row of its partition a row may be and still be stored,
    /// in milliseconds: such rows are inserted at their place in time (devices that buffer while
    /// offline, gateways that batch). Older rows are dropped and counted. 0 (default) = rows
    /// must arrive in time order, as an exchange feed does. At most 7 days.
    #[serde(default)]
    pub max_lateness_ms: u64,
    /// How long rows are kept, as a duration (`"90d"`, `"12h"`, `"30m"`, `"45s"`), measured
    /// back from the newest row of the table (event time, not the wall clock, so a backfill or
    /// a replay of old data is not deleted on arrival). Older sealed chunks are dropped and
    /// their segment files deleted. Unset (default) = kept forever.
    #[serde(default)]
    pub retention: Option<String>,
}

/// Parse a retention duration: a positive integer followed by `s`, `m`, `h`, `d` or `w`.
/// Returns nanoseconds.
pub fn parse_duration_ns(text: &str) -> Option<i64> {
    let text = text.trim();
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (num, unit) = text.split_at(split);
    let n: i64 = num.parse().ok().filter(|&n| n > 0)?;
    let unit_s: i64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return None,
    };
    n.checked_mul(unit_s)?.checked_mul(1_000_000_000)
}

fn default_max_dict_entries() -> usize {
    100_000
}
fn default_max_message_bytes() -> usize {
    1024 * 1024
}

/// Largest `max_message_bytes` a source may be given: a message is stored as one frame, and a
/// frame (16 MiB) must also hold the source id.
pub const MAX_SOURCE_MESSAGE_BYTES: usize = 16 * 1024 * 1024 - 64;

/// Where a Kafka source starts when it has no saved position for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceStart {
    /// Only messages published from now on.
    #[default]
    Latest,
    /// Everything the topic still retains.
    Earliest,
}

/// Message format of a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    /// One JSON object per message; `map` gives a field (or `a.b.c` path) per column.
    Json,
    /// One CSV record per message (or per line for files); `map` gives a 0-based column index per column.
    Csv,
    /// Sparkplug B (MQTT only): one row per metric of every BIRTH and DATA message; `map` uses
    /// `$metric`, `$value`, `$string`, `$timestamp`, `$datatype`, `$historical`, `$seq`,
    /// `$group`, `$node`, `$device`, `$type` and `$topic`.
    Sparkplug,
}

/// Transport of a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// NATS core subject (feature `nats`).
    Nats,
    /// Kafka topic (feature `kafka`).
    Kafka,
    /// A file replayed line by line (NDJSON or CSV); for tests and backfills.
    File,
    /// MQTT topic filter (feature `mqtt`), at-least-once delivery with a persistent session.
    Mqtt,
}

/// SASL mechanism of a Kafka source. The names are Kafka's, in lower case; the upper-case
/// spelling of a broker's `sasl.mechanism` is accepted too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    /// `PLAIN` (RFC 4616): the password itself is sent; only safe inside TLS.
    #[serde(rename = "plain", alias = "PLAIN")]
    Plain,
    /// `SCRAM-SHA-256` (RFC 7677): challenge-response, the password is not sent.
    #[serde(rename = "scram-sha-256", alias = "SCRAM-SHA-256")]
    ScramSha256,
    /// `SCRAM-SHA-512`.
    #[serde(rename = "scram-sha-512", alias = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    /// The mechanism name as Kafka spells it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// Unit of the timestamp field as it arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    /// Seconds (integer or decimal).
    Seconds,
    /// Milliseconds.
    Millis,
    /// Microseconds.
    Micros,
    /// Nanoseconds.
    #[default]
    Nanos,
    /// RFC 3339 text.
    Rfc3339,
}

/// One message source → one user table.
// each bool is an independent key of the configuration file, not a state of the source
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Source name (metrics label).
    pub name: String,
    /// Transport.
    pub kind: SourceKind,
    /// NATS URL (`nats://host:4222`), Kafka bootstrap servers (comma list), MQTT broker
    /// (`mqtt://host:1883`), or a file path.
    pub url: String,
    /// NATS subject, Kafka topic or MQTT topic filter (unused for files).
    #[serde(default)]
    pub subject: String,
    /// Reserved: consumer groups are not implemented (positions are kept by the server, see
    /// `docs/generic-ingest.md`). Setting it is a configuration error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Kafka: where to start a partition that has no saved position.
    #[serde(default)]
    pub start: SourceStart,
    /// Largest message accepted, in bytes; bigger ones are dropped and counted.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    /// Message format.
    pub format: SourceFormat,
    /// Destination table (one of `[[tables]]`).
    pub table: String,
    /// Column → field path (JSON) or column index (CSV, as text).
    pub map: std::collections::BTreeMap<String, String>,
    /// Unit of the timestamp field.
    #[serde(default)]
    pub ts_unit: TimeUnit,
    /// Use the host receive time when the message has no timestamp field.
    #[serde(default)]
    pub ts_from_receive: bool,
    /// JSON path of an array in each message: one row per element; column paths are read inside
    /// the element, `$msg.<path>` from the message around it. Unset: one row per message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<String>,
    /// Kafka: connect with TLS. (The Kafka `url` is a server list without a scheme; an MQTT
    /// source asks for TLS with an `mqtts://` url instead.)
    #[serde(default)]
    pub tls: bool,
    /// TLS sources (`mqtts://`, or Kafka with `tls = true`): PEM file of the CA(s) to trust
    /// instead of the public roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<PathBuf>,
    /// TLS sources: PEM client certificate chain (with `tls_key`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<PathBuf>,
    /// TLS sources: PEM private key of `tls_cert`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_key: Option<PathBuf>,
    /// Kafka: authenticate with SASL using this mechanism (with `sasl_username` and
    /// `sasl_password_env`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_mechanism: Option<SaslMechanism>,
    /// Kafka SASL user name (for a managed service, often an API key id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_username: Option<String>,
    /// Environment variable holding the Kafka SASL password (never the password itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_password_env: Option<String>,
    /// Resolved SASL password (from `sasl_password_env`); never serialized, redacted in `Debug`.
    #[serde(skip)]
    pub sasl_password: Option<Secret>,
    /// Kafka: accept SASL without TLS to a broker beyond loopback. Off by default: a `PLAIN`
    /// password and all data would cross the network in clear text. Only for a private network
    /// segment.
    #[serde(default)]
    pub allow_plaintext: bool,
    /// Sparkplug: ask an edge node to publish its BIRTH again (`NCMD` `Node Control/Rebirth`)
    /// when its DATA arrives before any BIRTH since the server started. Needs permission to
    /// publish on `spBv1.0/<group>/NCMD/<node>`.
    #[serde(default)]
    pub sparkplug_rebirth: bool,
}

impl std::fmt::Debug for SourceConfig {
    /// The URL may carry broker credentials (`nats://user:password@host`): never print it whole.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceConfig")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("url", &crate::redact_url(&self.url))
            .field("subject", &self.subject)
            .field("format", &self.format)
            .field("table", &self.table)
            .field("map", &self.map)
            .field("ts_unit", &self.ts_unit)
            .field("ts_from_receive", &self.ts_from_receive)
            .field("start", &self.start)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("tls", &self.tls)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password_env", &self.sasl_password_env)
            .field("sasl_password", &self.sasl_password)
            .field("allow_plaintext", &self.allow_plaintext)
            .finish_non_exhaustive()
    }
}

impl SourceConfig {
    /// True when the source connects with TLS: an `mqtts://` url, or Kafka with `tls = true`.
    #[must_use]
    pub fn uses_tls(&self) -> bool {
        match self.kind {
            SourceKind::Mqtt => self.url.starts_with("mqtts://"),
            SourceKind::Kafka => self.tls,
            SourceKind::Nats | SourceKind::File => false,
        }
    }

    /// True when every Kafka bootstrap server of `url` is a loopback address (`localhost`,
    /// `127.0.0.0/8`, `::1`). Only the configured list is looked at: where a broker's advertised
    /// listeners point is not known before connecting.
    #[must_use]
    pub fn kafka_bootstrap_is_loopback(&self) -> bool {
        let mut hosts = kafka_hosts(&self.url).peekable();
        hosts.peek().is_some()
            && hosts.all(|h| {
                h.eq_ignore_ascii_case("localhost")
                    || h.parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            })
    }

    /// TLS and SASL settings: which keys go together, on which transport, and that a password
    /// does not cross a network in clear text by accident. The reason comes without the
    /// `sources.<name>:` prefix.
    fn validate_connection_security(&self) -> Result<(), String> {
        let kafka = self.kind == SourceKind::Kafka;
        if self.tls && !kafka {
            return Err(
                "tls = true is for Kafka sources (an MQTT source uses an mqtts:// url)".to_owned(),
            );
        }
        let has_tls_files =
            self.tls_ca.is_some() || self.tls_cert.is_some() || self.tls_key.is_some();
        if has_tls_files && !self.uses_tls() {
            return Err(if kafka {
                "tls_ca / tls_cert / tls_key need tls = true".to_owned()
            } else {
                "tls_ca / tls_cert / tls_key need an mqtts:// source (or a Kafka source with tls = true)"
                    .to_owned()
            });
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err("tls_cert and tls_key must be set together".to_owned());
        }
        // the client library takes the TLS server name from the text before the first `:`
        if kafka && self.tls && self.url.contains('[') {
            return Err(
                "tls = true does not work with IPv6 literal addresses in url; use host names or IPv4 addresses"
                    .to_owned(),
            );
        }
        let sasl_keys = [
            self.sasl_mechanism.is_some(),
            self.sasl_username.is_some(),
            self.sasl_password_env.is_some(),
        ];
        let sasl = sasl_keys.contains(&true);
        if !kafka && (sasl || self.allow_plaintext) {
            return Err(
                "sasl_mechanism / sasl_username / sasl_password_env / allow_plaintext are for Kafka sources"
                    .to_owned(),
            );
        }
        if sasl {
            if sasl_keys.contains(&false) {
                return Err(
                    "sasl_mechanism, sasl_username and sasl_password_env must be set together"
                        .to_owned(),
                );
            }
            if self.sasl_username.as_deref().is_none_or(str::is_empty) {
                return Err("sasl_username is empty".to_owned());
            }
            if self.sasl_password.is_none() {
                return Err(format!(
                    "sasl_password_env names a variable ({}) that is unset or empty",
                    self.sasl_password_env.as_deref().unwrap_or_default()
                ));
            }
            if !self.tls && !self.allow_plaintext && !self.kafka_bootstrap_is_loopback() {
                return Err(
                    "SASL without TLS to a broker beyond loopback is refused: a PLAIN password and all data would cross the network in clear text, and a SCRAM exchange can be attacked offline. Set tls = true, or allow_plaintext = true for a private network segment"
                        .to_owned(),
                );
            }
        }
        if self.allow_plaintext && (self.tls || !sasl) {
            return Err(
                "allow_plaintext only applies to SASL without TLS; remove it (or tls = true)"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

/// Host part of every entry of a Kafka bootstrap list (`host:port,[v6]:port,host`).
fn kafka_hosts(url: &str) -> impl Iterator<Item = &str> {
    url.split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| match e.strip_prefix('[') {
            Some(rest) => rest.split(']').next().unwrap_or(rest),
            None => e.rsplit_once(':').map_or(e, |(host, _)| host),
        })
}

/// Continuous query kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuousKind {
    /// Incremental OHLCV bars per key and bucket; the output table has one row per closed bar
    /// plus the live (open) bar.
    Ohlcv,
    /// Incremental quantile sketches per key over tumbling windows.
    Percentiles,
    /// For every row of `table`, the latest row of `right_table` with the same key as of its
    /// timestamp (streaming ASOF); output has the left columns plus the right value columns.
    Asof,
    /// OHLCV over a trailing window of `width_ns`, emitted every `step_ns` per key: one row per
    /// step end whose window holds rows (`ts` is the window's exclusive end), plus the live
    /// window in `cq_<name>_live`.
    Rolling,
}

/// One continuous query, exposed as table `cq_<name>` and refreshed every snapshot tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuousConfig {
    /// Name; the SQL table is `cq_<name>`.
    pub name: String,
    /// Kind.
    pub kind: ContinuousKind,
    /// Input table.
    pub table: String,
    /// Key column (dict / u32).
    #[serde(default = "default_symbol")]
    pub key: String,
    /// Value column (`price` for OHLCV, the measured column for percentiles).
    #[serde(default)]
    pub value: Option<String>,
    /// Size column for OHLCV volume (optional).
    #[serde(default)]
    pub size: Option<String>,
    /// Bucket / window width in nanoseconds (OHLCV, percentiles, rolling).
    #[serde(default)]
    pub width_ns: Option<i64>,
    /// Step of a rolling window in nanoseconds (`rolling`; `width_ns` must be a multiple).
    #[serde(default)]
    pub step_ns: Option<i64>,
    /// Quantiles for `percentiles` (e.g. `[0.5, 0.99]`).
    #[serde(default)]
    pub quantiles: Vec<f64>,
    /// Right table for `asof`.
    #[serde(default)]
    pub right_table: Option<String>,
    /// Maximum rows kept in the output table (oldest closed rows are dropped beyond it).
    #[serde(default = "default_cq_rows")]
    pub max_rows: usize,
}

fn default_symbol() -> String {
    "symbol".to_owned()
}
fn default_cq_rows() -> usize {
    1_000_000
}

/// Replication: serve this node's WAL to standbys, or run as a standby of a primary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReplicationConfig {
    /// Primary's Flight address (`host:port`). Set = this node is a read-only standby: it
    /// ingests the primary's WAL records instead of a feed, writes its own WAL and serves queries.
    pub standby_of: Option<String>,
    /// Environment variable holding the primary's bearer token (defaults to `server.token_env`).
    pub primary_token_env: Option<String>,
    /// CA certificate (PEM) when the primary uses TLS.
    pub primary_tls_ca: Option<PathBuf>,
    /// Expected server name when it differs from the host part of `standby_of`.
    pub primary_tls_domain: Option<String>,
    /// Serve this node's WAL to standbys through the Flight ticket `__wal__:<lsn>` (needs
    /// `storage.wal_dir`). Off by default: the stream carries every ingested message verbatim.
    pub serve_wal: bool,
    /// Environment variable holding the token standbys must present for the WAL stream. Unset =
    /// the query token is accepted, which lets every SQL client read the raw log.
    pub token_env: Option<String>,
    /// Resolved replication token.
    #[serde(skip)]
    pub token: Option<Secret>,
    /// Concurrent WAL streams accepted.
    pub max_standbys: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            standby_of: None,
            primary_token_env: None,
            primary_tls_ca: None,
            primary_tls_domain: None,
            serve_wal: false,
            token_env: None,
            token: None,
            max_standbys: 4,
        }
    }
}

/// Feed source settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct FeedConfig {
    /// Run the built-in exchange feed. `false` = only `[[sources]]` feed the server (telemetry
    /// deployments); the market tables stay empty.
    pub enabled: bool,
    /// WebSocket base URL, or `file://<capture.ankc>[,<more>]` to serve recorded frames.
    pub url: String,
    /// REST base URL for depth snapshots.
    pub rest_url: String,
    /// Stream names (Binance combined-stream syntax).
    pub streams: Vec<String>,
    /// Fetch REST depth snapshots and align diffs.
    pub depth_snapshots: bool,
    /// Environment variable holding the exchange API key (never the key itself).
    pub api_key_env: Option<String>,
}

/// Storage settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// WAL directory; `None` disables durability.
    pub wal_dir: Option<PathBuf>,
    /// Keep this many sealed chunks per partition hot (uncompressed) before encoding to the warm tier.
    pub hot_sealed_chunks: usize,
    /// Memory for uncompressed rows, MiB: open chunks (counted twice: the chunk and its query
    /// view) and hot sealed chunks. Over it, the partitions with the oldest open rows are sealed
    /// and encoded first, then the hot window of every partition shrinks. 0 = no budget.
    pub hot_budget_mib: u64,
    /// An open chunk whose first row is older than this (seconds, wall clock) is sealed and
    /// encoded, filled or not: devices that report rarely do not keep rows uncompressed for
    /// days. 0 = never by age.
    pub max_open_age_s: u64,
    /// Directory for checkpointed segments; `None` keeps everything in memory.
    pub checkpoint_dir: Option<PathBuf>,
    /// Snapshot (query view swap) interval in milliseconds.
    pub snapshot_ms: u64,
    /// Interrupt moderation of the ingest loop, microseconds: while frames keep arriving (more
    /// than one wake-up per millisecond) the loop waits this long between batches, so that one
    /// wake-up takes many frames instead of one each. Rows become queryable at the next
    /// snapshot anyway, so the wait is not visible to queries. 0 turns it off. At most 10,000.
    pub ingest_coalesce_us: u64,
    /// Threads that map JSON and CSV messages to rows while the ingest loop applies them in log
    /// order (RFC-0001). Unset: the number of cores minus two (the ingest loop and the log
    /// thread), at most 32; 0: map in the ingest loop. At most 256.
    pub ingest_workers: Option<usize>,
    /// Memory budget for warm (encoded, in-memory) chunks in MiB; beyond it the oldest warm
    /// chunks are demoted to cold (segment on disk, decoded on demand). Needs `checkpoint_dir`.
    pub warm_budget_mib: u64,
    /// Recently read cold segment files kept in memory, compressed as on disk, in MiB (least
    /// recently used out first; 0: none). A cold chunk read again is decoded without reading
    /// its file or parsing its footer.
    pub segment_cache_mib: u64,
    /// Minimum seconds between WAL truncations after a checkpoint.
    pub checkpoint_interval_s: u64,
    /// Threads that encode sealed chunks (Vortex) and write their segments, off the ingest loop.
    /// Unset: a quarter of the cores, 1 to 16; 0: encode in the snapshot tick within
    /// `tier_budget_pct`. At most 256.
    pub tier_workers: Option<usize>,
    /// Share of one core (percent) the tiering step may use per snapshot interval (only when
    /// `tier_workers` is 0).
    pub tier_budget_pct: u8,
    /// WAL size, MiB, above which the partitions holding the oldest unpersisted rows are
    /// sealed and written to segments early so the log can be truncated (needs
    /// `checkpoint_dir`). A quiet partition would otherwise keep the whole log alive.
    pub wal_budget_mib: u64,
    /// Retention of the market tables (`trades`, `book_deltas`, `quotes`), as for a user
    /// table's `retention` (`"30d"`). Unset (default) = kept forever.
    pub market_retention: Option<String>,
}

/// Network settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Arrow Flight bind address.
    pub flight_addr: SocketAddr,
    /// Prometheus metrics bind address.
    pub metrics_addr: SocketAddr,
    /// Optional PostgreSQL wire protocol bind address (read-only; password = token).
    pub pg_addr: Option<SocketAddr>,
    /// Environment variable holding the bearer token (never the token itself).
    pub token_env: Option<String>,
    /// TLS certificate chain (PEM) for Flight; with `tls_key` enables TLS.
    pub tls_cert: Option<PathBuf>,
    /// TLS private key (PEM).
    pub tls_key: Option<PathBuf>,
    /// Client CA (PEM): when set, Flight requires client certificates (mutual TLS).
    pub tls_client_ca: Option<PathBuf>,
    /// Resolved bearer token (from the CLI or `token_env`); never serialized, redacted in `Debug`.
    #[serde(skip)]
    pub token: Option<Secret>,
    /// Environment variable holding a separate token for `/metrics` (defaults to the main token).
    /// Use it when Prometheus scrapes over a path you trust less than the SQL clients.
    pub metrics_token_env: Option<String>,
    /// Resolved metrics token.
    #[serde(skip)]
    pub metrics_token: Option<Secret>,
    /// Wall-clock budget per query (planning + execution + transfer), seconds.
    pub query_timeout_s: u64,
    /// Queries running at once over Flight; further ones wait (within their timeout).
    pub max_concurrent_queries: usize,
    /// Accept a Flight listener beyond loopback without TLS. Off by default: the bearer token
    /// and all data would cross the network in clear text. Only for a private network segment
    /// or behind a TLS-terminating proxy.
    pub allow_plaintext: bool,
    /// How `dict` columns appear in SQL: `names` (default), dictionary-encoded strings
    /// (`WHERE host = 'web-1'`, results carry the names); `ids`, the stored numeric ids, with
    /// the names in the `dictionaries` and `symbols` tables (the behaviour before RFC-0002).
    pub dict_columns: DictColumns,
}

/// How `dict` columns appear in SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DictColumns {
    /// Dictionary-encoded strings.
    #[default]
    Names,
    /// Numeric ids.
    Ids,
}

/// Shortest bearer token accepted when a listener is reachable beyond loopback.
pub const MIN_TOKEN_LEN: usize = 16;

/// A secret string: compares normally, prints as `***`, never serialized.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Secret(String);

impl Secret {
    /// Wrap a secret value.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The secret value (pass it on; do not log it).
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

/// Resource budgets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ResourceConfig {
    /// Locked memory the engine intends to use (UMEM, arenas), in MiB; checked against `RLIMIT_MEMLOCK`.
    pub memlock_mib: u64,
    /// Fail at start-up when the resident memory budget (MiB) cannot be satisfied.
    pub memory_budget_mib: u64,
    /// Memory pool for query execution (`DataFusion`), MiB; a query beyond it fails instead of
    /// taking the ingest process down.
    pub query_memory_mib: u64,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            url: "wss://data-stream.binance.vision/stream".into(),
            rest_url: "https://api.binance.com".into(),
            streams: vec!["btcusdt@trade".into(), "btcusdt@depth@100ms".into()],
            depth_snapshots: true,
            api_key_env: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            wal_dir: None,
            hot_sealed_chunks: 4,
            hot_budget_mib: 1024,
            max_open_age_s: 600,
            checkpoint_dir: None,
            snapshot_ms: 50,
            ingest_coalesce_us: 200,
            ingest_workers: None,
            tier_workers: None,
            warm_budget_mib: 512,
            segment_cache_mib: 256,
            checkpoint_interval_s: 60,
            tier_budget_pct: 5,
            wal_budget_mib: 1024,
            market_retention: None,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            flight_addr: "127.0.0.1:50051".parse().expect("static"),
            metrics_addr: "127.0.0.1:9100".parse().expect("static"),
            pg_addr: None,
            token_env: Some("ANKAA_TOKEN".into()),
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            token: None,
            metrics_token_env: None,
            metrics_token: None,
            query_timeout_s: 30,
            max_concurrent_queries: 32,
            allow_plaintext: false,
            dict_columns: DictColumns::Names,
        }
    }
}

/// Configuration errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// TOML syntax or unknown field.
    #[error("config parse error: {0}")]
    Parse(String),
    /// Semantic validation failure.
    #[error("invalid config: {0}")]
    Invalid(String),
    /// A start-up resource check failed.
    #[error("resource check failed: {0}")]
    Resource(String),
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            memlock_mib: 0,
            memory_budget_mib: 0,
            query_memory_mib: 1024,
        }
    }
}

impl Config {
    /// Parse and validate TOML text (resolving `server.token_env`).
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let mut cfg = Self::parse_toml(text)?;
        cfg.resolve_secrets();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse TOML text without validation (callers apply CLI overrides, then `validate`).
    pub fn parse_toml(text: &str) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Fill `server.token` / `server.metrics_token`, the replication token and the SASL password
    /// of every source from their `*_env` names when not already set.
    pub fn resolve_secrets(&mut self) {
        if self.server.token.is_none()
            && let Some(var) = &self.server.token_env
            && let Ok(v) = std::env::var(var)
            && !v.is_empty()
        {
            self.server.token = Some(Secret::new(v));
        }
        if self.server.metrics_token.is_none()
            && let Some(var) = &self.server.metrics_token_env
            && let Ok(v) = std::env::var(var)
            && !v.is_empty()
        {
            self.server.metrics_token = Some(Secret::new(v));
        }
        if self.replication.token.is_none()
            && let Some(var) = &self.replication.token_env
            && let Ok(v) = std::env::var(var)
            && !v.is_empty()
        {
            self.replication.token = Some(Secret::new(v));
        }
        for s in &mut self.sources {
            if s.sasl_password.is_none()
                && let Some(var) = &s.sasl_password_env
                && let Ok(v) = std::env::var(var)
                && !v.is_empty()
            {
                s.sasl_password = Some(Secret::new(v));
            }
        }
    }

    /// Serialize to TOML (for `ankaad config print`).
    pub fn to_toml(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_default()
    }

    /// Semantic validation (no I/O).
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> Result<(), ConfigError> {
        let inv = |m: &str| Err(ConfigError::Invalid(m.to_owned()));
        if !(self.feed.url.starts_with("wss://")
            || self.feed.url.starts_with("ws://")
            || self.feed.url.starts_with("file://"))
        {
            return inv("feed.url must start with ws://, wss:// or file:// (capture replay)");
        }
        if !(self.feed.rest_url.starts_with("https://")
            || self.feed.rest_url.starts_with("http://"))
        {
            return inv("feed.rest_url must start with http:// or https://");
        }
        if self.feed.enabled && self.feed.streams.is_empty() {
            return inv("feed.streams must not be empty (or set feed.enabled = false)");
        }
        // `feed.enabled = false` and no [[sources]] is allowed: such a server ingests nothing and
        // serves the data it has stored (a read-only copy, a container started before its
        // sources are configured); `serve` says so in its log
        for s in &self.feed.streams {
            // the symbol part ends up in a REST URL: letters, digits, `@` and `_` only
            if s.is_empty()
                || !s.contains('@')
                || !s
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '@' || c == '_')
            {
                return Err(ConfigError::Invalid(format!(
                    "feed.streams entry {s:?} is not a <symbol>@<stream> name"
                )));
            }
        }
        if self.storage.snapshot_ms < 5 {
            return inv("storage.snapshot_ms must be >= 5");
        }
        if self.storage.ingest_workers.is_some_and(|n| n > 256) {
            return inv("storage.ingest_workers must be <= 256");
        }
        if self.storage.tier_workers.is_some_and(|n| n > 256) {
            return inv("storage.tier_workers must be <= 256");
        }
        if self.storage.ingest_coalesce_us > 10_000 {
            return inv("storage.ingest_coalesce_us must be <= 10000 (10 ms)");
        }
        if self.storage.hot_sealed_chunks == 0 {
            return inv("storage.hot_sealed_chunks must be >= 1");
        }
        if !(1..=50).contains(&self.storage.tier_budget_pct) {
            return inv("storage.tier_budget_pct must be between 1 and 50");
        }
        if self.storage.checkpoint_interval_s == 0 {
            return inv("storage.checkpoint_interval_s must be >= 1");
        }
        if !(1..=86_400).contains(&self.server.query_timeout_s) {
            return inv("server.query_timeout_s must be between 1 and 86400");
        }
        if self.resources.query_memory_mib < 64 {
            return inv("resources.query_memory_mib must be >= 64");
        }
        if self.storage.checkpoint_dir.is_none() && self.storage.warm_budget_mib == 0 {
            return inv(
                "storage.warm_budget_mib = 0 needs storage.checkpoint_dir (nowhere to demote to)",
            );
        }
        for (name, mib) in [
            ("resources.memlock_mib", self.resources.memlock_mib),
            (
                "resources.memory_budget_mib",
                self.resources.memory_budget_mib,
            ),
            (
                "resources.query_memory_mib",
                self.resources.query_memory_mib,
            ),
            ("storage.warm_budget_mib", self.storage.warm_budget_mib),
            ("storage.segment_cache_mib", self.storage.segment_cache_mib),
        ] {
            if mib.checked_mul(1024 * 1024).is_none() {
                return Err(ConfigError::Invalid(format!("{name} is too large")));
            }
        }
        let token_configured = self
            .server
            .token
            .as_ref()
            .is_some_and(|t| !t.expose().is_empty());
        let loopback = self.server.flight_addr.ip().is_loopback()
            && self.server.metrics_addr.ip().is_loopback()
            && self.server.pg_addr.is_none_or(|a| a.ip().is_loopback());
        if self.server.pg_addr.is_some() && !token_configured {
            return inv("server.pg_addr requires a bearer token (it is the Postgres password)");
        }
        if !loopback && !token_configured {
            return inv(
                "binding beyond loopback requires a bearer token (--token, or server.token_env naming a set variable)",
            );
        }
        if !loopback {
            for (name, t) in [
                ("server.token", &self.server.token),
                ("server.metrics_token", &self.server.metrics_token),
                ("replication.token", &self.replication.token),
            ] {
                if t.as_ref().is_some_and(|t| t.expose().len() < MIN_TOKEN_LEN) {
                    return Err(ConfigError::Invalid(format!(
                        "{name} is shorter than {MIN_TOKEN_LEN} characters; a listener beyond loopback needs a long random token (e.g. `openssl rand -hex 32`)"
                    )));
                }
            }
        }
        if !self.server.flight_addr.ip().is_loopback()
            && self.server.tls_cert.is_none()
            && !self.server.allow_plaintext
        {
            return inv(
                "server.flight_addr is beyond loopback without TLS: the token and the data would travel in clear text. Set server.tls_cert and server.tls_key, or server.allow_plaintext = true for a private network or a TLS-terminating proxy",
            );
        }
        if self.server.max_concurrent_queries == 0 {
            return inv("server.max_concurrent_queries must be >= 1");
        }
        if self.storage.wal_budget_mib < 512 {
            return inv("storage.wal_budget_mib must be >= 512 (two WAL segments)");
        }
        if self
            .storage
            .market_retention
            .as_deref()
            .is_some_and(|r| parse_duration_ns(r).is_none())
        {
            return inv(
                "storage.market_retention must be a duration like \"30d\", \"12h\", \"90m\"",
            );
        }
        if self.server.tls_cert.is_some() != self.server.tls_key.is_some() {
            return inv("server.tls_cert and server.tls_key must be set together");
        }
        if self.server.tls_client_ca.is_some() && self.server.tls_cert.is_none() {
            return inv("server.tls_client_ca requires server.tls_cert and server.tls_key");
        }
        self.validate_user_tables()?;
        if self.replication.standby_of.is_some() && self.storage.wal_dir.is_none() {
            return inv(
                "replication.standby_of requires storage.wal_dir (the standby keeps its own WAL)",
            );
        }
        if self.replication.serve_wal && self.storage.wal_dir.is_none() {
            return inv("replication.serve_wal requires storage.wal_dir");
        }
        if self.replication.max_standbys == 0 {
            return inv("replication.max_standbys must be >= 1");
        }
        if self.replication.token_env.is_some()
            && self.replication.serve_wal
            && self.replication.token.is_none()
        {
            return inv(
                "replication.token_env names a variable that is unset or empty (the WAL stream would be unprotected)",
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn validate_user_tables(&self) -> Result<(), ConfigError> {
        let inv = |m: String| Err(ConfigError::Invalid(m));
        let reserved = ["trades", "book_deltas", "quotes", "symbols", "dictionaries"];
        let mut names = std::collections::BTreeSet::new();
        for t in &self.tables {
            if t.name.is_empty()
                || !t
                    .name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
                || t.name.starts_with("cq_")
            {
                return inv(format!("tables: {:?} is not a valid table name", t.name));
            }
            if reserved.contains(&t.name.as_str()) || !names.insert(t.name.clone()) {
                return inv(format!("tables: {:?} is reserved or duplicated", t.name));
            }
            match t.columns.first() {
                Some(c) if c.kind == ColumnKind::TimestampNs => {}
                _ => {
                    return inv(format!(
                        "tables.{}: first column must be timestamp_ns",
                        t.name
                    ));
                }
            }
            let mut cols = std::collections::BTreeSet::new();
            for c in &t.columns {
                if !cols.insert(c.name.as_str()) {
                    return inv(format!("tables.{}: duplicate column {:?}", t.name, c.name));
                }
            }
            if t.max_lateness_ms > 7 * 24 * 3600 * 1000 {
                return inv(format!(
                    "tables.{}: max_lateness_ms must be at most 7 days",
                    t.name
                ));
            }
            if let Some(r) = &t.retention
                && parse_duration_ns(r).is_none()
            {
                return inv(format!(
                    "tables.{}: retention {r:?} is not a duration like \"30d\", \"12h\", \"90m\"",
                    t.name
                ));
            }
            if t.max_dict_entries == 0 {
                return inv(format!("tables.{}: max_dict_entries must be >= 1", t.name));
            }
            for c in &t.columns {
                if c.nullable && matches!(c.kind, ColumnKind::TimestampNs | ColumnKind::Dict) {
                    return inv(format!(
                        "tables.{}: column {:?} cannot be nullable (timestamps and dict keys always hold a value)",
                        t.name, c.name
                    ));
                }
                if c.name.is_empty()
                    || !c
                        .name
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                {
                    return inv(format!(
                        "tables.{}: {:?} is not a valid column name",
                        t.name, c.name
                    ));
                }
            }
            if let Some(p) = &t.partition_by {
                match t.columns.iter().find(|c| &c.name == p) {
                    Some(c) if c.kind == ColumnKind::Dict => {}
                    _ => {
                        return inv(format!(
                            "tables.{}: partition_by must name a dict column",
                            t.name
                        ));
                    }
                }
            }
        }
        if self.sources.len() > usize::from(u16::MAX) {
            return inv("sources: too many sources".to_owned());
        }
        let mut source_names = std::collections::BTreeSet::new();
        for s in &self.sources {
            if s.name.is_empty()
                || !s
                    .name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                || !source_names.insert(s.name.as_str())
            {
                return inv(format!(
                    "sources: {:?} is not a valid, unique source name",
                    s.name
                ));
            }
            let Some(t) = self.tables.iter().find(|t| t.name == s.table) else {
                return inv(format!("sources.{}: unknown table {:?}", s.name, s.table));
            };
            if s.group.is_some() {
                return inv(format!(
                    "sources.{}: `group` is not supported (the server keeps Kafka positions itself); remove it",
                    s.name
                ));
            }
            if s.url.trim().is_empty() {
                return inv(format!("sources.{}: url is required", s.name));
            }
            // a message travels as one frame, and frames are limited to 16 MiB
            if !(16..=MAX_SOURCE_MESSAGE_BYTES).contains(&s.max_message_bytes) {
                return inv(format!(
                    "sources.{}: max_message_bytes must be between 16 and {MAX_SOURCE_MESSAGE_BYTES}",
                    s.name
                ));
            }
            match s.kind {
                SourceKind::Nats
                    if !s.url.starts_with("nats://") && !s.url.starts_with("tls://") =>
                {
                    return inv(format!(
                        "sources.{}: NATS url must start with nats:// or tls://",
                        s.name
                    ));
                }
                SourceKind::Mqtt
                    if !s.url.starts_with("mqtt://") && !s.url.starts_with("mqtts://") =>
                {
                    return inv(format!(
                        "sources.{}: MQTT url must start with mqtt:// or mqtts:// (TLS)",
                        s.name
                    ));
                }
                SourceKind::Nats | SourceKind::Kafka | SourceKind::Mqtt if s.subject.is_empty() => {
                    return inv(format!("sources.{}: subject/topic is required", s.name));
                }
                _ => {}
            }
            if s.format == SourceFormat::Sparkplug && s.kind != SourceKind::Mqtt {
                return inv(format!(
                    "sources.{}: format = \"sparkplug\" needs kind = \"mqtt\"",
                    s.name
                ));
            }
            if s.sparkplug_rebirth && s.format != SourceFormat::Sparkplug {
                return inv(format!(
                    "sources.{}: sparkplug_rebirth needs format = \"sparkplug\"",
                    s.name
                ));
            }
            if let Some(rows) = &s.rows {
                if s.format != SourceFormat::Json {
                    return inv(format!("sources.{}: rows needs format = \"json\"", s.name));
                }
                if rows.trim().is_empty()
                    || rows.split('.').any(str::is_empty)
                    || rows.starts_with('$')
                {
                    return inv(format!(
                        "sources.{}: rows = {rows:?} is not a JSON path (e.g. \"samples\" or \"data.points\")",
                        s.name
                    ));
                }
            }
            if let Err(reason) = s.validate_connection_security() {
                return inv(format!("sources.{}: {reason}", s.name));
            }
            if s.kind != SourceKind::Mqtt && s.map.values().any(|v| v.trim().starts_with("$topic"))
            {
                return inv(format!(
                    "sources.{}: $topic selectors need an mqtt source",
                    s.name
                ));
            }
            for col in s.map.keys() {
                if !t.columns.iter().any(|c| &c.name == col) {
                    return inv(format!(
                        "sources.{}: map names unknown column {:?}",
                        s.name, col
                    ));
                }
            }
            let ts_col = &t.columns[0].name;
            if !s.map.contains_key(ts_col) && !s.ts_from_receive {
                return inv(format!(
                    "sources.{}: map the timestamp column {ts_col:?} or set ts_from_receive",
                    s.name
                ));
            }
            // a column without a mapping has no value: every row would be rejected
            if let Some(c) = t
                .columns
                .iter()
                .skip(1)
                .find(|c| !s.map.contains_key(&c.name))
            {
                return inv(format!(
                    "sources.{}: column {:?} of table {:?} is not mapped",
                    s.name, c.name, t.name
                ));
            }
            if s.format == SourceFormat::Csv
                && let Some((col, v)) = s.map.iter().find(|(_, v)| v.trim().parse::<u16>().is_err())
            {
                return inv(format!(
                    "sources.{}: CSV map for {col:?} must be a column index, got {v:?}",
                    s.name
                ));
            }
        }
        let mut cq_names = std::collections::BTreeSet::new();
        for c in &self.continuous {
            if c.name.is_empty()
                || !c
                    .name
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                return inv(format!("continuous: {:?} is not a valid name", c.name));
            }
            if !cq_names.insert(c.name.clone()) {
                return inv(format!("continuous: duplicate name {:?}", c.name));
            }
            if c.name.ends_with("_live") {
                return inv(format!(
                    "continuous: {:?} must not end in `_live` (cq_<name>_live is the live table of <name>)",
                    c.name
                ));
            }
            if c.kind == ContinuousKind::Asof && c.key.is_empty() {
                return inv(format!(
                    "continuous.{}: asof needs a key column (an unkeyed join would mix the partitions of its inputs)",
                    c.name
                ));
            }
            match c.kind {
                ContinuousKind::Ohlcv | ContinuousKind::Percentiles => {
                    if c.width_ns.is_none_or(|w| w <= 0) {
                        return inv(format!("continuous.{}: width_ns must be > 0", c.name));
                    }
                    if c.value.is_none() {
                        return inv(format!("continuous.{}: value column is required", c.name));
                    }
                    if c.kind == ContinuousKind::Percentiles
                        && (c.quantiles.is_empty()
                            || c.quantiles.iter().any(|q| !(0.0..=1.0).contains(q)))
                    {
                        return inv(format!(
                            "continuous.{}: quantiles must be in [0, 1]",
                            c.name
                        ));
                    }
                }
                ContinuousKind::Asof => {
                    if c.right_table.is_none() {
                        return inv(format!("continuous.{}: right_table is required", c.name));
                    }
                }
                ContinuousKind::Rolling => {
                    let (Some(window), Some(step)) = (c.width_ns, c.step_ns) else {
                        return inv(format!(
                            "continuous.{}: rolling needs width_ns (the window) and step_ns",
                            c.name
                        ));
                    };
                    if step <= 0 || window < step || window % step != 0 {
                        return inv(format!(
                            "continuous.{}: width_ns must be a positive multiple of step_ns",
                            c.name
                        ));
                    }
                    if window / step > 100_000 {
                        return inv(format!(
                            "continuous.{}: a window may span at most 100000 steps",
                            c.name
                        ));
                    }
                    if c.value.is_none() {
                        return inv(format!("continuous.{}: value column is required", c.name));
                    }
                }
            }
            if c.max_rows == 0 {
                return inv(format!("continuous.{}: max_rows must be > 0", c.name));
            }
        }
        Ok(())
    }

    /// Fail-fast resource checks (I/O, platform-specific); creates the WAL and checkpoint directories.
    pub fn check_resources(&self) -> Result<(), ConfigError> {
        self.check_resources_impl(true)
    }

    /// The same checks without creating anything (`ankaad config check`).
    pub fn check_resources_dry_run(&self) -> Result<(), ConfigError> {
        self.check_resources_impl(false)
    }

    fn check_resources_impl(&self, create_dirs: bool) -> Result<(), ConfigError> {
        if self.resources.memlock_mib > 0 {
            let limit = memlock_limit_bytes();
            let need = self.resources.memlock_mib.saturating_mul(1024 * 1024);
            if let Some(l) = limit
                && l < need
            {
                return Err(ConfigError::Resource(format!(
                    "RLIMIT_MEMLOCK is {} MiB, config needs {} MiB (raise with `ulimit -l` or systemd LimitMEMLOCK)",
                    l / 1024 / 1024,
                    self.resources.memlock_mib
                )));
            }
        }
        if self.resources.memory_budget_mib > 0
            && let Some(phys) = physical_memory_bytes()
            && phys < self.resources.memory_budget_mib.saturating_mul(1024 * 1024)
        {
            return Err(ConfigError::Resource(format!(
                "memory budget {} MiB exceeds physical memory {} MiB",
                self.resources.memory_budget_mib,
                phys / 1024 / 1024
            )));
        }
        for path in [
            &self.server.tls_cert,
            &self.server.tls_key,
            &self.server.tls_client_ca,
        ]
        .into_iter()
        .flatten()
        {
            if !path.is_file() {
                return Err(ConfigError::Resource(format!(
                    "TLS file {} not found",
                    path.display()
                )));
            }
        }
        for (name, dir) in [
            ("wal_dir", &self.storage.wal_dir),
            ("checkpoint_dir", &self.storage.checkpoint_dir),
        ] {
            let Some(dir) = dir else { continue };
            if create_dirs {
                std::fs::create_dir_all(dir)
                    .map_err(|e| ConfigError::Resource(format!("{name} {}: {e}", dir.display())))?;
            } else if !dir.exists() {
                // dry run: the nearest existing ancestor must be a writable directory
                let mut p = dir.as_path();
                while let Some(parent) = p.parent() {
                    if parent.exists() {
                        if !parent.is_dir() {
                            return Err(ConfigError::Resource(format!(
                                "{name} {}: {} is not a directory",
                                dir.display(),
                                parent.display()
                            )));
                        }
                        break;
                    }
                    p = parent;
                }
            } else if !dir.is_dir() {
                return Err(ConfigError::Resource(format!(
                    "{name} {} exists and is not a directory",
                    dir.display()
                )));
            }
        }
        Ok(())
    }
}

/// Physical memory in bytes (`None` when unknown).
#[must_use]
pub fn physical_memory_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        // SAFETY: sysconf has no preconditions; negative results mean "unknown".
        let (pages, size) = unsafe {
            (
                libc::sysconf(libc::_SC_PHYS_PAGES),
                libc::sysconf(libc::_SC_PAGESIZE),
            )
        };
        if pages > 0 && size > 0 {
            return u64::try_from(pages)
                .ok()?
                .checked_mul(u64::try_from(size).ok()?);
        }
    }
    #[allow(unreachable_code)]
    None
}

/// Current soft `RLIMIT_MEMLOCK` in bytes (`None` = unlimited or unknown platform).
#[must_use]
pub fn memlock_limit_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes into the provided struct; no other preconditions.
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &raw mut lim) };
        if rc != 0 || lim.rlim_cur == libc::RLIM_INFINITY {
            return None;
        }
        #[allow(clippy::unnecessary_cast)] // rlim_t is u32 on some targets
        return Some(lim.rlim_cur as u64);
    }
    #[allow(unreachable_code)]
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_round_trip_and_validate() {
        let c = Config::default();
        c.validate().unwrap();
        let text = c.to_toml();
        let mut back = Config::from_toml(&text).unwrap();
        // the environment may hold ANKAA_TOKEN; secrets are not part of the round trip
        back.server.token = None;
        back.server.metrics_token = None;
        assert_eq!(back, c);
    }

    #[test]
    fn rejects_bad_settings() {
        assert!(matches!(
            Config::from_toml("[feed]\nurl = \"ftp://x\"").unwrap_err(),
            ConfigError::Invalid(_)
        ));
        assert!(matches!(
            Config::from_toml("[feed]\nstreams = []").unwrap_err(),
            ConfigError::Invalid(_)
        ));
        assert!(matches!(
            Config::from_toml("[storage]\nsnapshot_ms = 1").unwrap_err(),
            ConfigError::Invalid(_)
        ));
        assert!(matches!(
            Config::from_toml("[server]\ntls_cert = \"a.pem\"").unwrap_err(),
            ConfigError::Invalid(_)
        ));
        assert!(matches!(
            Config::from_toml("[nope]\nx = 1").unwrap_err(),
            ConfigError::Parse(_)
        ));
        assert!(matches!(
            Config::from_toml(
                "[server]\nflight_addr = \"0.0.0.0:50051\"\ntoken_env = \"ANKAA_TEST_UNSET_TOKEN\""
            )
            .unwrap_err(),
            ConfigError::Invalid(_)
        ));
    }

    #[test]
    fn token_resolution_and_redaction() {
        let mut c = Config::parse_toml(
            "[server]\nflight_addr = \"0.0.0.0:50051\"\ntoken_env = \"ANKAA_TEST_UNSET_TOKEN_2\"",
        )
        .unwrap();
        c.resolve_secrets();
        assert!(c.validate().is_err());
        c.server.token = Some(Secret::new("s3cret-0123456789abcdef".into()));
        c.server.allow_plaintext = true;
        c.validate().unwrap();
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("s3cret"), "{dbg}");
        assert!(!c.to_toml().contains("s3cret"));
    }

    #[test]
    fn memory_budget_and_tls_files() {
        let mut c = Config::default();
        c.resources.memory_budget_mib = u64::MAX / 1024 / 1024 / 4;
        if physical_memory_bytes().is_some() {
            assert!(c.check_resources().is_err());
        }
        let mut c = Config::default();
        c.server.tls_cert = Some("/nonexistent/cert.pem".into());
        c.server.tls_key = Some("/nonexistent/key.pem".into());
        assert!(matches!(
            c.check_resources().unwrap_err(),
            ConfigError::Resource(_)
        ));
    }

    #[test]
    fn user_tables_sources_continuous_and_replication_parse_and_validate() {
        let text = r#"
[server]
flight_addr = "127.0.0.1:50051"
[storage]
wal_dir = "/tmp/x"
[[tables]]
name = "telemetry"
partition_by = "rack"
columns = [ { name = "ts", type = "timestamp_ns" }, { name = "rack", type = "dict" }, { name = "temp", type = "fixed8" } ]
[[sources]]
name = "racks"
kind = "nats"
url = "nats://127.0.0.1:4222"
subject = "racks.>"
format = "json"
table = "telemetry"
map = { ts = "t", rack = "id", temp = "temp" }
ts_unit = "millis"
[[continuous]]
name = "temp_1m"
kind = "ohlcv"
table = "telemetry"
key = "rack"
value = "temp"
width_ns = 60000000000
[replication]
standby_of = "10.0.0.1:50051"
"#;
        let c = Config::from_toml(text).unwrap();
        assert_eq!(c.tables.len(), 1);
        assert_eq!(c.sources[0].kind, SourceKind::Nats);
        assert_eq!(c.continuous[0].kind, ContinuousKind::Ohlcv);
        assert_eq!(c.replication.standby_of.as_deref(), Some("10.0.0.1:50051"));
        // invalid variants
        for (bad, why) in [
            (
                text.replace("type = \"timestamp_ns\"", "type = \"int64\""),
                "first column",
            ),
            (
                text.replace(
                    "name = \"telemetry\"\npartition_by",
                    "name = \"trades\"\npartition_by",
                ),
                "reserved",
            ),
            (
                text.replace("map = { ts = \"t\", rack", "map = { rack"),
                "timestamp mapping",
            ),
            (
                text.replace("width_ns = 60000000000", "width_ns = 0"),
                "width",
            ),
            (
                text.replace("table = \"telemetry\"\nmap", "table = \"nope\"\nmap"),
                "unknown table",
            ),
            (
                text.replace("wal_dir = \"/tmp/x\"\n", ""),
                "standby needs wal",
            ),
            (
                text.replace(
                    "map = { ts = \"t\", rack = \"id\", temp = \"temp\" }",
                    "map = { ts = \"t\", rack = \"id\" }",
                ),
                "unmapped column",
            ),
            (
                text.replace("format = \"json\"", "format = \"json\"\ngroup = \"g1\""),
                "consumer group",
            ),
            (
                text.replace("name = \"racks\"", "name = \"rack s\""),
                "source name",
            ),
            (
                text.replace(
                    "name = \"telemetry\"\npartition_by",
                    "name = \"dictionaries\"\npartition_by",
                ),
                "reserved dictionaries",
            ),
            (
                text.replace(
                    "ts_unit = \"millis\"",
                    "ts_unit = \"millis\"\nmax_message_bytes = 1",
                ),
                "message size",
            ),
            (
                text.replace("[replication]", "[replication]\nmax_standbys = 0"),
                "max standbys",
            ),
        ] {
            assert!(Config::from_toml(&bad).is_err(), "{why}");
        }
    }

    #[test]
    fn beyond_loopback_needs_tls_and_a_real_token() {
        let base = "[server]\nflight_addr = \"0.0.0.0:50051\"";
        let mut c = Config::parse_toml(base).unwrap();
        c.server.token = Some(Secret::new("0123456789abcdef0123".into()));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("without TLS"), "{err}");
        c.server.allow_plaintext = true;
        c.validate().unwrap();
        c.server.token = Some(Secret::new("short".into()));
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("shorter than"), "{err}");
        // TLS configured: no opt-out needed
        let mut c = Config::parse_toml(base).unwrap();
        c.server.token = Some(Secret::new("0123456789abcdef0123".into()));
        c.server.tls_cert = Some("c.pem".into());
        c.server.tls_key = Some("k.pem".into());
        c.validate().unwrap();
        // loopback stays convenient for development
        let mut c = Config::default();
        c.server.token = Some(Secret::new("dev".into()));
        c.validate().unwrap();
        // stream names are restricted to what a symbol can be
        assert!(Config::from_toml("[feed]\nstreams = [\"btc&x=1@trade\"]").is_err());
        // broker credentials never reach Debug output
        let s = SourceConfig {
            name: "n".into(),
            kind: SourceKind::Nats,
            url: "nats://user:s3cret@broker:4222".into(),
            subject: "x".into(),
            group: None,
            start: SourceStart::Latest,
            max_message_bytes: 1024,
            format: SourceFormat::Json,
            table: "t".into(),
            map: std::collections::BTreeMap::new(),
            ts_unit: TimeUnit::Nanos,
            ts_from_receive: true,
            rows: None,
            tls: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            sasl_mechanism: Some(SaslMechanism::Plain),
            sasl_username: Some("key".into()),
            sasl_password_env: Some("KAFKA_PASSWORD".into()),
            sasl_password: Some(Secret::new("p4ssw0rd".into())),
            allow_plaintext: false,
            sparkplug_rebirth: false,
        };
        let shown = format!("{s:?}");
        assert!(!shown.contains("s3cret") && !shown.contains("p4ssw0rd"));
        assert!(shown.contains("KAFKA_PASSWORD") && shown.contains("***"));
    }

    /// A config with one Kafka source: `url`, and `extra` lines appended to the source.
    fn kafka_cfg(url: &str, extra: &str) -> String {
        format!(
            r#"
[feed]
enabled = false
streams = []
[[tables]]
name = "t"
columns = [{{ name = "ts", type = "timestamp_ns" }}, {{ name = "v", type = "float64" }}]
[[sources]]
name = "events"
kind = "kafka"
url = "{url}"
subject = "events"
format = "json"
table = "t"
map = {{ ts = "t", v = "v" }}
{extra}
"#
        )
    }

    /// Parse and validate with the SASL password of every source set to `password` (the tests
    /// do not touch the process environment).
    fn kafka_check(url: &str, extra: &str, password: Option<&str>) -> Result<Config, String> {
        let mut c = Config::parse_toml(&kafka_cfg(url, extra)).map_err(|e| e.to_string())?;
        for s in &mut c.sources {
            s.sasl_password = password.map(|p| Secret::new(p.to_owned()));
        }
        c.validate().map_err(|e| e.to_string())?;
        Ok(c)
    }

    #[test]
    fn kafka_tls_settings() {
        let ok = |url: &str, extra: &str| kafka_check(url, extra, None).unwrap();
        let err = |url: &str, extra: &str| kafka_check(url, extra, None).unwrap_err();
        // plaintext stays the default; TLS is asked for explicitly
        assert!(!ok("broker:9092", "").sources[0].uses_tls());
        assert!(ok("broker:9093", "tls = true").sources[0].uses_tls());
        ok("b1:9093,b2:9093", "tls = true\ntls_ca = \"ca.pem\"");
        ok(
            "broker:9093",
            "tls = true\ntls_ca = \"ca.pem\"\ntls_cert = \"c.pem\"\ntls_key = \"k.pem\"",
        );
        // certificate files do not switch TLS on by themselves
        let e = err("broker:9092", "tls_ca = \"ca.pem\"");
        assert!(
            e.contains("sources.events") && e.contains("need tls = true"),
            "{e}"
        );
        assert!(
            err("broker:9092", "tls = false\ntls_cert = \"c.pem\"").contains("need tls = true")
        );
        assert!(err("broker:9093", "tls = true\ntls_cert = \"c.pem\"").contains("together"));
        assert!(err("broker:9093", "tls = true\ntls_key = \"k.pem\"").contains("together"));
        assert!(err("[::1]:9093", "tls = true").contains("IPv6"));
        ok("[::1]:9092", "");
        // `tls` is the Kafka switch: MQTT has its url scheme
        let mqtt = kafka_cfg("mqtt://broker", "tls = true").replace("kafka", "mqtt");
        let e = Config::from_toml(&mqtt).unwrap_err().to_string();
        assert!(e.contains("mqtts://"), "{e}");
        assert!(
            Config::from_toml(&kafka_cfg("broker:9092", "tls = \"yes\""))
                .unwrap_err()
                .to_string()
                .contains("parse error")
        );
    }

    #[test]
    fn kafka_sasl_settings() {
        let sasl = |mechanism: &str| {
            format!(
                "sasl_mechanism = \"{mechanism}\"\nsasl_username = \"key\"\nsasl_password_env = \"ANKAA_TEST_KAFKA_PW\""
            )
        };
        let tls_sasl = |mechanism: &str| format!("tls = true\n{}", sasl(mechanism));
        let pw = Some("p4ssw0rd-from-env");
        for (text, want) in [
            ("plain", SaslMechanism::Plain),
            ("PLAIN", SaslMechanism::Plain),
            ("scram-sha-256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA-256", SaslMechanism::ScramSha256),
            ("scram-sha-512", SaslMechanism::ScramSha512),
            ("SCRAM-SHA-512", SaslMechanism::ScramSha512),
        ] {
            let c = kafka_check("broker:9093", &tls_sasl(text), pw).unwrap();
            assert_eq!(c.sources[0].sasl_mechanism, Some(want));
            assert_eq!(want.name(), text.to_ascii_uppercase());
        }
        // mechanisms the client library does not have are not accepted
        for unsupported in ["oauthbearer", "gssapi", "aws_msk_iam", "scram-sha-1"] {
            let e = kafka_check("broker:9093", &tls_sasl(unsupported), pw).unwrap_err();
            assert!(
                e.contains("parse error") && e.contains("scram-sha-512"),
                "{e}"
            );
        }
        // the three keys go together
        for partial in [
            "sasl_mechanism = \"plain\"",
            "sasl_username = \"key\"",
            "sasl_password_env = \"ANKAA_TEST_KAFKA_PW\"",
            "sasl_mechanism = \"plain\"\nsasl_username = \"key\"",
        ] {
            let e = kafka_check("broker:9093", &format!("tls = true\n{partial}"), pw).unwrap_err();
            assert!(e.contains("must be set together"), "{e}");
        }
        let empty_user = tls_sasl("plain").replace("\"key\"", "\"\"");
        assert!(
            kafka_check("broker:9093", &empty_user, pw)
                .unwrap_err()
                .contains("sasl_username is empty")
        );
        // a password variable that is not set stops the start, with the source and the variable
        let e = kafka_check("broker:9093", &tls_sasl("plain"), None).unwrap_err();
        assert!(
            e.contains("sources.events")
                && e.contains("ANKAA_TEST_KAFKA_PW")
                && e.contains("unset or empty"),
            "{e}"
        );
        // without TLS: loopback brokers only, unless allowed explicitly
        for local in [
            "127.0.0.1:9092",
            "localhost:9092",
            "LOCALHOST:9092, 127.0.0.2:9093",
            "[::1]:9092",
        ] {
            kafka_check(local, &sasl("scram-sha-512"), pw).unwrap();
        }
        for remote in [
            "broker:9092",
            "10.0.0.5:9092",
            "127.0.0.1:9092,broker:9092",
            "localhost.example.com:9092",
            "[2001:db8::1]:9092",
        ] {
            let e = kafka_check(remote, &sasl("scram-sha-512"), pw).unwrap_err();
            assert!(
                e.contains("sources.events") && e.contains("SASL without TLS"),
                "{remote}: {e}"
            );
            kafka_check(
                remote,
                &format!("{}\nallow_plaintext = true", sasl("plain")),
                pw,
            )
            .unwrap();
        }
        // allow_plaintext is not a general switch
        assert!(
            kafka_check(
                "broker:9093",
                &format!("{}\nallow_plaintext = true", tls_sasl("plain")),
                pw
            )
            .unwrap_err()
            .contains("allow_plaintext only applies")
        );
        assert!(
            kafka_check("broker:9092", "allow_plaintext = true", None)
                .unwrap_err()
                .contains("allow_plaintext only applies")
        );
        // SASL is a Kafka setting
        let nats = kafka_cfg("nats://broker:4222", &sasl("plain")).replace("kafka", "nats");
        let e = Config::from_toml(&nats).unwrap_err().to_string();
        assert!(e.contains("are for Kafka sources"), "{e}");
    }

    /// The password comes from the environment, is never written by `to_toml` and never shown.
    #[test]
    fn kafka_sasl_password_is_resolved_from_the_environment_and_never_printed() {
        // PATH is set in every test environment: its value stands in for a password
        let Ok(path) = std::env::var("PATH") else {
            return;
        };
        let extra = "tls = true\nsasl_mechanism = \"scram-sha-256\"\nsasl_username = \"key\"\nsasl_password_env = \"PATH\"";
        let c = Config::from_toml(&kafka_cfg("broker:9093", extra)).unwrap();
        let s = &c.sources[0];
        assert_eq!(s.sasl_password.as_ref().map(Secret::expose), Some(&*path));
        let (toml, debug) = (c.to_toml(), format!("{c:?}"));
        assert!(toml.contains("sasl_password_env = \"PATH\""));
        assert!(!toml.contains("sasl_password ="), "{toml}");
        assert!(!toml.contains(&path) && !debug.contains(&path));
        let unset = extra.replace("\"PATH\"", "\"ANKAA_TEST_UNSET_KAFKA_PASSWORD\"");
        let e = Config::from_toml(&kafka_cfg("broker:9093", &unset))
            .unwrap_err()
            .to_string();
        assert!(e.contains("ANKAA_TEST_UNSET_KAFKA_PASSWORD"), "{e}");
    }

    #[test]
    fn mqtt_tls_settings() {
        let cfg = |url: &str, tls: &str| {
            format!(
                r#"
[feed]
enabled = false
streams = []
[[tables]]
name = "t"
columns = [{{ name = "ts", type = "timestamp_ns" }}, {{ name = "v", type = "float64" }}]
[[sources]]
name = "plant"
kind = "mqtt"
url = "{url}"
subject = "a/#"
format = "json"
table = "t"
map = {{ ts = "t", v = "v" }}
{tls}
"#
            )
        };
        Config::from_toml(&cfg("mqtts://broker:8883", "")).unwrap();
        Config::from_toml(&cfg("mqtts://broker", "tls_ca = \"/etc/ankaa/ca.pem\"")).unwrap();
        Config::from_toml(&cfg(
            "mqtts://broker",
            "tls_cert = \"c.pem\"\ntls_key = \"k.pem\"",
        ))
        .unwrap();
        let err = |url: &str, tls: &str| Config::from_toml(&cfg(url, tls)).unwrap_err().to_string();
        assert!(err("mqtt://broker", "tls_ca = \"ca.pem\"").contains("need an mqtts://"));
        assert!(err("mqtts://broker", "tls_cert = \"c.pem\"").contains("together"));
        assert!(err("ssl://broker", "").contains("mqtts://"));
    }

    #[test]
    fn sparkplug_sources() {
        let cfg = |kind: &str, url: &str, format: &str, extra: &str| {
            format!(
                r#"
[feed]
enabled = false
streams = []
[[tables]]
name = "t"
columns = [{{ name = "ts", type = "timestamp_ns" }}, {{ name = "v", type = "float64" }}]
[[sources]]
name = "scada"
kind = "{kind}"
url = "{url}"
subject = "spBv1.0/#"
format = "{format}"
table = "t"
map = {{ ts = "$timestamp", v = "$value" }}
{extra}
"#
            )
        };
        let c = Config::from_toml(&cfg(
            "mqtt",
            "mqtt://b",
            "sparkplug",
            "sparkplug_rebirth = true",
        ))
        .unwrap();
        assert_eq!(c.sources[0].format, SourceFormat::Sparkplug);
        assert!(c.sources[0].sparkplug_rebirth);
        let err = |text: String| Config::from_toml(&text).unwrap_err().to_string();
        assert!(err(cfg("nats", "nats://b", "sparkplug", "")).contains("needs kind = \"mqtt\""));
        assert!(
            err(cfg("mqtt", "mqtt://b", "json", "sparkplug_rebirth = true"))
                .contains("needs format = \"sparkplug\"")
        );
        assert!(err(cfg("mqtt", "mqtt://b", "sparkplug", "rows = \"m\"")).contains("rows"));
    }

    #[test]
    fn feed_can_be_disabled_for_source_only_deployments() {
        let text = r#"
[feed]
enabled = false
streams = []
[[tables]]
name = "t"
columns = [ { name = "ts", type = "timestamp_ns" }, { name = "v", type = "float64" } ]
[[sources]]
name = "f"
kind = "file"
url = "/tmp/x.ndjson"
format = "json"
table = "t"
map = { ts = "t", v = "v" }
"#;
        let c = Config::from_toml(text).unwrap();
        assert!(!c.feed.enabled);
        assert_eq!(c.sources[0].max_message_bytes, 1024 * 1024);
        assert_eq!(c.sources[0].start, SourceStart::Latest);
        assert_eq!(c.tables[0].max_dict_entries, 100_000);
        // no feed and no source: a server that only serves what it has stored (warned at start)
        assert!(Config::from_toml("[feed]\nenabled = false\nstreams = []").is_ok());
        // an empty stream list still needs the feed switched off
        assert!(Config::from_toml("[feed]\nstreams = []").is_err());
        // the round trip keeps the new fields
        let again = Config::from_toml(&c.to_toml()).unwrap();
        assert_eq!(again.sources, c.sources);
    }

    #[test]
    fn serving_the_wal_is_opt_in_and_its_token_must_resolve() {
        let c = Config::default();
        assert!(!c.replication.serve_wal);
        assert!(
            Config::from_toml("[replication]\nserve_wal = true").is_err(),
            "needs wal_dir"
        );
        let ok = "[storage]\nwal_dir = \"/tmp/x\"\n[replication]\nserve_wal = true";
        Config::from_toml(ok).unwrap();
        let unset = format!("{ok}\ntoken_env = \"ANKAA_TEST_UNSET_REPL_TOKEN\"");
        assert!(Config::from_toml(&unset).is_err());
        let mut c = Config::parse_toml(&unset).unwrap();
        c.replication.token = Some(Secret::new("r3pl".into()));
        c.validate().unwrap();
        assert!(!format!("{c:?}").contains("r3pl"));
        assert!(!c.to_toml().contains("r3pl"));
    }

    #[test]
    fn retention_durations() {
        assert_eq!(parse_duration_ns("45s"), Some(45_000_000_000));
        assert_eq!(parse_duration_ns("90d"), Some(90 * 86_400 * 1_000_000_000));
        assert_eq!(parse_duration_ns(" 2w "), Some(14 * 86_400 * 1_000_000_000));
        for bad in [
            "",
            "d",
            "0d",
            "-1d",
            "1.5h",
            "10",
            "3y",
            "99999999999999999w",
        ] {
            assert_eq!(parse_duration_ns(bad), None, "{bad}");
        }
        let table = |r: &str| {
            format!(
                "[[tables]]\nname = \"m\"\nretention = \"{r}\"\ncolumns = [{{ name = \"ts\", type = \"timestamp_ns\" }}]"
            )
        };
        Config::from_toml(&table("30d")).unwrap();
        assert!(Config::from_toml(&table("30 days")).is_err());
        Config::from_toml("[storage]\nmarket_retention = \"7d\"").unwrap();
        assert!(Config::from_toml("[storage]\nmarket_retention = \"week\"").is_err());
    }

    #[test]
    fn memlock_check() {
        let mut c = Config::default();
        c.resources.memlock_mib = u64::MAX / 1024 / 1024 / 4; // absurd budget fails when a limit exists
        match memlock_limit_bytes() {
            Some(_) => assert!(matches!(
                c.check_resources().unwrap_err(),
                ConfigError::Resource(_)
            )),
            None => c.check_resources().unwrap(),
        }
    }
}
