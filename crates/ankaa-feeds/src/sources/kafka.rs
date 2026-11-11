//! Kafka topic source (feature `kafka`, pure-Rust `rskafka`). Reads every partition of a
//! topic without a consumer group: the next offset of each partition is kept by this task,
//! reported to the server as `partition=offset,...` and handed back on the next start, so a
//! restart continues where the durable data ends (at least once). A partition without a saved
//! position starts at the latest offset, or at the earliest retained one with
//! [`SourceOpts::start_earliest`]; a partition that appears while the source runs is read from
//! its beginning.
//!
//! **Order across partitions.** A table accepts rows in timestamp order per table partition,
//! and a topic's partitions are independent logs. Delivering one Kafka partition's batch after
//! another would make the second batch look old and have it rejected. Records are therefore
//! merged by their Kafka timestamp: each round delivers, across all partitions, the records up
//! to the oldest "newest timestamp fetched" of the partitions that still have data, and keeps
//! the rest for the next round. During a backlog this interleaves the partitions as they were
//! produced; caught up, it adds at most one fetch round of delay. (Producers whose event time
//! differs wildly from the Kafka timestamp still get rows rejected by the ordering rule.)
//!
//! Positions survive reconnects. When the broker no longer retains a saved offset the source
//! jumps to the earliest retained one and counts the reset.
//!
//! **TLS and SASL** ([`Security`]). [`SourceOpts::tls_enabled`] connects every broker of the
//! cluster with TLS ([`super::tls`]: rustls with the aws-lc-rs provider, the broker certificate
//! checked against the public roots or only a private CA, an optional client certificate). The
//! name checked in a broker's certificate is the host it is reached by: the bootstrap entry
//! first, then the address each broker advertises. [`SourceOpts::sasl`] authenticates every
//! connection with `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`, which is what the client library
//! implements with a user name and a password; `OAUTHBEARER` (it needs a token provider),
//! `GSSAPI` and `AWS_MSK_IAM` are not available. There is no re-authentication of a running
//! connection (KIP-368): a broker that limits the session lifetime closes the connection, and
//! the source connects again and continues from its positions.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use rskafka::client::{ClientBuilder, Credentials, SaslConfig};
use tokio::sync::{mpsc, watch};

use super::{
    Sasl, SaslMechanism, SourceOpts, SourceStats, deliver_wait, oversized, send_state,
    shutdown_wait,
};
use crate::capture::Frame;

/// One fetched record waiting for its turn: `(merge timestamp, offset, value)`.
type Buffered = (i64, i64, Option<Vec<u8>>);

/// Re-read the topic's partition list this often.
const METADATA_REFRESH: Duration = Duration::from_secs(30);
/// A fetch that takes longer than this counts as failed (the client retries forever inside).
const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
/// A partition that returns nothing although the broker says it has more holds the merge
/// back at most this long.
const STALL_GRACE: Duration = Duration::from_secs(2);

/// While no broker of the bootstrap list accepts the connection, say so this often (the client
/// library keeps trying on its own and names the cause in its own log lines).
const NOT_CONNECTED_REMINDER: Duration = Duration::from_secs(30);

/// What a connection needs beyond the address, built once when the source starts: the TLS
/// client configuration and the SASL credentials.
pub struct Security {
    tls: Option<Arc<rustls::ClientConfig>>,
    sasl: Option<SaslConfig>,
    mechanism: Option<SaslMechanism>,
}

impl std::fmt::Debug for Security {
    /// The credentials are left out (the client library's own `Debug` would print the password).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Security")
            .field("tls", &self.tls())
            .field("sasl", &self.sasl())
            .finish()
    }
}

impl Security {
    /// Read the TLS files (when TLS is on) and prepare the credentials. An error names the file
    /// and what is wrong with it.
    pub fn new(opts: &SourceOpts) -> Result<Self, String> {
        let tls = if opts.tls_enabled {
            Some(super::tls::client_config(&opts.tls)?)
        } else {
            None
        };
        Ok(Self {
            tls,
            sasl: opts.sasl.as_ref().map(sasl_config),
            mechanism: opts.sasl.as_ref().map(|s| s.mechanism),
        })
    }

    /// True when connections use TLS.
    #[must_use]
    pub fn tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Name of the SASL mechanism, `none` without SASL (for log lines).
    #[must_use]
    pub fn sasl(&self) -> &'static str {
        self.mechanism.map_or("none", SaslMechanism::name)
    }

    /// A client builder for `servers` with these settings. Every connection the client opens
    /// (bootstrap, then one per partition leader) uses them.
    fn builder(&self, servers: Vec<String>) -> ClientBuilder {
        let mut builder = ClientBuilder::new(servers);
        if let Some(tls) = &self.tls {
            builder = builder.tls_config(Arc::clone(tls));
        }
        if let Some(sasl) = &self.sasl {
            builder = builder.sasl_config(sasl.clone());
        }
        builder
    }
}

/// The client library's form of the credentials.
fn sasl_config(sasl: &Sasl) -> SaslConfig {
    let credentials = Credentials::new(sasl.username.clone(), sasl.password.clone());
    match sasl.mechanism {
        SaslMechanism::Plain => SaslConfig::Plain(credentials),
        SaslMechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
        SaslMechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
    }
}

/// Parse `0=12,1=40` into partition → next offset.
fn parse_state(state: &str) -> BTreeMap<i32, i64> {
    state
        .split(',')
        .filter_map(|kv| {
            let (p, o) = kv.split_once('=')?;
            let o: i64 = o.trim().parse().ok()?;
            (o >= 0).then_some((p.trim().parse().ok()?, o))
        })
        .collect()
}

fn format_state(offsets: &BTreeMap<i32, i64>) -> String {
    offsets
        .iter()
        .map(|(p, o)| format!("{p}={o}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Merge state of the partitions of one topic.
#[derive(Debug, Default)]
struct Merge {
    /// Fetched, not yet delivered, per partition, in offset order.
    buffers: BTreeMap<i32, VecDeque<Buffered>>,
    /// Highest merge timestamp fetched so far per partition (merge timestamps never go back
    /// within a partition, so offset order is kept).
    newest: BTreeMap<i32, i64>,
    /// Partitions whose last fetch reached the end of the log.
    caught_up: BTreeSet<i32>,
}

impl Merge {
    fn push(&mut self, p: i32, ts: i64, offset: i64, value: Option<Vec<u8>>) {
        let newest = self.newest.entry(p).or_insert(i64::MIN);
        *newest = (*newest).max(ts);
        self.buffers
            .entry(p)
            .or_default()
            .push_back((*newest, offset, value));
    }

    fn buffered(&self, p: i32) -> usize {
        self.buffers.get(&p).map_or(0, VecDeque::len)
    }

    /// Timestamp up to which records may be delivered now: the oldest "newest fetched" among
    /// the partitions that may still produce older records. `stalled` are partitions to ignore
    /// although they are neither buffered nor caught up.
    fn bound(&self, partitions: &[i32], stalled: &BTreeSet<i32>) -> i64 {
        let mut bound = i64::MAX;
        for p in partitions {
            match self.buffers.get(p).and_then(VecDeque::back) {
                Some((ts, _, _)) => bound = bound.min(*ts),
                None if self.caught_up.contains(p) || stalled.contains(p) => {}
                // nothing fetched yet and not at the end: its records could be older than
                // anything buffered elsewhere
                None => return i64::MIN,
            }
        }
        bound
    }

    /// Next record to deliver: the oldest front across partitions, if it is within `bound`.
    fn pop(&mut self, bound: i64) -> Option<(i32, Buffered)> {
        let p = self
            .buffers
            .iter()
            .filter_map(|(p, b)| b.front().map(|(ts, _, _)| (*ts, *p)))
            .min()
            .filter(|(ts, _)| *ts <= bound)?
            .1;
        self.buffers.get_mut(&p)?.pop_front().map(|r| (p, r))
    }

    fn clear(&mut self) {
        self.buffers.clear();
        self.newest.clear();
        self.caught_up.clear();
    }
}

/// Report the final position (after the frames already delivered) without racing shutdown.
async fn final_state(
    tx: &mpsc::Sender<Frame>,
    source_id: u16,
    delivered: &BTreeMap<i32, i64>,
    reported: &str,
) {
    let state = format_state(delivered);
    if state != reported && !state.is_empty() {
        super::send_state_bounded(tx, source_id, &state).await;
    }
}

/// Consume `topic` from `bootstrap` servers until shutdown.
#[allow(clippy::too_many_lines)]
pub async fn run(
    source_id: u16,
    bootstrap: &str,
    topic: &str,
    opts: &SourceOpts,
    tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
) -> SourceStats {
    use rskafka::client::error::{Error as KafkaError, ProtocolError};
    use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};

    let mut stats = SourceStats::default();
    let security = match Security::new(opts) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(source_id, %bootstrap, error = %e, "kafka source not started: TLS configuration");
            return stats;
        }
    };
    let (tls, sasl) = (security.tls(), security.sasl());
    let servers: Vec<String> = bootstrap
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    // next offset to deliver per partition: this is the position that is saved
    let mut delivered: BTreeMap<i32, i64> =
        opts.resume.as_deref().map(parse_state).unwrap_or_default();
    if !delivered.is_empty() {
        tracing::info!(source_id, %topic, positions = %format_state(&delivered), "kafka resumes from saved positions");
    }
    // after the first successful start, a partition we have no position for is a new one
    let mut started_once = !delivered.is_empty();
    let fetch_bytes = i32::try_from(opts.max_message_bytes.saturating_mul(4))
        .unwrap_or(i32::MAX)
        .max(1_048_576);
    let mut backoff = Duration::from_millis(500);
    let mut reported = format_state(&delivered);
    let mut merge = Merge::default();
    'connect: loop {
        // every failure path lands here: wait (interruptibly) before the next attempt
        macro_rules! retry {
            () => {{
                tokio::select! {
                    () = shutdown_wait(&mut shutdown) => {
                        final_state(&tx, source_id, &delivered, &reported).await;
                        return stats;
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue 'connect;
            }};
        }
        if *shutdown.borrow() {
            final_state(&tx, source_id, &delivered, &reported).await;
            return stats;
        }
        // what was fetched but not delivered is fetched again
        merge.clear();
        // the client library retries inside `build` for as long as no broker accepts the
        // connection (unreachable, certificate not trusted, credentials refused): stay
        // responsive to shutdown and keep saying which source is waiting
        let connecting = security.builder(servers.clone()).build();
        tokio::pin!(connecting);
        let connected = loop {
            tokio::select! {
                r = &mut connecting => break r,
                () = shutdown_wait(&mut shutdown) => {
                    final_state(&tx, source_id, &delivered, &reported).await;
                    return stats;
                }
                () = tokio::time::sleep(NOT_CONNECTED_REMINDER) => {
                    tracing::warn!(source_id, %bootstrap, tls, sasl,
                        "kafka: no broker connection yet; still trying (the cause is in the \"Failed to connect to broker\" lines)");
                }
            }
        };
        let client = match connected {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(source_id, %bootstrap, tls, sasl, error = %e, "kafka connect failed; retrying");
                retry!()
            }
        };
        stats.connects += 1;
        let partitions = match client.list_topics().await {
            Ok(topics) => topics
                .into_iter()
                .find(|t| t.name == topic)
                .map(|t| t.partitions)
                .unwrap_or_default(),
            Err(e) => {
                tracing::warn!(source_id, %topic, error = %e, "kafka topic lookup failed; retrying");
                retry!()
            }
        };
        if partitions.is_empty() {
            tracing::warn!(source_id, %topic, "kafka topic has no partitions (or does not exist); retrying");
            retry!()
        }
        let mut clients = Vec::new();
        for p in partitions.iter().copied() {
            let pc = match client
                .partition_client(topic.to_owned(), p, UnknownTopicHandling::Retry)
                .await
            {
                Ok(pc) => pc,
                Err(e) => {
                    // never run with a subset: a silently missing partition is silent data loss
                    tracing::warn!(source_id, %topic, partition = p, error = %e, "kafka partition client failed; retrying");
                    retry!()
                }
            };
            if let std::collections::btree_map::Entry::Vacant(slot) = delivered.entry(p) {
                let at = if opts.start_earliest || started_once {
                    OffsetAt::Earliest
                } else {
                    OffsetAt::Latest
                };
                match pc.get_offset(at).await {
                    Ok(o) => {
                        slot.insert(o);
                    }
                    Err(e) => {
                        tracing::warn!(source_id, %topic, partition = p, error = %e, "kafka offset lookup failed; retrying");
                        retry!()
                    }
                }
            }
            clients.push((p, pc));
        }
        started_once = true;
        let ids: Vec<i32> = clients.iter().map(|(p, _)| *p).collect();
        // next offset to fetch (ahead of `delivered` by what is buffered)
        let mut fetch_next: BTreeMap<i32, i64> = delivered.clone();
        tracing::info!(source_id, %topic, partitions = clients.len(), tls, sasl, positions = %format_state(&delivered), "kafka consuming");
        let mut last_report = tokio::time::Instant::now();
        let mut last_metadata = tokio::time::Instant::now();
        let mut stalled_since: BTreeMap<i32, tokio::time::Instant> = BTreeMap::new();
        loop {
            if *shutdown.borrow() {
                final_state(&tx, source_id, &delivered, &reported).await;
                return stats;
            }
            // 1. fetch every partition that has room, all at once
            // while some partition has a backlog, the ones at their end are only polled (no
            // long wait): otherwise every round would last as long as the idle partition's wait
            let backlog = ids.iter().any(|p| !merge.caught_up.contains(p));
            let fetches = clients
                .iter()
                .filter(|(p, _)| merge.buffered(*p) < 4096)
                .map(|(p, pc)| {
                    let wait_ms = if backlog && merge.caught_up.contains(p) {
                        0
                    } else {
                        250
                    };
                    let offset = fetch_next.get(p).copied().unwrap_or(0);
                    async move {
                        let r = tokio::time::timeout(
                            FETCH_TIMEOUT,
                            pc.fetch_records(offset, 1..fetch_bytes, wait_ms),
                        )
                        .await;
                        (*p, r)
                    }
                });
            let results = tokio::select! {
                () = shutdown_wait(&mut shutdown) => {
                    final_state(&tx, source_id, &delivered, &reported).await;
                    return stats;
                }
                r = futures::future::join_all(fetches) => r,
            };
            let mut fetched_any = false;
            let mut failed = false;
            for (p, res) in results {
                match res {
                    Ok(Ok((records, high_watermark))) => {
                        let mut next = fetch_next.get(&p).copied().unwrap_or(0);
                        for r in records {
                            next = r.offset + 1;
                            let ts = r.record.timestamp.timestamp_millis();
                            merge.push(p, ts, r.offset, r.record.value);
                            fetched_any = true;
                        }
                        fetch_next.insert(p, next);
                        if next >= high_watermark {
                            merge.caught_up.insert(p);
                        } else {
                            merge.caught_up.remove(&p);
                        }
                    }
                    Ok(Err(KafkaError::ServerError {
                        protocol_error: ProtocolError::OffsetOutOfRange,
                        ..
                    })) => {
                        // the saved position fell out of retention (or the topic was recreated)
                        let (_, pc) =
                            &clients[clients.iter().position(|(q, _)| *q == p).unwrap_or(0)];
                        match pc.get_offset(OffsetAt::Earliest).await {
                            Ok(o) => {
                                tracing::warn!(source_id, %topic, partition = p, was = fetch_next.get(&p).copied().unwrap_or(-1), now = o,
                                    "kafka position is no longer retained; continuing from the earliest retained offset (messages in between are lost)");
                                crate::counters::note_kafka_offset_reset();
                                fetch_next.insert(p, o);
                                if merge.buffered(p) == 0 {
                                    delivered.insert(p, o);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(source_id, %topic, partition = p, error = %e, "kafka offset lookup failed");
                                failed = true;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(source_id, %topic, partition = p, error = %e, "kafka fetch failed; reconnecting");
                        failed = true;
                    }
                    Err(_) => {
                        tracing::warn!(source_id, %topic, partition = p, "kafka fetch timed out; reconnecting");
                        failed = true;
                    }
                }
            }
            // 2. deliver, across partitions, everything up to the merge bound
            let now = tokio::time::Instant::now();
            let mut stalled = BTreeSet::new();
            for p in &ids {
                if merge.buffered(*p) == 0 && !merge.caught_up.contains(p) {
                    let since = *stalled_since.entry(*p).or_insert(now);
                    if now.duration_since(since) >= STALL_GRACE {
                        stalled.insert(*p);
                    }
                } else {
                    stalled_since.remove(p);
                }
            }
            let mut delivered_any = false;
            loop {
                let bound = merge.bound(&ids, &stalled);
                let mut progressed = false;
                while let Some((p, (_, offset, value))) = merge.pop(bound) {
                    if let Some(v) = &value
                        && !oversized(v.len(), opts, &mut stats)
                    {
                        // Kafka keeps the log: wait for space instead of dropping
                        if !deliver_wait(&tx, source_id, v, &mut stats, &mut shutdown).await {
                            // not delivered: its offset is not counted
                            final_state(&tx, source_id, &delivered, &reported).await;
                            return stats;
                        }
                    }
                    delivered.insert(p, offset + 1);
                    progressed = true;
                }
                // a partition that just emptied and is caught up no longer holds the others back
                if !progressed {
                    break;
                }
                delivered_any = true;
            }
            if fetched_any || delivered_any {
                backoff = Duration::from_millis(500);
            }
            // 3. report the position about once a second when it moved, and before reconnecting
            if failed || last_report.elapsed() >= Duration::from_secs(1) {
                let position = format_state(&delivered);
                if position != reported {
                    if !send_state(&tx, source_id, &position, &mut shutdown).await {
                        // shutdown won the race against the report: one bounded last attempt
                        final_state(&tx, source_id, &delivered, &reported).await;
                        return stats;
                    }
                    reported = position;
                }
                last_report = tokio::time::Instant::now();
            }
            if failed {
                retry!()
            }
            // 4. partitions added to the topic are picked up by a fresh start
            if last_metadata.elapsed() >= METADATA_REFRESH {
                last_metadata = tokio::time::Instant::now();
                if let Ok(topics) = client.list_topics().await
                    && let Some(t) = topics.into_iter().find(|t| t.name == topic)
                    && t.partitions.len() != ids.len()
                {
                    tracing::info!(source_id, %topic, was = ids.len(), now = t.partitions.len(), "kafka topic partitions changed; restarting the consumer");
                    continue 'connect;
                }
            }
            if !fetched_any && !delivered_any {
                tokio::select! {
                    () = shutdown_wait(&mut shutdown) => {
                        final_state(&tx, source_id, &delivered, &reported).await;
                        return stats;
                    }
                    () = tokio::time::sleep(Duration::from_millis(20)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_round_trip() {
        let m = parse_state("0=12, 3=40,1=0");
        assert_eq!(m.len(), 3);
        assert_eq!(m[&3], 40);
        assert_eq!(format_state(&m), "0=12,1=0,3=40");
        assert_eq!(parse_state("").len(), 0);
        assert_eq!(parse_state("x=1,2=y,-1,4=-9").len(), 0);
    }

    fn sasl(mechanism: SaslMechanism) -> Sasl {
        Sasl {
            mechanism,
            username: "api-key".into(),
            password: "s3cret-p4ss".into(),
        }
    }

    /// Each mechanism reaches the client library as itself, with the credentials unchanged.
    #[test]
    fn sasl_mechanisms_map_to_the_client() {
        for m in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
        ] {
            let credentials = match (m, sasl_config(&sasl(m))) {
                (SaslMechanism::Plain, SaslConfig::Plain(c))
                | (SaslMechanism::ScramSha256, SaslConfig::ScramSha256(c))
                | (SaslMechanism::ScramSha512, SaslConfig::ScramSha512(c)) => c,
                (m, other) => panic!("{m:?} became {other:?}"),
            };
            assert_eq!(credentials.username, "api-key");
            assert_eq!(credentials.password, "s3cret-p4ss");
        }
        assert_eq!(SaslMechanism::Plain.name(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.name(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.name(), "SCRAM-SHA-512");
    }

    /// What a connection is built with: nothing by default (plaintext, as before), TLS only
    /// when asked for, SASL with or without it; the password is in no `Debug` output.
    #[test]
    fn connection_security_follows_the_options() {
        use crate::sources::{TlsFiles, Transport};
        let dir = tempfile::tempdir().unwrap();
        let ca = rcgen::generate_simple_self_signed(vec!["broker".into()]).unwrap();
        let ca_pem = dir.path().join("ca.pem");
        std::fs::write(&ca_pem, ca.cert.pem()).unwrap();
        let private_ca = TlsFiles {
            ca: Some(ca_pem),
            ..TlsFiles::default()
        };
        let missing = TlsFiles {
            ca: Some(dir.path().join("missing.pem")),
            ..TlsFiles::default()
        };

        let s = Security::new(&SourceOpts::default()).unwrap();
        assert!(!s.tls() && s.sasl.is_none());
        assert_eq!(s.sasl(), "none");
        // certificate files alone do not switch TLS on (and are not read)
        let s = Security::new(&SourceOpts {
            tls: missing.clone(),
            ..SourceOpts::default()
        })
        .unwrap();
        assert!(!s.tls());
        // TLS with the public roots; TLS with a private CA and SASL
        let s = Security::new(&SourceOpts {
            tls_enabled: true,
            ..SourceOpts::default()
        })
        .unwrap();
        assert!(s.tls() && s.sasl.is_none());
        let opts = SourceOpts {
            tls_enabled: true,
            tls: private_ca,
            sasl: Some(sasl(SaslMechanism::ScramSha512)),
            ..SourceOpts::default()
        };
        let s = Security::new(&opts).unwrap();
        assert!(s.tls() && matches!(s.sasl, Some(SaslConfig::ScramSha512(_))));
        assert_eq!(s.sasl(), "SCRAM-SHA-512");
        let _ = s.builder(vec!["broker:9093".to_owned()]);
        for shown in [format!("{opts:?}"), format!("{s:?}")] {
            assert!(!shown.contains("s3cret-p4ss"), "{shown}");
        }
        assert!(format!("{opts:?}").contains("api-key"));
        // SASL without TLS (a loopback broker)
        let s = Security::new(&SourceOpts {
            sasl: Some(sasl(SaslMechanism::Plain)),
            ..SourceOpts::default()
        })
        .unwrap();
        assert!(!s.tls() && matches!(s.sasl, Some(SaslConfig::Plain(_))));
        assert_eq!(s.sasl(), "PLAIN");
        // a bad file is refused with its name, also by the check that runs before the start
        let bad = SourceOpts {
            tls_enabled: true,
            tls: missing,
            ..SourceOpts::default()
        };
        assert!(Security::new(&bad).unwrap_err().contains("missing.pem"));
        let transport = Transport::Kafka {
            bootstrap: "broker:9093".into(),
            topic: "t".into(),
        };
        assert!(
            crate::sources::check(&transport, &bad)
                .unwrap_err()
                .contains("missing.pem")
        );
        crate::sources::check(&transport, &opts).unwrap();
    }

    /// Two partitions with interleaved timestamps come out in time order, and nothing newer
    /// than what the slower partition has shown is delivered early.
    #[test]
    fn partitions_are_merged_in_time_order() {
        let ids = [0, 1];
        let none = BTreeSet::new();
        let mut m = Merge::default();
        // nothing known about partition 1 yet: nothing may be delivered
        for (offset, ts) in [(0, 10), (1, 20), (2, 30), (3, 40)] {
            m.push(0, ts, offset, None);
        }
        assert_eq!(m.bound(&ids, &none), i64::MIN);
        assert!(m.pop(m.bound(&ids, &none)).is_none());
        // partition 1 has shown records up to 25
        m.push(1, 15, 0, None);
        m.push(1, 25, 1, None);
        let bound = m.bound(&ids, &none);
        assert_eq!(bound, 25);
        let mut out = Vec::new();
        while let Some((p, (ts, off, _))) = m.pop(bound) {
            out.push((ts, p, off));
        }
        assert_eq!(out, vec![(10, 0, 0), (15, 1, 0), (20, 0, 1), (25, 1, 1)]);
        // partition 1 is empty and not known to be at its end: 30 and 40 wait
        assert_eq!(m.bound(&ids, &none), i64::MIN);
        // once it is caught up (or has been silent for the grace period) they flow
        m.caught_up.insert(1);
        let bound = m.bound(&ids, &none);
        assert_eq!(m.pop(bound).map(|(p, r)| (p, r.0)), Some((0, 30)));
        m.caught_up.clear();
        let stalled: BTreeSet<i32> = [1].into_iter().collect();
        assert_eq!(
            m.pop(m.bound(&ids, &stalled)).map(|(p, r)| (p, r.0)),
            Some((0, 40))
        );
        // timestamps that go backwards inside a partition keep their offset order
        let mut m = Merge::default();
        m.push(0, 50, 0, None);
        m.push(0, 45, 1, None);
        let single = [0];
        let bound = m.bound(&single, &none);
        assert_eq!(m.pop(bound).map(|(_, r)| r.1), Some(0));
        assert_eq!(m.pop(bound).map(|(_, r)| r.1), Some(1));
    }
}
