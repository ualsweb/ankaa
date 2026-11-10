# Replication and failover (standby)

A **standby** is an `ankaad` that ingests a primary's write-ahead log over Arrow Flight instead
of a feed, writes the records into its own WAL and tables, and serves read-only queries. It is
warm: queries are answered with the primary's data up to replication lag, and if the primary dies
the standby can be **promoted** by restarting it without the replication setting.

```
primary ──feed──▶ WAL ──▶ tables      standby: Flight DoGet "__wal__:<lsn>" ──▶ WAL ──▶ tables
            ▲                                     │
            └──────── serves the WAL ◀────────────┘   (replication token, TLS optional)
```

## Configure

Primary: serving the WAL is opt-in, because the stream carries every ingested message verbatim.

```toml
[storage]
wal_dir = "/var/lib/ankaa/wal"

[replication]
serve_wal = true                         # serve ticket "__wal__:<from_lsn>" on the Flight endpoint
token_env = "ANKAA_REPLICATION_TOKEN"    # standbys must present this token (recommended)
# max_standbys = 4                       # concurrent WAL streams accepted
```

Without `token_env` the WAL ticket accepts the query token, which lets every SQL client read the
raw log; the server warns about it at start. With it, the query token is refused for the WAL
ticket and the replication token is refused for nothing else.

Standby:

```toml
[storage]
wal_dir = "/var/lib/ankaa/wal"          # required: the standby keeps its own copy

[replication]
standby_of = "10.0.0.12:50051"           # primary's Flight address
primary_token_env = "ANKAA_REPLICATION_TOKEN"   # defaults to this node's query token
# primary_tls_ca = "/etc/ankaa/tls/ca.pem"   # when the primary uses TLS
# primary_tls_domain = "ankaa-primary"
# serve_wal = true                            # let further standbys chain from this one
```

or `ankaad serve --config standby.toml --standby-of 10.0.0.12:50051`.

## How it behaves

- **Same LSNs on both sides.** The standby appends every record to its own WAL under the
  primary's LSN, so its log is a record-for-record copy and "what I need next" is simply the end
  of its own log. A restarted standby continues exactly there: nothing is skipped or applied
  twice.
- **Only durable records are shipped.** The primary serves records up to its durable LSN, so a
  standby never holds a record that a power loss on the primary could still undo.
- **Divergence stops the standby.** If the primary no longer has the records a standby asks for
  (a checkpoint truncated them while the standby was away), if the standby's log is longer than
  the primary's, or if a record arrives out of sequence, the standby logs the reason and exits
  with an error instead of serving a copy with a hole in it. Re-seed it (below).
- Records are decoded through the same path as live frames, so books and user tables are
  maintained on the standby; continuous queries are computed there from the replicated rows.
- Backpressure is honoured (a standby never drops records). Transient failures reconnect with
  exponential backoff (0.5 s to 30 s); HTTP/2 keepalive detects a primary that vanished without
  closing the connection.
- A new standby started with an empty WAL directory works as long as the primary still has its
  log from LSN 1 (no checkpoint has truncated it yet). Otherwise seed it.
- The primary writes the id of every source into its log at start, and a standby adopts that
  numbering, so user frames reach the right table whatever order `[[sources]]` has in the
  standby's own configuration (the source names must match).

### Seeding a standby

Copy the primary's directories **in this order** while it runs, then start the standby:

1. `storage.wal_dir` (it includes `sources.json`, the source ids and positions),
2. `storage.checkpoint_dir`.

The WAL copy covers everything that was not yet in a segment when it was taken, and the later
segment copy can only contain more; rows present in both are recognised by their LSN and applied
once. Copying the segments first can leave a gap.

### Metrics

| Metric | Where | Meaning |
|---|---|---|
| `ankaa_replication_connected` | standby | 1 while the stream is open |
| `ankaa_replication_received_lsn` | standby | last primary LSN received |
| `ankaa_replication_applied_lsn` | standby | last primary LSN applied to the tables |
| `ankaa_replication_last_receive_age_seconds` | standby | seconds since the last batch arrived |
| `ankaa_replication_connects_total` | standby | (re)connections |
| `ankaa_replication_wal_streams` | primary | standbys connected |
| `ankaa_replication_wal_refused_total` | primary | refused WAL requests (token, stream limit) |

Lag in records = primary `ankaa_wal_durable_lsn` − standby `ankaa_replication_applied_lsn`.
An idle primary also makes the receive age grow; alert on the age only together with a non-zero
lag, or on `ankaa_replication_connected == 0`.

The primary never ingests a frame it has not logged (ingest waits for the log when the disk is
slow), so a standby that has caught up holds exactly the primary's tables. A frame the log could
not take within 2 s is dropped on the primary too and counted in `ankaa_wal_dropped_total`.

## Failover (manual, deliberate)

1. Confirm the primary is down or fenced: it must not keep writing while clients read from the
   promoted standby, or the two diverge.
2. Stop the standby, remove `replication.standby_of` (or the `--standby-of` flag), and start it
   with the feed and source configuration. It becomes a primary with everything it received;
   records the old primary made durable but had not yet streamed are lost (asynchronous
   replication). File and Kafka sources continue from the positions that were replicated.
3. Point clients at the new address (Flight, Postgres, metrics).
4. To rebuild the old primary as a standby, wipe its WAL and segments, seed it from the new
   primary and start it with `standby_of`.

There is no automatic election, no fencing of an old primary and no synchronous mode yet
(`docs/ROADMAP.md`, beyond v1.0).
