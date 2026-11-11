# Generic ingest: bring your own feed

ANKAA's storage, SQL, tiers and WAL are schema-generic. Any time series with a timestamp column
is ingested by declaring a table and a source in the server config: plant-floor and building
telemetry over MQTT, meter and grid readings, IoT sensors, application metrics, an internal
event feed. The market-data tables (`trades`, `book_deltas`, `quotes`) are one built-in schema
on the same machinery, not a special case of it.

## Declare a table

```toml
[[tables]]
name = "telemetry"
partition_by = "rack"            # a dict column; ordering and watermarks are per partition
columns = [
  { name = "ts",   type = "timestamp_ns" },   # the first column must be a timestamp
  { name = "rack", type = "dict" },           # string → dense u32 id (see `dictionaries`)
  { name = "temp", type = "fixed8" },         # decimal stored as i64 with 8 decimals, no floats
  { name = "ok",   type = "u8" },
  { name = "load", type = "float64", nullable = true },  # a reading may come without it
]
```

Types: `timestamp_ns`, `fixed8`, `int64`, `dict`, `u8`, `float64`. Table and column names are
SQL identifiers (letters, digits, `_`); `trades`, `book_deltas`, `quotes`, `symbols`,
`dictionaries` and the `cq_` prefix are reserved.

`nullable = true` lets a reading come without that value: a missing JSON field, a JSON `null`
or an empty CSV field is stored as SQL `NULL` (a validity bitmap next to the column, kept through
the WAL, the warm and cold tiers and recovery). Aggregates skip `NULL` like any SQL engine
(`count(load)` counts readings that have it, `avg(load)` averages them), `WHERE load IS NULL`
finds the gaps, and the bar, window and quantile functions and continuous queries ignore rows
without their value. A value that is present but malformed is still rejected. The timestamp and
`dict` columns cannot be nullable (they order and key every row). Without `nullable` a reading
that lacks the value is rejected and counted, as before.

`max_lateness_ms` (default 0, at most seven days) is how much older than the newest reading of
its partition a reading may be and still be stored: devices that buffer while offline and gateways
that batch send late as a matter of course. Such readings are inserted at their place in time
(`ankaa_late_rows_total`); older ones are dropped and counted (`ankaa_dropped_rows_total`).
Continuous queries over the table close their buckets that much later, so late readings still
count (see `docs/temporal-architecture.md`, "Ordering and watermarks").

`retention = "90d"` (units `s`, `m`, `h`, `d`, `w`; unset = keep forever) bounds how long rows
are kept, measured back from the newest row of the table, not from the wall clock: a backfill
of last year's data is not deleted as it arrives, and a server that was down does not purge
everything on start. Whole sealed chunks older than the window are dropped (from every tier) and
their segment files deleted; a query that is already running keeps reading the chunks it started
with, and a file goes once no query holds it. `ankaa_retention_expired_rows_total`,
`..._expired_chunks_total` and `..._segments_deleted_total` count the work. The market tables
take `storage.market_retention`. Continuous-query outputs are rebuilt from the retained rows on
a restart, so keep raw data at least as long as you need their history.

`max_dict_entries` (default 100,000) caps the distinct values of each `dict` column of the
table. Every value of the `partition_by` column is a partition with its own memory, so a
producer that sends unbounded keys (request ids, UUIDs) must not be able to grow the server
without limit: a row that would add a value beyond the cap is rejected and counted. A key costs
a few hundred bytes before it holds any data; raise the cap deliberately. A message that is
rejected for any reason adds nothing to a dictionary. Dictionary
values are at most 256 bytes; JSON strings, numbers and booleans are accepted as values, objects,
arrays and `null` are not.

## Attach a source

```toml
[[sources]]
name = "racks"
kind = "nats"                        # mqtt | nats | kafka | file
url = "nats://127.0.0.1:4222"        # MQTT: mqtt://host:1883; Kafka: bootstrap servers; file: a path
subject = "racks.>"                  # MQTT topic filter, NATS subject or Kafka topic
format = "json"                      # json | csv | sparkplug (MQTT)
table = "telemetry"
ts_unit = "millis"                   # seconds | millis | micros | nanos | rfc3339
map = { ts = "meta.time", rack = "meta.id", temp = "temp", ok = "ok", load = "load" }
# max_message_bytes = 1048576        # larger messages are dropped and counted (at most 16 MiB)
# start = "latest"                   # kafka only: latest | earliest, for a partition without a saved position
```

- `map` gives, per column, a JSON path (`a.b.0.c`) or a CSV field index; for an MQTT source
  also `$topic` (the whole topic) or `$topic.N` (its `N`-th `/` segment, from 0), because MQTT
  devices name the plant, line and machine in the topic, not in the payload. Every column must be
  mapped; the only exception is the timestamp when `ts_from_receive = true` (the host receive
  time is used). A configuration that leaves a column unmapped is refused at start.
- Several sources may feed one table; they share its dictionaries.
- Source names are identifiers (letters, digits, `_`, `-`) and must be unique. A name keeps the
  same internal id for the life of the data directory, so sources can be reordered, added and
  removed freely.
- Messages travel through the same bounded channel, write-ahead log and recovery path as exchange
  frames (`FrameKind::User`, payload = `[u16 source id][message]`), so a restart replays them and
  the checkpoint/tier machinery applies unchanged.
- A message that does not map is counted in `ankaa_user_rejected_total` and logged at debug
  level; it never stops the server.

### Several readings per message (`rows`)

Gateways and devices often send a batch: one message with the readings of the last seconds.
`rows` names the array that holds them; every element becomes a row. Column paths are read
inside each element; a path that starts with `$msg.` is read from the message around the array
(once per message, whatever the number of elements):

```toml
[[sources]]
name = "gateways"
kind = "mqtt"
url = "mqtts://broker.plant1:8883"
subject = "plant1/+/batch"
format = "json"
table = "power"
ts_unit = "millis"
rows = "samples"                       # {"machine":"press03","samples":[{"t":…,"kw":…},…]}
map = { ts = "t", machine = "$msg.machine", kw = "kw" }
```

- Each element is checked on its own: an element with a bad or missing value is refused and
  counted (`ankaa_user_rejected_total` counts rows), the others are stored. A message without the
  array is refused whole; an empty array stores nothing and is not an error.
- `ankaa_user_rows_total` counts rows, not messages.
- At most 65,536 rows per message, and dictionary values that are new in one message count
  against `max_dict_entries` together: a batch cannot overshoot the cap.
- A message is one WAL record however many rows it carries; recovery replays it exactly, also
  when its rows straddle a chunk boundary.
- `rows` needs `format = "json"`. `$topic` selectors work in the same mapping.

**How JSON is read.** The mapped paths are compiled once into a small trie, and each message is
read in one forward pass: values on a mapped path are recorded as byte ranges of the message,
everything else is skipped without being decoded (strings by a vectorised search for the next
quote or backslash, objects and arrays by counting brackets). No document tree is built and
nothing is allocated per message. Values on a mapped path are checked against the JSON grammar;
unmapped parts are checked for structure (balanced brackets, terminated strings), the way
on-demand JSON parsers work. Nesting deeper than 128 levels is refused. Numbers are converted
with correctly rounded parsing (`fixed8` columns with exact decimal parsing).

### Sparkplug B (`format = "sparkplug"`)

Sparkplug B is the MQTT payload of most industrial gateways and SCADA systems (Ignition, Kepware,
Cirrus Link, the Eclipse Tahu libraries): a protobuf message with a list of metrics, each with a
name or an alias, a datatype, a timestamp and a value. An MQTT source with
`format = "sparkplug"` stores one row per metric of every BIRTH and DATA message:

```toml
[[tables]]
name = "scada"
partition_by = "metric"
columns = [
  { name = "ts",     type = "timestamp_ns" },
  { name = "node",   type = "dict" },
  { name = "device", type = "dict" },
  { name = "metric", type = "dict" },
  { name = "value",  type = "float64", nullable = true },
  { name = "text",   type = "dict" },
]

[[sources]]
name = "plant1"
kind = "mqtt"
url = "mqtts://broker.plant1:8883"
subject = "spBv1.0/plant1/#"
format = "sparkplug"
table = "scada"
# sparkplug_rebirth = true           # ask nodes whose DATA comes before their BIRTH to rebirth
map = { ts = "$timestamp", node = "$node", device = "$device", metric = "$metric", value = "$value", text = "$string" }
```

| Selector | Value |
|---|---|
| `$metric` | metric name; for a DATA message that sends only the alias, the name its BIRTH gave |
| `$value` | the value. Numeric columns take integers, floats and booleans (0/1); a string value is `NULL` there. A `dict` column takes any value as text |
| `$string` | the value when it is a string (`String`, `Text`, `UUID`), otherwise none |
| `$timestamp` | the metric's timestamp, else the payload's (milliseconds since the epoch; `ts_unit` does not apply) |
| `$datatype` | the Sparkplug datatype code |
| `$historical` | 1 for a historical value (sent late from the node's buffer), else 0 |
| `$seq` | the payload's sequence number (0–255) |
| `$group`, `$node`, `$device`, `$type` | from the topic `spBv1.0/<group>/<type>/<node>[/<device>]`; `$device` is none for node-level metrics |
| `$topic`, `$topic.N` | as for any MQTT source |

- **Definitions.** A BIRTH (`NBIRTH`, `DBIRTH`) defines each metric: its datatype, whether it is
  transient, and the alias that later DATA messages may use instead of the name. A compliant edge
  node leaves the datatype out of DATA messages, so the server remembers the definitions per edge
  node and reads every DATA metric with them (an `Int32` sent as its 32-bit two's complement keeps
  its sign). An `NBIRTH` replaces what the node and its devices defined before; a `DBIRTH`
  replaces the device's metrics.
- **Not stored:** transient metrics (the specification says hosts do not historise them),
  `NDEATH`/`DDEATH`, commands (`NCMD`, `DCMD`), `STATE`, and topics outside `spBv1.0`. Metrics
  whose datatype has no scalar value (`DataSet`, `Template`, `Bytes`, `File`, arrays) are refused
  and counted; the other metrics of the message are stored. A DATA metric whose alias no BIRTH has
  defined is refused and counted.
- **Where a selector does not apply** (`$device` of a node metric, `$string` of a number), a
  nullable numeric column holds `NULL` and a `dict` column the empty string `''` (a dictionary
  key always holds a value).
- **Restarts and truncation.** BIRTH messages may be days older than any checkpoint. The server
  writes the definitions of its Sparkplug sources into the write-ahead log (at most once per
  `checkpoint_interval_s`, when they changed or when they hold truncation back) and truncates the
  log only up to the newest such record: a restart, a crash or a standby reads DATA messages by
  alias without waiting for a new BIRTH.
- **`sparkplug_rebirth = true`** makes the server publish a rebirth request (`NCMD` with
  `Node Control/Rebirth` = true, QoS 0) to an edge node whose DATA arrives before any BIRTH from
  it since the server started, at most once per 30 s per node. The broker account needs permission
  to publish on `spBv1.0/<group>/NCMD/<node>`. Off by default: the server is otherwise a read-only
  subscriber.
- Compressed payloads (the optional `algorithm` metric of Sparkplug 3.0) are not supported: they
  do not decode as metrics and the message is refused.
- Decoding reads the protobuf bytes once, in place: names and strings are slices of the message,
  numbers come from their varint or fixed-width fields, and nothing is allocated per metric.
  Limits: 65,536 metrics per message, 1,000,000 definitions per source, names up to 1 KiB.

### How values are converted

| Column type | Accepted | Notes |
|---|---|---|
| `timestamp_ns` | integer or decimal number, or text, in `ts_unit`; RFC 3339 text | Decimal seconds keep nanosecond precision. RFC 3339 is strict: `2026-02-30`, a sign inside a field or text after the offset are rejected; a missing offset means UTC. A timestamp more than 24 h ahead of the receive time is rejected (it would block every later row of its partition; the usual cause is a wrong `ts_unit`). |
| `fixed8` | decimal text or JSON number | Text and numbers with up to 8 decimals are exact. JSON numbers with more decimals or an exponent (`21.700000000000003`, `1e-3`) are rounded to the nearest 1e-8. Send decimals as strings when exactness beyond double precision matters. |
| `int64` | integer number or text | |
| `u8` | 0..255, or `true`/`false` | |
| `float64` | number or numeric text | `NaN` and infinities are rejected. |
| `dict` | text, number or boolean | See the caps above. |

CSV: fields are split on commas; a field may be wrapped in double quotes to contain commas
(`"rack 7, row B",21.5`). Lines starting with `#` and empty lines are skipped by the file source.

Rows are expected in timestamp order per partition, within `max_lateness_ms` (default 0: a row
older than the newest row of its partition is rejected and counted; equal timestamps are
accepted).

### Transports and delivery guarantees

| kind | Behaviour | Guarantee |
|---|---|---|
| `file` | Every non-empty line is a message (NDJSON or CSV), read as a stream. The task idles after the file so the data stays queryable. | **At least once.** The read position is saved; a restart continues after the last durable line, and lines appended while the server was down are ingested on the next start. A file replaced by a different one is read from the beginning. |
| `kafka` | Verified with Apache Kafka 3.9 (KRaft). All partitions of the topic are read concurrently, without a consumer group: the server keeps the next offset of every partition itself. Records of different partitions are merged by their Kafka timestamp before they are delivered, so a multi-partition topic arrives in time order (see below). Partitions added to the topic are picked up within 30 s and read from their beginning. Backpressure waits (nothing is dropped). | **At least once.** A restart resumes from the saved offsets, so messages published while the server was down are ingested. If the broker no longer retains a saved offset the source continues from the earliest retained one and `ankaa_kafka_offset_resets_total` increases. |
| `mqtt` | MQTT 3.1.1, QoS 1, persistent session (client id `ankaa-<source name>`, `clean_session = false`). A message is acknowledged once it is in the ingest channel; a full channel slows the broker down instead of dropping. `url` may carry `user:password@`; `mqtts://` connects with TLS (see below). | **At least once across outages and restarts** for what the broker keeps for the session (its queue limits apply); a crash loses the messages acknowledged but not yet in the WAL group commit (milliseconds). |
| `nats` | Core NATS subscription; the client reconnects, reconnections are counted. | **At most once.** Core NATS has no replay: messages published while the server is down, or arriving while the ingest channel is full (`ankaa_source_dropped_full_total`), are lost. |

**Order across Kafka partitions.** A table accepts rows in timestamp order per table partition,
while a topic's partitions are independent logs. The source therefore delivers, each round, the
records of all partitions up to the oldest "newest timestamp fetched" among the partitions that
still have data, in timestamp order, and keeps the rest for the next round: a backlog is replayed
interleaved as it was produced, and a caught-up source adds at most one fetch round (250 ms) of
delay. This uses the Kafka record timestamp; if your event time differs a lot from it, or two
sources feed the same table partition with overlapping times, rows that arrive behind a newer one
are rejected by the ordering rule and counted. The simplest safe layout is `partition_by` = the
Kafka message key.

"At least once" means: after a **clean stop** nothing is repeated; after a **crash**, the
messages read since the last saved position (about one second of them) are read again. Give rows
a timestamp that increases per partition if exact-once matters: a repeated older row is then
rejected by the ordering rule. If the WAL cannot take a source message at all (a disk stalled for more than 2 s) the server stops
instead of skipping it, and the restart reads it again. Positions are stored in `sources.json` next to the WAL (or next to
the segments when there is no WAL) and only advance once the messages before them are durable;
without `storage.wal_dir` and `storage.checkpoint_dir` nothing persists and every start begins
from scratch.

**MQTT over TLS.** `url = "mqtts://broker:8883"` (8883 when no port is given) connects with TLS
1.3 or 1.2 (rustls with the aws-lc-rs provider: AES-GCM runs on the Arm AES and PMULL
instructions). The broker certificate is checked against the public (Mozilla) roots, or, when
`tls_ca` names a PEM file, against those CAs only: a plant's private PKI is then not also trusting
every public CA. `tls_cert` and `tls_key` add a client certificate for brokers that authenticate
clients that way (AWS IoT Core, mutual-TLS listeners):

```toml
[[sources]]
name = "plant1"
kind = "mqtt"
url = "mqtts://broker.plant1:8883"
tls_ca = "/etc/ankaa/plant-ca.pem"         # optional: trust only this CA
tls_cert = "/etc/ankaa/ankaa-client.pem"   # optional, with tls_key: client certificate
tls_key = "/etc/ankaa/ankaa-client.key"
subject = "plant1/#"
# … format, table, map as above
```

The files are read and checked when the server starts: a missing file, a file without a
certificate, a key that does not match its certificate or a certificate without its key stops the
start with the file named. `tls_*` on anything but an `mqtts://` source is a configuration error.

Not supported yet: Kafka consumer groups (`group` is rejected), SASL and TLS options for Kafka,
TLS for NATS, and NATS credentials files. A NATS or MQTT URL may carry `user:password@`; it is
never logged. Connect to Kafka and NATS over a trusted network or a local TLS proxy.

An MQTT example, one table per plant measurement with the line and machine taken from the topic:

```toml
[[tables]]
name = "power"
partition_by = "machine"
max_lateness_ms = 300000          # gateways that buffer up to five minutes
retention = "180d"
columns = [
  { name = "ts",      type = "timestamp_ns" },
  { name = "line",    type = "dict" },
  { name = "machine", type = "dict" },
  { name = "kw",      type = "float64", nullable = true },
]

[[sources]]
name = "plant1"
kind = "mqtt"
url = "mqtt://broker.plant1:1883"
subject = "plant1/+/+/power"      # plant1/<line>/<machine>/power
format = "json"
table = "power"
ts_unit = "millis"
map = { ts = "t", line = "$topic.1", machine = "$topic.2", kw = "kw" }
```

### A server without the exchange feed

```toml
[feed]
enabled = false        # no built-in market feed: only [[sources]] feed this server
streams = []
```

## Query it

```sql
SELECT d.name AS rack, count(*), avg(temp)/1e8 AS avg_temp
FROM telemetry t JOIN dictionaries d
  ON d."table" = 'telemetry' AND d."column" = 'rack' AND d.id = t.rack
GROUP BY d.name;
```

`dictionaries(table, column, id, name)` holds the names behind every `dict` column of every user
table and is refreshed when a new value appears. Dictionaries are persisted next to the segments
(`dictionaries.json`) before any segment that uses their ids, so ids keep their names across
restarts and WAL truncation. User tables take part in the tiers (warm encoding, cold segments,
checkpoints) exactly like the market tables and appear under their own name in `checkpoint_dir`.

Equality and range predicates on a `dict` column prune whole chunks (`WHERE rack = 3`), and
scans run one partition per group of chunks in parallel.

## Changing a table

A table may gain columns without losing what it holds:

```toml
columns = [
  { name = "ts",   type = "timestamp_ns" },
  { name = "rack", type = "dict" },
  { name = "temp", type = "fixed8" },
  { name = "kw",   type = "float64", nullable = true },   # added: must be nullable, at the end
]
```

- Add the new columns **at the end** and make them **nullable**; map them in every source of the
  table (a message without the field gives `NULL`). Restart.
- An existing column may also become nullable (`nullable = true` on a column that had none).
- Rows stored before the change read `NULL` in the added columns, in SQL, over Flight and in
  Python. Nothing is rewritten: a segment written before the change is read as the new schema
  (the added columns cost nothing on disk and are not read from it); the write-ahead log is
  replayed through the new mapping. `segments restored … evolved=N` in the start log counts
  the segments read that way.
- Other changes (removing, renaming, retyping or reordering a column, adding one that is not
  nullable) stop the start with a message that names the files and the difference: restore the
  previous definition, or move the table's segment files out of `checkpoint_dir` to start the
  table empty.

## Limits (v1)

- Columns can only be added (nullable, at the end) or made nullable; see "Changing a table".
- `replay` ignores user frames: it only knows the market tables. Use `serve` with a `file` source
  for offline runs.
