# A historian for your Unified Namespace, in one binary

Plants that adopt a **Unified Namespace** (UNS) publish every machine, line and area to one MQTT broker
under a topic hierarchy, usually ISA-95 shaped:

```
acme/plant1/press-shop/line1/press03/telemetry   {"t": 1762859000123, "kw": 61.42, "temp_c": 48.1, "state": 1}
└┬─┘ └─┬──┘ └───┬────┘ └─┬─┘ └──┬──┘
enterprise site   area   line  machine
```

The broker holds the *current* state. The questions people ask are about *history*: power per machine
through the night shift, the p99 temperature over the last five minutes, which line stopped and when.
That usually means a historian, or a time-series database plus a bridge (Telegraf, Node-RED) plus a
dashboard, each with its own configuration.

This tutorial does it with ANKAA alone: subscribe to the namespace, turn the topic hierarchy into
columns, store every reading durably, keep live aggregates, and answer in SQL, milliseconds after a
reading arrives. It runs the [factory demo](../../examples/factory/) and then explains each part so you
can point it at your own broker.

**Time:** 15 minutes. **Needs:** Docker with Compose.

## 1. Start the demo

```sh
git clone https://github.com/ualsweb/ankaa && cd ankaa/examples/factory
docker compose up -d
```

Four containers start: Mosquitto, a simulated plant with 20 machines publishing once a second, ANKAA,
and Grafana. Open http://localhost:3000 for the dashboard, or query directly:

```sh
export PGPASSWORD=demo-token-not-secret
psql -h 127.0.0.1 -p 55432 -U ankaa -c "SELECT count(*) FROM telemetry"
```

Run it twice a few seconds apart: the count grows by about 20 per second.

![The Plant 1 dashboard of the demo](../assets/factory-dashboard.png)

## 2. Topics become columns

The whole namespace lands in **one table**. The source maps topic segments to columns with `$topic.N`
(the `N`-th `/`-separated segment, from 0) and payload fields with JSON paths:

```toml
[[tables]]
name = "telemetry"
partition_by = "machine"
max_lateness_ms = 300000
retention = "7d"
columns = [
  { name = "ts",      type = "timestamp_ns" },
  { name = "site",    type = "dict" },
  { name = "area",    type = "dict" },
  { name = "line",    type = "dict" },
  { name = "machine", type = "dict" },
  { name = "kw",      type = "fixed8",  nullable = true },
  { name = "temp_c",  type = "float64", nullable = true },
  { name = "state",   type = "u8" },
]

[[sources]]
name = "plant1"
kind = "mqtt"
url = "mqtt://mqtt:1883"
subject = "acme/+/+/+/+/telemetry"
format = "json"
table = "telemetry"
ts_unit = "millis"
map = { ts = "t", site = "$topic.1", area = "$topic.2", line = "$topic.3", machine = "$topic.4",
        kw = "kw", temp_c = "temp_c", state = "state" }
```

- `dict` columns store each distinct name once and keep a dense integer id per row. In SQL they are
  text: you filter, group and read them by name. A new machine appears as a new name, with no schema
  change.
- `partition_by = "machine"` gives every machine its own ordering, watermark and late-data window.
- The subscription uses MQTT QoS 1 with a persistent session: readings published while ANKAA is
  restarting are delivered when it comes back.

Readings per area and line:

```sql
SELECT area, line, count(*) AS readings
FROM telemetry
GROUP BY 1, 2 ORDER BY 1, 2;
```

## 3. Missing readings are NULL, not zero

Sensors drop values. In the demo about 2 % of readings come without `kw`. Because the column is
`nullable`, those are stored as `NULL`: an average is not dragged towards zero, and you can count the
gaps.

```sql
SELECT machine,
       count(*)              AS readings,
       count(kw)             AS with_kw,
       count(*) - count(kw)  AS missing_kw,
       avg(kw) / 1e8         AS avg_kw
FROM telemetry
GROUP BY 1 ORDER BY missing_kw DESC LIMIT 5;
```

`kw` is `fixed8`: exact decimals stored as integers ×10⁸, hence `/ 1e8`. Choose `float64` if you prefer.

## 4. Late data from gateways

Edge gateways buffer when the network drops and send everything when it returns. In the demo, the
line 4 gateway is offline one minute in three. `max_lateness_ms = 300000` lets a reading arrive up to
five minutes after newer readings of the same machine and still be stored **at its place in time**.
Older ones are dropped and counted, never silently merged.

```sh
curl -s -H "Authorization: Bearer demo-token-not-secret" http://127.0.0.1:9100/metrics \
  | grep -E '^ankaa_(late_rows_total|dropped_rows_total) '
```

`ankaa_late_rows_total` grows each time the line 4 gateway comes back. The charts show no hole for
line 4: the late readings are already in place.

## 5. Live aggregates, maintained as readings arrive

Dashboards that recompute aggregates over raw data on every refresh do the same work again and again.
A **continuous query** keeps the result up to date incrementally and is read like a table:

```toml
[[continuous]]
name = "power_1m"          # tables cq_power_1m (closed bars) and cq_power_1m_live (open bars)
kind = "ohlcv"
table = "telemetry"
key = "machine"
value = "kw"
width_ns = 60000000000

[[continuous]]
name = "temp_5m"
kind = "percentiles"
table = "telemetry"
key = "machine"
value = "temp_c"
width_ns = 300000000000
quantiles = [0.5, 0.99]
```

```sql
-- the current minute, per machine (bars stay open for the late-data window, so take the newest)
SELECT machine, open / 1e8 AS open_kw, high / 1e8 AS high_kw, close / 1e8 AS close_kw, count
FROM cq_power_1m_live
WHERE ts = (SELECT max(ts) FROM cq_power_1m_live)
ORDER BY high_kw DESC LIMIT 5;

-- the hottest machines in the current five-minute window
SELECT machine, p50, p99
FROM cq_temp_5m_live
WHERE ts = (SELECT max(ts) FROM cq_temp_5m_live)
ORDER BY p99 DESC LIMIT 5;
```

Bars respect the late-data window: a bar closes five minutes after its end, so late gateway readings
are counted in it; until then it stays in `cq_power_1m_live` next to the newer open bars. After a restart, the bars are rebuilt from the stored readings and come
out identical.

## 6. Ask the history anything

The raw table answers ad-hoc questions with standard SQL (Apache DataFusion):

```sql
-- when did robot09 stop, in the last 30 minutes?
SELECT date_bin(interval '1 minute', ts) AS minute, count(*) FILTER (WHERE state = 0) AS stopped_readings
FROM telemetry
WHERE machine = 'robot09' AND ts > now() - interval '30 minutes'
GROUP BY 1 HAVING count(*) FILTER (WHERE state = 0) > 0 ORDER BY 1;

-- press03 against the average of its line, per minute: the anomaly the simulator injects
SELECT date_bin(interval '1 minute', ts) AS minute,
       avg(kw) FILTER (WHERE machine = 'press03') / 1e8 AS press03_kw,
       avg(kw) / 1e8                                     AS line1_avg_kw
FROM telemetry
WHERE line = 'line1' AND ts > now() - interval '30 minutes'
GROUP BY 1 ORDER BY 1;
```

The same queries work from Python (`ankaa.connect(...)`, results as pandas or polars through Arrow),
from BI tools over the Postgres protocol, and over Arrow Flight.

## 7. Retention

`retention = "7d"` keeps a week of readings, measured back from the newest one (event time, so a
backfill of old data is not deleted on arrival). Whole compressed chunks older than that are dropped
and their files deleted; nothing has to be scheduled.

## 8. Point it at your plant

1. Copy `examples/factory/ankaad.toml`.
2. In `[[sources]]`, set `url` to your broker and `subject` to your topic filter:
   `mqtt://user:password@host:1883`, or `mqtts://host:8883` for TLS. With TLS, `tls_ca` makes ANKAA
   trust only your own CA, and `tls_cert` + `tls_key` present a client certificate to brokers that
   ask for one ([details](../generic-ingest.md)).
3. Adjust the `$topic.N` positions to your hierarchy and the column list and JSON paths to your
   payload. A reading that does not match is counted in `ankaa_user_rejected_total` and logged at debug
   level, never fatal. If your gateways publish **Sparkplug B** (`spBv1.0/...` topics) instead of
   JSON, use `format = "sparkplug"` with `$metric`, `$value`, `$timestamp`, `$node` and `$device`
   in `map`: one row per metric, aliases resolved from the BIRTH messages
   ([details](../generic-ingest.md#sparkplug-b-format--sparkplug)).
4. Run the container with your file mounted:

   ```sh
   docker run -d --name ankaa -e ANKAA_TOKEN=<at least 16 characters> \
     -v $PWD/ankaad.toml:/etc/ankaa/ankaad.toml:ro -v ankaa-data:/var/lib/ankaa \
     -p 127.0.0.1:55432:5432 -p 127.0.0.1:50051:50051 -p 127.0.0.1:9100:9100 \
     ghcr.io/ualsweb/ankaa:latest
   ```

5. Check `ankaa_user_rows_total` growing on `/metrics`, then point Grafana or your BI tool at port
   55432 (database `ankaa`, user `ankaa`, password = the token).

ANKAA is pre-alpha: read [STATUS](../STATUS.md) for what it does not do yet. If you try it on a real
plant, the [design-partner page](../design-partners.md) says how we can help.
