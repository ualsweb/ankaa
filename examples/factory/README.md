# Factory demo: an MQTT plant in SQL, live

A simulated plant publishes to an MQTT **Unified Namespace**; ANKAA subscribes, stores every reading,
keeps live aggregates, and Grafana shows it all through the Postgres protocol. One command, about two
minutes on a laptop.

```
plant simulator ──MQTT──▶ Mosquitto ──▶ ankaad ──Postgres wire──▶ Grafana
 20 machines, 1 Hz        acme/plant1/<area>/<line>/<machine>/telemetry
```

![The Plant 1 dashboard](../../docs/assets/factory-dashboard.png)

## Run it

Requires Docker with Compose (Docker Desktop, colima or Linux).

```sh
cd examples/factory
docker compose up -d
```

Open **http://localhost:3000**: the *Plant 1* dashboard loads without a login. Stop with
`docker compose down` (add `-v` to delete the stored data).

The first start pulls `ghcr.io/ualsweb/ankaa`. If that image is not available for your machine,
Compose builds it from the repository (a Rust release build, several minutes once).

## What it shows

| Panel | What happens underneath |
|---|---|
| Readings stored, machines | One table for the whole namespace; site, area, line and machine come from the topic (`$topic.1` … `$topic.4` in `ankaad.toml`) |
| Readings without kW (NULL) | About 2 % of readings arrive without a value. They are stored as `NULL`, not zero, and every aggregate skips them |
| Power per machine | 10-second averages computed from the raw table on every refresh, including readings from milliseconds ago |
| Live 1-minute bars | A continuous query (`cq_power_1m_live`) updated as readings arrive, not recomputed |
| Temperature median and p99 | Streaming quantiles per machine over 5-minute windows (`cq_temp_5m_live`) |

The simulator also misbehaves the way plants do: the line 4 gateway drops off one minute in three and
then sends what it buffered, late. The table accepts readings up to five minutes late
(`max_lateness_ms`) and places them at their time, so the charts and bars come out as if nothing had
happened. `press03` draws 25 % more power for two minutes every quarter hour, and `robot09` stops for
a minute every ten.

## Query it yourself

The token of this demo is `demo-token-not-secret` (it is also the Postgres password).

```sh
# any Postgres client
PGPASSWORD=demo-token-not-secret psql -h 127.0.0.1 -p 55432 -U ankaa -c "
  SELECT machine, count(*) AS readings, count(kw) AS with_kw, max(kw) / 1e8 AS max_kw
  FROM telemetry
  GROUP BY machine ORDER BY max_kw DESC LIMIT 5"

# metrics: late readings accepted, readings without a value, nothing dropped
curl -s -H "Authorization: Bearer demo-token-not-secret" http://127.0.0.1:9100/metrics \
  | grep -E '^ankaa_(late_rows_total|user_rows_total|user_rejected_total|dropped_rows_total) '
```

From Python, with the `ankaa` package (`crates/ankaa-py`):

```python
import ankaa
live = ankaa.connect("127.0.0.1:50051", token="demo-token-not-secret")
live.sql("SELECT * FROM percentiles('telemetry', 'temp_c', '0.5,0.99', 'machine')").to_pandas()
```

## Make it yours

Point `[[sources]]` in `ankaad.toml` at your broker and topic filter, adjust the `$topic.N`
positions to your hierarchy and the column names to your payload, and restart. The walkthrough is in
[the Unified Namespace tutorial](../../docs/tutorials/uns-historian.md); every option is in
[Bring your own feed](../../docs/generic-ingest.md).

`kw` is a `fixed8` column (exact decimals, stored as integers ×10⁸), which is why the queries divide by
`1e8`; use `float64` if you prefer floats.
