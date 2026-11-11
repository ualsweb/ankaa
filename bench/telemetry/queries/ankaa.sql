-- ANKAA dialect (Apache DataFusion SQL over `ankaad serve`, queried over Arrow Flight).
-- Table cpu (generated config, see harness/engines.py AnkaaServeEngine): ts timestamp_ns, the 10 tags as `dict`
-- columns (hostname is partition_by), the 10 fields float64. A `dict` column is a dictionary-encoded string in
-- SQL (RFC-0002): filters, GROUP BY and results use the names, the engine works on the ids underneath. So
-- these are the same statements the other engines run, nothing resolved by the client.
-- Buckets: date_bin(INTERVAL ..., ts) with DataFusion's default origin (Unix epoch).
-- lastpoint: max(ts) per host joined back to the table. TSBS points are unique per (host, ts); with duplicates
-- this shape would return every tied row (the diff against DuckDB's DISTINCT ON would then flag it).
-- Measured on the smoke dataset this beat DISTINCT ON (~3x) and row_number() (~4x) on ankaad.
-- {START}/{END} render as RFC 3339 string literals (DataFusion coerces them to Timestamp(ns, UTC)).

-- @query single-groupby-1-1-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-1-1-12
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-1-8-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-1-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-1-12
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-8-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query cpu-max-all-1
SELECT date_bin(INTERVAL '1 hour', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query cpu-max-all-8
SELECT date_bin(INTERVAL '1 hour', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query double-groupby-1
SELECT date_bin(INTERVAL '1 hour', ts) AS hour, hostname,
       avg(usage_user) AS mean_usage_user
FROM cpu
WHERE ts >= {START} AND ts < {END}
GROUP BY 1, 2
ORDER BY 1, 2

-- @query double-groupby-5
SELECT date_bin(INTERVAL '1 hour', ts) AS hour, hostname,
       avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
       avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
FROM cpu
WHERE ts >= {START} AND ts < {END}
GROUP BY 1, 2
ORDER BY 1, 2

-- @query double-groupby-all
SELECT date_bin(INTERVAL '1 hour', ts) AS hour, hostname,
       avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
       avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq,
       avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest,
       avg(usage_guest_nice) AS mean_usage_guest_nice
FROM cpu
WHERE ts >= {START} AND ts < {END}
GROUP BY 1, 2
ORDER BY 1, 2

-- @query high-cpu-1
SELECT ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
WHERE usage_user > 90.0 AND ts >= {START} AND ts < {END} AND hostname IN ({HOSTS})

-- @query high-cpu-all
SELECT ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
WHERE usage_user > 90.0 AND ts >= {START} AND ts < {END}

-- @query groupby-orderby-limit
SELECT date_bin(INTERVAL '1 minute', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE ts < {END}
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5

-- @query lastpoint
SELECT c.ts, c.hostname, c.region, c.datacenter, c.rack, c.os, c.arch, c.team, c.service, c.service_version, c.service_environment, c.usage_user, c.usage_system, c.usage_idle, c.usage_nice, c.usage_iowait, c.usage_irq, c.usage_softirq, c.usage_steal, c.usage_guest, c.usage_guest_nice
FROM cpu c
JOIN (SELECT hostname, max(ts) AS last_ts FROM cpu GROUP BY hostname) m
  ON c.hostname = m.hostname AND c.ts = m.last_ts
