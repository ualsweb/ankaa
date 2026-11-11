-- ANKAA dialect (Apache DataFusion SQL over `ankaad serve`, queried over Arrow Flight).
-- Table cpu (generated config, see harness/engines.py AnkaaServeEngine): ts timestamp_ns, the 10 tags as `dict`
-- columns (hostname is partition_by), the 10 fields float64. A `dict` column holds a dense UInt32 id per row;
-- the names are in the `dictionaries(table, column, id, name)` table (docs/generic-ingest.md "Query it").
-- So that ANKAA answers exactly the question the other engines answer, hosts are given BY NAME and results
-- carry names, never ids resolved by the client:
--   filter      hostname IN (SELECT id FROM dictionaries WHERE ... AND name IN ({HOSTS}))
--   group by    aggregate on the id, then join the names (the shape TSBS uses for TimescaleDB with a tags
--               table: `WITH cpu_avg AS (... GROUP BY tags_id) ... JOIN tags`)
--   row output  one join per tag column.
-- Buckets: date_bin(INTERVAL ..., ts) with DataFusion's default origin (Unix epoch).
-- lastpoint: max(ts) per host joined back to the table. TSBS points are unique per (host, ts); with duplicates
-- this shape would return every tied row (the diff against DuckDB's DISTINCT ON would then flag it).
-- Measured on the smoke dataset this beat DISTINCT ON (~3x) and row_number() (~4x) on ankaad.
-- {START}/{END} render as RFC 3339 string literals (DataFusion coerces them to Timestamp(ns, UTC)).

-- @query single-groupby-1-1-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-1-1-12
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-1-8-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-1-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-1-12
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-8-1
SELECT date_bin(INTERVAL '1 minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query cpu-max-all-1
SELECT date_bin(INTERVAL '1 hour', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query cpu-max-all-8
SELECT date_bin(INTERVAL '1 hour', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))
  AND ts >= {START} AND ts < {END}
GROUP BY 1 ORDER BY 1

-- @query double-groupby-1
WITH a AS (
  SELECT date_bin(INTERVAL '1 hour', ts) AS hour, hostname AS host_id,
         avg(usage_user) AS mean_usage_user
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  GROUP BY 1, 2
)
SELECT a.hour, d.name AS hostname, a.mean_usage_user
FROM a JOIN dictionaries d ON d."table" = 'cpu' AND d."column" = 'hostname' AND d.id = a.host_id
ORDER BY a.hour, hostname

-- @query double-groupby-5
WITH a AS (
  SELECT date_bin(INTERVAL '1 hour', ts) AS hour, hostname AS host_id,
         avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
         avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  GROUP BY 1, 2
)
SELECT a.hour, d.name AS hostname, a.mean_usage_user, a.mean_usage_system, a.mean_usage_idle, a.mean_usage_nice, a.mean_usage_iowait
FROM a JOIN dictionaries d ON d."table" = 'cpu' AND d."column" = 'hostname' AND d.id = a.host_id
ORDER BY a.hour, hostname

-- @query double-groupby-all
WITH a AS (
  SELECT date_bin(INTERVAL '1 hour', ts) AS hour, hostname AS host_id,
         avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
         avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq,
         avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest,
         avg(usage_guest_nice) AS mean_usage_guest_nice
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  GROUP BY 1, 2
)
SELECT a.hour, d.name AS hostname, a.mean_usage_user, a.mean_usage_system, a.mean_usage_idle, a.mean_usage_nice, a.mean_usage_iowait, a.mean_usage_irq, a.mean_usage_softirq, a.mean_usage_steal, a.mean_usage_guest, a.mean_usage_guest_nice
FROM a JOIN dictionaries d ON d."table" = 'cpu' AND d."column" = 'hostname' AND d.id = a.host_id
ORDER BY a.hour, hostname

-- @query high-cpu-1
SELECT c.ts, d_hostname.name AS hostname, d_region.name AS region, d_datacenter.name AS datacenter, d_rack.name AS rack, d_os.name AS os, d_arch.name AS arch, d_team.name AS team, d_service.name AS service, d_service_version.name AS service_version, d_service_environment.name AS service_environment,
       c.usage_user, c.usage_system, c.usage_idle, c.usage_nice, c.usage_iowait,
       c.usage_irq, c.usage_softirq, c.usage_steal, c.usage_guest, c.usage_guest_nice
FROM cpu c
JOIN dictionaries d_hostname ON d_hostname."table" = 'cpu' AND d_hostname."column" = 'hostname' AND d_hostname.id = c.hostname
JOIN dictionaries d_region ON d_region."table" = 'cpu' AND d_region."column" = 'region' AND d_region.id = c.region
JOIN dictionaries d_datacenter ON d_datacenter."table" = 'cpu' AND d_datacenter."column" = 'datacenter' AND d_datacenter.id = c.datacenter
JOIN dictionaries d_rack ON d_rack."table" = 'cpu' AND d_rack."column" = 'rack' AND d_rack.id = c.rack
JOIN dictionaries d_os ON d_os."table" = 'cpu' AND d_os."column" = 'os' AND d_os.id = c.os
JOIN dictionaries d_arch ON d_arch."table" = 'cpu' AND d_arch."column" = 'arch' AND d_arch.id = c.arch
JOIN dictionaries d_team ON d_team."table" = 'cpu' AND d_team."column" = 'team' AND d_team.id = c.team
JOIN dictionaries d_service ON d_service."table" = 'cpu' AND d_service."column" = 'service' AND d_service.id = c.service
JOIN dictionaries d_service_version ON d_service_version."table" = 'cpu' AND d_service_version."column" = 'service_version' AND d_service_version.id = c.service_version
JOIN dictionaries d_service_environment ON d_service_environment."table" = 'cpu' AND d_service_environment."column" = 'service_environment' AND d_service_environment.id = c.service_environment
WHERE c.usage_user > 90.0 AND c.ts >= {START} AND c.ts < {END}
  AND c.hostname IN (SELECT id FROM dictionaries WHERE "table" = 'cpu' AND "column" = 'hostname' AND name IN ({HOSTS}))

-- @query high-cpu-all
SELECT c.ts, d_hostname.name AS hostname, d_region.name AS region, d_datacenter.name AS datacenter, d_rack.name AS rack, d_os.name AS os, d_arch.name AS arch, d_team.name AS team, d_service.name AS service, d_service_version.name AS service_version, d_service_environment.name AS service_environment,
       c.usage_user, c.usage_system, c.usage_idle, c.usage_nice, c.usage_iowait,
       c.usage_irq, c.usage_softirq, c.usage_steal, c.usage_guest, c.usage_guest_nice
FROM cpu c
JOIN dictionaries d_hostname ON d_hostname."table" = 'cpu' AND d_hostname."column" = 'hostname' AND d_hostname.id = c.hostname
JOIN dictionaries d_region ON d_region."table" = 'cpu' AND d_region."column" = 'region' AND d_region.id = c.region
JOIN dictionaries d_datacenter ON d_datacenter."table" = 'cpu' AND d_datacenter."column" = 'datacenter' AND d_datacenter.id = c.datacenter
JOIN dictionaries d_rack ON d_rack."table" = 'cpu' AND d_rack."column" = 'rack' AND d_rack.id = c.rack
JOIN dictionaries d_os ON d_os."table" = 'cpu' AND d_os."column" = 'os' AND d_os.id = c.os
JOIN dictionaries d_arch ON d_arch."table" = 'cpu' AND d_arch."column" = 'arch' AND d_arch.id = c.arch
JOIN dictionaries d_team ON d_team."table" = 'cpu' AND d_team."column" = 'team' AND d_team.id = c.team
JOIN dictionaries d_service ON d_service."table" = 'cpu' AND d_service."column" = 'service' AND d_service.id = c.service
JOIN dictionaries d_service_version ON d_service_version."table" = 'cpu' AND d_service_version."column" = 'service_version' AND d_service_version.id = c.service_version
JOIN dictionaries d_service_environment ON d_service_environment."table" = 'cpu' AND d_service_environment."column" = 'service_environment' AND d_service_environment.id = c.service_environment
WHERE c.usage_user > 90.0 AND c.ts >= {START} AND c.ts < {END}

-- @query groupby-orderby-limit
SELECT date_bin(INTERVAL '1 minute', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE ts < {END}
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5

-- @query lastpoint
SELECT c.ts, d_hostname.name AS hostname, d_region.name AS region, d_datacenter.name AS datacenter, d_rack.name AS rack, d_os.name AS os, d_arch.name AS arch, d_team.name AS team, d_service.name AS service, d_service_version.name AS service_version, d_service_environment.name AS service_environment,
       c.usage_user, c.usage_system, c.usage_idle, c.usage_nice, c.usage_iowait,
       c.usage_irq, c.usage_softirq, c.usage_steal, c.usage_guest, c.usage_guest_nice
FROM cpu c
JOIN (SELECT hostname AS host_id, max(ts) AS last_ts FROM cpu GROUP BY hostname) m
  ON c.hostname = m.host_id AND c.ts = m.last_ts
JOIN dictionaries d_hostname ON d_hostname."table" = 'cpu' AND d_hostname."column" = 'hostname' AND d_hostname.id = c.hostname
JOIN dictionaries d_region ON d_region."table" = 'cpu' AND d_region."column" = 'region' AND d_region.id = c.region
JOIN dictionaries d_datacenter ON d_datacenter."table" = 'cpu' AND d_datacenter."column" = 'datacenter' AND d_datacenter.id = c.datacenter
JOIN dictionaries d_rack ON d_rack."table" = 'cpu' AND d_rack."column" = 'rack' AND d_rack.id = c.rack
JOIN dictionaries d_os ON d_os."table" = 'cpu' AND d_os."column" = 'os' AND d_os.id = c.os
JOIN dictionaries d_arch ON d_arch."table" = 'cpu' AND d_arch."column" = 'arch' AND d_arch.id = c.arch
JOIN dictionaries d_team ON d_team."table" = 'cpu' AND d_team."column" = 'team' AND d_team.id = c.team
JOIN dictionaries d_service ON d_service."table" = 'cpu' AND d_service."column" = 'service' AND d_service.id = c.service
JOIN dictionaries d_service_version ON d_service_version."table" = 'cpu' AND d_service_version."column" = 'service_version' AND d_service_version.id = c.service_version
JOIN dictionaries d_service_environment ON d_service_environment."table" = 'cpu' AND d_service_environment."column" = 'service_environment' AND d_service_environment.id = c.service_environment
