-- InfluxDB 3 Core dialect (SQL, Apache DataFusion). Shapes follow TSBS's InfluxDB generator
-- (tsbs/cmd/tsbs_generate_queries/databases/influx/devops.go), which is written in InfluxQL for InfluxDB 1.x;
-- here the same questions in InfluxDB 3 SQL as InfluxData documents it:
--   time buckets  date_bin(INTERVAL ..., time)  https://docs.influxdata.com/influxdb3/core/query-data/sql/aggregate-select/
--   last point    DISTINCT ON (hostname) ... ORDER BY hostname, time DESC (DataFusion SQL); InfluxDB 3's Last
--                 Value Cache is a separate feature that answers from memory and is not used here (see README).
-- Table cpu is created by the first write: tags are Dictionary(Int32, Utf8), fields Float64, `time` is
-- Timestamp(ns). Columns are aliased so every engine returns the same names (`time AS ts`).
-- {START}/{END} render as RFC 3339 string literals (DataFusion coerces them to the time column).

-- @query single-groupby-1-1-1
SELECT date_bin(INTERVAL '1 minute', time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-1-1-12
SELECT date_bin(INTERVAL '1 minute', time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-1-8-1
SELECT date_bin(INTERVAL '1 minute', time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-1-1
SELECT date_bin(INTERVAL '1 minute', time) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-1-12
SELECT date_bin(INTERVAL '1 minute', time) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query single-groupby-5-8-1
SELECT date_bin(INTERVAL '1 minute', time) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query cpu-max-all-1
SELECT date_bin(INTERVAL '1 hour', time) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query cpu-max-all-8
SELECT date_bin(INTERVAL '1 hour', time) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND time >= {START} AND time < {END}
GROUP BY 1 ORDER BY 1

-- @query double-groupby-1
SELECT date_bin(INTERVAL '1 hour', time) AS hour, hostname, avg(usage_user) AS mean_usage_user
FROM cpu
WHERE time >= {START} AND time < {END}
GROUP BY 1, 2 ORDER BY 1, 2

-- @query double-groupby-5
SELECT date_bin(INTERVAL '1 hour', time) AS hour, hostname,
       avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
       avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
FROM cpu
WHERE time >= {START} AND time < {END}
GROUP BY 1, 2 ORDER BY 1, 2

-- @query double-groupby-all
SELECT date_bin(INTERVAL '1 hour', time) AS hour, hostname,
       avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
       avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq,
       avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest,
       avg(usage_guest_nice) AS mean_usage_guest_nice
FROM cpu
WHERE time >= {START} AND time < {END}
GROUP BY 1, 2 ORDER BY 1, 2

-- @query high-cpu-1
SELECT time AS ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
WHERE usage_user > 90.0 AND hostname IN ({HOSTS}) AND time >= {START} AND time < {END}

-- @query high-cpu-all
SELECT time AS ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
WHERE usage_user > 90.0 AND time >= {START} AND time < {END}

-- @query groupby-orderby-limit
SELECT date_bin(INTERVAL '1 minute', time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE time < {END}
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5

-- @query lastpoint
SELECT DISTINCT ON (hostname)
       time AS ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
ORDER BY hostname, time DESC
