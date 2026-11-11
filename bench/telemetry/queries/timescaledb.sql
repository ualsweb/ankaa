-- TimescaleDB dialect. Shapes are those of TSBS's TimescaleDB generator
-- (tsbs/cmd/tsbs_generate_queries/databases/timescaledb/devops.go, use-time-bucket=true, tags in the table:
-- `hostname IN (...)`), with the column `ts` instead of `time` and explicit column lists instead of `*`.
-- Table: hypertable cpu(ts timestamptz, 10 tags text, 10 fields double precision), see harness/engines.py.
-- time_bucket for intervals below a month aligns to 2000-01-03 00:00 UTC, which is a whole number of hours
-- after the Unix epoch: 1-minute and 1-hour buckets are the same as date_bin / date_trunc elsewhere.
-- https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/
-- {START}/{END} render as timestamptz literals.

-- @query single-groupby-1-1-1
SELECT time_bucket('60 seconds', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute ORDER BY minute ASC

-- @query single-groupby-1-1-12
SELECT time_bucket('60 seconds', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute ORDER BY minute ASC

-- @query single-groupby-1-8-1
SELECT time_bucket('60 seconds', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute ORDER BY minute ASC

-- @query single-groupby-5-1-1
SELECT time_bucket('60 seconds', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute ORDER BY minute ASC

-- @query single-groupby-5-1-12
SELECT time_bucket('60 seconds', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute ORDER BY minute ASC

-- @query single-groupby-5-8-1
SELECT time_bucket('60 seconds', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute ORDER BY minute ASC

-- @query cpu-max-all-1
SELECT time_bucket('3600 seconds', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY hour ORDER BY hour

-- @query cpu-max-all-8
SELECT time_bucket('3600 seconds', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY hour ORDER BY hour

-- @query double-groupby-1
WITH cpu_avg AS (
  SELECT time_bucket('3600 seconds', ts) AS hour, hostname, avg(usage_user) AS mean_usage_user
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  GROUP BY 1, 2
)
SELECT hour, hostname, mean_usage_user
FROM cpu_avg
ORDER BY hour, hostname

-- @query double-groupby-5
WITH cpu_avg AS (
  SELECT time_bucket('3600 seconds', ts) AS hour, hostname,
         avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
         avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  GROUP BY 1, 2
)
SELECT hour, hostname, mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait
FROM cpu_avg
ORDER BY hour, hostname

-- @query double-groupby-all
WITH cpu_avg AS (
  SELECT time_bucket('3600 seconds', ts) AS hour, hostname,
         avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
         avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq,
         avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest,
         avg(usage_guest_nice) AS mean_usage_guest_nice
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  GROUP BY 1, 2
)
SELECT hour, hostname, mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait,
       mean_usage_irq, mean_usage_softirq, mean_usage_steal, mean_usage_guest, mean_usage_guest_nice
FROM cpu_avg
ORDER BY hour, hostname

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
SELECT time_bucket('60 seconds', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE ts < {END}
GROUP BY minute
ORDER BY minute DESC
LIMIT 5

-- @query lastpoint
SELECT DISTINCT ON (hostname)
       ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
ORDER BY hostname, ts DESC
