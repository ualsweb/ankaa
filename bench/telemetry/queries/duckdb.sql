-- DuckDB dialect: the REFERENCE every other engine is diffed against. Written for obviousness, not speed.
-- Table cpu (bench/telemetry/convert.py SCHEMA): ts TIMESTAMP_NS (UTC), 10 tags VARCHAR, 10 fields DOUBLE.
-- {START}/{END} render as TIMESTAMP_NS literals, {HOSTS} as a list of string literals.
-- Buckets: date_trunc (UTC, epoch-aligned for minute and hour).

-- @query single-groupby-1-1-1
SELECT date_trunc('minute', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute
ORDER BY minute

-- @query single-groupby-1-1-12
SELECT date_trunc('minute', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute
ORDER BY minute

-- @query single-groupby-1-8-1
SELECT date_trunc('minute', ts) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute
ORDER BY minute

-- @query single-groupby-5-1-1
SELECT date_trunc('minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute
ORDER BY minute

-- @query single-groupby-5-1-12
SELECT date_trunc('minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute
ORDER BY minute

-- @query single-groupby-5-8-1
SELECT date_trunc('minute', ts) AS minute,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY minute
ORDER BY minute

-- @query cpu-max-all-1
SELECT date_trunc('hour', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY hour
ORDER BY hour

-- @query cpu-max-all-8
SELECT date_trunc('hour', ts) AS hour,
       max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
GROUP BY hour
ORDER BY hour

-- @query double-groupby-1
SELECT date_trunc('hour', ts) AS hour, hostname, avg(usage_user) AS mean_usage_user
FROM cpu
WHERE ts >= {START} AND ts < {END}
GROUP BY hour, hostname
ORDER BY hour, hostname

-- @query double-groupby-5
SELECT date_trunc('hour', ts) AS hour, hostname,
       avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
       avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
FROM cpu
WHERE ts >= {START} AND ts < {END}
GROUP BY hour, hostname
ORDER BY hour, hostname

-- @query double-groupby-all
SELECT date_trunc('hour', ts) AS hour, hostname,
       avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
       avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq,
       avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest,
       avg(usage_guest_nice) AS mean_usage_guest_nice
FROM cpu
WHERE ts >= {START} AND ts < {END}
GROUP BY hour, hostname
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
SELECT date_trunc('minute', ts) AS minute, max(usage_user) AS max_usage_user
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
