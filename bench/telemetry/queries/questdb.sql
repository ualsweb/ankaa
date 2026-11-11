-- QuestDB dialect. Shapes are those of TSBS's QuestDB generator
-- (tsbs/cmd/tsbs_generate_queries/databases/questdb/devops.go): SAMPLE BY for time buckets, LATEST ON for
-- the last point, `hostname IN (...)` on a SYMBOL column. Differences from TSBS, so that every engine answers
-- the same question:
--   * designated timestamp is `ts` (TSBS: `timestamp`) and columns are listed instead of `*`;
--   * ALIGN TO CALENDAR is written out (it is the default since QuestDB 7.4) so buckets start on whole
--     minutes / hours like date_bin / time_bucket elsewhere;
--   * results are wrapped in ORDER BY so the row order is the same as in the other dialects;
--   * groupby-orderby-limit takes the 5 LATEST minutes before END (ORDER BY minute DESC LIMIT 5), as in
--     the TimescaleDB and InfluxDB generators. TSBS's QuestDB query (`SAMPLE BY 1m LIMIT 5` without an
--     ORDER BY) returns the 5 earliest minutes instead, a different (cheaper) question.
-- Table: cpu (tags SYMBOL, fields DOUBLE, ts TIMESTAMP designated, PARTITION BY DAY WAL), see harness/engines.py.
-- {START}/{END} render as ISO-8601 string literals (microseconds); QuestDB stores microseconds (TSBS stamps
-- are whole seconds, nothing is lost).

-- @query single-groupby-1-1-1
SELECT * FROM (
  SELECT ts AS minute, max(usage_user) AS max_usage_user
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute

-- @query single-groupby-1-1-12
SELECT * FROM (
  SELECT ts AS minute, max(usage_user) AS max_usage_user
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute

-- @query single-groupby-1-8-1
SELECT * FROM (
  SELECT ts AS minute, max(usage_user) AS max_usage_user
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute

-- @query single-groupby-5-1-1
SELECT * FROM (
  SELECT ts AS minute,
         max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
         max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute

-- @query single-groupby-5-1-12
SELECT * FROM (
  SELECT ts AS minute,
         max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
         max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute

-- @query single-groupby-5-8-1
SELECT * FROM (
  SELECT ts AS minute,
         max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
         max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute

-- @query cpu-max-all-1
SELECT * FROM (
  SELECT ts AS hour,
         max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
         max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
         max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
         max(usage_guest_nice) AS max_usage_guest_nice
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1h ALIGN TO CALENDAR
) ORDER BY hour

-- @query cpu-max-all-8
SELECT * FROM (
  SELECT ts AS hour,
         max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle,
         max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq,
         max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest,
         max(usage_guest_nice) AS max_usage_guest_nice
  FROM cpu
  WHERE hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}
  SAMPLE BY 1h ALIGN TO CALENDAR
) ORDER BY hour

-- @query double-groupby-1
SELECT * FROM (
  SELECT ts AS hour, hostname, avg(usage_user) AS mean_usage_user
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  SAMPLE BY 1h ALIGN TO CALENDAR
) ORDER BY hour, hostname

-- @query double-groupby-5
SELECT * FROM (
  SELECT ts AS hour, hostname,
         avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
         avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  SAMPLE BY 1h ALIGN TO CALENDAR
) ORDER BY hour, hostname

-- @query double-groupby-all
SELECT * FROM (
  SELECT ts AS hour, hostname,
         avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle,
         avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq,
         avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest,
         avg(usage_guest_nice) AS mean_usage_guest_nice
  FROM cpu
  WHERE ts >= {START} AND ts < {END}
  SAMPLE BY 1h ALIGN TO CALENDAR
) ORDER BY hour, hostname

-- @query high-cpu-1
SELECT ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
WHERE usage_user > 90.0 AND hostname IN ({HOSTS}) AND ts >= {START} AND ts < {END}

-- @query high-cpu-all
SELECT ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
WHERE usage_user > 90.0 AND ts >= {START} AND ts < {END}

-- @query groupby-orderby-limit
SELECT * FROM (
  SELECT ts AS minute, max(usage_user) AS max_usage_user
  FROM cpu
  WHERE ts < {END}
  SAMPLE BY 1m ALIGN TO CALENDAR
) ORDER BY minute DESC
LIMIT 5

-- @query lastpoint
SELECT ts, hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment,
       usage_user, usage_system, usage_idle, usage_nice, usage_iowait, usage_irq, usage_softirq, usage_steal,
       usage_guest, usage_guest_nice
FROM cpu
LATEST ON ts PARTITION BY hostname
