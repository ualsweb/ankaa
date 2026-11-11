# TSBS cpu-only: gcp-c4a-standard-4 (2026-11-11T23:21:57+00:00)

Dataset: TSBS cpu-only, 25,920,000 rows, 1000 hosts, 2016-01-01T00:00:00Z .. 2016-01-04T00:00:00Z every 10 s, seed 123 (tsbs 8323e59c7402).  
Engines measured one at a time on the same machine; 20 runs after 3 warm-up per query.

## Load

| engine | method | rows/s | load s | post-load step | disk MiB | peak memory MiB (load) |
|---|---|---:|---:|---|---:|---:|
| ankaa | ankaad serve, [[sources]] kind=file format=csv (cpu.csv), wal_dir + checkpoint_dir | 1,106,552 | 23.4 | compress, persist, cut the log 262 s; idle after 5 s | 426 | 5,125 |
| influxdb3 | gRPC returned resource exhausted error, with message: Error while execution Flig | | | | | |
| timescaledb | parallel COPY FROM STDIN (FORMAT csv), 4 connections x 50000 rows/transaction; hypertable chunk_interval 12 hours, index (hostname, ts DESC) | 383,254 | 67.6 | columnstore 23.5 s; idle after 5 s | 1,311 | 37 |
| questdb | ILP over HTTP POST /write precision=n, 75000 lines/request, 4 concurrent; table pre-created (SYMBOL tags, DOUBLE fields, PARTITION BY DAY WAL) | 801,304 | 32.3 | idle after 10 s | 5,841 | 491 |

Peak memory: ankaad = VmHWM (peak resident set); containers = highest sampled anonymous memory of the container's cgroup (cgroup memory.peak, which also counts page cache, is in the JSON). Disk = `du -sk` of the data directories after the post-load step and idle wait.

## Queries

Cells: median ms (p95 ms), client-observed. `mismatch` = differs from the DuckDB reference (time not comparable); `unstable` = first answer differed, a repeat matched; `n/a` = not expressible; `†` = large result, transfer-dominated.

| query | ankaa | influxdb3 | timescaledb-rowstore | timescaledb | questdb |
|---|---:|---:|---:|---:|---:|
| single-groupby-1-1-1 | 0.737 (0.785) | error | 0.292 (0.560) | 0.223 (0.663) | 1.183 (2.503) |
| single-groupby-1-1-12 | 0.745 (0.766) | error | 2.328 (3.258) | 1.154 (1.684) | 3.984 (6.938) |
| single-groupby-1-8-1 | 0.735 (0.820) | error | 1.524 (2.076) | 0.741 (1.262) | 1.403 (2.429) |
| single-groupby-5-1-1 | 0.782 (0.839) | error | 0.411 (0.683) | 0.323 (0.688) | 1.297 (1.814) |
| single-groupby-5-1-12 | 0.878 (0.900) | error | 2.917 (3.944) | 1.874 (2.475) | 3.440 (4.390) |
| single-groupby-5-8-1 | 0.876 (0.899) | error | 1.798 (2.093) | 1.714 (2.276) | 1.104 (1.403) |
| cpu-max-all-1 | 0.877 (0.939) | error | 2.307 (2.359) | 1.078 (1.711) | 1.632 (1.911) |
| cpu-max-all-8 | 1.009 (1.081) | error | 15.7 (16.9) | 10.4 (11.2) | 4.591 (5.341) |
| double-groupby-1 | 14.6 (14.9) | error | 1051.7 (1054.9) | 671.4 (678.3) | 71.4 (90.8) |
| double-groupby-5 | 34.1 (34.5) | error | 1348.5 (1353.1) | 1080.6 (1085.5) | 93.2 (100.4) |
| double-groupby-all | 59.2 (59.6) | error | 1707.0 (1717.7) | 1568.2 (1574.9) | 138.1 (142.4) |
| high-cpu-1 | 0.833 (0.895) | error | 4.070 (4.645) | 1.605 (2.213) | 6.015 (7.032) |
| high-cpu-all † | 58.2 (62.9) | error | 1822.9 (1836.0) | 1370.8 (1389.6) | 630.8 (633.9) |
| groupby-orderby-limit | 8.771 (8.943) | error | 4.125 (4.406) | 44.1 (44.8) | 9.440 (11.4) |
| lastpoint | 0.799 (0.852) | error | 30.6 (31.1) | 386.5 (387.4) | 2.278 (2.727) |

Engines and clients:

- `ankaa` (ankaad 0.1.0): client-observed over Arrow Flight (pyarrow, DoGet with the SQL as ticket) until the last batch
- `influxdb3` (influxdb3 3.12.2 (revision cc9769917a)): -
- `timescaledb-rowstore` (timescaledb 2.31.0 on PostgreSQL 17.11 (rowstore, before columnstore conversion)): client-observed over PostgreSQL wire (psycopg 3, binary results): execute + fetchall + Arrow
- `timescaledb` (timescaledb 2.31.0 on PostgreSQL 17.11): client-observed over PostgreSQL wire (psycopg 3, binary results): execute + fetchall + Arrow
- `questdb` (questdb Build Information: QuestDB 10.1.0): client-observed over PostgreSQL wire (psycopg): execute + fetchall

Environment: hostname=gcp-c4a-standard-4; system=Linux; release=7.0.0-1017-gcp; machine=aarch64; git_commit=5e5c758a70159ade53c1fdf282b093e95f345529; dmi_product_name=Google Compute Engine

Label: c4a-standard-4 (4 vCPU Axion, 16 GB), Ubuntu 24.04
