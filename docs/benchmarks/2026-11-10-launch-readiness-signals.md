# Launch-readiness pass — 2026-11-10

Measurements taken while reviewing and hardening the engine before its first run on real
hardware. Same tools as the earlier passes (criterion, wall-clock runs of the release binary) on
an Apple M4 Pro laptop. **Development signals, not product claims** (`docs/measurement-protocol.md`):
they show the order of magnitude of new code paths and whether a change moved something, nothing
more. The numbers that count come from `scripts/first-light.sh` on an Arm server.

## New paths (criterion, `bench/benches/hot_paths.rs`)

| Path | Input | Result |
|---|---|---|
| `generic_ingest/map_json_1024` | 1,024 JSON messages, 6 columns (nested timestamp and key, decimal string, int, bool, float) | 497 µs: about 2.1 M messages/s per core. Almost all of it is building the JSON tree. |
| `generic_ingest/map_csv_1024` | the same rows as CSV | 100 µs: about 10 M messages/s per core |
| `continuous_100k/ohlcv_push_16_keys` | 100,000 rows into incremental OHLCV, 16 keys | 0.47 ms (about 210 M rows/s) |
| `continuous_100k/percentiles_push_16_keys` | 100,000 rows into per-key sketches | 2.1 ms (about 47 M rows/s) |
| `wal_tail/catch_up_100k_x_200B` | follower reading 100,000 records of 200 B from a 22 MB log | 6.3 ms (about 16 M records/s); before this pass a follower re-read the whole segment for every 4,096 records |
| `wal_tail/idle_poll` | follower with nothing new to read | 22 µs per poll (one `stat`, one directory listing; a caught-up standby polls 20 times a second) |
| `store/chunk_append_row` | one row into an open chunk | 14.6 ns (13.4 ns before columns were allocated lazily; a key now costs bytes instead of half a megabyte of reserved address space) |

The two-phase mapper (convert every column, then add new dictionary values) cost about 5 % on
the JSON path against the single-pass version it replaced.

## End to end (release binary, `scripts/first-light.sh` on the laptop)

| What | Input | Result |
|---|---|---|
| `ankaad replay` | 54.8 MB capture: the bundled 20 s Binance sample repeated 300 times, 122,100 frames, 1.13 M events | 0.185 s warm (five runs: 0.184–0.186 s after the first) |
| Crash recovery | the same capture ingested by `serve` with a WAL and segments, `kill -9`, restart | 11 segments re-attached in under 1 ms, 122,100 WAL frames replayed in 149 ms, serving again 0.21 s after process start, row counts identical before and after |

## What the recovery check found

The recovery check is new in this pass, and the first time it ran it failed: 65,100 trades before
the crash, 49,561 after. The log held 92,900 of the 122,100 frames. The ingest loop handed frames
to the WAL thread through a bounded queue and dropped them from the log (while still applying
them to the tables) when the queue was full, which a replay at full speed, or a Kafka backlog,
fills at once. The frames were queryable until the restart and then gone.

Ingest now waits for the log, so what is in the tables is in the log, and the group commit grows
with the time `fdatasync` takes (10 ms after the first buffered record, or 256 KiB with an empty
queue, or 8 MiB) so throughput follows the disk instead of a fixed batch size. With that change
the same run recovers every row.

## Not measured in this pass

- The effect of key pruning and partitioned scans on query latency (covered by correctness tests
  only): needs a table with many keys and many chunks, which the bundled sample does not give.
- Ingest throughput of `serve` with the WAL on a server-class NVMe volume: on the laptop the
  volume's `fsync` is the limit and says nothing about a server.
- Start-up time with continuous queries over a large history (the rebuild reads every retained
  input row once per query).
