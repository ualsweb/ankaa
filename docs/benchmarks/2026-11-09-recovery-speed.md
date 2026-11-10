# WAL recovery speed (F-19) — development signal

**Date:** 2026-11-09 · **Host:** Apple M4 Pro laptop, macOS, release build, APFS SSD · **Not a product claim**
(`docs/measurement-protocol.md`): the target "10 GB WAL recovered in < 60 s" is measured on server hardware with NVMe.

## Method
`crates/ankaa-feeds/examples/make_big_wal.rs` writes the bundled 20 s Binance capture 400 times into a WAL,
shifting exchange timestamps by one day and all update/trade ids by 1e9 per copy so every replayed row is
appended (nothing is dropped as out of order). Then `ankaad serve --wal-dir <dir> --duration-secs 1` recovers
through the normal decode → book sync → append path and logs the time.

```sh
cargo build --release -p ankaad -p ankaa-feeds --example make_big_wal
target/release/examples/make_big_wal bench/data/binance-btcusdt-trade-depth-20s.ankc /tmp/ankaa-bigwal 400
ANKAA_TOKEN=devtoken target/release/ankaad serve --wal-dir /tmp/ankaa-bigwal --no-depth-snapshots \
  --streams btcusdt@trade --duration-secs 1
```

## Result
| WAL | Frames | Rows appended | Recovery time | Throughput |
|---|---|---|---|---|
| 73.2 MiB (1 segment) | 163,600 | 1,483,600 | 214 ms | ≈ 342 MiB/s · ≈ 6.9 M rows/s · ≈ 0.76 M frames/s |

Linear extrapolation to a 10 GB WAL on this laptop: ≈ 30 s. The path is CPU-bound (JSON decode + book
updates), so the server figure depends on single-core speed and page-cache state more than on the SSD.
Run the same two commands on the target machine before quoting a number.
