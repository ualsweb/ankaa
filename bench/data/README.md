# Sample data

`binance-btcusdt-trade-depth-20s.ankc`: 20 seconds of Binance spot public market data (`btcusdt@trade`
and `btcusdt@depth@100ms`) recorded on 2026-11-09 with `ankaa-capture` from the public, keyless
`data-stream.binance.vision` endpoint. It is included only as a small, fixed input for tests, demos and
regression benchmarks of the decoder and store. Market data remains subject to the originating venue's
terms; do not use this file for anything other than exercising this software. Regenerate your own with
`cargo run -p ankaa-feeds --features capture --bin ankaa-capture`.
