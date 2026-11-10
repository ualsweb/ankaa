# ANKAA from Python

`ankaa` is the Python package for ANKAA-ENGINE. It runs SQL over an `ankaad` server (Arrow
Flight) or over a `.ankc` capture file in-process, and hands results to Python as Arrow tables
through the Arrow C Data Interface (PyCapsule protocol): the column buffers move from Rust to
`pyarrow` / `polars` without a row copy.

## Install

Until the package is on PyPI, build it from the repository with [maturin](https://www.maturin.rs):

```sh
cd crates/ankaa-py
python3 -m venv .venv && . .venv/bin/activate
pip install maturin pyarrow            # polars, pandas, matplotlib, jupyter as you like
maturin develop --release              # builds the native module into the venv
python -c "import ankaa; print(ankaa.__version__)"
```

Requirements: Rust 1.95+ (see the repository's `rust-toolchain` / `docs/dev-environment.md`),
Python 3.10+. The wheel uses the stable ABI (`abi3`), so one build serves every CPython >= 3.10.

Once published: `pip install ankaa`.

## Quickstart

`quickstart.ipynb` opens the fixture capture shipped with the repository
(`crates/ankaa-feeds/tests/fixtures/binance-btcusdt-trade-depth-60.ankc`, a 60-frame sample of Binance
BTCUSDT trades and depth diffs, a few seconds of data) and runs five queries: trades per second, 1 s OHLCV, an ASOF
join of trades to the top of book, trade-size percentiles and the top-of-book spread. Each
result is converted to pandas or polars and one is plotted with matplotlib.

```sh
pip install jupyter pandas polars matplotlib
jupyter lab examples/python/quickstart.ipynb
```

## API in one screen

```python
import ankaa

ds = ankaa.open_capture("capture.ankc")        # Dataset: in-process DataFusion
ds.tables()                                    # ['book_deltas', 'quotes', 'symbols', 'trades']
ds.stats()                                     # frames, events, trades, deltas, gaps, ...
ds.sql("SELECT count(*) FROM trades")          # pyarrow.Table
ds.sql_polars("SELECT * FROM quotes")          # polars.DataFrame
ds.schema("SELECT ts, price FROM trades")      # pyarrow.Schema, nothing executed

c = ankaa.connect("127.0.0.1:50051", token="...",            # Client: Arrow Flight
                  tls_ca="ca.pem", tls_cert="c.pem", tls_key="k.pem")  # TLS / mTLS optional
c.tables(); c.sql("..."); c.schema("...")
```

Tables and SQL functions are the ones `ankaad` exposes (`docs/data-model.md`):

| table | columns |
|---|---|
| `trades` | `ts, symbol, price(8), size(8), side, seq` |
| `book_deltas` | `ts, symbol, side, price(8), size(8), first_update_id, update_id` |
| `quotes` | `ts, symbol, bid(8), bid_size(8), ask(8), ask_size(8), update_id` |
| `symbols` | `symbol, name` |

`(8)` columns are `Int64` ticks with eight decimals: divide by `1e8`. `side` is `0 = buy/bid`,
`1 = sell/ask`. Table functions: `ohlcv('trades', bucket_ns)`,
`asof_join('trades', 'quotes', 'symbol'[, tolerance_ns])`,
`percentiles('trades', 'size', '0.5,0.99'[, 'symbol'])`.

## Threads

`Dataset.sql` and `Client.sql` release the GIL while the engine or the network runs, so
queries from several Python threads execute concurrently. A `Dataset` or `Client` owns a small
tokio runtime; create one per process (or per capture) and share it.

## Tests

```sh
cd crates/ankaa-py
maturin develop --release --features test-server   # adds an in-process Flight server for the tests
pytest
```

`ANKAA_LIVE_TEST=1 ANKAA_ADDR=host:port ANKAA_TOKEN=... pytest tests/test_flight.py -k live`
runs one test against a real `ankaad serve`.
