# ankaa (Python)

Python bindings for ANKAA-ENGINE: run SQL over an `ankaad` Flight server or over a `.ankc`
capture file in-process, and get the result back as a `pyarrow.Table` through the Arrow
PyCapsule interface (no row copies between Rust and Python).

```python
import ankaa

ds = ankaa.open_capture("binance-btcusdt-trade-depth-60.ankc")
ds.sql("SELECT count(*) AS n FROM trades")           # pyarrow.Table
ds.sql_polars("SELECT * FROM ohlcv('trades', 1000000000)")  # polars.DataFrame

c = ankaa.connect("127.0.0.1:50051", token="...")
c.tables()
c.sql("SELECT symbol, count(*) FROM trades GROUP BY symbol")
```

Build from source: `pip install maturin && maturin develop --release` inside `crates/ankaa-py`
(see `examples/python/README.md` at the repository root).
