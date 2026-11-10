"""In-process `open_capture` over the Binance fixture."""

from __future__ import annotations

import threading
import time

import pyarrow as pa
import pytest

import ankaa


def test_version():
    assert ankaa.__version__ == ankaa._native.__version__
    assert ankaa.__version__.count(".") == 2


def test_tables_and_stats(dataset):
    assert dataset.tables() == ["book_deltas", "quotes", "symbols", "trades"]
    st = dataset.stats()
    for k in ("frames", "events", "trades", "deltas", "gaps"):
        assert k in st, k
    assert st["frames"] > 0
    assert st["trades"] > 0
    assert st["deltas"] > 0
    assert st["events"] >= st["trades"] + st["deltas"]
    assert st["gaps"] == 0
    assert "Dataset(" in repr(dataset)


def test_count(dataset):
    t = dataset.sql("SELECT count(*) AS n FROM trades")
    assert isinstance(t, pa.Table)
    assert t.num_rows == 1
    n = t.column("n")[0].as_py()
    assert n == dataset.stats()["trades"]


def test_group_by(dataset):
    t = dataset.sql(
        "SELECT s.name, t.side, count(*) AS n, sum(t.size) / 1e8 AS qty "
        "FROM trades t JOIN symbols s ON s.symbol = t.symbol "
        "GROUP BY s.name, t.side ORDER BY s.name, t.side"
    )
    assert t.num_rows >= 1
    names = set(t.column("name").to_pylist())
    assert names == {"BTCUSDT"}
    assert sum(t.column("n").to_pylist()) == dataset.stats()["trades"]
    assert all(q > 0 for q in t.column("qty").to_pylist())


def test_schema(dataset):
    s = dataset.schema("SELECT ts, price, symbol FROM trades")
    assert isinstance(s, pa.Schema)
    assert s.names == ["ts", "price", "symbol"]
    assert pa.types.is_timestamp(s.field("ts").type)
    assert s.field("ts").type.unit == "ns"
    assert s.field("price").type == pa.int64()
    assert s.field("price").metadata[b"ankaa.scale"] == b"8"


def test_ohlcv(dataset):
    t = dataset.sql(
        "SELECT symbol, bucket, open, high, low, close, volume, count "
        "FROM ohlcv('trades', 1000000000) ORDER BY bucket"
    )
    assert t.num_rows > 1
    rows = t.to_pylist()
    assert sum(r["count"] for r in rows) == dataset.stats()["trades"]
    for r in rows:
        assert r["low"] <= r["open"] <= r["high"]
        assert r["low"] <= r["close"] <= r["high"]
    buckets = [r["bucket"] for r in rows]
    assert buckets == sorted(buckets)


def test_asof_join(dataset):
    t = dataset.sql(
        "SELECT ts, price, quotes_bid, quotes_ask "
        "FROM asof_join('trades', 'quotes', 'symbol') ORDER BY ts"
    )
    assert t.num_rows == dataset.stats()["trades"]
    matched = t.filter(pa.compute.is_valid(t.column("quotes_bid")))
    assert matched.num_rows > 0, "no trade found a quote at or before it"
    for r in matched.to_pylist():
        assert r["quotes_bid"] <= r["quotes_ask"]


def test_percentiles(dataset):
    t = dataset.sql("SELECT count, p50, p99 FROM percentiles('trades', 'size', '0.5,0.99')")
    r = t.to_pylist()[0]
    assert r["count"] == dataset.stats()["trades"]
    assert 0 < r["p50"] <= r["p99"]


def test_bad_sql_raises(dataset):
    with pytest.raises(ValueError):
        dataset.sql("SELECT nope FROM nowhere")


def test_zero_copy_handoff(dataset):
    raw = dataset.sql_arrow("SELECT ts, price, size FROM trades")
    assert hasattr(raw, "__arrow_c_stream__")
    assert hasattr(raw, "__arrow_c_schema__")
    t = pa.table(raw)
    assert t.nbytes > 0
    assert t.num_rows == dataset.stats()["trades"]
    # the same native object can be exported more than once (each export is a fresh stream)
    t2 = pa.RecordBatchReader.from_stream(raw).read_all()
    assert t2.equals(t)


def test_polars(dataset):
    pl = pytest.importorskip("polars")
    df = dataset.sql_polars("SELECT ts, price / 1e8 AS price FROM trades ORDER BY ts LIMIT 10")
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (10, 2)
    assert df["price"].dtype == pl.Float64
    df2 = ankaa.to_polars(dataset.sql("SELECT count(*) AS n FROM quotes"))
    assert df2["n"][0] > 0


def test_pandas(dataset):
    pytest.importorskip("pandas")
    df = dataset.sql("SELECT ts, price FROM trades LIMIT 5").to_pandas()
    assert len(df) == 5
    assert str(df["ts"].dtype).startswith("datetime64[ns")


def test_gil_released_concurrent_queries(dataset):
    """Queries from several threads run concurrently (GIL released in `sql`) and stay correct."""
    q = (
        "SELECT ts, price, quotes_bid FROM asof_join('trades', 'quotes', 'symbol') "
        "ORDER BY ts"
    )
    expected = dataset.sql(q)
    n_threads, per_thread = 4, 5
    errors: list[BaseException] = []
    results: list[int] = []
    barrier = threading.Barrier(n_threads)

    def worker():
        try:
            barrier.wait()
            for _ in range(per_thread):
                t = dataset.sql(q)
                assert t.equals(expected)
                results.append(t.num_rows)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    concurrent = time.perf_counter() - t0
    assert not errors, errors
    assert len(results) == n_threads * per_thread

    t0 = time.perf_counter()
    for _ in range(n_threads * per_thread):
        dataset.sql(q)
    serial = time.perf_counter() - t0
    # Informative only: with the GIL held the concurrent run could not be faster than serial.
    print(f"\nconcurrent {concurrent*1e3:.0f} ms vs serial {serial*1e3:.0f} ms")


def test_trivial_query_overhead(dataset):
    """Per-call overhead of Dataset.sql for a trivial query (informative, printed with -s)."""
    dataset.sql("SELECT 1 AS x")
    n = 200
    t0 = time.perf_counter()
    for _ in range(n):
        dataset.sql("SELECT 1 AS x")
    per_call = (time.perf_counter() - t0) / n
    print(f"\nDataset.sql('SELECT 1') per call: {per_call*1e6:.0f} us")
    assert per_call < 0.5


def test_open_missing_file():
    with pytest.raises(ValueError):
        ankaa.open_capture("/nonexistent/capture.ankc")
