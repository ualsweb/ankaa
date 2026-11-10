"""Flight client against an in-process server (cargo feature `test-server`) or a live `ankaad`."""

from __future__ import annotations

import threading

import pyarrow as pa
import pytest

import ankaa
from ankaa import _native

TOKEN = "test-token-0123456789"


@pytest.fixture(scope="module")
def server(fixture_path):
    if not getattr(_native, "HAS_TEST_SERVER", False):
        pytest.skip("build with `maturin develop --features test-server` for the Flight tests")
    port, handle = _native._test_server(fixture_path, TOKEN)
    assert port == handle.port
    yield handle
    handle.stop()


@pytest.fixture(scope="module")
def client(server):
    return ankaa.connect(server.addr, token=TOKEN)


def test_tables(client):
    assert client.tables() == ["book_deltas", "quotes", "symbols", "trades"]
    assert "Client(" in repr(client)
    assert client.addr.startswith("127.0.0.1:")


def test_sql(client, dataset):
    t = client.sql("SELECT count(*) AS n FROM trades")
    assert isinstance(t, pa.Table)
    assert t.column("n")[0].as_py() == dataset.stats()["trades"]
    remote = client.sql("SELECT ts, price, size FROM trades ORDER BY ts")
    local = dataset.sql("SELECT ts, price, size FROM trades ORDER BY ts")
    assert remote.equals(local)
    assert remote.nbytes > 0


def test_sql_table_function(client):
    t = client.sql("SELECT bucket, close FROM ohlcv('trades', 1000000000) ORDER BY bucket")
    assert t.num_rows > 1


def test_empty_result_keeps_schema(client):
    t = client.sql("SELECT ts, price FROM trades WHERE price < 0")
    assert t.num_rows == 0
    assert t.schema.names == ["ts", "price"]


def test_schema(client):
    s = client.schema("SELECT ts, price FROM trades")
    assert isinstance(s, pa.Schema)
    assert s.names == ["ts", "price"]


def test_bad_sql(client):
    with pytest.raises(ValueError):
        client.sql("SELECT nope FROM nowhere")


def test_ddl_rejected(client):
    with pytest.raises(ValueError):
        client.sql("CREATE TABLE x (a int)")


def test_wrong_token(server):
    bad = ankaa.connect(server.addr, token="nope")
    with pytest.raises(ConnectionError):
        bad.tables()
    anon = ankaa.connect(server.addr)
    with pytest.raises(ConnectionError):
        anon.sql("SELECT 1")


def test_connect_refused():
    with pytest.raises(ConnectionError):
        ankaa.connect("127.0.0.1:9", connect_timeout_s=2.0).tables()


def test_concurrent_clients(client):
    errors: list[BaseException] = []

    def worker():
        try:
            for _ in range(5):
                t = client.sql("SELECT count(*) AS n FROM book_deltas")
                assert t.num_rows == 1
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=worker) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors, errors


def test_live_server(live_addr):
    """Only with ANKAA_LIVE_TEST=1 (and ANKAA_ADDR / ANKAA_TOKEN) against a running `ankaad serve`."""
    if live_addr is None:
        pytest.skip("set ANKAA_LIVE_TEST=1 to query a running ankaad")
    import os

    c = ankaa.connect(live_addr, token=os.environ.get("ANKAA_TOKEN"))
    assert "trades" in c.tables()
    assert c.sql("SELECT count(*) AS n FROM trades").num_rows == 1
