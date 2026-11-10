"""ANKAA-ENGINE Python bindings.

Two entry points:

* :func:`open_capture` builds an in-process dataset from a ``.ankc`` capture file (the same
  decode path as ``ankaad replay``) and runs SQL on it with DataFusion.
* :func:`connect` talks to a running ``ankaad serve`` over Arrow Flight.

Both return query results through the Arrow PyCapsule interface. When ``pyarrow`` is
installed the result is a :class:`pyarrow.Table`; otherwise the native object (which exposes
``__arrow_c_stream__`` / ``__arrow_c_schema__``) is returned and can be consumed by any Arrow
library (polars, nanoarrow, duckdb, ...).

Fixed-point columns (``price``, ``size``, ``bid``, ``ask``...) are ``Int64`` ticks with eight
decimal places: divide by ``1e8`` in SQL or after the fact. ``ts`` is a nanosecond UTC timestamp.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from . import _native

if TYPE_CHECKING:  # pragma: no cover
    import polars as pl
    import pyarrow as pa

__version__: str = _native.__version__
__all__ = [
    "Client",
    "Dataset",
    "connect",
    "open_capture",
    "to_polars",
    "to_pyarrow",
    "__version__",
]


def to_pyarrow(obj: Any) -> "pa.Table | Any":
    """Convert an Arrow-stream-bearing object to ``pyarrow.Table`` when pyarrow is installed.

    The conversion goes through ``__arrow_c_stream__`` (Arrow C Data Interface), so the column
    buffers are handed over, not copied. Without pyarrow the object is returned unchanged.
    """
    try:
        import pyarrow as pa
    except ImportError:
        return obj
    if isinstance(obj, pa.Table):
        return obj
    try:
        return pa.table(obj)
    except TypeError:
        return pa.RecordBatchReader.from_stream(obj).read_all()


def to_polars(obj: Any) -> "pl.DataFrame":
    """Convert an Arrow-stream-bearing object (or ``pyarrow.Table``) to ``polars.DataFrame``."""
    import polars as pl

    # `pl.DataFrame` consumes `__arrow_c_stream__` directly (a stream of struct arrays becomes
    # columns); `pl.from_arrow` would wrap the same stream in one Struct series.
    return pl.DataFrame(obj)


def _to_schema(obj: Any) -> "pa.Schema | Any":
    try:
        import pyarrow as pa
    except ImportError:
        return obj
    return pa.schema(obj)


class Dataset:
    """SQL over one ``.ankc`` capture, decoded in memory.

    Tables: ``trades`` (``ts, symbol, price, size, side, seq``), ``book_deltas``
    (``ts, symbol, side, price, size, first_update_id, update_id``), ``quotes`` (top of book
    after every exchange message: ``ts, symbol, bid, bid_size, ask, ask_size, update_id``) and
    ``symbols`` (``symbol, name``). Table functions: ``ohlcv(table, bucket_ns)``,
    ``asof_join(left, right, key[, tolerance_ns])``, ``percentiles(table, column, 'q1,q2'[, key])``.

    The GIL is released while decoding and while a query runs, so several Python threads can
    query the same dataset concurrently.
    """

    __slots__ = ("_native",)

    def __init__(self, native: "_native.Dataset") -> None:
        self._native = native

    @property
    def path(self) -> str:
        """Capture file this dataset was built from."""
        return self._native.path

    def sql(self, query: str) -> "pa.Table":
        """Run ``query`` and return the result as a ``pyarrow.Table``.

        Raises ``ValueError`` on SQL errors.
        """
        return to_pyarrow(self._native.sql(query))

    def sql_arrow(self, query: str) -> Any:
        """Run ``query`` and return the raw native result (PyCapsule interface, no pyarrow)."""
        return self._native.sql(query)

    def sql_polars(self, query: str) -> "pl.DataFrame":
        """Run ``query`` and return a ``polars.DataFrame``."""
        return to_polars(self._native.sql(query))

    def schema(self, query: str) -> "pa.Schema":
        """Result schema of ``query`` without executing it."""
        return _to_schema(self._native.schema(query))

    def tables(self) -> list[str]:
        """Registered table names."""
        return list(self._native.tables())

    def stats(self) -> dict[str, int]:
        """Decode counters: ``frames, events, trades, deltas, gaps, snapshots, dropped, ...``."""
        return dict(self._native.stats())

    def __repr__(self) -> str:
        return repr(self._native)


class Client:
    """Arrow Flight client bound to one ``ankaad serve`` endpoint.

    Every call sends the bearer token (when given) in the ``authorization`` metadata. The GIL is
    released while the request is in flight and while the result is decoded.
    """

    __slots__ = ("_native",)

    def __init__(self, native: "_native.Client") -> None:
        self._native = native

    @property
    def addr(self) -> str:
        """``host:port`` this client is connected to."""
        return self._native.addr

    def sql(self, query: str) -> "pa.Table":
        """Run ``query`` on the server (``GetFlightInfo`` + ``DoGet``) and return a ``pyarrow.Table``.

        Raises ``ValueError`` for SQL errors reported by the server and ``ConnectionError`` for
        authentication or transport failures.
        """
        return to_pyarrow(self._native.sql(query))

    def sql_arrow(self, query: str) -> Any:
        """Run ``query`` and return the raw native result (PyCapsule interface, no pyarrow)."""
        return self._native.sql(query)

    def sql_polars(self, query: str) -> "pl.DataFrame":
        """Run ``query`` and return a ``polars.DataFrame``."""
        return to_polars(self._native.sql(query))

    def schema(self, query: str) -> "pa.Schema":
        """Result schema of ``query`` as planned by the server (nothing is executed)."""
        return _to_schema(self._native.schema(query))

    def tables(self) -> list[str]:
        """Names of the tables the server exposes (``ListFlights``)."""
        return list(self._native.tables())

    def __repr__(self) -> str:
        return repr(self._native)


def open_capture(path: str, expect_snapshots: bool = False) -> Dataset:
    """Decode a ``.ankc`` capture into in-memory tables and return a :class:`Dataset`.

    :param path: capture file written by ``ankaa-capture`` (Binance JSON or SBE frames).
    :param expect_snapshots: ``True`` for captures recorded with depth snapshots (``--snapshots``):
        depth diffs are buffered per symbol until the snapshot arrives. ``False`` applies diffs
        from a cold start, which is what plain ``trade`` + ``depth`` captures need.
    """
    return Dataset(_native.open_capture(path, expect_snapshots))


def connect(
    addr: str,
    token: str | None = None,
    tls_ca: str | None = None,
    tls_domain: str | None = None,
    tls_cert: str | None = None,
    tls_key: str | None = None,
    connect_timeout_s: float = 10.0,
) -> Client:
    """Connect to an ``ankaad`` Flight endpoint and return a :class:`Client`.

    :param addr: ``host:port`` (plain HTTP/2 unless ``tls_ca`` is given).
    :param token: bearer token, required when the server has ``server.token`` set.
    :param tls_ca: path to the PEM CA certificate that signed the server certificate; enables TLS.
    :param tls_domain: expected server name when it differs from the host part of ``addr``.
    :param tls_cert: path to a PEM client certificate (mutual TLS); needs ``tls_key``.
    :param tls_key: path to the PEM private key for ``tls_cert``.
    :param connect_timeout_s: TCP/TLS connect timeout in seconds.
    """
    return Client(
        _native.connect(addr, token, tls_ca, tls_domain, tls_cert, tls_key, connect_timeout_s)
    )
