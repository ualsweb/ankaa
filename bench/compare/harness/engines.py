"""Native-client adapters. Each engine exposes:

    name, dialect, version(), load(data_dir, manifest), run(sql) -> RunResult, close()

`run` executes one rendered SQL statement and returns the full result as an Arrow table together with the
client-observed wall time (request sent -> last row received) and, when the engine reports it, its own
server-side execution time. Timing loops live in run.py; ANKAA's replay path overrides `time_query`
because one process can run the same statement many times and report per-run server times.
"""
from __future__ import annotations

import io
import json
import os
import statistics
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .export import BASE_TABLES, SCHEMAS, find_ankaad, read_result_csv, replay


@dataclass
class RunResult:
    table: pa.Table
    client_ms: float
    server_ms: float | None = None


@dataclass
class Timing:
    rows: int
    client_ms: list[float]
    server_ms: list[float] | None
    timing_scope: str  # what the numbers measure


def _summary(xs: list[float]) -> dict:
    s = sorted(xs)
    if not s:
        return {}
    p95 = s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))]
    return {"n": len(s), "min_ms": s[0], "median_ms": statistics.median(s), "p95_ms": p95, "max_ms": s[-1], "all_ms": xs}


class Engine:
    name = "base"
    dialect = "base"
    timing_scope = "client-observed: request sent to last row received, result fully materialised"

    def version(self) -> str:
        raise NotImplementedError

    def load(self, data_dir: Path, manifest: dict) -> None:
        raise NotImplementedError

    def run(self, sql: str) -> RunResult:
        raise NotImplementedError

    def close(self) -> None:
        pass

    def time_query(self, sql: str, warmup: int, n: int) -> Timing:
        for _ in range(warmup):
            self.run(sql)
        client, server, rows = [], [], 0
        for _ in range(n):
            r = self.run(sql)
            rows = r.table.num_rows
            client.append(r.client_ms)
            if r.server_ms is not None:
                server.append(r.server_ms)
        return Timing(rows, client, server or None, self.timing_scope)


# --------------------------------------------------------------------------------------------- ANKAA
class AnkaaReplayEngine(Engine):
    """ANKAA through `ankaad replay`: the capture is replayed into memory in a fresh process and every
    statement runs there. Times are the engine's own per-query numbers (DataFusion plan + execute +
    collect), which exclude the replay itself and any transfer; this is the ANKAA path available today."""

    name = "ankaa"
    dialect = "ankaa"
    timing_scope = "server-side: ankaad per-query time (plan+execute+collect), excludes capture replay and transfer"

    def __init__(self, captures: list[Path] | None = None, expect_snapshots: bool = False, workdir: Path | None = None):
        self.captures = captures or []
        self.expect_snapshots = expect_snapshots
        self.ankaad = find_ankaad()
        self.workdir = workdir or Path(tempfile.mkdtemp(prefix="ankaa-bench-"))

    def version(self) -> str:
        import subprocess

        return subprocess.run([str(self.ankaad), "--version"], capture_output=True, text=True).stdout.strip()

    def load(self, data_dir: Path, manifest: dict) -> None:
        if not self.captures:
            self.captures = [Path(c["path"]) for c in manifest["captures"]]
            self.expect_snapshots = bool(manifest.get("expect_snapshots", False))

    def run(self, sql: str) -> RunResult:
        res = replay(self.captures, [sql], self.workdir / "run", self.expect_snapshots, self.ankaad)
        return RunResult(read_result_csv(res.csv_paths[0]), res.wall_s * 1e3, res.server_ms[0])

    def time_query(self, sql: str, warmup: int, n: int) -> Timing:
        res = replay(self.captures, [sql] * (warmup + n), self.workdir / "time", self.expect_snapshots, self.ankaad)
        table = read_result_csv(res.csv_paths[-1])
        return Timing(table.num_rows, res.server_ms[warmup:], res.server_ms[warmup:], self.timing_scope)


class AnkaaFlightEngine(Engine):
    """ANKAA through a running `ankaad serve` (Arrow Flight). The server must already hold the dataset
    (today `serve` ingests live streams; a capture-backed serve mode is on the roadmap). Timing is
    client-observed over gRPC: one DoGet with the SQL text as ticket, until the last batch is read."""

    name = "ankaa-flight"
    dialect = "ankaa"

    def __init__(self, location: str = "grpc://127.0.0.1:50051", token: str | None = None):
        from pyarrow import flight

        self.flight = flight
        self.location = location
        self.client = flight.FlightClient(location)
        tok = token or os.environ.get("ANKAA_TOKEN")
        self.options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {tok}".encode())]) if tok else None

    def version(self) -> str:
        # fail fast when no server is listening; the probe must carry the token because the server
        # answers unauthenticated calls with an auth error, which pyarrow's wait_for_available
        # does not retry
        deadline = time.perf_counter() + 5.0
        while True:
            try:
                list(self.client.list_flights(options=self.options))
                break
            except self.flight.FlightUnavailableError:
                if time.perf_counter() > deadline:
                    raise
                time.sleep(0.2)
        return f"ankaad over Arrow Flight at {self.location}"

    def load(self, data_dir: Path, manifest: dict) -> None:
        # Nothing to load: the server owns its data. Verification against the oracle still runs.
        pass

    def run(self, sql: str) -> RunResult:
        # one call, as ANKAA's own clients make it: the SQL text is the ticket of DoGet. (A generic
        # Flight client that asks GetFlightInfo first pays a second round trip and a second planning.)
        t0 = time.perf_counter()
        reader = self.client.do_get(self.flight.Ticket(sql.encode()), self.options)
        table = reader.read_all()
        return RunResult(table, (time.perf_counter() - t0) * 1e3, None)


# -------------------------------------------------------------------------------------------- DuckDB
class DuckDBEngine(Engine):
    """DuckDB in-process (Python). Tables are plain in-memory tables created from the Parquet export;
    the result is fetched as Arrow."""

    name = "duckdb"
    dialect = "duckdb"
    timing_scope = "client-observed in-process: execute + fetch Arrow"

    def __init__(self, threads: int | None = None):
        import duckdb

        self.con = duckdb.connect()
        if threads:
            self.con.execute(f"SET threads = {int(threads)}")
        self._duckdb = duckdb

    def version(self) -> str:
        return f"duckdb {self._duckdb.__version__}"

    def load(self, data_dir: Path, manifest: dict) -> None:
        for name in BASE_TABLES:
            p = (data_dir / f"{name}.parquet").resolve()
            self.con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{p}')")

    def run(self, sql: str) -> RunResult:
        t0 = time.perf_counter()
        rel = self.con.sql(sql)
        # duckdb >= 2.0: to_arrow_table(); older: arrow()
        table = rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.arrow()
        if not isinstance(table, pa.Table):
            table = table.read_all()
        return RunResult(table, (time.perf_counter() - t0) * 1e3, None)

    def close(self) -> None:
        self.con.close()


# ---------------------------------------------------------------------------------------- ClickHouse
_CH_TYPES = {
    "trades": "(ts DateTime64(9, 'UTC'), symbol UInt32, price Int64, size Int64, side UInt8, seq Int64) "
              "ENGINE = MergeTree ORDER BY (symbol, ts)",
    "quotes": "(ts DateTime64(9, 'UTC'), symbol UInt32, bid Int64, bid_size Int64, ask Int64, ask_size Int64, update_id Int64) "
              "ENGINE = MergeTree ORDER BY (symbol, ts)",
    "book_deltas": "(ts DateTime64(9, 'UTC'), symbol UInt32, side UInt8, price Int64, size Int64, first_update_id Int64, update_id Int64) "
                   "ENGINE = MergeTree ORDER BY (symbol, ts)",
    "symbols": "(symbol UInt32, name String) ENGINE = MergeTree ORDER BY symbol",
}


class ClickHouseEngine(Engine):
    """ClickHouse over its native HTTP interface (port 8123). Results are requested as Arrow IPC
    (`default_format=Arrow`); the `X-ClickHouse-Summary` header carries the server elapsed time."""

    name = "clickhouse"
    dialect = "clickhouse"

    def __init__(self, url: str = "http://127.0.0.1:8123", user: str = "default", password: str = "", database: str = "default"):
        import requests

        self.requests = requests
        self.url = url.rstrip("/")
        self.auth = (user, password)
        self.database = database
        self.session = requests.Session()

    def _post(self, sql: str, data: bytes | None = None, fmt: str | None = None, **settings) -> "requests.Response":
        params = {"database": self.database, **{k: str(v) for k, v in settings.items()}}
        if fmt:
            params["default_format"] = fmt
        if data is None:
            r = self.session.post(self.url, params=params, data=sql.encode(), auth=self.auth, timeout=600)
        else:
            params["query"] = sql
            r = self.session.post(self.url, params=params, data=data, auth=self.auth, timeout=600)
        if r.status_code != 200:
            raise RuntimeError(f"clickhouse: HTTP {r.status_code}: {r.text[:2000]}\nSQL: {sql[:500]}")
        return r

    def version(self) -> str:
        return "clickhouse " + self._post("SELECT version()").text.strip()

    def load(self, data_dir: Path, manifest: dict) -> None:
        for name, ddl in _CH_TYPES.items():
            self._post(f"DROP TABLE IF EXISTS {name}")
            self._post(f"CREATE TABLE {name} {ddl}")
            self._post(f"INSERT INTO {name} FORMAT Parquet", data=(data_dir / f"{name}.parquet").read_bytes())
            self._post(f"OPTIMIZE TABLE {name} FINAL")

    def run(self, sql: str) -> RunResult:
        t0 = time.perf_counter()
        r = self._post(sql, fmt="Arrow", output_format_arrow_string_as_string=1)
        content = r.content
        client_ms = (time.perf_counter() - t0) * 1e3
        table = pa.ipc.open_file(pa.BufferReader(content)).read_all() if content else pa.table({})
        server_ms = None
        try:
            summary = json.loads(r.headers.get("X-ClickHouse-Summary", "{}"))
            if "elapsed_ns" in summary:
                server_ms = int(summary["elapsed_ns"]) / 1e6
        except (ValueError, TypeError):
            pass
        return RunResult(table, client_ms, server_ms)


# ------------------------------------------------------------------------------------------- QuestDB
_QDB_TYPES = {
    "trades": [("ts", "TIMESTAMP"), ("symbol", "INT"), ("price", "LONG"), ("size", "LONG"), ("side", "INT"), ("seq", "LONG")],
    "quotes": [("ts", "TIMESTAMP"), ("symbol", "INT"), ("bid", "LONG"), ("bid_size", "LONG"), ("ask", "LONG"),
               ("ask_size", "LONG"), ("update_id", "LONG")],
    "book_deltas": [("ts", "TIMESTAMP"), ("symbol", "INT"), ("side", "INT"), ("price", "LONG"), ("size", "LONG"),
                    ("first_update_id", "LONG"), ("update_id", "LONG")],
    "symbols": [("symbol", "INT"), ("name", "STRING")],
}


class QuestDBEngine(Engine):
    """QuestDB. Loading goes through the REST `/imp` CSV endpoint (schema given explicitly, `ts` as the
    designated timestamp, microsecond precision). Queries run either over the PostgreSQL wire protocol
    (port 8812, psycopg) or over REST `/exec` (JSON, carries server-side `timings.execute`)."""

    name = "questdb"
    dialect = "questdb"

    def __init__(self, http_url: str = "http://127.0.0.1:9000", pg_dsn: str = "host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb",
                 transport: str = "pg"):
        import requests

        self.requests = requests
        self.http = http_url.rstrip("/")
        self.session = requests.Session()
        self.transport = transport
        self.pg = None
        if transport == "pg":
            import psycopg

            self.pg = psycopg.connect(pg_dsn, autocommit=True)
        self.name = "questdb" if transport == "pg" else "questdb-rest"
        self.timing_scope = (
            "client-observed over PostgreSQL wire (psycopg): execute + fetchall"
            if transport == "pg"
            else "client-observed over REST /exec (JSON); server_ms = timings.execute"
        )

    def _exec(self, sql: str) -> dict:
        r = self.session.get(f"{self.http}/exec", params={"query": sql, "timings": "true", "count": "true"}, timeout=600)
        body = r.json()
        if r.status_code != 200 or "error" in body:
            raise RuntimeError(f"questdb: {body.get('error', r.text[:500])}\nSQL: {sql[:500]}")
        return body

    def version(self) -> str:
        body = self._exec("SELECT build()")
        return "questdb " + str(body["dataset"][0][0]).split(",")[0]

    def load(self, data_dir: Path, manifest: dict) -> None:
        for name, cols in _QDB_TYPES.items():
            self._exec(f"DROP TABLE IF EXISTS {name}")
            schema = []
            for c, t in cols:
                entry = {"name": c, "type": t}
                if t == "TIMESTAMP":
                    entry["pattern"] = "yyyy-MM-ddTHH:mm:ss.SSSUUUz"
                schema.append(entry)
            # micro-precision ISO CSV (QuestDB timestamps are microseconds)
            table = pq.read_table(data_dir / f"{name}.parquet")
            buf = io.BytesIO()
            import pyarrow.csv as pcsv

            cast_cols = []
            for f in table.schema:
                col = table[f.name]
                if pa.types.is_timestamp(f.type):
                    # QuestDB's TIMESTAMP holds microseconds. A capture with order-book snapshots
                    # has nanosecond rows (a snapshot is stamped 1 ns before the update it
                    # precedes): they are floored, which keeps their order against every
                    # millisecond-stamped event.
                    col = pa.compute.floor_temporal(col, 1, "microsecond").cast(pa.timestamp("us")).cast(pa.string())
                    col = pa.compute.replace_substring_regex(col, pattern=r"^(\S+) (\S+)$", replacement=r"\1T\2Z")
                cast_cols.append(col)
            pcsv.write_csv(pa.table(cast_cols, names=table.column_names), buf)
            params = {"name": name, "schema": json.dumps(schema), "overwrite": "true", "fmt": "json"}
            if any(t == "TIMESTAMP" for _, t in cols):
                params.update({"timestamp": "ts", "partitionBy": "DAY"})
            r = self.session.post(f"{self.http}/imp", params=params, files={"data": (f"{name}.csv", buf.getvalue(), "text/csv")}, timeout=3600)
            body = r.json()
            if r.status_code != 200 or body.get("status") != "OK" or body.get("rowsRejected", 0):
                raise RuntimeError(f"questdb import of {name} failed: {body}")
            self._wait_applied(name, table.num_rows)

    def _wait_applied(self, name: str, rows: int, timeout_s: float = 600.0) -> None:
        """Block until every imported row is visible to queries.

        QuestDB acknowledges an import once the rows are in its write-ahead log and applies them to
        the table in the background, so a query sent straight after `/imp` can see part of the
        table (a first run here verified `q02` against half of it). The load is done when the
        table's row count is the one that was sent and its WAL has nothing left to apply."""
        deadline = time.monotonic() + timeout_s
        seen = -1
        while True:
            seen = int(self._exec(f"SELECT count() FROM {name}")["dataset"][0][0])
            pending = False
            try:
                wal = self._exec(f"SELECT writerTxn, sequencerTxn, suspended FROM wal_tables() WHERE name = '{name}'")
                for writer, sequencer, suspended in wal.get("dataset", []):
                    if suspended:
                        raise RuntimeError(f"questdb: WAL of {name} is suspended; the table is incomplete")
                    pending = pending or writer != sequencer
            except RuntimeError as e:
                if "suspended" in str(e):
                    raise
                # not a WAL table, or a version without wal_tables(): the row count decides
            if seen == rows and not pending:
                return
            if seen > rows:
                raise RuntimeError(f"questdb: {name} holds {seen} rows, {rows} were imported")
            if time.monotonic() > deadline:
                raise RuntimeError(f"questdb: {name} shows {seen} of {rows} rows after {timeout_s:.0f} s")
            time.sleep(0.2)

    def run(self, sql: str) -> RunResult:
        if self.pg is not None:
            t0 = time.perf_counter()
            with self.pg.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                names = [d.name for d in cur.description]
            client_ms = (time.perf_counter() - t0) * 1e3
            import pandas as pd

            return RunResult(pa.Table.from_pandas(pd.DataFrame(rows, columns=names), preserve_index=False), client_ms, None)
        t0 = time.perf_counter()
        body = self._exec(sql)
        client_ms = (time.perf_counter() - t0) * 1e3
        names = [c["name"] for c in body["columns"]]
        types = [c["type"] for c in body["columns"]]
        cols = {}
        for i, (n, t) in enumerate(zip(names, types)):
            vals = [row[i] for row in body.get("dataset", [])]
            if t.upper() == "TIMESTAMP":
                cols[n] = pa.array(vals, pa.string()).cast(pa.timestamp("us"))
            else:
                cols[n] = pa.array(vals)
        server = body.get("timings", {}).get("execute")
        return RunResult(pa.table(cols), client_ms, (server / 1e6) if server else None)

    def close(self) -> None:
        if self.pg is not None:
            self.pg.close()


# ----------------------------------------------------------------------------------------- registry
def make_engine(name: str, args) -> Engine:
    """Build an engine from CLI args (see run.py)."""
    if name == "ankaa":
        return AnkaaReplayEngine()
    if name == "ankaa-flight":
        return AnkaaFlightEngine(args.flight, args.token)
    if name == "duckdb":
        return DuckDBEngine(threads=args.duckdb_threads)
    if name == "clickhouse":
        return ClickHouseEngine(args.clickhouse_url)
    if name == "questdb":
        return QuestDBEngine(args.questdb_url, args.questdb_dsn, transport=args.questdb_transport)
    raise ValueError(f"unknown engine {name!r}")


ENGINE_NAMES = ("ankaa", "ankaa-flight", "duckdb", "clickhouse", "questdb")
