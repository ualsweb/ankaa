"""Native-client adapters (bench/compare tick set and bench/telemetry TSBS cpu-only). Each engine exposes:

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
from collections.abc import Callable, Iterator
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


# ------------------------------------------------------------------------- telemetry (bench/telemetry)
# TSBS cpu-only: one table `cpu` (ts, 10 tags, 10 float64 fields). The dataset manifest written by
# bench/telemetry/generate.py names the tag and field columns and the three files every engine loads
# from (cpu.parquet, cpu.csv, cpu.lp). `load()` returns a dict of facts about the load (wall time,
# rows, method) that bench/telemetry/run.py stores next to the query times.


def telemetry_columns(manifest: dict) -> tuple[list[str], list[str]]:
    """(tags, fields) of the cpu table, in file order."""
    return list(manifest["tags"]), list(manifest["fields"])


def iter_line_batches(path: Path, lines: int) -> "Iterator[bytes]":
    """Yield the file as byte blocks of `lines` whole lines each (the last block may be shorter)."""
    import itertools

    with path.open("rb", buffering=1 << 22) as f:
        while True:
            block = list(itertools.islice(f, lines))
            if not block:
                return
            yield b"".join(block)


def run_parallel(blocks: "Iterator[bytes]", send: "Callable[[bytes], None]", workers: int) -> int:
    """Send blocks with `workers` threads, at most 2 x workers blocks in flight (the file is streamed,
    never held in memory). Re-raises the first failure. Returns the number of blocks sent."""
    import concurrent.futures as cf
    import threading

    slots = threading.BoundedSemaphore(2 * workers)
    sent = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        futures: set[cf.Future] = set()

        def release(_f: cf.Future) -> None:
            slots.release()

        for block in blocks:
            slots.acquire()
            fut = pool.submit(send, block)
            fut.add_done_callback(release)
            futures.add(fut)
            sent += 1
            done = {f for f in futures if f.done()}
            for f in done:
                f.result()
            futures -= done
        for f in cf.as_completed(futures):
            f.result()
    return sent


def _wait_count(count: "Callable[[], int]", rows: int, what: str, timeout_s: float, interval_s: float = 0.2) -> float:
    """Poll `count()` until it equals `rows`; return the seconds waited."""
    t0 = time.monotonic()
    seen = -1
    while True:
        seen = count()
        if seen == rows:
            return time.monotonic() - t0
        if seen > rows:
            raise RuntimeError(f"{what}: {seen} rows visible, {rows} were loaded")
        if time.monotonic() - t0 > timeout_s:
            raise RuntimeError(f"{what}: {seen} of {rows} rows visible after {timeout_s:.0f} s")
        time.sleep(interval_s)


def _rows_to_arrow(rows: list[tuple], names: list[str]) -> pa.Table:
    """DB-API rows -> Arrow, column by column (no pandas round trip)."""
    if not rows:
        return pa.table({n: pa.array([], pa.null()) for n in names})
    cols = list(zip(*rows))
    return pa.table({n: pa.array(list(c)) for n, c in zip(names, cols)})


class AnkaaServeEngine(AnkaaFlightEngine):
    """ANKAA as a server: `ankaad serve` with a generated configuration, loaded through its own bulk
    path (a `file` source reading cpu.csv, docs/generic-ingest.md), durable (wal_dir + checkpoint_dir),
    queried over Arrow Flight like `ankaa-flight`.

    The load clock starts when the process is spawned (the file source starts reading during start-up)
    and stops when `ankaa_user_rows_total` on /metrics reaches the dataset's row count; a non-zero
    `ankaa_user_rejected_total` fails the load. Rows are then visible to queries at the next snapshot
    swap (storage.snapshot_ms, 50 ms by default); `SELECT count(*)` is checked before returning."""

    name = "ankaa"
    dialect = "ankaa"
    timing_scope = "client-observed over Arrow Flight (pyarrow, DoGet with the SQL as ticket) until the last batch"

    def __init__(self, workdir: Path, flight_port: int = 50051, metrics_port: int = 9100, token: str | None = None,
                 warm_budget_mib: int | None = None, query_memory_mib: int | None = None, query_timeout_s: int = 600,
                 load_timeout_s: float = 7200.0):
        import secrets

        self.ankaad = find_ankaad()
        self.workdir = Path(workdir)
        self.token = token or os.environ.get("ANKAA_TOKEN") or secrets.token_hex(24)
        self.flight_port, self.metrics_port = flight_port, metrics_port
        self.warm_budget_mib, self.query_memory_mib = warm_budget_mib, query_memory_mib
        self.query_timeout_s, self.load_timeout_s = query_timeout_s, load_timeout_s
        self.proc = None
        self.log_path = self.workdir / "ankaad.log"
        super().__init__(f"grpc://127.0.0.1:{flight_port}", self.token)

    # -- process -------------------------------------------------------------------------------
    @property
    def pid(self) -> int | None:
        return self.proc.pid if self.proc is not None else None

    def config_text(self, data_dir: Path, manifest: dict) -> str:
        tags, fields = telemetry_columns(manifest)
        csv_cols = manifest["files"]["csv"]["columns"]
        cols = ['  { name = "ts", type = "timestamp_ns" },']
        cols += [f'  {{ name = "{t}", type = "dict" }},' for t in tags]
        cols += [f'  {{ name = "{f}", type = "float64" }},' for f in fields]
        mapping = ", ".join(f'{c} = "{i}"' for i, c in enumerate(csv_cols))
        # max_open_age_s: rows that stopped arriving are compressed and persisted after a minute instead
        # of the default ten (the same reason InfluxDB runs with INFLUXDB3_FORCE_SNAPSHOT_MAX_AGE=1m): the
        # at-rest state is reached soon after the load, see wait_persisted().
        storage = [f'wal_dir = "{self.workdir / "wal"}"', f'checkpoint_dir = "{self.workdir / "segments"}"',
                   "max_open_age_s = 60"]
        if self.warm_budget_mib:
            storage.append(f"warm_budget_mib = {int(self.warm_budget_mib)}")
        resources = [f"query_memory_mib = {int(self.query_memory_mib)}"] if self.query_memory_mib else []
        return "\n".join([
            "# generated by bench/telemetry (harness/engines.py AnkaaServeEngine); docs/generic-ingest.md",
            "[feed]", "enabled = false", "streams = []", "",
            "[server]", f'flight_addr = "127.0.0.1:{self.flight_port}"', f'metrics_addr = "127.0.0.1:{self.metrics_port}"',
            'token_env = "ANKAA_TOKEN"', f"query_timeout_s = {int(self.query_timeout_s)}", "",
            "[storage]", *storage, "",
            "[resources]", *resources, "",
            "[[tables]]", 'name = "cpu"', 'partition_by = "hostname"', "columns = [", *cols, "]", "",
            "[[sources]]", 'name = "tsbs"', 'kind = "file"', f'url = "{(data_dir / manifest["files"]["csv"]["path"]).resolve()}"',
            'format = "csv"', 'table = "cpu"', 'ts_unit = "rfc3339"', f"map = {{ {mapping} }}", "",
        ])

    def _metrics(self) -> dict[str, float]:
        import urllib.request

        req = urllib.request.Request(f"http://127.0.0.1:{self.metrics_port}/metrics",
                                     headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req, timeout=5) as r:
            text = r.read().decode()
        out = {}
        for line in text.splitlines():
            if line and not line.startswith("#"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        out[parts[0]] = float(parts[1])
                    except ValueError:
                        pass
        return out

    def _check_alive(self) -> None:
        if self.proc is not None and self.proc.poll() is not None:
            tail = self.log_path.read_text(errors="replace")[-3000:] if self.log_path.exists() else ""
            raise RuntimeError(f"ankaad exited with {self.proc.returncode}; log tail:\n{tail}")

    def start(self, data_dir: Path, manifest: dict) -> float:
        """Write the config, wipe old state, spawn `ankaad serve`; return the spawn instant (perf_counter)."""
        import shutil
        import subprocess

        for sub in ("wal", "segments"):
            shutil.rmtree(self.workdir / sub, ignore_errors=True)
        self.workdir.mkdir(parents=True, exist_ok=True)
        cfg = self.workdir / "ankaad.toml"
        cfg.write_text(self.config_text(data_dir, manifest))
        check = subprocess.run([str(self.ankaad), "config", "check", str(cfg)], capture_output=True, text=True)
        if check.returncode != 0:
            raise RuntimeError(f"ankaad config check failed:\n{check.stdout}{check.stderr}")
        env = dict(os.environ, ANKAA_TOKEN=self.token, RUST_LOG=os.environ.get("RUST_LOG", "info"))
        log = self.log_path.open("wb")
        t0 = time.perf_counter()
        self.proc = subprocess.Popen([str(self.ankaad), "serve", "--config", str(cfg)], stdout=log, stderr=subprocess.STDOUT, env=env)
        return t0

    def load(self, data_dir: Path, manifest: dict) -> dict:
        rows = int(manifest["rows"])
        t0 = self.start(data_dir, manifest)
        n, rejected, first_row_s = 0, 0, None
        deadline = time.monotonic() + self.load_timeout_s
        while True:
            self._check_alive()
            try:
                m = self._metrics()
            except OSError:
                m = {}
            n = int(m.get("ankaa_user_rows_total", 0))
            rejected = int(m.get("ankaa_user_rejected_total", 0))
            if first_row_s is None and n > 0:
                first_row_s = time.perf_counter() - t0
            if rejected:
                raise RuntimeError(f"ankaad rejected {rejected} rows (see {self.log_path}, RUST_LOG=debug for details)")
            if n >= rows:
                break
            if time.monotonic() > deadline:
                raise RuntimeError(f"ankaad stored {n} of {rows} rows after {self.load_timeout_s:.0f} s")
            time.sleep(0.05)
        wall = time.perf_counter() - t0
        if n != rows:
            raise RuntimeError(f"ankaad stored {n} rows, the dataset has {rows}")
        self.version()  # Flight is up
        visible_wait = _wait_count(lambda: int(self.run("SELECT count(*) AS n FROM cpu").table.column(0)[0].as_py()),
                                   rows, "ankaa", 120)
        return {"method": "ankaad serve, [[sources]] kind=file format=csv (cpu.csv), wal_dir + checkpoint_dir",
                "wall_s": wall, "rows": rows, "first_row_s": first_row_s, "visible_wait_s": visible_wait,
                "config": str(self.workdir / "ankaad.toml")}

    def wait_persisted(self, timeout_s: float = 1800.0) -> dict:
        """Block until the server has compressed and persisted what the load left open and has cut the
        log behind it: at least `max_open_age_s` plus a checkpoint interval after the load (130 s), then
        until the tiers and the WAL size have not changed for 75 s. This is the at-rest state the other
        engines are measured in (TimescaleDB after its columnstore conversion, InfluxDB after its Parquet
        persist); before it, the disk figure would count the whole log and queries would read rows that
        are still uncompressed."""
        t0 = time.monotonic()
        last, stable_since = None, t0
        while True:
            self._check_alive()
            m = self._metrics()
            state = tuple(int(m.get(k, -1)) for k in (
                "ankaa_wal_bytes", 'ankaa_tier_chunks{tier="hot"}', 'ankaa_tier_chunks{tier="warm"}',
                'ankaa_tier_chunks{tier="cold"}', "ankaa_tier_sealed_for_age_total"))
            now = time.monotonic()
            if state != last:
                last, stable_since = state, now
            elif now - t0 >= 130.0 and now - stable_since >= 75.0:
                return {"persist_wait_s": round(now - t0, 1), "wal_bytes": state[0], "hot_chunks": state[1],
                        "warm_chunks": state[2], "cold_segments": state[3], "sealed_for_age": state[4]}
            if now - t0 > timeout_s:
                raise RuntimeError(f"ankaa: tiers and WAL still changing after {timeout_s:.0f} s: {state}")
            time.sleep(2.0)

    def data_paths(self) -> list[Path]:
        return [self.workdir / "wal", self.workdir / "segments"]

    def close(self) -> None:
        import signal

        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=60)
            except Exception:  # noqa: BLE001
                self.proc.kill()
                self.proc.wait()
        self.proc = None


class InfluxDB3Engine(Engine):
    """InfluxDB 3 Core (the open-source edition) in Docker.

    Load: the line protocol file (cpu.lp, float fields) POSTed to `/api/v3/write_lp?precision=nanosecond`
    in batches of 10,000 lines ("The optimal batch size is 10,000 lines of line protocol or 10 MBs",
    https://docs.influxdata.com/influxdb3/core/write-data/best-practices/optimize-writes/), several
    requests in flight ("For high write throughput, you can send multiple concurrent write requests",
    https://docs.influxdata.com/influxdb3/core/write-data/http-api/v3-write-lp/). Writes are acknowledged
    after the WAL flush (no_sync=false, the default), as every other engine here is durable on ack.
    Optional gzip bodies (`gzip=True`), which the same best-practices page recommends for network links.

    Queries: Arrow Flight (`flight`, the transport of InfluxData's own v3 client libraries:
    DoGet with a JSON ticket {database, sql_query, query_type}), or HTTP `/api/v3/query_sql?format=parquet`
    (`http`)."""

    name = "influxdb3"
    dialect = "influxdb3"

    def __init__(self, url: str = "http://127.0.0.1:8181", database: str = "tsbs", token: str | None = None,
                 transport: str = "flight", batch_lines: int = 10_000, writers: int = 4, gzip: bool = False,
                 load_timeout_s: float = 7200.0):
        import requests

        self.requests = requests
        self.url = url.rstrip("/")
        self.database = database
        self.token = token
        self.transport = transport
        self.batch_lines, self.writers, self.gzip = batch_lines, writers, gzip
        self.load_timeout_s = load_timeout_s
        self.session = requests.Session()
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.timing_scope = (
            "client-observed over Arrow Flight (pyarrow, DoGet with the v3 JSON ticket) until the last batch"
            if transport == "flight" else "client-observed over HTTP /api/v3/query_sql?format=parquet, Parquet decoded")
        self._flight = None

    def _flight_client(self):
        if self._flight is None:
            from pyarrow import flight

            host = urllib.parse.urlparse(self.url)
            self._flight = (flight, flight.FlightClient(f"grpc://{host.hostname}:{host.port or 8181}"))
        return self._flight

    def version(self) -> str:
        r = self.session.get(f"{self.url}/ping", timeout=10)
        if r.status_code != 200:
            raise RuntimeError(f"influxdb3: /ping HTTP {r.status_code}: {r.text[:300]}")
        try:
            body = r.json()
            return f"influxdb3 {body.get('version', '?')} (revision {body.get('revision', '?')})"
        except ValueError:
            return "influxdb3 " + r.headers.get("x-influxdb-version", "?")

    def _write(self, block: bytes) -> None:
        import gzip as gz

        headers = {"Content-Type": "text/plain; charset=utf-8"}
        if self.gzip:
            block = gz.compress(block, compresslevel=1)
            headers["Content-Encoding"] = "gzip"
        params = {"db": self.database, "precision": "nanosecond", "accept_partial": "false"}
        for attempt in range(5):
            r = self.session.post(f"{self.url}/api/v3/write_lp", params=params, data=block, headers=headers, timeout=600)
            if r.status_code in (200, 204):
                return
            if r.status_code in (429, 503) and attempt < 4:  # back-pressure: retry the same batch
                time.sleep(0.5 * (attempt + 1))
                continue
            raise RuntimeError(f"influxdb3 write: HTTP {r.status_code}: {r.text[:1000]}")

    def count(self) -> int:
        t = self.run("SELECT count(*) AS n FROM cpu").table
        return int(t.column(0)[0].as_py()) if t.num_rows else 0

    def load(self, data_dir: Path, manifest: dict) -> dict:
        rows = int(manifest["rows"])
        lp = data_dir / manifest["files"]["lp"]["path"]
        t0 = time.perf_counter()
        batches = run_parallel(iter_line_batches(lp, self.batch_lines), self._write, self.writers)
        acked = time.perf_counter() - t0
        visible_wait = _wait_count(self.count, rows, "influxdb3", self.load_timeout_s, 1.0)
        return {"method": f"HTTP /api/v3/write_lp precision=nanosecond, {self.batch_lines} lines/request, "
                          f"{self.writers} concurrent, gzip={self.gzip}",
                "wall_s": time.perf_counter() - t0, "acked_s": acked, "visible_wait_s": visible_wait,
                "rows": rows, "requests": batches}

    def wait_persisted(self, rows: int, timeout_s: float = 1800.0) -> dict:
        """Block until every row is in Parquet files (`system.parquet_files`), i.e. no longer only in
        the write buffer: the at-rest state the other engines are queried in. With
        INFLUXDB3_FORCE_SNAPSHOT_MAX_AGE=1m (docker-compose.yml) this takes about a minute after the load."""
        sql = ("SELECT count(*) AS files, sum(row_count) AS rows, sum(size_bytes) AS bytes "
               "FROM system.parquet_files WHERE table_name = 'cpu'")
        t0 = time.monotonic()
        while True:
            rec = self.run(sql).table.to_pylist()[0]
            persisted = int(rec.get("rows") or 0)
            if persisted >= rows:
                return {"persist_wait_s": round(time.monotonic() - t0, 1), "parquet_files": int(rec["files"]),
                        "parquet_rows": persisted, "parquet_bytes": int(rec.get("bytes") or 0)}
            if time.monotonic() - t0 > timeout_s:
                raise RuntimeError(f"influxdb3: {persisted} of {rows} rows in Parquet after {timeout_s:.0f} s")
            time.sleep(2.0)

    def run(self, sql: str) -> RunResult:
        if self.transport == "flight":
            flight, client = self._flight_client()
            ticket = json.dumps({"database": self.database, "sql_query": sql, "query_type": "sql"}).encode()
            opts = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {self.token}".encode())]) if self.token else None
            t0 = time.perf_counter()
            table = client.do_get(flight.Ticket(ticket), opts).read_all()
            return RunResult(table, (time.perf_counter() - t0) * 1e3, None)
        t0 = time.perf_counter()
        r = self.session.get(f"{self.url}/api/v3/query_sql", params={"db": self.database, "q": sql, "format": "parquet"}, timeout=600)
        if r.status_code != 200:
            raise RuntimeError(f"influxdb3: HTTP {r.status_code}: {r.text[:2000]}\nSQL: {sql[:500]}")
        table = pq.read_table(pa.BufferReader(r.content)) if r.content else pa.table({})
        return RunResult(table, (time.perf_counter() - t0) * 1e3, None)

    def close(self) -> None:
        if self._flight is not None:
            self._flight[1].close()
            self._flight = None
        self.session.close()


class TimescaleDBEngine(Engine):
    """TimescaleDB (PostgreSQL extension) in Docker, the official image (which runs timescaledb-tune on
    first start: shared_buffers, work_mem, parallel workers sized to the container,
    https://docs.timescale.com/self-hosted/latest/install/installation-docker/).

    Schema: a hypertable on `ts` with 12-hour chunks (TSBS's default --chunk-time) and an index on
    (hostname, ts DESC) as TSBS's loader creates (its partition index), created before the load as TSBS
    does. Load: parallel COPY ... FROM STDIN (FORMAT csv) of cpu.csv over `workers` connections, each
    COPY one transaction of `batch_rows` rows, the scheme of Timescale's timescaledb-parallel-copy tool
    (https://github.com/timescale/timescaledb-parallel-copy). `columnstore()` then converts every chunk
    to the columnstore (compression) with segmentby = hostname and orderby = ts DESC, as Timescale
    recommends for analytical queries filtered by a device/host column
    (https://docs.timescale.com/use-timescale/latest/compression/about-compression/), followed by
    VACUUM ANALYZE. Queries over psycopg 3 (binary result format)."""

    name = "timescaledb"
    dialect = "timescaledb"
    timing_scope = "client-observed over PostgreSQL wire (psycopg 3, binary results): execute + fetchall + Arrow"

    def __init__(self, dsn: str = "host=127.0.0.1 port=5432 user=postgres password=tsbs dbname=postgres",
                 workers: int = 4, batch_rows: int = 50_000, chunk_interval: str = "12 hours"):
        import psycopg

        self.psycopg = psycopg
        self.dsn = dsn
        self.workers, self.batch_rows, self.chunk_interval = workers, batch_rows, chunk_interval
        self.conn = psycopg.connect(dsn, autocommit=True)

    def version(self) -> str:
        with self.conn.cursor() as cur:
            cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            row = cur.fetchone()
            cur.execute("SHOW server_version")
            pg = cur.fetchone()[0]
        return f"timescaledb {row[0] if row else 'not installed'} on PostgreSQL {pg}"

    def _exec(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def create(self, manifest: dict) -> None:
        tags, fields = telemetry_columns(manifest)
        cols = ["ts TIMESTAMPTZ NOT NULL"] + [f"{t} TEXT" for t in tags] + [f"{f} DOUBLE PRECISION" for f in fields]
        self._exec("CREATE EXTENSION IF NOT EXISTS timescaledb")
        self._exec("DROP TABLE IF EXISTS cpu")
        self._exec(f"CREATE TABLE cpu ({', '.join(cols)})")
        self._exec(f"SELECT create_hypertable('cpu', by_range('ts', INTERVAL '{self.chunk_interval}'))")
        self._exec("CREATE INDEX ON cpu (hostname, ts DESC)")

    def _copy(self, conn, cols: str, block: bytes) -> None:
        with conn.cursor() as cur:
            with cur.copy(f"COPY cpu ({cols}) FROM STDIN (FORMAT csv)") as cp:
                cp.write(block)

    def load(self, data_dir: Path, manifest: dict) -> dict:
        import threading

        rows = int(manifest["rows"])
        csv_path = data_dir / manifest["files"]["csv"]["path"]
        cols = ", ".join(manifest["files"]["csv"]["columns"])
        self.create(manifest)
        local = threading.local()
        conns: list = []
        lock = threading.Lock()

        def send(block: bytes) -> None:
            conn = getattr(local, "conn", None)
            if conn is None:
                conn = local.conn = self.psycopg.connect(self.dsn, autocommit=True)
                with lock:
                    conns.append(conn)
            self._copy(conn, cols, block)

        t0 = time.perf_counter()
        try:
            batches = run_parallel(iter_line_batches(csv_path, self.batch_rows), send, self.workers)
        finally:
            for c in conns:
                c.close()
        copy_s = time.perf_counter() - t0
        t1 = time.perf_counter()
        self._exec("ANALYZE cpu")
        analyze_s = time.perf_counter() - t1
        n = int(self.run("SELECT count(*) AS n FROM cpu").table.column(0)[0].as_py())
        if n != rows:
            raise RuntimeError(f"timescaledb: cpu holds {n} rows, the dataset has {rows}")
        return {"method": f"parallel COPY FROM STDIN (FORMAT csv), {self.workers} connections x {self.batch_rows} rows/transaction; "
                          f"hypertable chunk_interval {self.chunk_interval}, index (hostname, ts DESC)",
                "wall_s": copy_s, "rows": rows, "copies": batches, "analyze_s": analyze_s}

    def columnstore(self) -> dict:
        """Convert every chunk to the columnstore (compressed), in parallel over `workers` sessions, then
        VACUUM ANALYZE. Returns timings and sizes before/after."""
        import concurrent.futures as cf

        before = self.sizes()
        self._exec("ALTER TABLE cpu SET (timescaledb.enable_columnstore = true, "
                   "timescaledb.segmentby = 'hostname', timescaledb.orderby = 'ts DESC')")
        with self.conn.cursor() as cur:
            cur.execute("SELECT show_chunks('cpu')::text")
            chunks = [r[0] for r in cur.fetchall()]

        def convert(chunk: str) -> None:
            with self.psycopg.connect(self.dsn, autocommit=True) as c:
                c.execute(f"CALL convert_to_columnstore('{chunk}')")

        t0 = time.perf_counter()
        with cf.ThreadPoolExecutor(max_workers=self.workers) as pool:
            list(pool.map(convert, chunks))
        convert_s = time.perf_counter() - t0
        t1 = time.perf_counter()
        self._exec("VACUUM ANALYZE cpu")
        vacuum_s = time.perf_counter() - t1
        return {"method": "ALTER TABLE cpu SET (timescaledb.enable_columnstore, segmentby = 'hostname', orderby = 'ts DESC'); "
                          f"CALL convert_to_columnstore(chunk) for {len(chunks)} chunks, {self.workers} in parallel; VACUUM ANALYZE",
                "wall_s": convert_s, "vacuum_analyze_s": vacuum_s, "chunks": len(chunks),
                "bytes_before": before, "bytes_after": self.sizes()}

    def sizes(self) -> dict:
        with self.conn.cursor() as cur:
            cur.execute("SELECT hypertable_size('cpu'), pg_database_size(current_database())")
            hs, db = cur.fetchone()
        return {"hypertable_bytes": int(hs or 0), "database_bytes": int(db or 0)}

    def run(self, sql: str) -> RunResult:
        t0 = time.perf_counter()
        with self.conn.cursor(binary=True) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            names = [d.name for d in cur.description]
        table = _rows_to_arrow(rows, names)
        return RunResult(table, (time.perf_counter() - t0) * 1e3, None)

    def close(self) -> None:
        self.conn.close()


class QuestDBTelemetryEngine(QuestDBEngine):
    """QuestDB with the cpu table. Load: InfluxDB line protocol over HTTP (`POST /write`, port 9000),
    QuestDB's recommended ingestion path (https://questdb.com/docs/reference/api/ilp/overview/), the
    same cpu.lp file InfluxDB loads, `batch_lines` lines per request (75,000 = the HTTP auto-flush row
    count of QuestDB's own clients, https://questdb.com/docs/configuration-string/), `writers` requests in
    flight. The table is created first so that types are explicit: tags SYMBOL (as ILP would create them;
    hostname with a capacity for every host, as the symbol docs advise,
    https://questdb.com/docs/concept/symbol/), fields DOUBLE, `ts` the designated timestamp, PARTITION BY
    DAY, WAL. The load is complete when every row is visible and the WAL is applied (`_wait_applied`).
    Queries as QuestDBEngine (PostgreSQL wire by default)."""

    def __init__(self, http_url: str = "http://127.0.0.1:9000",
                 pg_dsn: str = "host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb",
                 transport: str = "pg", batch_lines: int = 75_000, writers: int = 4, load_timeout_s: float = 7200.0):
        super().__init__(http_url, pg_dsn, transport)
        self.batch_lines, self.writers, self.load_timeout_s = batch_lines, writers, load_timeout_s

    def create(self, manifest: dict) -> None:
        tags, fields = telemetry_columns(manifest)
        hosts = int(manifest.get("hosts", 0)) or 1024
        capacity = 1 << max(8, (hosts - 1).bit_length())
        cols = ["ts TIMESTAMP"]
        cols += [f"{t} SYMBOL CAPACITY {capacity} CACHE" if t == "hostname" else f"{t} SYMBOL" for t in tags]
        cols += [f"{f} DOUBLE" for f in fields]
        self._exec("DROP TABLE IF EXISTS cpu")
        self._exec(f"CREATE TABLE cpu ({', '.join(cols)}) TIMESTAMP(ts) PARTITION BY DAY WAL")

    def _write(self, block: bytes) -> None:
        r = self.session.post(f"{self.http}/write", params={"precision": "n"}, data=block, timeout=600)
        if r.status_code not in (200, 204):
            raise RuntimeError(f"questdb ILP/HTTP: HTTP {r.status_code}: {r.text[:1000]}")

    def load(self, data_dir: Path, manifest: dict) -> dict:
        rows = int(manifest["rows"])
        lp = data_dir / manifest["files"]["lp"]["path"]
        self.create(manifest)
        t0 = time.perf_counter()
        batches = run_parallel(iter_line_batches(lp, self.batch_lines), self._write, self.writers)
        acked = time.perf_counter() - t0
        t1 = time.perf_counter()
        self._wait_applied("cpu", rows, self.load_timeout_s)
        return {"method": f"ILP over HTTP POST /write precision=n, {self.batch_lines} lines/request, {self.writers} concurrent; "
                          "table pre-created (SYMBOL tags, DOUBLE fields, PARTITION BY DAY WAL)",
                "wall_s": time.perf_counter() - t0, "acked_s": acked, "visible_wait_s": time.perf_counter() - t1,
                "rows": rows, "requests": batches}


    def sizes(self) -> dict:
        """QuestDB's own accounting of the table on disk (`table_storage()`), when this version has it."""
        try:
            body = self._exec("SELECT * FROM table_storage() WHERE tableName = 'cpu'")
        except RuntimeError as e:
            return {"error": str(e).splitlines()[0]}
        cols = [c["name"] for c in body.get("columns", [])]
        rows = body.get("dataset", [])
        return dict(zip(cols, rows[0])) if rows else {}


class DuckDBTelemetryEngine(DuckDBEngine):
    """DuckDB with the cpu table materialised in memory from cpu.parquet (the reference)."""

    def load(self, data_dir: Path, manifest: dict) -> dict:
        p = (data_dir / manifest["files"]["parquet"]["path"]).resolve()
        t0 = time.perf_counter()
        self.con.execute(f"CREATE OR REPLACE TABLE cpu AS SELECT * FROM read_parquet('{p}')")
        wall = time.perf_counter() - t0
        n = self.con.execute("SELECT count(*) FROM cpu").fetchall()[0][0]
        if n != int(manifest["rows"]):
            raise RuntimeError(f"duckdb: cpu holds {n} rows, the dataset has {manifest['rows']}")
        return {"method": "CREATE TABLE cpu AS SELECT * FROM read_parquet(cpu.parquet), in memory", "wall_s": wall, "rows": n}

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
