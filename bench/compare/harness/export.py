"""Run `ankaad replay` and read what it writes.

`ankaad replay <capture.ankc> --sql ... --sql ... --out-dir <dir>` writes q01.csv, q02.csv, ... (one per
--sql, RFC 3339 nanosecond timestamps) and prints `-- <rows> rows in <ms> ms` per query. The base tables
are exported as plain `SELECT * FROM <table> ORDER BY ts, <id>` and converted to Parquet with an explicit
schema so every engine loads identical rows.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from . import REPO_DIR

_TIMING_RE = re.compile(r"^-- (\d+) rows in ([0-9.]+) ms", re.MULTILINE)

# Export order and sort keys of the base tables.
BASE_TABLES: dict[str, str] = {
    "trades": "SELECT * FROM trades ORDER BY ts, seq",
    "quotes": "SELECT * FROM quotes ORDER BY ts, update_id",
    "book_deltas": "SELECT * FROM book_deltas ORDER BY ts, update_id, side, price",
    "symbols": "SELECT * FROM symbols ORDER BY symbol",
}

# Arrow schemas of the exported tables (ts is UTC; stored tz-naive so DuckDB keeps TIMESTAMP_NS).
SCHEMAS: dict[str, pa.Schema] = {
    "trades": pa.schema(
        [("ts", pa.timestamp("ns")), ("symbol", pa.uint32()), ("price", pa.int64()), ("size", pa.int64()),
         ("side", pa.uint8()), ("seq", pa.int64())]
    ),
    "quotes": pa.schema(
        [("ts", pa.timestamp("ns")), ("symbol", pa.uint32()), ("bid", pa.int64()), ("bid_size", pa.int64()),
         ("ask", pa.int64()), ("ask_size", pa.int64()), ("update_id", pa.int64())]
    ),
    "book_deltas": pa.schema(
        [("ts", pa.timestamp("ns")), ("symbol", pa.uint32()), ("side", pa.uint8()), ("price", pa.int64()),
         ("size", pa.int64()), ("first_update_id", pa.int64()), ("update_id", pa.int64())]
    ),
    "symbols": pa.schema([("symbol", pa.uint32()), ("name", pa.string())]),
}


def find_ankaad() -> Path:
    env = os.environ.get("ANKAAD")
    if env:
        p = Path(env)
        if p.exists():
            return p
        raise FileNotFoundError(f"ANKAAD={env} does not exist")
    for cand in (REPO_DIR / "target" / "release" / "ankaad", REPO_DIR / "target" / "debug" / "ankaad"):
        if cand.exists():
            return cand
    found = shutil.which("ankaad")
    if found:
        return Path(found)
    raise FileNotFoundError(
        "ankaad binary not found: run `cargo build --release -p ankaad` in the repo root or set ANKAAD=/path/to/ankaad"
    )


@dataclass
class ReplayResult:
    csv_paths: list[Path]
    rows: list[int]
    server_ms: list[float]  # per query, as printed by ankaad (query execution only, excludes replay/load)
    wall_s: float  # whole process incl. capture replay
    stdout: str
    stderr: str


def replay(
    captures: list[Path] | Path,
    sqls: list[str],
    out_dir: Path,
    expect_snapshots: bool = False,
    ankaad: Path | None = None,
    timeout: float = 1800,
) -> ReplayResult:
    """Run one `ankaad replay` process with all `sqls`; return CSV paths in query order."""
    caps = [captures] if isinstance(captures, Path) else list(captures)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("q*.csv"):
        old.unlink()
    cmd = [str(ankaad or find_ankaad()), "replay", *map(str, caps), "--out-dir", str(out_dir)]
    if expect_snapshots:
        cmd.append("--expect-snapshots")
    for s in sqls:
        cmd += ["--sql", s]
    env = dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "warn"))
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)
    wall = time.perf_counter() - t0
    if proc.returncode != 0:
        raise RuntimeError(
            f"ankaad replay failed (exit {proc.returncode})\ncmd: {' '.join(cmd)}\n"
            f"stderr:\n{proc.stderr[-4000:]}\nstdout:\n{proc.stdout[-2000:]}"
        )
    timings = _TIMING_RE.findall(proc.stdout)
    if len(timings) != len(sqls):
        raise RuntimeError(f"expected {len(sqls)} timing lines, got {len(timings)}:\n{proc.stdout[-2000:]}")
    paths = [out_dir / f"q{i + 1:02d}.csv" for i in range(len(sqls))]
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise RuntimeError(f"ankaad did not write {missing}")
    return ReplayResult(
        csv_paths=paths,
        rows=[int(r) for r, _ in timings],
        server_ms=[float(ms) for _, ms in timings],
        wall_s=wall,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def read_result_csv(path: Path, schema: pa.Schema | None = None) -> pa.Table:
    """Read an ankaad CSV. Timestamps (`...T...Z` with 9 fraction digits) become timestamp[ns, UTC];
    integers int64; empty cells null. With `schema`, columns are cast to it."""
    if path.stat().st_size == 0:
        return pa.table({})
    table = pcsv.read_csv(
        path,
        parse_options=pcsv.ParseOptions(newlines_in_values=False),
        convert_options=pcsv.ConvertOptions(
            timestamp_parsers=[pcsv.ISO8601],
            null_values=["", "NULL", "null"],
            strings_can_be_null=True,
        ),
    )
    if schema is not None:
        table = table.select(schema.names)
        cols = []
        for f in schema:
            col = table[f.name]
            if pa.types.is_timestamp(f.type) and pa.types.is_timestamp(col.type) and col.type.tz is not None:
                col = col.cast(pa.timestamp(col.type.unit, tz="UTC")).cast(pa.timestamp(f.type.unit))
            cols.append(col.cast(f.type))
        table = pa.table(cols, schema=schema)
    return table


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def export_tables(captures: list[Path] | Path, out_dir: Path, expect_snapshots: bool = False) -> dict:
    """Export the base tables of a capture as CSV + Parquet into `out_dir`; write and return `manifest.json`."""
    from .queries import time_window

    caps = [captures] if isinstance(captures, Path) else list(captures)
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / "_export"
    res = replay(caps, list(BASE_TABLES.values()), tmp, expect_snapshots=expect_snapshots)
    tables: dict[str, dict] = {}
    for name, csv_path in zip(BASE_TABLES, res.csv_paths):
        table = read_result_csv(csv_path, SCHEMAS[name])
        csv_out = out_dir / f"{name}.csv"
        shutil.move(str(csv_path), csv_out)
        pq.write_table(table, out_dir / f"{name}.parquet", compression="zstd")
        info: dict = {"rows": table.num_rows, "csv": csv_out.name, "parquet": f"{name}.parquet"}
        if "ts" in table.column_names and table.num_rows:
            info["ts_min_ns"] = pc.min(table["ts"]).cast(pa.int64()).as_py()
            info["ts_max_ns"] = pc.max(table["ts"]).cast(pa.int64()).as_py()
        tables[name] = info
    shutil.rmtree(tmp, ignore_errors=True)
    tr = tables["trades"]
    t0, t1 = time_window(tr.get("ts_min_ns", 0), tr.get("ts_max_ns", 0))
    manifest = {
        "captures": [{"path": str(p), "bytes": p.stat().st_size, "sha256": sha256_file(p)} for p in caps],
        "expect_snapshots": expect_snapshots,
        "tables": tables,
        "t0_ns": t0,
        "t1_ns": t1,
        "ankaad": str(find_ankaad()),
        "replay_stderr_tail": res.stderr[-1000:],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def load_manifest(data_dir: Path) -> dict:
    return json.loads((data_dir / "manifest.json").read_text())


def read_parquet_tables(data_dir: Path) -> dict[str, pa.Table]:
    return {name: pq.read_table(data_dir / f"{name}.parquet") for name in BASE_TABLES}
