#!/usr/bin/env python3
"""Prepare a dataset for the comparison: export the capture with `ankaad replay --out-dir` to CSV +
Parquet, then load the very same rows into every engine.

    bench/.venv/bin/python bench/compare/prepare.py                      # bundled 20 s capture
    bench/.venv/bin/python bench/compare/prepare.py --capture big.ankc --data-dir bench/compare/data/big
    bench/.venv/bin/python bench/compare/prepare.py --engines duckdb      # no containers needed

Precision: the export keeps nanosecond timestamps. ClickHouse stores DateTime64(9) (exact); QuestDB stores
microseconds (nanoseconds truncated: exact for Binance's millisecond stamps, documented in the manifest).
DuckDB loads the Parquet at run time (TIMESTAMP_NS) and needs no preparation beyond the export.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import COMPARE_DIR, DEFAULT_CAPTURE  # noqa: E402
from harness import export  # noqa: E402
from harness.engines import ClickHouseEngine, DuckDBEngine, QuestDBEngine  # noqa: E402


def wait_http(url: str, timeout: float) -> None:
    import requests

    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                return
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = str(e)
        time.sleep(1)
    raise SystemExit(f"{url} not reachable after {timeout:.0f}s ({last}); is `docker compose up -d` running?")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--capture", nargs="+", type=Path, default=[DEFAULT_CAPTURE], help=".ankc file(s), in time order")
    ap.add_argument("--expect-snapshots", action="store_true", help="capture recorded with --snapshots")
    ap.add_argument("--data-dir", type=Path, default=None, help="export directory (default bench/compare/data/<capture stem>)")
    ap.add_argument("--engines", default="duckdb,clickhouse,questdb", help="comma list of duckdb,clickhouse,questdb")
    ap.add_argument("--skip-export", action="store_true", help="reuse an existing export in --data-dir")
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--questdb-url", default="http://127.0.0.1:9000")
    ap.add_argument("--questdb-dsn", default="host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb")
    ap.add_argument("--wait", type=float, default=120, help="seconds to wait for containers")
    args = ap.parse_args()

    data_dir = args.data_dir or (COMPARE_DIR / "data" / args.capture[0].stem)
    if args.skip_export and (data_dir / "manifest.json").exists():
        manifest = export.load_manifest(data_dir)
        print(f"+ reusing export in {data_dir}")
    else:
        print(f"+ exporting {', '.join(map(str, args.capture))} -> {data_dir}")
        manifest = export.export_tables(args.capture, data_dir, expect_snapshots=args.expect_snapshots)
    for name, info in manifest["tables"].items():
        print(f"  {name:12s} {info['rows']:>12,d} rows")
    print(f"  window [T0, T1) = {manifest['t0_ns']} .. {manifest['t1_ns']} ns")

    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    for name in engines:
        t0 = time.time()
        if name == "duckdb":
            eng = DuckDBEngine()
        elif name == "clickhouse":
            wait_http(args.clickhouse_url + "/ping", args.wait)
            eng = ClickHouseEngine(args.clickhouse_url)
        elif name == "questdb":
            wait_http(args.questdb_url + "/exec?query=select%201", args.wait)
            eng = QuestDBEngine(args.questdb_url, args.questdb_dsn, transport="rest")
        else:
            raise SystemExit(f"unknown engine {name}")
        print(f"+ loading {name} ({eng.version()})")
        eng.load(data_dir, manifest)
        for table, info in manifest["tables"].items():
            n = eng.run(f"SELECT count(*) AS n FROM {table}").table.column(0)[0].as_py()
            flag = "" if n == info["rows"] else "   <-- MISMATCH"
            print(f"  {table:12s} {n:>12,d} rows{flag}")
            if n != info["rows"]:
                raise SystemExit(f"{name}: {table} has {n} rows, export has {info['rows']}")
        eng.close()
        print(f"  done in {time.time() - t0:.1f}s")
    print("+ ready: bench/.venv/bin/python bench/compare/run.py --data-dir", data_dir)


if __name__ == "__main__":
    main()
