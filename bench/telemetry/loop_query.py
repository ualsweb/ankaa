#!/usr/bin/env python3
"""Run one query of the telemetry set against a running `ankaad serve` in a loop, for profiling.

    loop_query.py --query single-groupby-1-1-1 --seconds 8 --flight grpc://127.0.0.1:50071 --token $T
    loop_query.py --sql "SELECT * FROM latest('cpu', 'hostname')" --seconds 8 ...
    loop_query.py --list                      # the ids of the set

Prints runs, rows and the median / p95 of the client-observed time. It exists so that a sampling
profiler attached to the server sees one query shape at a time (scripts/profile-telemetry.sh).
Not a benchmark: one client, no verification of the result.
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "compare"))

from harness import queries  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--query", help="id of a query in queries/manifest.json")
    ap.add_argument("--sql", help="a statement of your own instead of --query")
    ap.add_argument("--list", action="store_true", help="print the query ids and exit")
    ap.add_argument("--seconds", type=float, default=8.0)
    ap.add_argument("--flight", default="grpc://127.0.0.1:50071")
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    qs, _ = queries.load_manifest(HERE / "queries" / "manifest.json")
    if args.list:
        print("\n".join(q.id for q in qs))
        return 0
    if args.sql:
        sql = args.sql
    else:
        by_id = {q.id: q for q in qs}
        if args.query not in by_id:
            raise SystemExit(f"unknown query {args.query!r}; --list shows the ids")
        text = queries.load_dialect("ankaa", HERE / "queries" / "ankaa.sql")[args.query]
        import json

        params = {q["id"]: q.get("params", {}) for q in json.loads((HERE / "queries" / "manifest.json").read_text())["queries"]}
        sql = queries.render_params(text, "ankaa", params[args.query])

    from pyarrow import flight

    client = flight.FlightClient(args.flight)
    headers = [(b"authorization", f"Bearer {args.token}".encode())] if args.token else []
    opts = flight.FlightCallOptions(headers=headers)
    ticket = flight.Ticket(sql.encode())
    times, rows = [], 0
    end = time.monotonic() + args.seconds
    while time.monotonic() < end:
        t0 = time.perf_counter()
        rows = client.do_get(ticket, opts).read_all().num_rows
        times.append((time.perf_counter() - t0) * 1e3)
    times.sort()
    p95 = times[min(len(times) - 1, int(len(times) * 0.95))]
    print(f"runs {len(times)}  rows {rows}  median {statistics.median(times):.3f} ms  p95 {p95:.3f} ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
