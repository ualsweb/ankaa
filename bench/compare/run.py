#!/usr/bin/env python3
"""Time the tick query set on every engine, after verifying each result against the DuckDB reference.

    bench/.venv/bin/python bench/compare/run.py --data-dir bench/compare/data/<name> \
        --engines ankaa,duckdb,clickhouse,questdb --runs 10 --warmup 2

Writes results/<date>-<host>.json (raw per-run times, hardware/software facts, verification status) and
results/<date>-<host>.md (median / p95 table). On anything that is not aarch64 Linux the run is forced
into smoke mode: output goes to results/smoke/ (git-ignored) and is labelled as not a result.

ANKAA paths: `ankaa` replays the capture in a fresh `ankaad replay` process and reads the engine's own
per-query time; `ankaa-flight` talks to a running `ankaad serve` over Arrow Flight (--flight, --token).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import COMPARE_DIR  # noqa: E402
from harness import export, queries, sysinfo  # noqa: E402
from harness.compare import diff_tables  # noqa: E402
from harness.engines import ENGINE_NAMES, DuckDBEngine, make_engine  # noqa: E402


def fmt_ms(x: float | None) -> str:
    if x is None:
        return "-"
    return f"{x:.3f}" if x < 10 else f"{x:.1f}"


def markdown(report: dict) -> str:
    meta, rows = report["meta"], report["results"]
    engines = report["engines"]
    out = []
    if report["smoke"]:
        out.append("# SMOKE TEST - NOT A BENCHMARK RESULT\n")
        out.append("This run did not happen on Arm server hardware (or was started with --smoke). The numbers only show "
                   "that the harness works. Do not quote them.\n")
    else:
        out.append(f"# Tick query set: {meta['hostname']} ({meta['timestamp_utc']})\n")
    out.append(f"Dataset: {report['dataset']['summary']}  ")
    out.append(f"Runs per query: {report['runs']} after {report['warmup']} warm-up; cells are median ms (p95 ms); "
               f"`mismatch` = result differs from the DuckDB reference (time not comparable); `unstable` = the first answer "
               f"differed and a repeat matched; `n/a` = not expressible.\n")
    out.append("| query | " + " | ".join(engines) + " |")
    out.append("|---|" + "---|" * len(engines))
    by = {(r["engine"], r["query"]): r for r in rows}
    for q in report["queries"]:
        cells = []
        for e in engines:
            r = by.get((e, q["id"]))
            if r is None or r.get("status") == "unsupported":
                cells.append("n/a")
            elif r.get("status") == "error":
                cells.append("error")
            else:
                t = r["client"] if r.get("client") else r.get("server", {})
                cell = f"{fmt_ms(t.get('median_ms'))} ({fmt_ms(t.get('p95_ms'))})"
                if not r.get("verified", False):
                    cell += " mismatch"
                elif r.get("unstable_result"):
                    cell += " unstable"
                cells.append(cell)
        out.append(f"| {q['id']} | " + " | ".join(cells) + " |")
    out.append("")
    out.append("Timing scope per engine:\n")
    for e in engines:
        scope = next((r["timing_scope"] for r in rows if r["engine"] == e and r.get("timing_scope")), "-")
        out.append(f"- `{e}` ({report['versions'].get(e, '?')}): {scope}")
    out.append("")
    out.append("Environment: " + "; ".join(f"{k}={v}" for k, v in meta.items() if k in ("hostname", "system", "release", "machine", "cpu_brand", "dmi_product_name", "git_commit") and v))
    return "\n".join(out) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", type=Path, required=True, help="directory written by prepare.py")
    ap.add_argument("--engines", default="ankaa,duckdb,clickhouse,questdb", help=f"comma list of {', '.join(ENGINE_NAMES)}")
    ap.add_argument("--queries", default=None, help="comma list of query ids (default: all with timing=true)")
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--no-verify", action="store_true", help="skip the result check against DuckDB")
    ap.add_argument("--load", action="store_true", help="(re)load containers from the export before timing")
    ap.add_argument("--results-dir", type=Path, default=COMPARE_DIR / "results")
    ap.add_argument("--smoke", action="store_true", help="label the run as a smoke test (forced off Arm servers)")
    ap.add_argument("--label", default=None, help="free text stored in the JSON (instance type, notes)")
    ap.add_argument("--flight", default="grpc://127.0.0.1:50051")
    ap.add_argument("--token", default=None, help="bearer token for ankaa-flight (or env ANKAA_TOKEN)")
    ap.add_argument("--duckdb-threads", type=int, default=None)
    ap.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    ap.add_argument("--questdb-url", default="http://127.0.0.1:9000")
    ap.add_argument("--questdb-dsn", default="host=127.0.0.1 port=8812 user=admin password=quest dbname=qdb")
    ap.add_argument("--questdb-transport", choices=("pg", "rest"), default="pg")
    args = ap.parse_args()

    manifest = export.load_manifest(args.data_dir)
    all_queries, alpha = queries.load_manifest()
    wanted = set(args.queries.split(",")) if args.queries else None
    qs = [q for q in all_queries if (wanted and q.id in wanted) or (not wanted and q.timing)]
    t0, t1 = manifest["t0_ns"], manifest["t1_ns"]
    dialects = {d: queries.load_dialect(d) for d in queries.DIALECTS}

    ankaad = None
    try:
        ankaad = export.find_ankaad()
    except FileNotFoundError:
        pass
    meta = sysinfo.collect(ankaad)
    smoke = args.smoke or not meta["publishable_platform"]
    if smoke and not args.smoke:
        print("! not aarch64 Linux: forcing smoke mode (results go to results/smoke/, labelled as not a result)")

    ref = DuckDBEngine()
    ref.load(args.data_dir, manifest)
    ref_results: dict[str, object] = {}
    if not args.no_verify:
        for q in qs:
            ref_results[q.id] = ref.run(queries.render(dialects["duckdb"][q.id], "duckdb", t0, t1)).table

    engine_names = [e.strip() for e in args.engines.split(",") if e.strip()]
    results, versions = [], {}
    for name in engine_names:
        print(f"== {name}")
        try:
            eng = make_engine(name, args)
            versions[name] = eng.version()
            if args.load or name in ("ankaa", "duckdb", "ankaa-flight"):
                eng.load(args.data_dir, manifest)
        except Exception as e:  # noqa: BLE001
            print(f"   cannot start {name}: {e}")
            versions[name] = f"unavailable: {e}"
            for q in qs:
                results.append({"engine": name, "query": q.id, "status": "error", "error": str(e)})
            continue
        for q in qs:
            sql = dialects[eng.dialect].get(q.id)
            if sql is None:
                results.append({"engine": name, "query": q.id, "status": "unsupported"})
                print(f"   {q.id:28s} n/a (not expressible in {eng.dialect})")
                continue
            sql = queries.render(sql, eng.dialect, t0, t1)
            rec: dict = {"engine": name, "query": q.id, "status": "ok", "sql": sql, "timing_scope": eng.timing_scope}
            try:
                if not args.no_verify:
                    # A wrong answer is asked for again (twice, a second apart) before it is
                    # recorded: it tells a result that is wrong every time (the query or the
                    # loaded data) from one that was wrong once (the engine was not settled).
                    # Either way the first answer is kept in the JSON.
                    first_problems = None
                    for attempt in range(1, 4):
                        got = eng.run(sql).table
                        d = diff_tables(ref_results[q.id], got, q.spec_for(name), alpha=alpha, expected_name="duckdb", actual_name=name)
                        if d.ok:
                            break
                        first_problems = first_problems or d.problems
                        print(f"   {q.id:28s} RESULT MISMATCH vs duckdb (attempt {attempt}):\n      " + "\n      ".join(d.problems[:6]))
                        time.sleep(1.0)
                    rec["verified"] = d.ok
                    rec["verify_attempts"] = attempt
                    if not d.ok:
                        rec["mismatch"] = d.problems
                    elif first_problems:
                        rec["unstable_result"] = first_problems
                        print(f"   {q.id:28s} matched on attempt {attempt}; the first answer differed (recorded as unstable_result)")
                else:
                    rec["verified"] = None
                timing = eng.time_query(sql, args.warmup, args.runs)
                rec["rows"] = timing.rows
                rec["client"] = _summary(timing.client_ms)
                rec["server"] = _summary(timing.server_ms) if timing.server_ms else None
                t = rec["client"] or rec["server"]
                print(f"   {q.id:28s} median {fmt_ms(t['median_ms']):>9} ms  p95 {fmt_ms(t['p95_ms']):>9} ms  rows {timing.rows}"
                      + ("" if rec["verified"] in (True, None) else "  [mismatch]"))
            except Exception as e:  # noqa: BLE001
                rec.update(status="error", error=str(e), trace=traceback.format_exc()[-2000:])
                print(f"   {q.id:28s} ERROR {str(e).splitlines()[0][:160]}")
            results.append(rec)
        eng.close()
    ref.close()

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    host = meta["hostname"]  # a label, never the machine's real hostname (see sysinfo.host_label)
    out_dir = args.results_dir / "smoke" if smoke else args.results_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = manifest["tables"]
    report = {
        "smoke": smoke,
        "label": args.label,
        "meta": meta,
        "protocol": "docs/benchmarks/comparison-method.md",
        "dataset": {
            "manifest": manifest,
            "summary": ", ".join(f"{k}={v['rows']:,d}" for k, v in tables.items()),
        },
        "runs": args.runs,
        "warmup": args.warmup,
        "engines": engine_names,
        "versions": versions,
        "queries": [{"id": q.id, "title": q.title, "description": q.description} for q in qs],
        "results": results,
    }
    base = out_dir / f"{date}-{host}"
    (base.with_suffix(".json")).write_text(json.dumps(report, indent=2, default=str))
    (base.with_suffix(".md")).write_text(markdown(report))
    print(f"+ wrote {base}.json and {base}.md" + ("  (SMOKE TEST, not a result)" if smoke else ""))


def _summary(xs):
    from harness.engines import _summary as s

    return s(list(xs))


if __name__ == "__main__":
    main()
