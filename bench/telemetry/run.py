#!/usr/bin/env python3
"""TSBS cpu-only on ANKAA, InfluxDB 3 Core, TimescaleDB and QuestDB: load each engine through its fastest
documented bulk path (timed), verify all 15 query types against DuckDB, then time them.

    bench/.venv/bin/python bench/telemetry/run.py --data-dir bench/telemetry/data/tsbs-cpu-s1000-3d-seed123 \
        --runs 20 --warmup 3 --label "c4a-standard-4, Ubuntu 24.04"

One engine at a time: the DuckDB reference answers are computed first (then DuckDB is closed), then
for every engine: start it on an empty volume (a container via docker-compose.yml, or `ankaad serve`),
load and time the load, run the engine's post-load step (TimescaleDB: columnstore conversion; InfluxDB:
wait until every row is persisted to Parquet), wait until it is idle, record disk and memory, verify every
query against the reference, time every query (warm-up + N runs), stop it. Nothing else of ours runs
while an engine is measured; other running containers are refused unless --allow-other-containers.

Writes results/<date>-<host>.json (every number, every SQL text, hardware facts) and .md (tables). On
anything that is not aarch64 Linux the run is forced into smoke mode: output goes to results/smoke/ and is
labelled as not a result (docs/benchmarks/comparison-method.md).
"""
from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "compare"))
sys.path.insert(0, str(HERE))

from harness import export, queries, sysinfo  # noqa: E402
from harness.compare import diff_tables  # noqa: E402
from harness.engines import (  # noqa: E402
    AnkaaServeEngine,
    DuckDBTelemetryEngine,
    Engine,
    InfluxDB3Engine,
    QuestDBTelemetryEngine,
    TimescaleDBEngine,
    _summary,
)
import probes  # noqa: E402

QUERIES_DIR = HERE / "queries"
COMPOSE = HERE / "docker-compose.yml"
PROJECT = "ankaa-tsbs"
ENGINES = ("ankaa", "influxdb3", "timescaledb", "questdb", "duckdb")
CONTAINERS = {
    "influxdb3": {"service": "influxdb3", "container": "ankaa-tsbs-influxdb3", "volume": "influxdb3-data",
                  "data_paths": ["/var/lib/influxdb3"]},
    "timescaledb": {"service": "timescaledb", "container": "ankaa-tsbs-timescaledb", "volume": "timescaledb-data",
                    "data_paths": ["/var/lib/postgresql/data"]},
    "questdb": {"service": "questdb", "container": "ankaa-tsbs-questdb", "volume": "questdb-data",
                "data_paths": ["/var/lib/questdb/db"]},
}


# ---------------------------------------------------------------------------------------- utilities
def log(msg: str) -> None:
    print(msg, flush=True)


def fmt_ms(x: float | None) -> str:
    if x is None:
        return "-"
    return f"{x:.3f}" if x < 10 else f"{x:.1f}"


def mib(x: int | None) -> str:
    return "-" if not x else f"{x / 1024**2:,.0f}"


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), *args]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{r.stderr[-2000:]}")
    return r


def other_containers() -> list[str]:
    r = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
    return [n for n in r.stdout.split() if not n.startswith("ankaa-tsbs-")]


def container_down(engine: str, drop_volume: bool) -> None:
    c = CONTAINERS[engine]
    compose("rm", "--stop", "--force", "-v", c["service"], check=False)
    if drop_volume:
        subprocess.run(["docker", "volume", "rm", "-f", f"{PROJECT}_{c['volume']}"], capture_output=True, text=True)


def container_up(engine: str) -> None:
    container_down(engine, drop_volume=True)  # always load into an empty volume
    compose("up", "-d", CONTAINERS[engine]["service"])


def image_of(engine: str) -> str:
    r = subprocess.run(["docker", "inspect", "-f", "{{.Config.Image}} {{.Image}}", CONTAINERS[engine]["container"]],
                       capture_output=True, text=True)
    return r.stdout.strip()


def wait_until(what: str, ready, timeout_s: float = 180.0) -> float:
    t0 = time.monotonic()
    last = None
    while time.monotonic() - t0 < timeout_s:
        try:
            if ready():
                return time.monotonic() - t0
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(1.0)
    raise RuntimeError(f"{what} not ready after {timeout_s:.0f} s ({last})")


def http_ok(url: str) -> bool:
    import requests

    return requests.get(url, timeout=5).status_code == 200


def pg_ready(dsn: str) -> bool:
    import psycopg

    with psycopg.connect(dsn, connect_timeout=5) as c:
        return c.execute("SELECT count(*) FROM pg_available_extensions WHERE name = 'timescaledb'").fetchone()[0] == 1


# ------------------------------------------------------------------------------------- the dataset
def check_dataset(manifest: dict, qmanifest: dict) -> None:
    ds = qmanifest["dataset"]
    problems = []
    if manifest.get("start") != ds["start"] or manifest.get("end") != ds["end"]:
        problems.append(f"dataset covers {manifest.get('start')} .. {manifest.get('end')}, the query parameters need "
                        f"{ds['start']} .. {ds['end']} (generate.py defaults)")
    if int(manifest.get("scale", 0)) < int(ds["min_scale"]):
        problems.append(f"scale {manifest.get('scale')} < {ds['min_scale']}: the fixed host names go up to host_{ds['min_scale'] - 1}")
    for kind in ("parquet", "csv", "lp"):
        if kind not in manifest.get("files", {}):
            problems.append(f"manifest lists no {kind} file")
    if problems:
        raise SystemExit("dataset does not fit the query set:\n  " + "\n  ".join(problems))


# --------------------------------------------------------------------------------- verify and time
def verify_and_time(eng: Engine, name: str, qs: list, sqls: dict[str, str], params: dict, refs: dict, alpha: float,
                    args) -> list[dict]:
    out = []
    for q in qs:
        rec: dict = {"engine": name, "query": q.id, "status": "ok", "timing_scope": eng.timing_scope}
        if q.id not in sqls:
            rec["status"] = "unsupported"
            log(f"   {q.id:24s} n/a (not expressible in {eng.dialect})")
            out.append(rec)
            continue
        try:
            sql = queries.render_params(sqls[q.id], eng.dialect, params[q.id])
            rec["sql"] = sql
            if refs:
                first_problems, attempt, d = None, 0, None
                for attempt in range(1, 4):
                    got = eng.run(sql).table
                    d = diff_tables(refs[q.id], got, q.spec_for(name), alpha=alpha, expected_name="duckdb", actual_name=name)
                    if d.ok:
                        break
                    first_problems = first_problems or d.problems
                    log(f"   {q.id:24s} RESULT MISMATCH vs duckdb (attempt {attempt}):\n      " + "\n      ".join(d.problems[:6]))
                    time.sleep(1.0)
                rec["verified"] = d.ok
                rec["verify_attempts"] = attempt
                if not d.ok:
                    rec["mismatch"] = d.problems
                elif first_problems:
                    rec["unstable_result"] = first_problems
            else:
                rec["verified"] = None
            timing = eng.time_query(sql, args.warmup, args.runs)
            rec["rows"] = timing.rows
            rec["client"] = _summary(list(timing.client_ms))
            rec["server"] = _summary(list(timing.server_ms)) if timing.server_ms else None
            t = rec["client"] or rec["server"]
            log(f"   {q.id:24s} median {fmt_ms(t['median_ms']):>9} ms  p95 {fmt_ms(t['p95_ms']):>9} ms  rows {timing.rows}"
                + ("" if rec["verified"] in (True, None) else "  [mismatch]"))
        except Exception as e:  # noqa: BLE001
            rec.update(status="error", error=str(e), trace=traceback.format_exc()[-2000:])
            log(f"   {q.id:24s} ERROR {str(e).splitlines()[0][:200] if str(e) else type(e).__name__}")
        out.append(rec)
    return out


def reference_child(data_dir: Path, manifest: dict, qs: list, sqls: dict[str, str], params: dict, alpha: float, args,
                    time_it: bool, out_dir: Path) -> None:
    """Child process: load DuckDB, write every reference answer to <out_dir>/<query>.parquet and
    reference.json (version, load facts, and DuckDB's own timings when `time_it`)."""
    import shutil

    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    ref = DuckDBTelemetryEngine()
    load = ref.load(data_dir, manifest)
    load["rows_per_s"] = load["rows"] / load["wall_s"]
    log(f"   load: {load['rows']:,d} rows in {load['wall_s']:.1f} s (Parquet -> in-memory table)")
    refs = {}
    if not args.no_verify:
        for q in qs:
            refs[q.id] = ref.run(queries.render_params(sqls[q.id], "duckdb", params[q.id])).table
            pq.write_table(refs[q.id], out_dir / f"{q.id}.parquet")
            log(f"   reference {q.id:24s} rows {refs[q.id].num_rows}")
    results = verify_and_time(ref, "duckdb", qs, sqls, params, refs, alpha, args) if time_it else []
    (out_dir / "reference.json").write_text(json.dumps({"version": ref.version(), "load": load, "results": results}, default=str))
    ref.close()


# ---------------------------------------------------------------------------------------- engines
def make(name: str, args, workdir: Path) -> Engine:
    if name == "ankaa":
        return AnkaaServeEngine(workdir / "ankaa", args.ankaa_flight_port, args.ankaa_metrics_port, args.token,
                                warm_budget_mib=args.ankaa_warm_budget_mib, query_memory_mib=args.ankaa_query_memory_mib)
    if name == "influxdb3":
        return InfluxDB3Engine(args.influxdb3_url, transport=args.influxdb3_transport, batch_lines=args.influxdb3_batch_lines,
                               writers=args.writers, gzip=args.influxdb3_gzip)
    if name == "timescaledb":
        return TimescaleDBEngine(args.timescaledb_dsn, workers=args.writers, batch_rows=args.timescaledb_batch_rows)
    if name == "questdb":
        return QuestDBTelemetryEngine(args.questdb_url, args.questdb_dsn, transport=args.questdb_transport,
                                      batch_lines=args.questdb_batch_lines, writers=args.writers)
    raise ValueError(f"unknown engine {name!r}")


def start_container(name: str, args) -> None:
    container_up(name)
    if name == "influxdb3":
        wait_until("influxdb3", lambda: http_ok(args.influxdb3_url.rstrip("/") + "/ping"))
    elif name == "timescaledb":
        wait_until("timescaledb", lambda: pg_ready(args.timescaledb_dsn), timeout_s=300)
    elif name == "questdb":
        wait_until("questdb", lambda: http_ok(args.questdb_url.rstrip("/") + "/exec?query=select%201"))


def measure_engine(name: str, args, data_dir: Path, manifest: dict, qs, dialect_sql, params, refs, alpha, workdir: Path) -> dict:
    """Start, load, settle, measure, verify + time, stop. Returns {versions, ingest, results}."""
    rec: dict = {"engine": name, "results": [], "ingest": {}}
    eng = None
    try:
        if name in CONTAINERS:
            start_container(name, args)
            rec["image"] = image_of(name)
        eng = make(name, args, workdir)
        if name == "ankaa":
            rec["version"] = subprocess.run([str(eng.ankaad), "--version"], capture_output=True, text=True).stdout.strip()
        else:
            rec["version"] = eng.version()
        log(f"   version: {rec['version']}")

        probe: probes.Probe | None = None
        if name in CONTAINERS:
            probe = probes.ContainerProbe(CONTAINERS[name]["container"], CONTAINERS[name]["data_paths"])

        # ---- load (timed)
        if name == "ankaa":
            # the server starts inside load(); sample once it exists
            import threading

            box: dict = {}

            def attach() -> None:
                while eng.pid is None and "done" not in box:
                    time.sleep(0.01)
                if eng.pid is not None:
                    box["probe"] = probes.ProcessProbe(eng.pid, eng.data_paths())
                    box["sampler"] = probes.Sampler(box["probe"]).__enter__()

            th = threading.Thread(target=attach, daemon=True)
            th.start()
            try:
                load = eng.load(data_dir, manifest)
            finally:
                box["done"] = True
                th.join(timeout=5)
                if "sampler" in box:
                    box["sampler"].__exit__(None, None, None)
            probe = box.get("probe")
            load["memory"] = box["sampler"].result() if "sampler" in box else {}
        else:
            with probes.Sampler(probe) as smp:
                load = eng.load(data_dir, manifest)
            load["memory"] = smp.result()
        load["rows_per_s"] = load["rows"] / load["wall_s"] if load.get("wall_s") else None
        rec["ingest"]["load"] = load
        log(f"   load: {load['rows']:,d} rows in {load['wall_s']:.1f} s = {load['rows_per_s']:,.0f} rows/s ({load['method']})")

        # ---- engine-specific post-load step (part of the at-rest state; timed separately)
        if name == "timescaledb" and args.timescaledb_rowstore:
            rec["ingest"]["rowstore_settle"] = probes.wait_idle(probe, args.settle_max_s, args.settle_cpu_pct)
            rec["ingest"]["rowstore_disk"] = eng.sizes()
            log("   timing the rowstore (before compression) as timescaledb-rowstore")
            rec["rowstore_results"] = verify_and_time(eng, "timescaledb-rowstore", qs, dialect_sql["timescaledb"], params, refs, alpha, args)
        if name == "timescaledb":
            with probes.Sampler(probe) as smp:
                cs = eng.columnstore()
            cs["memory"] = smp.result()
            rec["ingest"]["columnstore"] = cs
            log(f"   columnstore: {cs['chunks']} chunks in {cs['wall_s']:.1f} s, "
                f"{mib(cs['bytes_before']['hypertable_bytes'])} -> {mib(cs['bytes_after']['hypertable_bytes'])} MiB")
        if name == "influxdb3":
            rec["ingest"]["persist"] = eng.wait_persisted(int(manifest["rows"]), args.influxdb3_persist_timeout_s)
            log(f"   persisted: {rec['ingest']['persist']}")

        # ---- settle, then disk and memory at rest
        rec["ingest"]["settle"] = probes.wait_idle(probe, args.settle_max_s, args.settle_cpu_pct) if probe else {}
        log(f"   settle: {rec['ingest']['settle']}")
        rec["ingest"]["disk_bytes"] = probe.disk_bytes() if probe else {}
        if name in ("timescaledb", "questdb"):
            rec["ingest"]["sql_sizes"] = eng.sizes()  # the engine's own accounting of the table
        rec["ingest"]["disk_total_bytes"] = sum(rec["ingest"]["disk_bytes"].values()) or None
        rec["ingest"]["memory_at_rest"] = probe.memory() if probe else {}
        log(f"   disk: {mib(rec['ingest']['disk_total_bytes'])} MiB {rec['ingest']['disk_bytes']}")
        rec["probe"] = {"kind": probe.kind, "source": probe.source} if probe else None

        # ---- verify + time
        with probes.Sampler(probe) as smp:
            rec["results"] = verify_and_time(eng, name, qs, dialect_sql[eng.dialect], params, refs, alpha, args)
        rec["ingest"]["memory_queries"] = smp.result()
    except Exception as e:  # noqa: BLE001
        log(f"   {name} FAILED: {e}")
        rec["error"] = str(e)
        rec["trace"] = traceback.format_exc()[-3000:]
        done = {r["query"] for r in rec["results"]}
        rec["results"] += [{"engine": name, "query": q.id, "status": "error", "error": f"engine failed: {e}"} for q in qs if q.id not in done]
    finally:
        if eng is not None:
            try:
                eng.close()
            except Exception:  # noqa: BLE001
                pass
        if name in CONTAINERS and not args.keep_containers:
            container_down(name, drop_volume=not args.keep_volumes)
    return rec


# --------------------------------------------------------------------------------------- report
def markdown(report: dict) -> str:
    meta = report["meta"]
    out = []
    if report["smoke"]:
        out.append("# SMOKE TEST - NOT A BENCHMARK RESULT\n")
        out.append("This run did not happen on Arm server hardware (or was started with --smoke). The numbers only show "
                   "that the harness works. Do not quote them.\n")
    else:
        out.append(f"# TSBS cpu-only: {meta['hostname']} ({meta['timestamp_utc']})\n")
    ds = report["dataset"]
    out.append(f"Dataset: TSBS cpu-only, {ds['rows']:,d} rows, {ds['hosts']} hosts, {ds['start']} .. {ds['end']} every "
               f"{ds['interval_s']} s, seed {ds['seed']} (tsbs {ds['generator']['commit'][:12]}).  ")
    out.append(f"Engines measured one at a time on the same machine; {report['runs']} runs after {report['warmup']} warm-up per query.\n")

    out.append("## Load\n")
    out.append("| engine | method | rows/s | load s | post-load step | disk MiB | peak memory MiB (load) |")
    out.append("|---|---|---:|---:|---|---:|---:|")
    for e in report["engines"]:
        ing = report["ingest"].get(e) or {}
        load = ing.get("load") or {}
        if not load:
            out.append(f"| {e} | {report['errors'].get(e, 'not loaded')[:80]} | | | | | |")
            continue
        post = []
        if "columnstore" in ing:
            post.append(f"columnstore {ing['columnstore']['wall_s']:.1f} s")
        if "persist" in ing:
            post.append(f"persist to Parquet {ing['persist']['persist_wait_s']:.0f} s")
        if ing.get("settle", {}).get("settle_s"):
            post.append(f"idle after {ing['settle']['settle_s']:.0f} s")
        mem = load.get("memory", {})
        peak = mem.get("peak_rss_bytes") or mem.get("sampled_max_anon_bytes") or mem.get("sampled_max_rss_bytes") or mem.get("sampled_max_current_bytes")
        out.append(f"| {e} | {load['method']} | {load['rows_per_s']:,.0f} | {load['wall_s']:.1f} | {'; '.join(post) or '-'} | "
                   f"{mib(ing.get('disk_total_bytes'))} | {mib(peak)} |")
    out.append("")
    out.append("Peak memory: ankaad = VmHWM (peak resident set); containers = highest sampled anonymous memory of the "
               "container's cgroup (cgroup memory.peak, which also counts page cache, is in the JSON). Disk = `du -sk` of the "
               "data directories after the post-load step and idle wait.\n")

    out.append("## Queries\n")
    out.append("Cells: median ms (p95 ms), client-observed. `mismatch` = differs from the DuckDB reference (time not "
               "comparable); `unstable` = first answer differed, a repeat matched; `n/a` = not expressible; "
               "`†` = large result, transfer-dominated.\n")
    cols = report["columns"]
    out.append("| query | " + " | ".join(cols) + " |")
    out.append("|---|" + "---:|" * len(cols))
    by = {(r["engine"], r["query"]): r for r in report["results"]}
    for q in report["queries"]:
        cells = []
        for e in cols:
            r = by.get((e, q["id"]))
            if r is None or r.get("status") == "unsupported":
                cells.append("n/a")
            elif r.get("status") == "error":
                cells.append("error")
            else:
                t = r.get("client") or r.get("server") or {}
                cell = f"{fmt_ms(t.get('median_ms'))} ({fmt_ms(t.get('p95_ms'))})"
                if r.get("verified") is False:
                    cell += " mismatch"
                elif r.get("unstable_result"):
                    cell += " unstable"
                cells.append(cell)
        mark = " †" if q.get("transfer_heavy") else ""
        out.append(f"| {q['id']}{mark} | " + " | ".join(cells) + " |")
    out.append("")
    out.append("Engines and clients:\n")
    for e in cols:
        scope = next((r["timing_scope"] for r in report["results"] if r["engine"] == e and r.get("timing_scope")), "-")
        out.append(f"- `{e}` ({report['versions'].get(e, '?')}): {scope}")
    out.append("")
    out.append("Environment: " + "; ".join(f"{k}={v}" for k, v in meta.items()
                                          if k in ("hostname", "system", "release", "machine", "cpu_brand", "dmi_product_name", "git_commit") and v))
    if report.get("label"):
        out.append(f"\nLabel: {report['label']}")
    return "\n".join(out) + "\n"


# ------------------------------------------------------------------------------------------- main
def main() -> None:
    cpus = os.cpu_count() or 4
    ram = probes.total_memory_bytes()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", type=Path, required=True, help="directory written by generate.py")
    ap.add_argument("--engines", default="ankaa,influxdb3,timescaledb,questdb", help=f"comma list of {', '.join(ENGINES)} "
                    "(duckdb = also TIME the in-memory reference)")
    ap.add_argument("--queries", default=None, help="comma list of query ids (default: all with timing=true)")
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--no-verify", action="store_true", help="skip the result check (never for a report)")
    ap.add_argument("--results-dir", type=Path, default=HERE / "results")
    ap.add_argument("--workdir", type=Path, default=HERE / "work", help="ANKAA's WAL + segments live here")
    ap.add_argument("--smoke", action="store_true", help="label the run as a smoke test (forced off Arm servers)")
    ap.add_argument("--label", default=None, help="free text stored in the report (instance type, notes)")
    ap.add_argument("--allow-other-containers", action="store_true", help="run although other containers are up")
    ap.add_argument("--keep-volumes", action="store_true", help="keep each engine's data volume after its run")
    ap.add_argument("--keep-containers", action="store_true", help="leave each container running after its run (debugging)")
    ap.add_argument("--writers", type=int, default=cpus, help=f"concurrent load requests / COPY connections (default: CPUs = {cpus})")
    ap.add_argument("--settle-max-s", type=float, default=600.0, help="longest wait for an engine to go idle after its load")
    ap.add_argument("--settle-cpu-pct", type=float, default=5.0, help="idle = below this %% of one core over 5 s")
    # ANKAA
    ap.add_argument("--token", default=None, help="bearer token for ankaad (default: random per run)")
    ap.add_argument("--ankaa-flight-port", type=int, default=50071)
    ap.add_argument("--ankaa-metrics-port", type=int, default=9171)
    ap.add_argument("--ankaa-warm-budget-mib", type=int, default=(ram // 4) >> 20 or None,
                    help="storage.warm_budget_mib (default 25%% of RAM, as timescaledb-tune sizes shared_buffers)")
    ap.add_argument("--ankaa-query-memory-mib", type=int, default=(ram // 5) >> 20 or None,
                    help="resources.query_memory_mib (default 20%% of RAM, as InfluxDB 3's exec memory pool)")
    # InfluxDB 3
    ap.add_argument("--influxdb3-url", default="http://127.0.0.1:8181")
    ap.add_argument("--influxdb3-transport", choices=("flight", "http"), default="flight")
    ap.add_argument("--influxdb3-batch-lines", type=int, default=10_000)
    ap.add_argument("--influxdb3-gzip", action="store_true", help="gzip write bodies (InfluxData recommends it on networks)")
    ap.add_argument("--influxdb3-persist-timeout-s", type=float, default=1800.0)
    # TimescaleDB
    ap.add_argument("--timescaledb-dsn", default="host=127.0.0.1 port=5433 user=postgres password=tsbs dbname=postgres")
    ap.add_argument("--timescaledb-batch-rows", type=int, default=50_000)
    ap.add_argument("--timescaledb-rowstore", action="store_true",
                    help="also verify + time the rowstore before the columnstore conversion (column `timescaledb-rowstore`)")
    # QuestDB
    ap.add_argument("--questdb-url", default="http://127.0.0.1:9300")
    ap.add_argument("--questdb-dsn", default="host=127.0.0.1 port=8813 user=admin password=quest dbname=qdb")
    ap.add_argument("--questdb-transport", choices=("pg", "rest"), default="pg")
    ap.add_argument("--questdb-batch-lines", type=int, default=75_000)
    args = ap.parse_args()

    data_dir = args.data_dir.resolve()
    manifest = json.loads((data_dir / "manifest.json").read_text())
    qmanifest = json.loads((QUERIES_DIR / "manifest.json").read_text())
    check_dataset(manifest, qmanifest)
    all_queries, alpha = queries.load_manifest(QUERIES_DIR / "manifest.json")
    params = {q["id"]: q["params"] for q in qmanifest["queries"]}
    extra = {q["id"]: q for q in qmanifest["queries"]}
    wanted = set(args.queries.split(",")) if args.queries else None
    qs = [q for q in all_queries if (wanted and q.id in wanted) or (not wanted and q.timing)]
    if wanted and len(qs) != len(wanted):
        raise SystemExit(f"unknown query ids: {sorted(wanted - {q.id for q in qs})}")
    dialect_sql = {d: queries.load_dialect(d, QUERIES_DIR / f"{d}.sql") for d in ("duckdb", "ankaa", "influxdb3", "timescaledb", "questdb")}
    engine_names = [e.strip() for e in args.engines.split(",") if e.strip()]
    unknown = [e for e in engine_names if e not in ENGINES]
    if unknown:
        raise SystemExit(f"unknown engines {unknown}; choose from {ENGINES}")

    ankaad = None
    try:
        ankaad = export.find_ankaad()
    except FileNotFoundError:
        if "ankaa" in engine_names:
            raise
    meta = sysinfo.collect(ankaad)
    meta["cpus"] = cpus
    meta["mem_total_bytes"] = ram
    smoke = args.smoke or not meta["publishable_platform"]
    if smoke and not args.smoke:
        log("! not aarch64 Linux: forcing smoke mode (results go to results/smoke/, labelled as not a result)")
    if args.no_verify and not smoke:
        raise SystemExit("--no-verify is for smoke runs only: a report's times must be verified (comparison-method.md rule 4)")

    if any(e in CONTAINERS for e in engine_names):
        others = other_containers()
        if others and not args.allow_other_containers:
            msg = f"other containers are running ({', '.join(others)}): they compete for the CPUs being measured"
            if smoke:
                log(f"! {msg} (smoke run: continuing)")
            else:
                raise SystemExit(msg + ". Stop them or pass --allow-other-containers (recorded in the report).")
        for e in CONTAINERS:
            container_down(e, drop_volume=False)

    # ---- the reference (and DuckDB's own timing, if asked), before any other engine runs. It runs in a
    # child process that exits before the first engine starts, so DuckDB's memory is returned to the OS.
    workdir = args.workdir.resolve()
    log("== duckdb (reference, in a child process)")
    ref_dir = workdir / "reference"
    ctx = multiprocessing.get_context("spawn")
    child = ctx.Process(target=reference_child, args=(data_dir, manifest, qs, dialect_sql["duckdb"], params, alpha, args,
                                                       "duckdb" in engine_names, ref_dir))
    child.start()
    child.join()
    if child.exitcode != 0:
        raise SystemExit(f"the DuckDB reference failed (exit {child.exitcode}); see the output above")
    ref_out = json.loads((ref_dir / "reference.json").read_text())
    refs: dict = {} if args.no_verify else {q.id: pq.read_table(ref_dir / f"{q.id}.parquet") for q in qs}
    versions: dict = {"duckdb": ref_out["version"]}
    ingest: dict = {}
    results: list[dict] = []
    if "duckdb" in engine_names:
        ingest["duckdb"] = {"load": ref_out["load"]}
        results += ref_out["results"]

    errors: dict = {}
    images: dict = {}
    columns = [e for e in engine_names if e == "duckdb"]
    for name in [e for e in engine_names if e != "duckdb"]:
        log(f"== {name}")
        rec = measure_engine(name, args, data_dir, manifest, qs, dialect_sql, params, refs, alpha, workdir)
        versions[name] = rec.get("version", f"unavailable: {rec.get('error', '?')}")
        ingest[name] = rec["ingest"] | {"probe": rec.get("probe")}
        if rec.get("image"):
            images[name] = rec["image"]
        if rec.get("error"):
            errors[name] = rec["error"]
        if rec.get("rowstore_results"):
            results += rec["rowstore_results"]
            columns.append("timescaledb-rowstore")
            versions["timescaledb-rowstore"] = versions[name] + " (rowstore, before columnstore conversion)"
        results += rec["results"]
        columns.append(name)
        if name == "ankaa" and not args.keep_volumes:
            import shutil

            for sub in ("wal", "segments"):
                shutil.rmtree(workdir / "ankaa" / sub, ignore_errors=True)

    if any(e in CONTAINERS for e in engine_names) and not (args.keep_containers or args.keep_volumes):
        compose("down", check=False)  # the project network; containers and volumes are already gone

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    host = meta["hostname"]  # a label, never the machine's real hostname (harness/sysinfo.host_label)
    out_dir = args.results_dir / "smoke" if smoke else args.results_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "smoke": smoke,
        "label": args.label,
        "meta": meta,
        "protocol": "docs/benchmarks/comparison-method.md + bench/telemetry/README.md",
        "dataset": {k: manifest[k] for k in ("dataset", "generator", "scale", "start", "end", "interval_s", "seed", "rows",
                                             "hosts", "ts_min_ns", "ts_max_ns", "source_lp", "files")},
        "runs": args.runs,
        "warmup": args.warmup,
        "settings": {k: v for k, v in vars(args).items() if k not in ("token",)} | {"data_dir": str(data_dir)},
        "other_containers_running": other_containers() if args.allow_other_containers else [],
        "engines": [e for e in engine_names],
        "columns": columns,
        "versions": versions,
        "images": images,
        "errors": errors,
        "ingest": ingest,
        "queries": [{"id": q.id, "title": q.title, "description": q.description, "params": params[q.id],
                     "transfer_heavy": bool(extra[q.id].get("transfer_heavy"))} for q in qs],
        "results": results,
    }
    base = out_dir / f"{date}-{host}"
    base.with_suffix(".json").write_text(json.dumps(report, indent=2, default=str))
    base.with_suffix(".md").write_text(markdown(report))
    log(f"+ wrote {base}.json and {base}.md" + ("  (SMOKE TEST, not a result)" if smoke else ""))


if __name__ == "__main__":
    main()
