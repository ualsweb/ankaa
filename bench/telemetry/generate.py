#!/usr/bin/env python3
"""Generate the TSBS cpu-only dataset and the files every engine loads from.

    go install github.com/timescale/tsbs/cmd/tsbs_generate_data@8323e59c74027b108f4ad5ec5d3e498b0101a02e
    bench/.venv/bin/python bench/telemetry/generate.py                       # scale 1000, 3 days, seed 123
    bench/.venv/bin/python bench/telemetry/generate.py --scale 4000          # TSBS README scale (needs ~32 GB RAM)
    bench/.venv/bin/python bench/telemetry/generate.py --scale 100 --data-dir bench/telemetry/data/smoke

1. `tsbs_generate_data --use-case=cpu-only --format=influx` (the pinned commit; checked with
   `go version -m`) writes the line protocol: the single source of truth. Its sha256 and line count are
   recorded; the generator is deterministic for a given seed, so the same flags give the same bytes.
2. convert.py streams it into cpu.parquet (DuckDB), cpu.csv (ANKAA, TimescaleDB) and cpu.lp (InfluxDB,
   QuestDB) and checks every output's row count against the parsed lines and against
   scale x (end - start) / interval.
3. manifest.json describes the dataset (generator, flags, hashes, rows, hosts, time range, columns).

The raw TSBS output is deleted after conversion unless --keep-raw (cpu.lp holds the same points).
Disk: about 0.9 GB raw + 0.85 GB lp + 0.3 GB csv + 0.03 GB parquet per 100 hosts x 3 days.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import convert  # noqa: E402

TSBS_MODULE = "github.com/timescale/tsbs"
TSBS_COMMIT = "8323e59c74027b108f4ad5ec5d3e498b0101a02e"
TSBS_PSEUDO_VERSION = "v0.0.0-20260527045238-8323e59c7402"
INSTALL_HINT = f"go install {TSBS_MODULE}/cmd/tsbs_generate_data@{TSBS_COMMIT}"


def find_tsbs(explicit: Path | None) -> Path:
    if explicit:
        if not explicit.exists():
            raise SystemExit(f"{explicit} does not exist")
        return explicit
    found = shutil.which("tsbs_generate_data")
    if found:
        return Path(found)
    for env in ("GOBIN",):
        if os.environ.get(env):
            cand = Path(os.environ[env]) / "tsbs_generate_data"
            if cand.exists():
                return cand
    gopath = subprocess.run(["go", "env", "GOPATH"], capture_output=True, text=True).stdout.strip() if shutil.which("go") else ""
    for root in filter(None, [gopath, str(Path.home() / "go")]):
        cand = Path(root) / "bin" / "tsbs_generate_data"
        if cand.exists():
            return cand
    raise SystemExit(f"tsbs_generate_data not found; install the pinned commit with:\n  {INSTALL_HINT}")


def tsbs_module_version(binary: Path) -> str | None:
    """The tsbs module version embedded in the binary (`go version -m`), or None without a Go toolchain."""
    if not shutil.which("go"):
        return None
    out = subprocess.run(["go", "version", "-m", str(binary)], capture_output=True, text=True).stdout
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0] == "mod" and parts[1] == TSBS_MODULE:
            return parts[2]
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", type=int, default=1000, help="hosts (TSBS --scale); default 1000")
    ap.add_argument("--start", default="2016-01-01T00:00:00Z", help="TSBS --timestamp-start (the query manifest assumes this)")
    ap.add_argument("--end", default="2016-01-04T00:00:00Z", help="TSBS --timestamp-end (3 days; the query manifest needs them all)")
    ap.add_argument("--interval", default="10s", help="TSBS --log-interval (whole seconds)")
    ap.add_argument("--seed", type=int, default=123, help="TSBS --seed (123 as in the TSBS README)")
    ap.add_argument("--data-dir", type=Path, default=None, help="default bench/telemetry/data/tsbs-cpu-s<scale>-<days>d-seed<seed>")
    ap.add_argument("--tsbs", type=Path, default=None, help="path to tsbs_generate_data (default: PATH, $GOBIN, $GOPATH/bin)")
    ap.add_argument("--allow-other-tsbs", action="store_true", help="accept a generator that is not the pinned commit")
    ap.add_argument("--keep-raw", action="store_true", help="keep the raw TSBS output (cpu.tsbs.lp)")
    ap.add_argument("--block-mib", type=int, default=64)
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if not args.interval.endswith("s") or not args.interval[:-1].isdigit():
        raise SystemExit("--interval must be whole seconds, e.g. 10s")
    interval_s = int(args.interval[:-1])
    span_s = int((end - start).total_seconds())
    if span_s <= 0 or span_s % interval_s:
        raise SystemExit("--end - --start must be a positive multiple of --interval")
    expected_rows = args.scale * span_s // interval_s
    days = span_s / 86400
    name = f"tsbs-cpu-s{args.scale}-{days:g}d-seed{args.seed}"
    data_dir = args.data_dir or (HERE / "data" / name)
    data_dir.mkdir(parents=True, exist_ok=True)

    tsbs = find_tsbs(args.tsbs)
    version = tsbs_module_version(tsbs)
    if version != TSBS_PSEUDO_VERSION and not args.allow_other_tsbs:
        raise SystemExit(f"{tsbs} is tsbs {version or '(unknown: no Go toolchain to read it)'}, the pinned one is "
                         f"{TSBS_PSEUDO_VERSION} (commit {TSBS_COMMIT}).\n  {INSTALL_HINT}\n"
                         "or pass --allow-other-tsbs (recorded in the manifest).")
    raw = data_dir / "cpu.tsbs.lp"
    cmd = [str(tsbs), "--use-case=cpu-only", "--format=influx", f"--scale={args.scale}", f"--seed={args.seed}",
           f"--timestamp-start={args.start}", f"--timestamp-end={args.end}", f"--log-interval={args.interval}",
           f"--file={raw}"]
    print(f"+ {' '.join(cmd)}")
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    gen_s = time.perf_counter() - t0
    if proc.returncode != 0:
        raise SystemExit(f"tsbs_generate_data failed ({proc.returncode}):\n{proc.stderr[-3000:]}")
    raw_lines = convert.count_lines(raw)
    print(f"  {raw_lines:,d} lines, {raw.stat().st_size / 1e9:.2f} GB in {gen_s:.1f} s")
    if raw_lines != expected_rows:
        raise SystemExit(f"TSBS wrote {raw_lines} lines, expected scale x span / interval = {expected_rows}")
    raw_info = {"path": raw.name, "bytes": raw.stat().st_size, "rows": raw_lines}
    print("  sha256 ...")
    raw_info["sha256"] = convert.sha256_file(raw)

    print(f"+ converting -> {data_dir}")
    info = convert.convert(raw, data_dir, args.block_mib)
    convert.verify_outputs(data_dir, info)
    if info["rows"] != expected_rows or info["hosts"] != args.scale:
        raise SystemExit(f"converted {info['rows']} rows / {info['hosts']} hosts, expected {expected_rows} / {args.scale}")

    manifest = {
        "dataset": name,
        "use_case": "TSBS cpu-only (DevOps), one table `cpu`",
        "generator": {"tool": "tsbs_generate_data", "module": TSBS_MODULE, "commit": TSBS_COMMIT,
                      "module_version": version, "pinned": version == TSBS_PSEUDO_VERSION, "binary": str(tsbs),
                      "args": cmd[1:-1], "seconds": round(gen_s, 3)},
        "scale": args.scale, "start": args.start, "end": args.end, "interval_s": interval_s, "seed": args.seed,
        "rows": info["rows"], "hosts": info["hosts"], "ts_min_ns": info["ts_min_ns"], "ts_max_ns": info["ts_max_ns"],
        "table": "cpu", "tags": convert.TAGS, "fields": convert.FIELDS,
        "types": {"ts": "timestamp ns UTC", "tags": "string", "fields": "float64 (TSBS prints integers; converted exactly)"},
        "source_lp": raw_info | {"kept": bool(args.keep_raw)},
        "files": info["files"],
        "convert_s": info["convert_s"],
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (data_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    if not args.keep_raw:
        raw.unlink()
    print(f"+ {info['rows']:,d} rows, {info['hosts']} hosts, {args.start} .. {args.end}")
    print(f"+ ready: bench/.venv/bin/python bench/telemetry/run.py --data-dir {data_dir}")


if __name__ == "__main__":
    main()
