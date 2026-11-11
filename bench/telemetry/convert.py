#!/usr/bin/env python3
"""Stream the TSBS cpu-only line protocol into the files every engine loads from.

    python bench/telemetry/convert.py <cpu.tsbs.lp> <out-dir>

Input: the output of `tsbs_generate_data --use-case=cpu-only --format=influx`, one point per line:

    cpu,hostname=host_0,region=...,service_environment=test usage_user=58i,...,usage_guest_nice=38i 1451606400000000000

Outputs (in <out-dir>), all with the same rows in the same order:

    cpu.parquet  explicit schema (SCHEMA below): ts timestamp[ns] (UTC, stored tz-naive), 10 tags as
                 strings, 10 fields as float64; zstd. Loaded by DuckDB (the reference).
    cpu.csv      no header, columns in SCHEMA order, ts as RFC 3339 UTC text with whole seconds
                 (`2016-01-01T00:00:10Z`). Loaded by ANKAA (file source) and TimescaleDB (COPY).
    cpu.lp       the input with every integer field written as a float (`58i` -> `58`). Loaded by
                 InfluxDB 3 Core and QuestDB (both ingest line protocol natively), so every engine
                 stores float64 fields.

The TSBS generator of the pinned commit writes the CPU fields as integers (`58i`). The task's schema
is float64 everywhere; the integers are converted exactly (|v| <= 100), so values are identical
across engines and only the storage type changes from what the generator printed.

The parser is vectorised (pyarrow compute over blocks of lines): every batch is checked for the
expected measurement, tag keys, field keys and order, and for the absence of escapes, so a line that
does not have the exact TSBS shape stops the conversion instead of being guessed at.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

MEASUREMENT = "cpu"
TAGS = ["hostname", "region", "datacenter", "rack", "os", "arch", "team", "service", "service_version",
        "service_environment"]
FIELDS = ["usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait", "usage_irq",
          "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice"]
COLUMNS = ["ts", *TAGS, *FIELDS]
SCHEMA = pa.schema([("ts", pa.timestamp("ns"))] + [(t, pa.string()) for t in TAGS] + [(f, pa.float64()) for f in FIELDS])

NS = 1_000_000_000


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def _element(lists: pa.Array, i: int, key: str, what: str) -> pa.Array:
    """`key=value` element i of every row -> the values; fail when a row has another key there."""
    kv = pc.list_element(lists, i)
    prefix = key + "="
    ok = pc.starts_with(kv, prefix)
    if not pc.all(ok).as_py():
        bad = pc.filter(kv, pc.invert(ok))[0].as_py()
        raise ValueError(f"{what} {i} is {bad!r}, expected {prefix}<value>")
    return pc.utf8_slice_codeunits(kv, len(prefix))


@dataclass
class Stats:
    rows: int = 0
    ts_min_ns: int | None = None
    ts_max_ns: int | None = None
    hosts: set[str] = field(default_factory=set)
    unsorted_pairs: int = 0  # adjacent rows with a decreasing timestamp (TSBS output is time-ordered)
    last_ts: int | None = None


def parse_batch(batch: pa.RecordBatch) -> tuple[pa.Table, pa.Array]:
    """One block of LP lines -> (typed table in SCHEMA, float line-protocol lines)."""
    series, fields, ts = batch.column(0), batch.column(1), batch.column(2)
    for name, col in (("series", series), ("fields", fields)):
        if col.null_count:
            raise ValueError(f"line without {name}")
        if pc.any(pc.match_substring(col, "\\")).as_py() or pc.any(pc.match_substring(col, '"')).as_py():
            raise ValueError(f"escaped or quoted {name} value: not the TSBS cpu-only shape")
    if ts.null_count:
        raise ValueError("line without a timestamp")

    tag_lists = pc.split_pattern(series, ",")
    n_tags = pc.list_value_length(tag_lists)
    if not pc.all(pc.equal(n_tags, len(TAGS) + 1)).as_py():
        raise ValueError(f"series key without exactly {len(TAGS)} tags: {series[0].as_py()!r}")
    meas = pc.list_element(tag_lists, 0)
    if not pc.all(pc.equal(meas, MEASUREMENT)).as_py():
        raise ValueError(f"measurement other than {MEASUREMENT!r}")
    field_lists = pc.split_pattern(fields, ",")
    if not pc.all(pc.equal(pc.list_value_length(field_lists), len(FIELDS))).as_py():
        raise ValueError(f"field set without exactly {len(FIELDS)} fields: {fields[0].as_py()!r}")

    cols: list[pa.Array] = [ts.cast(pa.timestamp("ns"))]
    for i, t in enumerate(TAGS, start=1):
        cols.append(_element(tag_lists, i, t, "tag"))
    for i, f in enumerate(FIELDS):
        raw = _element(field_lists, i, f, "field")
        # LP numbers: `58i` integer, `58` / `58.5` float. Both become float64.
        cols.append(pc.replace_substring_regex(raw, r"i$", "").cast(pa.float64()))
    table = pa.Table.from_arrays(cols, schema=SCHEMA)

    float_fields = pc.replace_substring_regex(fields, r"(\d)i(,|$)", r"\1\2")
    lp = pc.binary_join_element_wise(series, float_fields, ts.cast(pa.string()), " ")
    lp = pc.binary_join_element_wise(lp, pa.scalar(""), "\n")  # every line ends with a newline
    return table, lp


def write_lines(f, arr: pa.Array) -> None:
    """Write a string array whose values already end in a newline, straight from its data buffer."""
    arr = arr.combine_chunks() if isinstance(arr, pa.ChunkedArray) else arr
    arr = arr.cast(pa.large_string()) if arr.type != pa.large_string() else arr
    offsets = arr.buffers()[1]
    off = np.frombuffer(offsets, dtype=np.int64)[arr.offset: arr.offset + len(arr) + 1]
    f.write(memoryview(arr.buffers()[2])[int(off[0]): int(off[-1])])


def csv_view(table: pa.Table) -> pa.Table:
    """The CSV representation: ts as RFC 3339 UTC with whole seconds, everything else as is."""
    ts_ns = table["ts"].cast(pa.int64()).to_numpy()
    if (ts_ns % NS != 0).any():
        raise ValueError("timestamps with a fraction of a second: the CSV writer only emits whole seconds "
                         "(TSBS --log-interval must be a whole number of seconds)")
    ts_text = pc.strftime(table["ts"].cast(pa.timestamp("s", tz="UTC")), format="%Y-%m-%dT%H:%M:%SZ")
    return table.set_column(0, pa.field("ts", pa.string()), ts_text)


def convert(src: Path, out_dir: Path, block_mib: int = 64, progress: bool = True) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    pq_path, csv_path, lp_path = out_dir / "cpu.parquet", out_dir / "cpu.csv", out_dir / "cpu.lp"
    reader = pcsv.open_csv(
        src,
        read_options=pcsv.ReadOptions(column_names=["series", "fields", "ts"], block_size=block_mib << 20),
        parse_options=pcsv.ParseOptions(delimiter=" ", quote_char=False, escape_char=False, double_quote=False),
        convert_options=pcsv.ConvertOptions(column_types={"series": pa.string(), "fields": pa.string(), "ts": pa.int64()},
                                            strings_can_be_null=False, quoted_strings_can_be_null=False),
    )
    st = Stats()
    t0 = time.perf_counter()
    csv_opts = pcsv.WriteOptions(include_header=False, quoting_style="none")
    with pq.ParquetWriter(pq_path, SCHEMA, compression="zstd") as pw, \
            pcsv.CSVWriter(csv_path, pa.schema([("ts", pa.string())] + list(SCHEMA)[1:]), write_options=csv_opts) as cw, \
            lp_path.open("wb") as lw:
        for batch in reader:
            if batch.num_rows == 0:
                continue
            table, lp = parse_batch(batch)
            pw.write_table(table)
            cw.write_table(csv_view(table))
            write_lines(lw, lp)
            ts = table["ts"].cast(pa.int64())
            lo, hi = pc.min(ts).as_py(), pc.max(ts).as_py()
            st.ts_min_ns = lo if st.ts_min_ns is None else min(st.ts_min_ns, lo)
            st.ts_max_ns = hi if st.ts_max_ns is None else max(st.ts_max_ns, hi)
            arr = ts.combine_chunks() if isinstance(ts, pa.ChunkedArray) else ts
            if st.last_ts is not None and arr[0].as_py() < st.last_ts:
                st.unsorted_pairs += 1
            if len(arr) > 1:
                st.unsorted_pairs += pc.sum(pc.less(arr[1:], arr[:-1])).as_py() or 0
            st.last_ts = arr[-1].as_py()
            st.hosts.update(pc.unique(table["hostname"]).to_pylist())
            st.rows += table.num_rows
            if progress:
                el = time.perf_counter() - t0
                print(f"\r  {st.rows:>13,d} rows  {st.rows / max(el, 1e-9):>10,.0f} rows/s", end="", file=sys.stderr, flush=True)
    if progress:
        print(file=sys.stderr)
    if st.rows == 0:
        raise ValueError(f"{src} holds no lines")
    if st.unsorted_pairs:
        raise ValueError(f"{src}: {st.unsorted_pairs} places where the timestamp decreases; TSBS output is "
                         "time-ordered and the ANKAA file source relies on per-host order")
    return {
        "rows": st.rows,
        "hosts": len(st.hosts),
        "ts_min_ns": st.ts_min_ns,
        "ts_max_ns": st.ts_max_ns,
        "convert_s": round(time.perf_counter() - t0, 3),
        "files": {
            "parquet": {"path": pq_path.name},
            "csv": {"path": csv_path.name, "header": False, "columns": COLUMNS, "ts_format": "RFC 3339 UTC, whole seconds"},
            "lp": {"path": lp_path.name, "fields": "float (integer suffix removed)", "precision": "ns"},
        },
    }


def count_lines(path: Path) -> int:
    n = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            n += chunk.count(b"\n")
    return n


def verify_outputs(out_dir: Path, info: dict) -> None:
    """Row counts of the three outputs must equal the parsed line count."""
    rows = info["rows"]
    pq_rows = pq.ParquetFile(out_dir / "cpu.parquet").metadata.num_rows
    csv_rows = count_lines(out_dir / "cpu.csv")
    lp_rows = count_lines(out_dir / "cpu.lp")
    bad = {k: v for k, v in (("parquet", pq_rows), ("csv", csv_rows), ("lp", lp_rows)) if v != rows}
    if bad:
        raise ValueError(f"row counts differ from the {rows} parsed lines: {bad}")
    for kind in ("parquet", "csv", "lp"):
        p = out_dir / info["files"][kind]["path"]
        info["files"][kind].update(bytes=p.stat().st_size, sha256=sha256_file(p), rows=rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("src", type=Path, help="TSBS cpu-only influx output")
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--block-mib", type=int, default=64, help="lines parsed per block (memory ~ 10x this)")
    args = ap.parse_args()
    info = convert(args.src, args.out_dir, args.block_mib)
    verify_outputs(args.out_dir, info)
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
