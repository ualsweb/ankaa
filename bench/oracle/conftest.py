"""Fixtures of the differential oracle.

Session scope: export the base tables once (`ankaad replay --out-dir`), run all ANKAA queries once in a
single `ankaad replay` process, load the export into DuckDB once. Set ANKAA_ORACLE_CAPTURE to test
another `.ankc` (add ANKAA_ORACLE_EXPECT_SNAPSHOTS=1 for captures recorded with `--snapshots`).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "compare"))

from harness import DEFAULT_CAPTURE  # noqa: E402
from harness import export, queries  # noqa: E402
from harness.engines import DuckDBEngine  # noqa: E402

QUERIES, ALPHA = queries.load_manifest()
ANKAA_SQL = queries.load_dialect("ankaa")
DUCKDB_SQL = queries.load_dialect("duckdb")


def pytest_addoption(parser):
    parser.addoption("--ankc", dest="ankc", default=os.environ.get("ANKAA_ORACLE_CAPTURE", str(DEFAULT_CAPTURE)),
                     help=".ankc capture to replay (default: the bundled 20 s Binance capture)")
    parser.addoption("--expect-snapshots", dest="expect_snapshots", action="store_true",
                     default=os.environ.get("ANKAA_ORACLE_EXPECT_SNAPSHOTS", "") == "1")


@pytest.fixture(scope="session")
def capture(request) -> Path:
    p = Path(request.config.getoption("ankc")).resolve()
    if not p.exists():
        pytest.exit(f"capture not found: {p}")
    return p


@pytest.fixture(scope="session")
def data_dir(tmp_path_factory, request, capture) -> Path:
    out = tmp_path_factory.mktemp("export")
    export.export_tables(capture, out, expect_snapshots=request.config.getoption("expect_snapshots"))
    return out


@pytest.fixture(scope="session")
def manifest(data_dir) -> dict:
    return export.load_manifest(data_dir)


@pytest.fixture(scope="session")
def window(manifest) -> tuple[int, int]:
    return manifest["t0_ns"], manifest["t1_ns"]


@pytest.fixture(scope="session")
def ankaa_results(tmp_path_factory, request, capture, window):
    """{query_id: pyarrow.Table} from one `ankaad replay` run over the whole query set."""
    t0, t1 = window
    sqls = [queries.render(ANKAA_SQL[q.id], "ankaa", t0, t1) for q in QUERIES]
    res = export.replay(capture, sqls, tmp_path_factory.mktemp("ankaa"),
                        expect_snapshots=request.config.getoption("expect_snapshots"))
    return {q.id: export.read_result_csv(p) for q, p in zip(QUERIES, res.csv_paths)}


@pytest.fixture(scope="session")
def duck(data_dir, manifest):
    eng = DuckDBEngine()
    eng.load(data_dir, manifest)
    yield eng
    eng.close()


@pytest.fixture(scope="session")
def tables(data_dir):
    return export.read_parquet_tables(data_dir)
