"""F-06 / F-07 / F-10 oracle: every query of the set, ANKAA vs DuckDB (SQL, ASOF JOIN, quantile_cont)."""
from __future__ import annotations

import pytest

from conftest import ALPHA, DUCKDB_SQL, QUERIES
from harness import queries
from harness.compare import diff_tables


@pytest.mark.parametrize("q", QUERIES, ids=[q.id for q in QUERIES])
def test_matches_duckdb(q, duck, ankaa_results, window):
    t0, t1 = window
    ref = duck.run(queries.render(DUCKDB_SQL[q.id], "duckdb", t0, t1)).table
    got = ankaa_results[q.id]
    d = diff_tables(ref, got, q.spec_for("ankaa"), alpha=ALPHA, expected_name="duckdb", actual_name="ankaa")
    assert d.ok, f"{q.id}: {q.title}\n{d.message()}"


def test_dataset_is_not_empty(manifest):
    assert manifest["tables"]["trades"]["rows"] > 0
    assert manifest["tables"]["quotes"]["rows"] > 0


def test_exports_are_time_ordered(tables):
    import pyarrow.compute as pc

    for name in ("trades", "quotes", "book_deltas"):
        ts = tables[name]["ts"].cast("int64").to_numpy()
        assert (ts[1:] >= ts[:-1]).all(), f"{name} export not sorted by ts"
        assert pc.count(tables[name]["ts"], mode="only_null").as_py() == 0
