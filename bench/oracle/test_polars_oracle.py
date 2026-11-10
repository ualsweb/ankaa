"""F-08 oracle: time-bucket aggregates (OHLCV, VWAP, counts) and simple aggregates, ANKAA vs Polars."""
from __future__ import annotations

import polars as pl
import pytest

from conftest import ALPHA, QUERIES
from harness.compare import diff_tables
from polars_ref import REFERENCES

BY_ID = {q.id: q for q in QUERIES}


@pytest.fixture(scope="session")
def pl_tables(tables) -> dict[str, pl.DataFrame]:
    return {name: pl.from_arrow(t) for name, t in tables.items()}


@pytest.mark.parametrize("qid", list(REFERENCES), ids=list(REFERENCES))
def test_matches_polars(qid, pl_tables, ankaa_results, window):
    q = BY_ID[qid]
    t0, t1 = window
    ref = REFERENCES[qid](pl_tables, t0, t1).to_arrow()
    got = ankaa_results[qid]
    d = diff_tables(ref, got, q.spec_for("ankaa"), alpha=ALPHA, expected_name="polars", actual_name="ankaa")
    assert d.ok, f"{qid}: {q.title}\n{d.message()}"
