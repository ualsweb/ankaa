"""Normalise result tables from different engines and diff them.

Rules (bench/compare/queries/manifest.json):
  keys      identify a row; both sides are sorted by them (or by every column when there are no keys)
  exact     integers / timestamps / strings must be equal (NULL == NULL); numbers compare by value,
            so an Int64 1 equals a Float64 1.0
  approx    |a - e| <= rtol * max(|e|, 1e-300)                (floating-point aggregates)
  quantile  sketch results (alpha = 0.01). ANKAA's `percentiles()` returns a value within alpha of the
            order statistic sorted[floor(q * (n - 1))] (0-based). The reference emits that statistic as
            the column itself and, optionally, the next one as `<col>_hi`; the check passes when
            |a - e| <= alpha * |e|, or when e * (1 - alpha) <= a <= e_hi * (1 + alpha) (so engines whose
            percentile convention picks the upper neighbour or interpolates between the two also pass).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import pyarrow as pa

from .queries import CompareSpec

MAX_SHOWN = 10


def to_frame(table: pa.Table | pd.DataFrame) -> pd.DataFrame:
    """Lower-case column names; timestamps -> int64 ns (UTC); bool -> int; decimals -> float."""
    if isinstance(table, pd.DataFrame):
        df = table.copy()
    else:
        cols = {}
        for name in table.column_names:
            col = table[name]
            t = col.type
            if pa.types.is_timestamp(t):
                if t.tz is not None:
                    col = col.cast(pa.timestamp(t.unit, tz="UTC"))
                col = col.cast(pa.timestamp("ns", tz=col.type.tz)).cast(pa.int64())
            elif pa.types.is_date(t):
                col = col.cast(pa.timestamp("ns")).cast(pa.int64())
            elif pa.types.is_boolean(t):
                col = col.cast(pa.int64())
            elif pa.types.is_decimal(t):
                col = col.cast(pa.float64())
            elif pa.types.is_dictionary(t):
                col = col.cast(t.value_type)
            cols[name] = col.to_pandas()
        df = pd.DataFrame(cols)
    df.columns = [str(c).lower() for c in df.columns]
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            if getattr(s.dt, "tz", None) is not None:
                s = s.dt.tz_convert("UTC").dt.tz_localize(None)
            df[c] = s.astype("datetime64[ns]").astype("int64")
        elif pd.api.types.is_bool_dtype(s):
            df[c] = s.astype("int64")
        elif s.dtype == object:
            # python ints / Decimals / datetimes / None from DB-API cursors
            if all(_is_null(v) or hasattr(v, "timestamp") for v in s):
                df[c] = s.map(lambda v: None if _is_null(v) else _py_dt_to_ns(v))
            else:
                try:
                    df[c] = pd.to_numeric(s, errors="raise")
                except (ValueError, TypeError):
                    pass
    return df


def _py_dt_to_ns(v) -> int:
    from datetime import timezone

    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return int(v.timestamp()) * 1_000_000_000 + v.microsecond * 1000


@dataclass
class DiffResult:
    ok: bool
    problems: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.ok

    def message(self) -> str:
        return "\n".join(self.problems) if self.problems else "equal"


def _is_null(v) -> bool:
    if v is None or v is pd.NA or v is pd.NaT:
        return True
    try:
        return isinstance(v, float) and math.isnan(v)
    except TypeError:
        return False


def _num(v):
    if _is_null(v):
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def _exact_eq(a, e) -> bool:
    if _is_null(a) and _is_null(e):
        return True
    if _is_null(a) or _is_null(e):
        return False
    a2, e2 = _num(a), _num(e)
    if isinstance(a2, (int, float)) and isinstance(e2, (int, float)):
        return a2 == e2
    return str(a) == str(e)


def _approx_eq(a, e, rtol: float) -> bool:
    if _is_null(a) and _is_null(e):
        return True
    if _is_null(a) or _is_null(e):
        return False
    a2, e2 = float(a), float(e)
    return abs(a2 - e2) <= rtol * max(abs(e2), 1e-300)


def _quantile_ok(a, lo, hi, alpha: float) -> bool:
    if _is_null(a) and _is_null(lo):
        return True
    if _is_null(a) or _is_null(lo):
        return False
    if _approx_eq(a, lo, alpha):
        return True
    if _is_null(hi):
        return False
    a2, lo2, hi2 = float(a), float(lo), float(hi)
    return min(lo2, hi2) * (1 - alpha) <= a2 <= max(lo2, hi2) * (1 + alpha)


def _fast_pass(a: pd.Series, e: pd.Series, kind: str, rtol: float) -> np.ndarray | None:
    """Boolean mask of rows that certainly satisfy the rule (False = check the row one by one).

    Only accepts what the per-value rules accept: both NULL; both integer columns and equal; both
    numeric and (exact) equal / (approx) within rtol, compared as float64; object columns equal under
    Python `==` (equal strings, or numbers equal by value). None = no fast path for these dtypes."""
    an, en = a.isna().to_numpy(), e.isna().to_numpy()
    both_null = an & en
    num = pd.api.types.is_numeric_dtype
    if num(a.dtype) and num(e.dtype) and not pd.api.types.is_bool_dtype(a.dtype) and not pd.api.types.is_bool_dtype(e.dtype):
        ints = pd.api.types.is_integer_dtype(a.dtype), pd.api.types.is_integer_dtype(e.dtype)
        if kind == "exact" and all(ints):
            eq = a.to_numpy(dtype=np.int64, na_value=0) == e.to_numpy(dtype=np.int64, na_value=0)
            return both_null | (eq & ~an & ~en)
        av = a.to_numpy(dtype=np.float64, na_value=np.nan)
        ev = e.to_numpy(dtype=np.float64, na_value=np.nan)
        if kind == "exact" and any(ints) and np.nanmax(np.abs(np.concatenate([av, ev])), initial=0.0) >= 2.0**53:
            return None  # an integer beyond 2^53 does not survive float64: leave it to the exact rule
        with np.errstate(invalid="ignore"):
            eq = (av == ev) if kind == "exact" else (np.abs(av - ev) <= rtol * np.maximum(np.abs(ev), 1e-300))
        return both_null | (eq & ~an & ~en)
    if kind == "exact" and (a.dtype == object or pd.api.types.is_string_dtype(a.dtype)) \
            and (e.dtype == object or pd.api.types.is_string_dtype(e.dtype)):
        av = a.astype(object).to_numpy(dtype=object, copy=True)
        ev = e.astype(object).to_numpy(dtype=object, copy=True)
        av[an] = None
        ev[en] = None
        eq = np.fromiter((x == y for x, y in zip(av, ev)), dtype=bool, count=len(av)) if len(av) else np.zeros(0, bool)
        return both_null | (eq & ~an & ~en)
    return None


def _fmt_row(df: pd.DataFrame, i: int, cols: list[str]) -> str:
    return "{" + ", ".join(f"{c}={df.iloc[i][c]!r}" for c in cols if c in df.columns) + "}"


def diff_tables(
    expected: pa.Table | pd.DataFrame,
    actual: pa.Table | pd.DataFrame,
    spec: CompareSpec,
    alpha: float = 0.01,
    expected_name: str = "reference",
    actual_name: str = "ankaa",
) -> DiffResult:
    e = to_frame(expected)
    a = to_frame(actual)
    problems: list[str] = []
    want = [c.lower() for c in spec.columns]
    if len(e) == 0 and len(a) == 0:
        return DiffResult(True, [])  # ankaad writes no header for an empty result; empty == empty
    for side, df in ((expected_name, e), (actual_name, a)):
        missing = [c for c in want if c not in df.columns]
        if missing:
            problems.append(f"{side}: missing columns {missing} (has {list(df.columns)})")
    if problems:
        return DiffResult(False, problems)
    keys = [k.lower() for k in spec.keys] or want
    e = e.sort_values(keys, kind="mergesort").reset_index(drop=True)
    a = a.sort_values(keys, kind="mergesort").reset_index(drop=True)
    if len(e) != len(a):
        problems.append(f"row count differs: {expected_name}={len(e)} {actual_name}={len(a)}")
        ek = set(map(tuple, e[keys].astype(str).itertuples(index=False)))
        ak = set(map(tuple, a[keys].astype(str).itertuples(index=False)))
        only_e = sorted(ek - ak)[:MAX_SHOWN]
        only_a = sorted(ak - ek)[:MAX_SHOWN]
        if only_e:
            problems.append(f"  keys only in {expected_name}: {only_e}")
        if only_a:
            problems.append(f"  keys only in {actual_name}: {only_a}")
        return DiffResult(False, problems)
    n = len(e)
    width = max(len(expected_name), len(actual_name))
    quant = [k.lower() for k in spec.quantile]
    approx = {k.lower(): v for k, v in spec.approx.items()}
    for c in want:
        if c in quant:
            hi = f"{c}_hi" if f"{c}_hi" in e.columns else None
            check = lambda x, y: _approx_eq(x, y, alpha)  # noqa: E731
            kind = f"quantile (alpha={alpha}{', band to ' + hi if hi else ''})"
        elif c in approx:
            check = lambda x, y, r=approx[c]: _approx_eq(x, y, r)  # noqa: E731
            kind = f"approx (rtol={approx[c]})"
        else:
            check = _exact_eq
            kind = "exact"
        if c in quant and f"{c}_hi" in e.columns:
            bad = [i for i in range(n) if not _quantile_ok(a.iloc[i][c], e.iloc[i][c], e.iloc[i][f"{c}_hi"], alpha)]
        else:
            # Vectorised pre-check: rows it accepts are certainly equal under the rule; every other row
            # goes through the per-value rule below, so the verdict is the per-value rule's alone.
            if c in quant:
                passed = _fast_pass(a[c], e[c], "approx", alpha)
            elif c in approx:
                passed = _fast_pass(a[c], e[c], "approx", approx[c])
            else:
                passed = _fast_pass(a[c], e[c], "exact", 0.0)
            candidates = range(n) if passed is None else np.flatnonzero(~passed)
            bad = [int(i) for i in candidates if not check(a[c].iat[i], e[c].iat[i])]
        if bad:
            problems.append(f"column {c!r} ({kind}): {len(bad)}/{n} rows differ; first {min(len(bad), MAX_SHOWN)}:")
            for i in bad[:MAX_SHOWN]:
                problems.append(f"  row {i}: {expected_name:<{width}} {_fmt_row(e, i, keys + [c])}")
                problems.append(f"  row {i}: {actual_name:<{width}} {_fmt_row(a, i, keys + [c])}")
    return DiffResult(not problems, problems)
