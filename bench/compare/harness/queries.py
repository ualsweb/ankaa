"""The tick query set: one manifest, one SQL file per dialect, `{T0}`/`{T1}` placeholders."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import QUERIES_DIR

DIALECTS = ("ankaa", "duckdb", "clickhouse", "questdb")
_BLOCK_RE = re.compile(r"^--\s*@query\s+(\S+)\s*$", re.MULTILINE)


@dataclass
class CompareSpec:
    keys: list[str] = field(default_factory=list)
    exact: list[str] = field(default_factory=list)
    approx: dict[str, float] = field(default_factory=dict)
    quantile: list[str] = field(default_factory=list)

    def with_overrides(self, ov: dict | None) -> "CompareSpec":
        """Apply an engine override: columns listed under `approx` move out of `exact`."""
        if not ov:
            return self
        approx = dict(self.approx)
        approx.update(ov.get("approx", {}))
        exact = [c for c in self.exact if c not in approx]
        return CompareSpec(list(self.keys), exact, approx, list(self.quantile))

    @property
    def columns(self) -> list[str]:
        return self.keys + self.exact + list(self.approx) + self.quantile


@dataclass
class Query:
    id: str
    title: str
    description: str
    compare: CompareSpec
    timing: bool
    engine_overrides: dict[str, dict]

    def spec_for(self, engine: str) -> CompareSpec:
        return self.compare.with_overrides(self.engine_overrides.get(engine))


def load_manifest(path: Path | None = None) -> tuple[list[Query], float]:
    """Return (queries, percentile_alpha)."""
    data = json.loads((path or QUERIES_DIR / "manifest.json").read_text())
    out = []
    for q in data["queries"]:
        c = q.get("compare", {})
        out.append(
            Query(
                id=q["id"],
                title=q.get("title", q["id"]),
                description=q.get("description", ""),
                compare=CompareSpec(
                    keys=list(c.get("keys", [])),
                    exact=list(c.get("exact", [])),
                    approx=dict(c.get("approx", {})),
                    quantile=list(c.get("quantile", [])),
                ),
                timing=bool(q.get("timing", True)),
                engine_overrides=dict(q.get("engine_overrides", {})),
            )
        )
    return out, float(data.get("percentile_alpha", 0.01))


def load_dialect(dialect: str, path: Path | None = None) -> dict[str, str]:
    """Parse `<dialect>.sql` into {query_id: sql}. Comment-only lines before the first block are ignored."""
    text = (path or QUERIES_DIR / f"{dialect}.sql").read_text()
    parts = _BLOCK_RE.split(text)
    out: dict[str, str] = {}
    # parts = [preamble, id1, sql1, id2, sql2, ...]
    for i in range(1, len(parts), 2):
        qid, body = parts[i], parts[i + 1]
        lines = [ln for ln in body.strip().splitlines() if not ln.strip().startswith("--")]
        sql = "\n".join(lines).strip().rstrip(";")
        if sql:
            out[qid] = sql
    return out


def _dt(ns: int) -> datetime:
    return datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc)


def _iso(ns: int, digits: int, sep: str = "T", suffix: str = "Z") -> str:
    frac = ns % 1_000_000_000
    base = _dt(ns).strftime(f"%Y-%m-%d{sep}%H:%M:%S")
    return f"{base}.{frac:09d}"[: len(base) + 1 + digits] + suffix


def render_time(dialect: str, ns: int) -> str:
    """Render an absolute UTC instant (integer nanoseconds) as a literal of the dialect."""
    if dialect == "ankaa":
        return f"'{_iso(ns, 9)}'"
    if dialect == "duckdb":
        return f"TIMESTAMP_NS '{_iso(ns, 9, sep=' ', suffix='')}'"
    if dialect == "clickhouse":
        return f"toDateTime64('{_iso(ns, 9, sep=' ', suffix='')}', 9, 'UTC')"
    if dialect == "questdb":
        return f"'{_iso(ns, 6)}'"
    if dialect == "timescaledb":
        # PostgreSQL timestamptz has microsecond resolution
        return f"TIMESTAMPTZ '{_iso(ns, 6, sep=' ', suffix='+00')}'"
    if dialect == "influxdb3":
        return f"'{_iso(ns, 9)}'"
    raise ValueError(f"unknown dialect {dialect!r}")


def render(sql: str, dialect: str, t0_ns: int, t1_ns: int) -> str:
    return sql.replace("{T0}", render_time(dialect, t0_ns)).replace("{T1}", render_time(dialect, t1_ns))


def parse_utc(text: str) -> int:
    """`2016-01-02T06:57:30Z` (RFC 3339, UTC, whole seconds) -> integer nanoseconds."""
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1_000_000_000


def render_params(sql: str, dialect: str, params: dict) -> str:
    """Fill the placeholders of the telemetry set (bench/telemetry/queries): {START}/{END} (RFC 3339 UTC
    in the manifest, rendered as a literal of the dialect) and {HOSTS} (string literals, comma separated).
    A placeholder whose parameter is missing is an error, so a query never runs half-rendered."""
    out = sql
    for key, ph in (("start", "{START}"), ("end", "{END}")):
        if ph in out:
            if not params.get(key):
                raise ValueError(f"{ph} used but the manifest gives no {key!r}")
            out = out.replace(ph, render_time(dialect, parse_utc(params[key])))
    if "{HOSTS}" in out:
        hosts = params.get("hosts") or []
        if not hosts:
            raise ValueError("{HOSTS} used but the manifest gives no hosts")
        if any("'" in h for h in hosts):
            raise ValueError(f"host names with quotes: {hosts}")
        out = out.replace("{HOSTS}", ", ".join(f"'{h}'" for h in hosts))
    return out


def time_window(t_min_ns: int, t_max_ns: int) -> tuple[int, int]:
    """The canonical [T0, T1) window of a dataset: the middle half of the trade time span,
    bounds floored to whole seconds (so the same literal is exact in every engine)."""
    span = max(t_max_ns - t_min_ns, 1)
    s = 1_000_000_000
    t0 = (t_min_ns + span // 4) // s * s
    t1 = (t_min_ns + 3 * span // 4) // s * s
    if t1 <= t0:
        t1 = t0 + s
    return t0, t1
