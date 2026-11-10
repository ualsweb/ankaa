"""Polars reference implementations (F-08: time-bucket aggregates differential vs Polars).

Each function takes the exported base tables as Polars DataFrames and returns a DataFrame whose columns
match the ANKAA result of the same query id."""
from __future__ import annotations

import polars as pl

SCALE = 100_000_000


def _ohlcv(trades: pl.DataFrame, every: str) -> pl.DataFrame:
    t = trades.sort(["ts", "seq"])
    gb = t.group_by_dynamic("ts", every=every, closed="left", label="left", group_by="symbol")
    out = gb.agg(
        pl.col("price").first().alias("open"),
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("size").sum().alias("volume"),
        (pl.col("price").cast(pl.Int128) * pl.col("size").cast(pl.Int128)).sum().alias("notional_raw"),
        pl.len().alias("count"),
    )
    # Σ price·size / 1e8 (size scale), truncated like ANKAA's i128 division
    notional = [int(v) // SCALE for v in out["notional_raw"].to_list()]
    return (
        out.with_columns(pl.Series("notional", notional, dtype=pl.Int64))
        .drop("notional_raw")
        .rename({"ts": "bucket"})
        .select(["symbol", "bucket", "open", "high", "low", "close", "volume", "notional", "count"])
        .sort(["symbol", "bucket"])
    )


def q05_ohlcv_1s(tables: dict[str, pl.DataFrame], t0: int, t1: int) -> pl.DataFrame:
    return _ohlcv(tables["trades"], "1s")


def q06_ohlcv_5s(tables: dict[str, pl.DataFrame], t0: int, t1: int) -> pl.DataFrame:
    return _ohlcv(tables["trades"], "5s")


def _window(df: pl.DataFrame, t0: int, t1: int) -> pl.DataFrame:
    return df.filter((pl.col("ts").cast(pl.Int64) >= t0) & (pl.col("ts").cast(pl.Int64) < t1))


def q01_range_count(tables, t0, t1) -> pl.DataFrame:
    return _window(tables["trades"], t0, t1).select(
        pl.len().alias("n"), pl.col("size").sum().alias("volume"), pl.col("price").min().alias("lo"), pl.col("price").max().alias("hi")
    )


def q02_agg_by_symbol(tables, t0, t1) -> pl.DataFrame:
    return (
        tables["trades"]
        .group_by("symbol")
        .agg(
            pl.len().alias("n"), pl.col("price").min().alias("lo"), pl.col("price").max().alias("hi"),
            pl.col("size").sum().alias("volume"), pl.col("ts").min().alias("first_ts"), pl.col("ts").max().alias("last_ts"),
        )
        .sort("symbol")
    )


def q04_vwap_by_symbol(tables, t0, t1) -> pl.DataFrame:
    w = _window(tables["trades"], t0, t1)
    return (
        w.group_by("symbol")
        .agg(
            pl.len().alias("n"),
            (((pl.col("price") / SCALE) * (pl.col("size") / SCALE)).sum() / (pl.col("size") / SCALE).sum()).alias("vwap"),
        )
        .sort("symbol")
    )


def q10_crossed_quotes(tables, t0, t1) -> pl.DataFrame:
    return tables["quotes"].select(pl.len().alias("n_quotes"), (pl.col("bid") >= pl.col("ask")).cast(pl.Int64).sum().alias("crossed"))


def q11_spread_stats(tables, t0, t1) -> pl.DataFrame:
    sp = pl.col("ask") - pl.col("bid")
    return (
        tables["quotes"]
        .group_by("symbol")
        .agg(
            pl.len().alias("n"), sp.min().alias("min_spread"), sp.max().alias("max_spread"), sp.sum().alias("sum_spread"),
            sp.mean().alias("avg_spread"), (pl.col("bid_size") + pl.col("ask_size")).mean().alias("avg_depth"),
        )
        .sort("symbol")
    )


def q14_trades_per_second(tables, t0, t1) -> pl.DataFrame:
    t = tables["trades"].sort("ts")
    return (
        t.group_by_dynamic("ts", every="1s", closed="left", label="left")
        .agg(pl.len().alias("n"), pl.col("size").sum().alias("volume"))
        .rename({"ts": "bucket"})
        .sort("bucket")
    )


def q15_book_delta_stats(tables, t0, t1) -> pl.DataFrame:
    return (
        tables["book_deltas"]
        .group_by(["symbol", "side"])
        .agg(pl.len().alias("n"), (pl.col("size") == 0).cast(pl.Int64).sum().alias("deletes"), pl.col("update_id").max().alias("last_update_id"))
        .sort(["symbol", "side"])
    )


REFERENCES = {
    "q01_range_count": q01_range_count,
    "q02_agg_by_symbol": q02_agg_by_symbol,
    "q04_vwap_by_symbol": q04_vwap_by_symbol,
    "q05_ohlcv_1s": q05_ohlcv_1s,
    "q06_ohlcv_5s": q06_ohlcv_5s,
    "q10_crossed_quotes": q10_crossed_quotes,
    "q11_spread_stats": q11_spread_stats,
    "q14_trades_per_second": q14_trades_per_second,
    "q15_book_delta_stats": q15_book_delta_stats,
}
