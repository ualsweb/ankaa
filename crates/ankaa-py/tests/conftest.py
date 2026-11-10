from __future__ import annotations

import os
from pathlib import Path

import pytest

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "ankaa-feeds"
    / "tests"
    / "fixtures"
    / "binance-btcusdt-trade-depth-60.ankc"
)


@pytest.fixture(scope="session")
def fixture_path() -> str:
    if not FIXTURE.exists():
        pytest.skip(f"fixture missing: {FIXTURE}")
    return str(FIXTURE)


@pytest.fixture(scope="session")
def dataset(fixture_path: str):
    import ankaa

    return ankaa.open_capture(fixture_path)


@pytest.fixture(scope="session")
def live_addr() -> str | None:
    """`ANKAA_LIVE_TEST=1` enables tests against a running `ankaad serve` (`ANKAA_ADDR`, `ANKAA_TOKEN`)."""
    if os.environ.get("ANKAA_LIVE_TEST") != "1":
        return None
    return os.environ.get("ANKAA_ADDR", "127.0.0.1:50051")
