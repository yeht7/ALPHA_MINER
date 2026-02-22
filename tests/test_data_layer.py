"""Tests for the data layer – cache logic (offline) + optional live smoke test."""

from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from data_layer import cache_manager
from data_layer.cache_manager import merge_and_save, read_cache, write_cache

TEST_CACHE = Path("./test_cache_tmp")


@pytest.fixture(autouse=True)
def _use_tmp_cache(monkeypatch):
    """Redirect cache to a temp dir and clean up after each test."""
    monkeypatch.setattr(cache_manager, "CACHE_DIR", TEST_CACHE)
    TEST_CACHE.mkdir(exist_ok=True)
    yield
    shutil.rmtree(TEST_CACHE, ignore_errors=True)


def _make_df(dates: list[str], close_start: float = 100.0) -> pl.DataFrame:
    n = len(dates)
    return pl.DataFrame({
        "datetime": dates,
        "open": [close_start + i for i in range(n)],
        "high": [close_start + i + 1 for i in range(n)],
        "low": [close_start + i - 1 for i in range(n)],
        "close": [close_start + i + 0.5 for i in range(n)],
        "volume": [1000 * (i + 1) for i in range(n)],
    })


class TestCacheManager:
    def test_read_empty(self):
        assert read_cache("AAPL", "1 day") is None

    def test_write_and_read(self):
        df = _make_df(["2025-01-02", "2025-01-03"])
        write_cache("AAPL", "1 day", df)
        loaded = read_cache("AAPL", "1 day")
        assert loaded is not None
        assert loaded.shape == df.shape
        assert loaded["datetime"].to_list() == df["datetime"].to_list()

    def test_merge_deduplicates(self):
        old = _make_df(["2025-01-02", "2025-01-03"])
        write_cache("MSFT", "1 day", old)
        new = _make_df(["2025-01-03", "2025-01-06"], close_start=200.0)
        merged = merge_and_save("MSFT", "1 day", new)
        assert merged["datetime"].to_list() == ["2025-01-02", "2025-01-03", "2025-01-06"]
        assert len(merged) == 3

    def test_merge_on_empty_cache(self):
        df = _make_df(["2025-06-01", "2025-06-02"])
        merged = merge_and_save("TSLA", "1 day", df)
        assert len(merged) == 2
        assert read_cache("TSLA", "1 day") is not None


class TestNeedsUpdate:
    def test_none_cache(self):
        from data_layer.api import _needs_update
        assert _needs_update(None, date.today()) is True

    def test_fresh_cache(self):
        from data_layer.api import _needs_update
        today = date.today()
        df = _make_df([str(today - timedelta(days=1))])
        assert _needs_update(df, today) is False

    def test_stale_cache(self):
        from data_layer.api import _needs_update
        df = _make_df(["2020-01-01"])
        assert _needs_update(df, date.today()) is True
