"""Unit tests for Part 4 – Evaluation & Tear Sheet."""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from evaluation.bucket_stats import (
    compute_bucket_returns,
    compute_cumulative_bucket_returns,
    compute_ls_spread,
)
from evaluation.data_prep import join_sector
from evaluation.ic_stats import compute_ic, compute_ic_summary
from evaluation.tearsheet import create_full_tearsheet, evaluate

# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------

DATES = [f"2025-01-{d:02d}" for d in range(2, 31)]
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "JPM", "XOM", "JNJ", "V"]
RNG = np.random.default_rng(42)


def _random_aligned(n_dates: int = 20, tickers: list[str] | None = None) -> pl.DataFrame:
    """Generate random normally-distributed signals and returns."""
    tickers = tickers or TICKERS
    dates = DATES[:n_dates]
    rows = []
    for dt in dates:
        for tk in tickers:
            rows.append({
                "datetime": dt,
                "ticker": tk,
                "factor_value": float(RNG.standard_normal()),
                "forward_return": float(RNG.standard_normal() * 0.02),
                "close": float(100 + RNG.standard_normal() * 5),
            })
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# 4.1 – Data Prep
# ---------------------------------------------------------------------------


class TestDataPrep:
    def test_join_sector_default_map(self):
        df = _random_aligned()
        result = join_sector(df)
        assert "gics_sector" in result.columns
        assert result.filter(pl.col("gics_sector") == "Unknown").height == 0

    def test_join_sector_custom_dict(self):
        df = _random_aligned(tickers=["FOO", "BAR"])
        result = join_sector(df, {"FOO": "Tech", "BAR": "Finance"})
        assert set(result["gics_sector"].unique().to_list()) == {"Tech", "Finance"}

    def test_idempotent(self):
        df = join_sector(_random_aligned())
        result = join_sector(df)
        assert result.columns.count("gics_sector") == 1


# ---------------------------------------------------------------------------
# 4.2 – IC Stats
# ---------------------------------------------------------------------------


class TestICStats:
    def test_ic_shape(self):
        ic_df = compute_ic(_random_aligned(n_dates=15))
        assert set(ic_df.columns) >= {"datetime", "ic", "rank_ic", "n_stocks"}
        assert ic_df.height == 15

    def test_noise_ic_near_zero(self):
        """Random signals should have IC IR close to 0."""
        big = _random_aligned(n_dates=29)
        summary = compute_ic_summary(compute_ic(big))
        assert abs(summary["ic_ir"]) < 2.0, f"IC IR too large for noise: {summary['ic_ir']}"
        assert abs(summary["rank_ic_ir"]) < 2.0

    def test_perfect_signal_high_ic(self):
        """If factor_value == forward_return, IC should be ~1."""
        df = _random_aligned(n_dates=15)
        df = df.with_columns(pl.col("forward_return").alias("factor_value"))
        ic_df = compute_ic(df)
        assert ic_df["ic"].mean() > 0.9


# ---------------------------------------------------------------------------
# 4.3 – Bucket Stats
# ---------------------------------------------------------------------------


class TestBucketStats:
    def test_bucket_count(self):
        br = compute_bucket_returns(_random_aligned(), quantiles=5)
        assert set(br["bucket"].unique().to_list()) == {1, 2, 3, 4, 5}

    def test_neutral_requires_sector(self):
        with pytest.raises(ValueError, match="gics_sector"):
            compute_bucket_returns(_random_aligned(), by_sector=True)

    def test_neutral_runs_with_sector(self):
        df = join_sector(_random_aligned())
        br = compute_bucket_returns(df, quantiles=3, by_sector=True)
        assert br.height > 0

    def test_ls_spread(self):
        br = compute_bucket_returns(_random_aligned(), quantiles=5)
        ls = compute_ls_spread(br, quantiles=5)
        assert "ls_return" in ls.columns
        assert ls.height > 0

    def test_cumulative_returns(self):
        br = compute_bucket_returns(_random_aligned(), quantiles=3)
        cum = compute_cumulative_bucket_returns(br)
        assert "cumulative_return" in cum.columns


# ---------------------------------------------------------------------------
# 4.4 & 4.5 – Plotting & Tear Sheet
# ---------------------------------------------------------------------------


class TestTearSheet:
    def test_evaluate_returns_expected_keys(self):
        result = evaluate(_random_aligned())
        expected_keys = {
            "ic_df", "ic_summary", "bucket_returns", "bucket_returns_neutral",
            "cum_bucket", "ls_spread", "bucketed_with_sector", "quantiles",
        }
        assert expected_keys <= set(result.keys())

    def test_tearsheet_saves_png(self):
        result = evaluate(_random_aligned())
        with tempfile.TemporaryDirectory() as tmp:
            out = create_full_tearsheet(result, f"{tmp}/tearsheet.png")
            assert Path(out).exists()
            assert Path(out).stat().st_size > 1000

    def test_tearsheet_saves_pdf(self):
        result = evaluate(_random_aligned())
        with tempfile.TemporaryDirectory() as tmp:
            out = create_full_tearsheet(result, f"{tmp}/tearsheet.pdf")
            assert Path(out).exists()
            assert Path(out).stat().st_size > 1000
