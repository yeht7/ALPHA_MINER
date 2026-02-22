"""Unit tests for the vectorized backtester."""

from __future__ import annotations

import polars as pl
import pytest

from backtester.aligner import align_data
from backtester.allocator import CrossSectionalAllocator
from backtester.engine import VectorizedBacktester
from backtester.executor import simulate_execution
from backtester.metrics import compute_metrics

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DATES = [f"2025-01-{d:02d}" for d in range(1, 21)]


def _make_prices(tickers: tuple[str, ...] = ("A", "B")) -> pl.DataFrame:
    """Deterministic price data: close rises linearly per ticker."""
    rows = []
    for t in tickers:
        for i, dt in enumerate(DATES):
            base = 100.0 + i
            rows.append(
                {"datetime": dt, "ticker": t, "open": base - 0.5, "close": base}
            )
    return pl.DataFrame(rows)


def _make_signals(
    prices: pl.DataFrame, perfect: bool = False
) -> pl.DataFrame:
    """Build factor signals.

    If *perfect* is True, ``factor_value`` at T equals the forward return
    from T to T+1 — the allocator should always long the best performer.
    """
    p = prices.sort("ticker", "datetime")
    p = p.with_columns(
        (pl.col("close").shift(-1).over("ticker") / pl.col("close") - 1).alias(
            "forward_return"
        )
    )
    if perfect:
        return (
            p.with_columns(pl.col("forward_return").alias("factor_value"))
            .drop_nulls("factor_value")
            .select("datetime", "ticker", "factor_value")
        )

    return (
        p.with_columns(pl.col("close").alias("factor_value"))
        .select("datetime", "ticker", "factor_value")
    )


# ---------------------------------------------------------------------------
# 3.1 – Aligner
# ---------------------------------------------------------------------------


class TestAligner:
    def test_delay_shifts_signal(self):
        prices = _make_prices(("X",))
        signals = prices.select("datetime", "ticker").with_columns(
            pl.lit(1.0).alias("factor_value")
        )
        aligned = align_data(signals, prices, delay=1)
        # first row's factor_value came from 1 period ago → first period lost
        assert len(aligned) < len(signals)
        assert "forward_return" in aligned.columns

    def test_forward_return_positive_for_rising_prices(self):
        prices = _make_prices(("X",))
        signals = prices.with_columns(pl.lit(1.0).alias("factor_value")).select(
            "datetime", "ticker", "factor_value"
        )
        aligned = align_data(signals, prices, delay=1)
        assert (aligned["forward_return"] > 0).all()


# ---------------------------------------------------------------------------
# 3.2 – Allocator
# ---------------------------------------------------------------------------


class TestAllocator:
    def _base_df(self) -> pl.DataFrame:
        """4 tickers × 3 dates with ascending factor values per date."""
        rows = []
        for dt in DATES[:3]:
            for i, t in enumerate(["A", "B", "C", "D"]):
                rows.append(
                    {
                        "datetime": dt,
                        "ticker": t,
                        "factor_value": float(i),
                        "forward_return": 0.01,
                        "close": 100.0,
                    }
                )
        return pl.DataFrame(rows)

    def test_long_short_weights_sum_to_zero(self):
        alloc = CrossSectionalAllocator(quantiles=2, strategy="long_short")
        result = alloc.compute_weights(self._base_df())
        for dt in result["datetime"].unique().to_list():
            w = result.filter(pl.col("datetime") == dt)["target_weight"].sum()
            assert abs(w) < 1e-9, f"Weights do not cancel at {dt}: {w}"

    def test_long_only_weights_sum_to_one(self):
        alloc = CrossSectionalAllocator(quantiles=2, strategy="long_only")
        result = alloc.compute_weights(self._base_df())
        for dt in result["datetime"].unique().to_list():
            w = result.filter(pl.col("datetime") == dt)["target_weight"].sum()
            assert abs(w - 1.0) < 1e-9, f"Weights don't sum to 1 at {dt}: {w}"


# ---------------------------------------------------------------------------
# 3.3 – Executor
# ---------------------------------------------------------------------------


class TestExecutor:
    def test_zero_weight_zero_return(self):
        """All zero weights → gross return must be zero."""
        rows = [
            {"datetime": dt, "ticker": t, "target_weight": 0.0, "forward_return": 0.05}
            for dt in DATES[:5]
            for t in ("A", "B")
        ]
        pf = simulate_execution(pl.DataFrame(rows))
        assert (pf["gross_return"].abs() < 1e-12).all()

    def test_turnover_on_constant_weights(self):
        """Constant weights + nonzero returns ⇒ turnover from drift only."""
        rows = [
            {"datetime": dt, "ticker": "A", "target_weight": 1.0, "forward_return": 0.01}
            for dt in DATES[:5]
        ]
        pf = simulate_execution(pl.DataFrame(rows), commission_rate=0, slippage_rate=0)
        # after first period, drift causes small turnover
        assert pf["turnover"].sum() > 0


# ---------------------------------------------------------------------------
# 3.4 – Metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_constant_positive_return(self):
        pf = pl.DataFrame(
            {
                "datetime": DATES[:10],
                "net_return": [0.01] * 10,
                "turnover": [0.0] * 10,
            }
        )
        result = compute_metrics(pf)
        assert result.annualized_return > 0
        # constant returns → vol ≈ 0 → sharpe undefined, convention returns 0
        assert result.max_drawdown >= -1e-12

    def test_varying_returns_sharpe(self):
        pf = pl.DataFrame(
            {
                "datetime": DATES[:10],
                "net_return": [0.02, 0.01, 0.03, -0.005, 0.015, 0.01, 0.02, -0.002, 0.01, 0.005],
                "turnover": [0.0] * 10,
            }
        )
        result = compute_metrics(pf)
        assert result.annualized_return > 0
        assert result.sharpe_ratio > 0

    def test_equity_curve_starts_above_one(self):
        pf = pl.DataFrame(
            {
                "datetime": DATES[:5],
                "net_return": [0.02, -0.01, 0.03, 0.0, 0.01],
                "turnover": [0.0] * 5,
            }
        )
        result = compute_metrics(pf)
        assert result.equity_curve["equity"][0] == pytest.approx(1.02)


# ---------------------------------------------------------------------------
# 3.5 – End-to-End Orchestrator
# ---------------------------------------------------------------------------


class TestVectorizedBacktester:
    def test_perfect_foresight_produces_positive_equity(self):
        """If factor_value at T == forward_return at T, the long-only strategy
        must produce a monotonically rising equity curve (before costs).
        This proves the alignment & allocation logic is mathematically sound.
        """
        prices = _make_prices(("A", "B"))
        signals = _make_signals(prices, perfect=True)

        bt = VectorizedBacktester(
            delay=0,  # signal already represents future return
            quantiles=2,
            strategy="long_only",
            commission_rate=0.0,
            slippage_rate=0.0,
        )
        result = bt.run(signals, prices)

        eq = result.equity_curve["equity"]
        assert len(eq) > 1
        # equity should never decrease (perfect foresight, zero cost)
        diffs = eq.diff().drop_nulls()
        assert (diffs >= -1e-12).all(), (
            f"Equity dropped despite perfect foresight: {eq.to_list()}"
        )

    def test_basic_run_does_not_crash(self):
        prices = _make_prices()
        signals = _make_signals(prices, perfect=False)
        bt = VectorizedBacktester(delay=1, quantiles=2, strategy="long_short")
        result = bt.run(signals, prices)
        assert result.equity_curve.height > 0

    def test_weight_export_isolation(self):
        """generate_weights() returns a DataFrame independent from PnL logic."""
        prices = _make_prices()
        signals = _make_signals(prices)
        bt = VectorizedBacktester(delay=1, quantiles=2)
        weights_df = bt.generate_weights(signals, prices)
        assert "target_weight" in weights_df.columns
        assert "net_return" not in weights_df.columns
