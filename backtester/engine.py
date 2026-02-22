"""Orchestrator: glue align → allocate → execute → metrics."""

from __future__ import annotations

from typing import Literal

import polars as pl

from backtester.aligner import align_data
from backtester.allocator import CrossSectionalAllocator
from backtester.executor import simulate_execution
from backtester.metrics import BacktestResult, compute_metrics


class VectorizedBacktester:
    """End-to-end vectorized back-test pipeline.

    Weight generation is isolated from PnL calculation so target weights
    can be exported to a live execution platform (e.g., Alpha Arena).
    """

    def __init__(
        self,
        delay: int = 1,
        quantiles: int = 5,
        strategy: Literal["long_short", "long_only"] = "long_short",
        commission_rate: float = 0.001,
        slippage_rate: float = 0.0005,
    ) -> None:
        self.delay = delay
        self.allocator = CrossSectionalAllocator(
            quantiles=quantiles, strategy=strategy
        )
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

    def generate_weights(
        self,
        signals: pl.DataFrame,
        prices: pl.DataFrame,
    ) -> pl.DataFrame:
        """Align data and compute target weights (portable to live trading)."""
        aligned = align_data(signals, prices, delay=self.delay)
        return self.allocator.compute_weights(aligned)

    def run(
        self,
        signals: pl.DataFrame,
        prices: pl.DataFrame,
    ) -> BacktestResult:
        """Full back-test: alignment → weights → execution → metrics."""
        weighted = self.generate_weights(signals, prices)
        portfolio = simulate_execution(
            weighted,
            commission_rate=self.commission_rate,
            slippage_rate=self.slippage_rate,
        )
        return compute_metrics(portfolio)
