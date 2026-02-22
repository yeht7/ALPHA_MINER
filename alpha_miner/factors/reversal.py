"""Short-term reversal factor: negative past-N-day return.

Intuition: stocks that fell over the look-back window tend to mean-revert
upward in the near future, and vice-versa.  Negating the return makes
*high factor_value → expected future outperformance*, which aligns with
the framework's long-top-quantile convention.
"""

from __future__ import annotations

import polars as pl

from alpha_miner.base import OUTPUT_SCHEMA, BaseFactor


class ShortTermReversal(BaseFactor):
    """factor_value = -return_{t-window → t} per ticker."""

    def __init__(self, window: int = 5) -> None:
        self.window = window

    def compute(self, data: pl.DataFrame) -> pl.DataFrame:
        w = self.window
        return (
            data.sort("ticker", "datetime")
            .with_columns(
                (
                    -(pl.col("close") / pl.col("close").shift(w).over("ticker") - 1)
                ).alias("factor_value")
            )
            .drop_nulls("factor_value")
            .select(OUTPUT_SCHEMA)
        )
