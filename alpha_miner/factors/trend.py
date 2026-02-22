"""Momentum / trend factor: Intraday VWAP Deviation."""

from __future__ import annotations

import polars as pl

from alpha_miner.base import OUTPUT_SCHEMA, BaseFactor


class IntradayVWAPDeviation(BaseFactor):
    """Percentage deviation of close from rolling VWAP.

    rolling_vwap = rolling_sum(close * volume, window) / rolling_sum(volume, window)
    factor_value = (close - rolling_vwap) / rolling_vwap
    """

    def __init__(self, window: int = 20) -> None:
        self.window = window

    def compute(self, data: pl.DataFrame) -> pl.DataFrame:
        w = self.window
        return (
            data
            .with_columns((pl.col("close") * pl.col("volume")).alias("_cv"))
            .with_columns([
                pl.col("_cv")
                  .rolling_sum(window_size=w, min_samples=w)
                  .over("ticker")
                  .alias("_rolling_cv"),
                pl.col("volume")
                  .rolling_sum(window_size=w, min_samples=w)
                  .over("ticker")
                  .alias("_rolling_vol"),
            ])
            .with_columns(
                (pl.col("_rolling_cv") / pl.col("_rolling_vol")).alias("_vwap")
            )
            .with_columns(
                ((pl.col("close") - pl.col("_vwap")) / pl.col("_vwap")).alias("factor_value")
            )
            .select(OUTPUT_SCHEMA)
        )
