"""Market microstructure proxy: Volume-Price Trend (signed volume EMA)."""

from __future__ import annotations

import polars as pl

from alpha_miner.base import OUTPUT_SCHEMA, BaseFactor


class VolumePriceTrend(BaseFactor):
    """Signed-volume exponential moving average as an order-flow proxy.

    signed_volume = volume  when close > open
                  = -volume when close < open
                  = 0       otherwise

    factor_value  = EWM(signed_volume, span=span) per ticker
    """

    def __init__(self, span: int = 20) -> None:
        self.span = span

    def compute(self, data: pl.DataFrame) -> pl.DataFrame:
        alpha = 2.0 / (self.span + 1)
        return (
            data
            .with_columns(
                pl.when(pl.col("close") > pl.col("open"))
                  .then(pl.col("volume"))
                  .when(pl.col("close") < pl.col("open"))
                  .then(-pl.col("volume"))
                  .otherwise(0)
                  .alias("_signed_vol")
            )
            .with_columns(
                pl.col("_signed_vol")
                  .ewm_mean(alpha=alpha, adjust=False, min_samples=1)
                  .over("ticker")
                  .alias("factor_value")
            )
            .select(OUTPUT_SCHEMA)
        )
