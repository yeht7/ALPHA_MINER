"""Cross-sectional ranking and portfolio weight allocation."""

from __future__ import annotations

from typing import Literal

import polars as pl


class CrossSectionalAllocator:
    """Rank tickers cross-sectionally and assign portfolio weights."""

    def __init__(
        self,
        quantiles: int = 5,
        strategy: Literal["long_short", "long_only"] = "long_short",
    ) -> None:
        self.quantiles = quantiles
        self.strategy = strategy

    def compute_weights(self, df: pl.DataFrame) -> pl.DataFrame:
        """Append a ``target_weight`` column based on cross-sectional factor rank.

        For each datetime cross-section:
        - Rank tickers by ``factor_value``.
        - Divide into *quantiles* buckets.
        - Assign weights according to *strategy*.
        """
        q = self.quantiles

        ranked = df.with_columns(
            pl.col("factor_value")
            .rank(method="ordinal")
            .over("datetime")
            .alias("_rank"),
            pl.col("factor_value").count().over("datetime").alias("_n"),
        ).with_columns(
            # quantile bucket 1..q  (1 = lowest factor, q = highest)
            ((pl.col("_rank") - 1) * q / pl.col("_n"))
            .floor()
            .clip(0, q - 1)
            .cast(pl.Int32)
            .add(1)
            .alias("_bucket")
        )

        if self.strategy == "long_short":
            ranked = self._long_short_weights(ranked, q)
        else:
            ranked = self._long_only_weights(ranked, q)

        return ranked.drop("_rank", "_n", "_bucket")

    # ------------------------------------------------------------------
    @staticmethod
    def _long_short_weights(df: pl.DataFrame, q: int) -> pl.DataFrame:
        """Top bucket → equal positive weight, bottom → equal negative, rest → 0."""
        n_long = (
            (pl.col("_bucket") == q).cast(pl.Int32).sum().over("datetime")
        )
        n_short = (
            (pl.col("_bucket") == 1).cast(pl.Int32).sum().over("datetime")
        )
        return df.with_columns(
            pl.when(pl.col("_bucket") == q)
            .then(1.0 / n_long)
            .when(pl.col("_bucket") == 1)
            .then(-1.0 / n_short)
            .otherwise(0.0)
            .alias("target_weight")
        )

    @staticmethod
    def _long_only_weights(df: pl.DataFrame, q: int) -> pl.DataFrame:
        """Top bucket → equal positive weight summing to 1.0, rest → 0."""
        n_long = (
            (pl.col("_bucket") == q).cast(pl.Int32).sum().over("datetime")
        )
        return df.with_columns(
            pl.when(pl.col("_bucket") == q)
            .then(1.0 / n_long)
            .otherwise(0.0)
            .alias("target_weight")
        )
