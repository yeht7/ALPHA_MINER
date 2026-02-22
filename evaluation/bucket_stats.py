"""Bucket (quantile) returns and long/short spread computation."""

from __future__ import annotations

import polars as pl


def _assign_buckets(df: pl.DataFrame, quantiles: int, group_cols: list[str]) -> pl.DataFrame:
    """Rank ``factor_value`` within *group_cols* and assign quantile bucket 1..q."""
    return (
        df.with_columns(
            pl.col("factor_value").rank(method="ordinal").over(group_cols).alias("_rank"),
            pl.col("factor_value").count().over(group_cols).alias("_n"),
        )
        .with_columns(
            ((pl.col("_rank") - 1) * quantiles / pl.col("_n"))
            .floor()
            .clip(0, quantiles - 1)
            .cast(pl.Int32)
            .add(1)
            .alias("bucket")
        )
        .drop("_rank", "_n")
    )


def compute_bucket_returns(
    df: pl.DataFrame,
    quantiles: int = 5,
    by_sector: bool = False,
) -> pl.DataFrame:
    """Compute mean forward return per quantile bucket per day.

    Parameters
    ----------
    df : DataFrame with ``datetime``, ``ticker``, ``factor_value``,
        ``forward_return``, and optionally ``gics_sector``.
    quantiles : Number of buckets.
    by_sector : If True, rank within each sector (industry neutralisation)
        then equal-weight aggregate across sectors.

    Returns
    -------
    DataFrame with ``datetime``, ``bucket``, ``mean_return``.
    """
    if by_sector:
        if "gics_sector" not in df.columns:
            raise ValueError("by_sector=True requires 'gics_sector' column")
        bucketed = _assign_buckets(df, quantiles, ["datetime", "gics_sector"])
        sector_bucket = bucketed.group_by("datetime", "gics_sector", "bucket").agg(
            pl.col("forward_return").mean().alias("sector_mean_return")
        )
        return (
            sector_bucket.group_by("datetime", "bucket")
            .agg(pl.col("sector_mean_return").mean().alias("mean_return"))
            .sort("datetime", "bucket")
        )

    bucketed = _assign_buckets(df, quantiles, ["datetime"])
    return (
        bucketed.group_by("datetime", "bucket")
        .agg(pl.col("forward_return").mean().alias("mean_return"))
        .sort("datetime", "bucket")
    )


def compute_ls_spread(bucket_returns: pl.DataFrame, quantiles: int = 5) -> pl.DataFrame:
    """Top-minus-Bottom (long/short) spread return per day.

    Returns
    -------
    DataFrame with ``datetime``, ``ls_return`` (top bucket minus bottom bucket).
    """
    top = bucket_returns.filter(pl.col("bucket") == quantiles).select(
        "datetime", pl.col("mean_return").alias("top_return")
    )
    bottom = bucket_returns.filter(pl.col("bucket") == 1).select(
        "datetime", pl.col("mean_return").alias("bottom_return")
    )
    return (
        top.join(bottom, on="datetime", how="inner")
        .with_columns((pl.col("top_return") - pl.col("bottom_return")).alias("ls_return"))
        .select("datetime", "ls_return")
        .sort("datetime")
    )


def compute_bucket_avg_return(
    df: pl.DataFrame,
    quantiles: int = 5,
) -> pl.DataFrame:
    """Overall mean forward_return per bucket (across all dates).

    Returns ``bucket``, ``avg_return``.
    """
    bucketed = _assign_buckets(df, quantiles, ["datetime"])
    return (
        bucketed.group_by("bucket")
        .agg(pl.col("forward_return").mean().alias("avg_return"))
        .sort("bucket")
    )


def compute_bucket_avg_return_demean(
    df: pl.DataFrame,
    quantiles: int = 5,
) -> pl.DataFrame:
    """Same as ``compute_bucket_avg_return`` but with cross-sectional demeaned returns.

    For each datetime, subtract the cross-sectional mean of forward_return
    before aggregating per bucket.

    Returns ``bucket``, ``avg_return_demean``.
    """
    demeaned = df.with_columns(
        (pl.col("forward_return") - pl.col("forward_return").mean().over("datetime"))
        .alias("forward_return_demean")
    )
    bucketed = _assign_buckets(demeaned, quantiles, ["datetime"])
    return (
        bucketed.group_by("bucket")
        .agg(pl.col("forward_return_demean").mean().alias("avg_return_demean"))
        .sort("bucket")
    )


def compute_cumulative_bucket_returns(bucket_returns: pl.DataFrame) -> pl.DataFrame:
    """Cumulative returns per bucket for charting equity curves.

    Returns
    -------
    DataFrame with ``datetime``, ``bucket``, ``cumulative_return``.
    """
    return (
        bucket_returns.sort("bucket", "datetime")
        .with_columns(
            (1 + pl.col("mean_return")).cum_prod().over("bucket").alias("cumulative_return")
        )
        .select("datetime", "bucket", "cumulative_return")
    )
