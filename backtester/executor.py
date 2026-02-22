"""Execution simulation: portfolio return, turnover, and transaction costs."""

from __future__ import annotations

import polars as pl


def simulate_execution(
    df: pl.DataFrame,
    commission_rate: float = 0.001,
    slippage_rate: float = 0.0005,
) -> pl.DataFrame:
    """Compute per-period portfolio returns net of transaction costs.

    Parameters
    ----------
    df : DataFrame with ``datetime, ticker, target_weight, forward_return``.
    commission_rate / slippage_rate : round-trip cost factors applied on turnover.

    Returns
    -------
    Time-series DataFrame (one row per datetime):
        ``datetime, gross_return, turnover, cost, net_return``
    """
    cost_rate = commission_rate + slippage_rate

    # w_{i,t-1} drifted by asset return: w_drift = w_{t-1} * (1 + r_t)
    ordered = df.sort("ticker", "datetime")

    ordered = ordered.with_columns(
        pl.col("target_weight")
        .shift(1)
        .over("ticker")
        .fill_null(0.0)
        .alias("_prev_weight"),
    ).with_columns(
        (pl.col("_prev_weight") * (1.0 + pl.col("forward_return"))).alias(
            "_drifted_weight"
        )
    )

    # per-asset contribution to gross return: w_{i,t-1} * r_{i,t}
    ordered = ordered.with_columns(
        (pl.col("_prev_weight") * pl.col("forward_return")).alias("_pnl_contrib"),
        (pl.col("target_weight") - pl.col("_drifted_weight"))
        .abs()
        .alias("_abs_trade"),
    )

    portfolio = (
        ordered.group_by("datetime")
        .agg(
            pl.col("_pnl_contrib").sum().alias("gross_return"),
            pl.col("_abs_trade").sum().alias("turnover"),
        )
        .sort("datetime")
    )

    portfolio = portfolio.with_columns(
        (pl.col("turnover") * cost_rate).alias("cost"),
    ).with_columns(
        (pl.col("gross_return") - pl.col("cost")).alias("net_return"),
    )

    return portfolio
