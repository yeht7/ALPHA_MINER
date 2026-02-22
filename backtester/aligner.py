"""Data alignment: merge signals with prices and compute forward returns."""

from __future__ import annotations

import polars as pl


def align_data(
    signals: pl.DataFrame,
    prices: pl.DataFrame,
    delay: int = 1,
) -> pl.DataFrame:
    """Merge signal and price DataFrames, shift signals forward by *delay* periods.

    Parameters
    ----------
    signals : DataFrame with ``datetime, ticker, factor_value``
    prices  : DataFrame with at least ``datetime, ticker, close``
    delay   : Number of periods to shift. ``delay=1`` means the signal at *T*
              captures the return from *T* to *T+1*.

    Returns
    -------
    DataFrame with columns:
        ``datetime, ticker, factor_value, close, forward_return``
    where ``forward_return`` = (close_{t+1} - close_t) / close_t
    and ``factor_value`` is the signal from *delay* periods ago.
    """
    price_cols = ["datetime", "ticker", "close"]
    if "open" in prices.columns:
        price_cols.append("open")

    p = prices.select(price_cols).sort("ticker", "datetime")

    # forward return per ticker: r_{t→t+1}
    p = p.with_columns(
        (pl.col("close").shift(-1).over("ticker") / pl.col("close") - 1).alias(
            "forward_return"
        )
    )

    # shift signal: align factor at T with return starting delay periods later
    s = signals.sort("ticker", "datetime").with_columns(
        pl.col("factor_value").shift(delay).over("ticker").alias("factor_value")
    )

    merged = s.join(p, on=["datetime", "ticker"], how="inner")
    return merged.drop_nulls(subset=["factor_value", "forward_return"]).sort(
        "datetime", "ticker"
    )
