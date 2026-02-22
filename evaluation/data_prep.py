"""Data preparation: join aligned factor data with sector metadata."""

from __future__ import annotations

import polars as pl

DEFAULT_SECTOR_MAP: dict[str, str] = {
    "AAPL": "Information Technology",
    "MSFT": "Information Technology",
    "GOOGL": "Communication Services",
    "META": "Communication Services",
    "AMZN": "Consumer Discretionary",
    "TSLA": "Consumer Discretionary",
    "NVDA": "Information Technology",
    "JPM": "Financials",
    "JNJ": "Health Care",
    "V": "Financials",
    "UNH": "Health Care",
    "XOM": "Energy",
    "PG": "Consumer Staples",
    "LLY": "Health Care",
    "MA": "Financials",
}


def join_sector(
    aligned: pl.DataFrame,
    sector_map: dict[str, str] | pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Attach ``gics_sector`` column to the aligned DataFrame.

    Parameters
    ----------
    aligned : DataFrame with at least ``ticker`` column.
    sector_map : Either a ``{ticker: sector}`` dict, a DataFrame with
        ``ticker, gics_sector`` columns, or *None* to use ``DEFAULT_SECTOR_MAP``.

    Returns
    -------
    DataFrame with an additional ``gics_sector`` column.
    Rows whose ticker has no mapping are assigned ``"Unknown"``.
    """
    if sector_map is None:
        sector_map = DEFAULT_SECTOR_MAP

    if isinstance(sector_map, dict):
        sector_df = pl.DataFrame(
            {"ticker": list(sector_map.keys()), "gics_sector": list(sector_map.values())}
        )
    else:
        sector_df = sector_map

    if "gics_sector" in aligned.columns:
        return aligned

    return aligned.join(sector_df, on="ticker", how="left").with_columns(
        pl.col("gics_sector").fill_null("Unknown")
    )
