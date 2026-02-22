"""Local Parquet cache – read / merge / write historical bar data."""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl

CACHE_DIR = Path(os.getenv("DATA_CACHE_DIR", "./data_cache"))


def _cache_path(ticker: str, bar_size: str) -> Path:
    safe = bar_size.replace(" ", "_")
    return CACHE_DIR / f"{ticker}_{safe}.parquet"


def read_cache(ticker: str, bar_size: str) -> pl.DataFrame | None:
    path = _cache_path(ticker, bar_size)
    if not path.exists():
        return None
    return pl.read_parquet(path)


def write_cache(ticker: str, bar_size: str, df: pl.DataFrame) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(ticker, bar_size)
    df.write_parquet(path)
    return path


def merge_and_save(
    ticker: str, bar_size: str, new_df: pl.DataFrame
) -> pl.DataFrame:
    """Merge new data with existing cache, dedup by datetime, save."""
    cached = read_cache(ticker, bar_size)
    if cached is not None and len(cached) > 0:
        combined = pl.concat([cached, new_df])
    else:
        combined = new_df

    combined = combined.unique(subset=["datetime"]).sort("datetime")
    write_cache(ticker, bar_size, combined)
    return combined
