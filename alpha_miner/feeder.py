"""Load cached Parquet files into a single long-format Polars DataFrame."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from data_layer.cache_manager import CACHE_DIR

REQUIRED_COLS = ["datetime", "open", "high", "low", "close", "volume"]


def load_data(
    tickers: list[str],
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    bar_size: str = "1 day",
    cache_dir: Path | None = None,
) -> pl.DataFrame:
    """Read cached Parquet for each ticker, concat into one long DataFrame.

    Returns columns: ``datetime | ticker | open | high | low | close | volume``
    sorted by ``(datetime, ticker)``.
    """
    root = cache_dir or CACHE_DIR
    safe_bar = bar_size.replace(" ", "_")

    frames: list[pl.DataFrame] = []
    for t in tickers:
        path = root / f"{t}_{safe_bar}.parquet"
        if not path.exists():
            continue
        df = pl.read_parquet(path).select(REQUIRED_COLS).with_columns(
            pl.lit(t).alias("ticker")
        )
        frames.append(df)

    if not frames:
        return pl.DataFrame(schema={c: pl.Utf8 for c in ["datetime", "ticker"]}
                            | {c: pl.Float64 for c in ["open", "high", "low", "close", "volume"]})

    combined = pl.concat(frames)

    if start_date is not None:
        combined = combined.filter(pl.col("datetime") >= str(start_date))
    if end_date is not None:
        combined = combined.filter(pl.col("datetime") <= str(end_date))

    return combined.sort("datetime", "ticker")
