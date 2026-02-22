"""High-level unified interface for fetching stock data."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

import polars as pl

from data_layer.cache_manager import merge_and_save, read_cache
from data_layer.fetcher import fetch_historical_data
from data_layer.ib_client import IBClientManager

logger = logging.getLogger(__name__)


def _needs_update(cached: pl.DataFrame | None, end_date: date) -> bool:
    if cached is None or len(cached) == 0:
        return True
    last = cached["datetime"].sort().to_list()[-1]
    # cached datetime may be str "YYYY-MM-DD" or "YYYYMMDD"
    last_date = date.fromisoformat(str(last).replace(" ", "")[:10])
    # allow 1-day tolerance for weekends/holidays
    return last_date < end_date - timedelta(days=3)


async def _fetch_one(
    mgr: IBClientManager,
    ticker: str,
    duration_str: str,
    bar_size: str,
    end_date_str: str,
    what_to_show: str,
) -> tuple[str, pl.DataFrame]:
    df = await fetch_historical_data(
        mgr, ticker,
        end_date=end_date_str,
        duration_str=duration_str,
        bar_size=bar_size,
        what_to_show=what_to_show,
    )
    merged = merge_and_save(ticker, bar_size, df) if len(df) > 0 else df
    return ticker, merged


async def get_stock_data(
    tickers: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    timeframe: str = "1 day",
    what_to_show: str = "ADJUSTED_LAST",
    ib_manager: IBClientManager | None = None,
) -> dict[str, pl.DataFrame]:
    """Fetch historical data for multiple tickers (cache-aware, rate-limited).

    Returns {ticker: polars.DataFrame} with columns:
        datetime, open, high, low, close, volume
    """
    end_date = end_date or date.today()
    start_date = start_date or end_date - timedelta(days=365)
    delta_days = (end_date - start_date).days
    duration_str = f"{delta_days} D" if delta_days <= 365 else "1 Y"
    # ADJUSTED_LAST does not support endDateTime; pass empty string
    end_date_str = "" if what_to_show == "ADJUSTED_LAST" else end_date.strftime("%Y%m%d-%H:%M:%S")

    results: dict[str, pl.DataFrame] = {}
    to_fetch: list[str] = []

    for t in tickers:
        cached = read_cache(t, timeframe)
        if cached is not None and not _needs_update(cached, end_date):
            logger.info("Cache hit for %s", t)
            results[t] = cached
        else:
            to_fetch.append(t)

    if not to_fetch:
        return results

    owns_connection = ib_manager is None
    mgr = ib_manager or IBClientManager()

    try:
        if owns_connection:
            await mgr.connect()

        tasks = [
            _fetch_one(mgr, t, duration_str, timeframe, end_date_str, what_to_show)
            for t in to_fetch
        ]
        for coro in asyncio.as_completed(tasks):
            ticker, df = await coro
            results[ticker] = df

    finally:
        if owns_connection:
            await mgr.disconnect()

    return results
