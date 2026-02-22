"""Fetch historical bar data from IBKR and return as Polars DataFrames."""

from __future__ import annotations

import logging

import polars as pl
from ib_async import Stock

from data_layer.ib_client import IBClientManager

logger = logging.getLogger(__name__)

EMPTY_SCHEMA = {
    "datetime": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}


def _bars_to_polars(bars) -> pl.DataFrame:
    return pl.DataFrame({
        "datetime": [str(b.date) for b in bars],
        "open": [float(b.open) for b in bars],
        "high": [float(b.high) for b in bars],
        "low": [float(b.low) for b in bars],
        "close": [float(b.close) for b in bars],
        "volume": [float(b.volume) for b in bars],
    })


async def fetch_historical_data(
    mgr: IBClientManager,
    ticker: str,
    end_date: str = "",
    duration_str: str = "1 Y",
    bar_size: str = "1 day",
    what_to_show: str = "ADJUSTED_LAST",
) -> pl.DataFrame:
    """Fetch historical bars for a single US stock."""
    contract = Stock(ticker, "SMART", "USD")
    await mgr.ib.qualifyContractsAsync(contract)

    await mgr.pacing.acquire()

    bars = await mgr.ib.reqHistoricalDataAsync(
        contract,
        endDateTime=end_date,
        durationStr=duration_str,
        barSizeSetting=bar_size,
        whatToShow=what_to_show,
        useRTH=True,
        formatDate=1,
    )

    if not bars:
        logger.warning("No data returned for %s", ticker)
        return pl.DataFrame(schema=EMPTY_SCHEMA)

    df = _bars_to_polars(bars)
    logger.info("Fetched %d bars for %s", len(df), ticker)
    return df
