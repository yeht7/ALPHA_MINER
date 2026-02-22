"""Batch-fetch 10Y adjusted daily data for the top 300 US stocks.

Features:
- Skips tickers already cached with sufficient data
- Respects IBKR pacing (via PacingLimiter)
- Prints live progress and ETA
- Logs failures but continues (no single ticker blocks the batch)

Usage:
    IB_GATEWAY_HOST=192.168.1.202 IB_GATEWAY_PORT=4011 uv run python scripts/batch_fetch.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_layer.cache_manager import merge_and_save, read_cache
from data_layer.fetcher import fetch_historical_data
from data_layer.ib_client import IBClientManager
from data_layer.universe import TOP_300_TICKERS

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("batch_fetch")
logger.setLevel(logging.INFO)

DURATION = "10 Y"
BAR_SIZE = "1 day"
WHAT_TO_SHOW = "ADJUSTED_LAST"
MIN_BARS_EXPECTED = 2400  # ~10Y of trading days


def _already_cached(ticker: str) -> bool:
    cached = read_cache(ticker, BAR_SIZE)
    return cached is not None and len(cached) >= MIN_BARS_EXPECTED


async def run() -> None:
    to_fetch = [t for t in TOP_300_TICKERS if not _already_cached(t)]
    already = len(TOP_300_TICKERS) - len(to_fetch)

    if not to_fetch:
        logger.info("All %d tickers already cached. Done.", len(TOP_300_TICKERS))
        return

    logger.info(
        "Fetching %d tickers (%d already cached)", len(to_fetch), already
    )

    mgr = IBClientManager()
    await mgr.connect()

    ok, fail = 0, 0
    t0 = time.monotonic()

    try:
        for i, ticker in enumerate(to_fetch, 1):
            elapsed = time.monotonic() - t0
            rate = ok / elapsed * 60 if elapsed > 0 and ok > 0 else 0
            remaining = (len(to_fetch) - i) / (rate / 60) if rate > 0 else 0
            eta = f"{remaining/60:.0f}h{remaining%60:.0f}m" if rate > 0 else "..."

            try:
                df = await fetch_historical_data(
                    mgr, ticker,
                    duration_str=DURATION,
                    bar_size=BAR_SIZE,
                    what_to_show=WHAT_TO_SHOW,
                )
                if len(df) > 0:
                    merge_and_save(ticker, BAR_SIZE, df)
                    ok += 1
                    logger.info(
                        "[%3d/%d] %-6s  %d bars  OK   (%.0f/min, ETA %s)",
                        i, len(to_fetch), ticker, len(df), rate, eta,
                    )
                else:
                    fail += 1
                    logger.warning("[%3d/%d] %-6s  EMPTY", i, len(to_fetch), ticker)
            except Exception as exc:
                fail += 1
                logger.error(
                    "[%3d/%d] %-6s  FAIL: %s", i, len(to_fetch), ticker, exc
                )
    finally:
        await mgr.disconnect()

    total_time = time.monotonic() - t0
    logger.info(
        "Done: %d OK, %d failed, %.1f minutes total",
        ok, fail, total_time / 60,
    )


if __name__ == "__main__":
    asyncio.run(run())
