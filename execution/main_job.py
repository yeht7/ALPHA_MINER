"""Top-level rebalance cycle – the single entry-point for paper trading."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import polars as pl

from alpha_miner.factors.reversal import ShortTermReversal
from alpha_miner.pipeline import FactorPipeline
from backtester.engine import VectorizedBacktester
from data_layer.ib_client import IBClientManager
from execution.risk_manager import RiskController
from execution.router import OrderRouter, OrderType
from execution.state_manager import PortfolioManager
from execution.target_translator import calculate_order_delta
from execution.tracker import TradeTracker

logger = logging.getLogger(__name__)

CACHE_DIR = Path("./data_cache")
BAR_SIZE = "1 day"
SAFE_BAR = BAR_SIZE.replace(" ", "_")


def _discover_tickers() -> list[str]:
    suffix = f"_{SAFE_BAR}.parquet"
    return sorted(p.name.removesuffix(suffix) for p in CACHE_DIR.glob(f"*{suffix}"))


def _generate_target_weights(tickers: list[str]) -> pl.DataFrame:
    """Run the alpha pipeline and return the latest target weights."""
    factor = ShortTermReversal(window=5)
    pipeline = FactorPipeline(factors=[factor], tickers=tickers, bar_size=BAR_SIZE, cache_dir=CACHE_DIR)
    results = pipeline.run()
    signals = results["ShortTermReversal"]

    frames = []
    for t in tickers:
        path = CACHE_DIR / f"{t}_{SAFE_BAR}.parquet"
        if not path.exists():
            continue
        df = pl.read_parquet(path).select("datetime", "open", "high", "low", "close", "volume")
        df = df.with_columns(pl.lit(t).alias("ticker"))
        frames.append(df)
    prices = pl.concat(frames).sort("datetime", "ticker")

    bt = VectorizedBacktester(delay=1, quantiles=5, strategy="long_short")
    return bt.generate_weights(signals, prices)


async def run_rebalance_cycle(
    *,
    dry_run: bool = True,
    order_type: OrderType = "MKT",
    ib_host: str | None = None,
    ib_port: int | None = None,
    client_id: int | None = None,
    max_position_pct: float = 0.05,
    max_gross_leverage: float = 1.0,
    restricted: list[str] | None = None,
) -> None:
    """Execute one full rebalance cycle.

    1. Connect to IB Gateway (Paper Trading, port 4002).
    2. Generate target weights from the Alpha Miner.
    3. Fetch live prices & account state.
    4. Calculate order deltas and apply risk checks.
    5. Route orders (dry-run by default).
    6. Track fills and flush logs.
    """
    tickers = _discover_tickers()
    if not tickers:
        logger.error("No cached data found in %s – run data fetch first", CACHE_DIR)
        return

    logger.info("=== Rebalance Cycle Start (dry_run=%s) ===", dry_run)

    # Step 1 – Alpha pipeline (CPU-bound, runs synchronously)
    logger.info("Generating target weights for %d tickers…", len(tickers))
    weights = _generate_target_weights(tickers)

    # Step 2 – Connect to IB
    mgr = IBClientManager(host=ib_host, port=ib_port, client_id=client_id)
    async with mgr:
        ib = mgr.ib
        portfolio = PortfolioManager(ib)
        router = OrderRouter(ib)
        tracker = TradeTracker(ib)
        risk = RiskController(
            max_position_pct=max_position_pct,
            max_gross_leverage=max_gross_leverage,
            restricted_list=restricted or [],
        )

        # Step 3 – Fetch account state & live prices
        nlv = await portfolio.get_account_value()
        current_positions = await portfolio.get_current_positions()

        target_tickers = weights.get_column("ticker").unique().to_list()
        live_prices = await portfolio.get_live_prices(target_tickers)

        # Step 4 – Delta & risk
        deltas = calculate_order_delta(weights, current_positions, nlv, live_prices)
        validated = risk.validate(deltas, nlv, live_prices)

        if not validated:
            logger.info("No orders to execute after risk checks")
            tracker.detach()
            return

        # Step 5 – Route
        trades = await router.route_orders(validated, order_type=order_type, dry_run=dry_run)

        # Step 6 – Wait & flush
        if trades:
            await tracker.wait_for_fills(trades, timeout=120.0)
            tracker.flush(tag="rebalance")

        tracker.detach()

    logger.info("=== Rebalance Cycle Complete ===")


async def run_scheduled(interval_seconds: float = 86400.0, **kwargs) -> None:
    """Run rebalance cycles on a fixed interval (default: daily)."""
    while True:
        try:
            await run_rebalance_cycle(**kwargs)
        except Exception:
            logger.exception("Rebalance cycle failed")
        logger.info("Next rebalance in %.0f seconds", interval_seconds)
        await asyncio.sleep(interval_seconds)


def main() -> None:
    """CLI entry-point for a single rebalance cycle."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    asyncio.run(run_rebalance_cycle(dry_run=True))


if __name__ == "__main__":
    main()
