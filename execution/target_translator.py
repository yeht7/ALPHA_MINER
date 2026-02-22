"""Target weight → order delta translation engine."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OrderDelta:
    ticker: str
    action: str          # "BUY" or "SELL"
    quantity: int         # always positive
    target_weight: float
    target_shares: int
    current_shares: int


def calculate_order_delta(
    target_weights: pl.DataFrame,
    current_positions: dict[str, int],
    nlv: float,
    current_prices: dict[str, float],
) -> list[OrderDelta]:
    """Translate target portfolio weights into concrete share deltas.

    Only the *latest* datetime cross-section of ``target_weights`` is used,
    since we are executing a single rebalance snapshot.
    """
    latest_dt = target_weights.select(pl.col("datetime").max()).item()
    snapshot = target_weights.filter(pl.col("datetime") == latest_dt)
    logger.info("Using target snapshot @ %s (%d tickers)", latest_dt, len(snapshot))

    orders: list[OrderDelta] = []
    for row in snapshot.iter_rows(named=True):
        ticker = row["ticker"]
        weight = row["target_weight"]
        price = current_prices.get(ticker)
        if price is None or price <= 0:
            logger.warning("No valid price for %s – skipping", ticker)
            continue

        target_dollar = weight * nlv
        target_shares = int(target_dollar / price)
        current_shares = current_positions.get(ticker, 0)
        delta = target_shares - current_shares

        if delta == 0:
            continue

        orders.append(OrderDelta(
            ticker=ticker,
            action="BUY" if delta > 0 else "SELL",
            quantity=abs(delta),
            target_weight=weight,
            target_shares=target_shares,
            current_shares=current_shares,
        ))

    logger.info("Generated %d order deltas (from %d targets)", len(orders), len(snapshot))
    return orders
