"""Order routing – build IB contracts / orders and submit (or dry-run log)."""

from __future__ import annotations

import logging
from typing import Literal

from ib_async import IB, LimitOrder, MarketOrder, Stock, Trade

from execution.target_translator import OrderDelta

logger = logging.getLogger(__name__)

OrderType = Literal["MKT", "MOC", "LMT"]


class OrderRouter:
    """Convert validated OrderDeltas into IB orders and optionally execute."""

    def __init__(self, ib: IB) -> None:
        self._ib = ib

    async def route_orders(
        self,
        orders: list[OrderDelta],
        order_type: OrderType = "MKT",
        limit_prices: dict[str, float] | None = None,
        dry_run: bool = True,
    ) -> list[Trade]:
        """Build and (optionally) submit orders to IB Gateway.

        Returns the list of ``ib_async.Trade`` objects. In dry-run mode,
        no real trades are placed – the constructed orders are logged only.
        """
        trades: list[Trade] = []

        for od in orders:
            contract = Stock(od.ticker, "SMART", "USD")
            self._ib.qualifyContracts(contract)

            ib_order = self._build_order(od, order_type, limit_prices)

            if dry_run:
                logger.info(
                    "[DRY RUN] %s %d × %s  (type=%s)",
                    od.action, od.quantity, od.ticker, order_type,
                )
                continue

            trade = self._ib.placeOrder(contract, ib_order)
            logger.info(
                "PLACED  %s %d × %s  orderId=%s",
                od.action, od.quantity, od.ticker, trade.order.orderId,
            )
            trades.append(trade)

        return trades

    @staticmethod
    def _build_order(
        od: OrderDelta,
        order_type: OrderType,
        limit_prices: dict[str, float] | None,
    ) -> MarketOrder | LimitOrder:
        if order_type == "LMT":
            prices = limit_prices or {}
            lmt_price = prices.get(od.ticker, 0.0)
            return LimitOrder(action=od.action, totalQuantity=od.quantity, lmtPrice=lmt_price)
        # MKT and MOC both use MarketOrder; MOC requires tif='MOC' but ib_insync
        # doesn't have a dedicated class – we set tif on a MarketOrder.
        order = MarketOrder(action=od.action, totalQuantity=od.quantity)
        if order_type == "MOC":
            order.tif = "MOC"  # time-in-force: Market On Close
        return order
