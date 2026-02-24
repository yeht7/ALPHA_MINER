"""Account state synchronization – positions & NLV from IB Gateway."""

from __future__ import annotations

import asyncio
import logging

from ib_async import IB

logger = logging.getLogger(__name__)


class PortfolioManager:
    """Read-only snapshot of the IB account: positions and net-liquidation value."""

    def __init__(self, ib: IB) -> None:
        self._ib = ib

    async def get_current_positions(self) -> dict[str, int]:
        """Return ``{ticker: signed_quantity}`` for every position in the account."""
        positions: dict[str, int] = {}
        for pos in self._ib.positions():
            symbol = pos.contract.symbol
            qty = int(pos.position)
            positions[symbol] = positions.get(symbol, 0) + qty
        logger.info("Fetched %d positions from IB", len(positions))
        return positions

    async def get_account_value(self) -> float:
        """Return Net Liquidation Value (NLV) from the account summary."""
        summary = await self._ib.accountSummaryAsync()
        for item in summary:
            if item.tag == "NetLiquidation" and item.currency == "USD":
                nlv = float(item.value)
                logger.info("Account NLV = $%.2f", nlv)
                return nlv
        raise RuntimeError("NetLiquidation not found in accountSummary")

    async def get_live_prices(self, tickers: list[str]) -> dict[str, float]:
        """Snapshot last prices for *tickers* via IB market-data."""
        from ib_async import Stock

        prices: dict[str, float] = {}
        contracts = [Stock(t, "SMART", "USD") for t in tickers]
        await self._ib.qualifyContractsAsync(*contracts)

        for contract in contracts:
            ticker_obj = self._ib.reqMktData(contract, snapshot=True)
            await asyncio.sleep(0.5)
            price = ticker_obj.marketPrice()
            if price and price == price:  # guard against NaN
                prices[contract.symbol] = float(price)
            else:
                logger.warning("No price for %s – skipping", contract.symbol)
            self._ib.cancelMktData(contract)

        logger.info("Fetched live prices for %d / %d tickers", len(prices), len(tickers))
        return prices
