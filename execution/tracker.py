"""Async trade tracker – monitor fills and persist execution logs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from ib_async import IB, Trade

logger = logging.getLogger(__name__)

LOG_DIR = Path("./trade_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)


class TradeTracker:
    """Subscribe to IB order events, accumulate fills, and flush to Parquet."""

    def __init__(self, ib: IB, log_dir: Path = LOG_DIR) -> None:
        self._ib = ib
        self._log_dir = log_dir
        self._fills: list[dict] = []
        self._ib.orderStatusEvent += self._on_order_status

    def _on_order_status(self, trade: Trade) -> None:
        status = trade.orderStatus.status
        symbol = trade.contract.symbol
        logger.info("ORDER STATUS  %s  %s  (filled=%s)", symbol, status, trade.orderStatus.filled)

        if status == "Filled":
            self._record_fill(trade)

    def _record_fill(self, trade: Trade) -> None:
        for fill in trade.fills:
            rec = {
                "timestamp": fill.time or datetime.now(timezone.utc),
                "ticker": trade.contract.symbol,
                "action": trade.order.action,
                "quantity": int(fill.execution.shares),
                "price": float(fill.execution.price),
                "commission": float(fill.commissionReport.commission)
                if fill.commissionReport
                else 0.0,
                "order_id": trade.order.orderId,
            }
            self._fills.append(rec)
            logger.info("FILL  %s %s %d @ %.2f", rec["action"], rec["ticker"], rec["quantity"], rec["price"])

    async def wait_for_fills(self, trades: list[Trade], timeout: float = 60.0) -> None:
        """Block until all *trades* are done (Filled / Cancelled / Inactive) or *timeout*."""
        import asyncio

        terminal = {"Filled", "Cancelled", "Inactive", "ApiCancelled"}
        deadline = asyncio.get_event_loop().time() + timeout

        while True:
            remaining = [t for t in trades if t.orderStatus.status not in terminal]
            if not remaining:
                break
            if asyncio.get_event_loop().time() >= deadline:
                logger.warning("Timeout: %d orders still pending", len(remaining))
                break
            await self._ib.sleepAsync(0.5)

    def flush(self, tag: str = "") -> Path | None:
        """Write accumulated fills to a timestamped Parquet file and clear buffer."""
        if not self._fills:
            logger.info("No fills to flush")
            return None

        df = pl.DataFrame(self._fills)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        name = f"fills_{tag}_{ts}.parquet" if tag else f"fills_{ts}.parquet"
        path = self._log_dir / name
        df.write_parquet(path)
        logger.info("Flushed %d fills → %s", len(self._fills), path)
        self._fills.clear()
        return path

    def detach(self) -> None:
        """Unsubscribe from IB events."""
        self._ib.orderStatusEvent -= self._on_order_status
