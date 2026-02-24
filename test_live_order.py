"""实盘下单连通性测试 — 复用 Part 5 执行引擎结构，买入 1 股 SPY 后立即平仓."""

import asyncio
import logging
import os
import sys
from pathlib import Path

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, val = line.partition("=")
        os.environ[key.strip()] = val.strip()

from ib_async import Stock

from data_layer.ib_client import IBClientManager
from execution.router import OrderRouter
from execution.target_translator import OrderDelta
from execution.tracker import TradeTracker

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

TICKER = "SPY"
QTY = 1


async def _get_nlv(ib) -> float:
    """异步获取 NLV，规避 accountSummary() 同步调用在 asyncio.run 下的兼容问题."""
    items = await ib.accountSummaryAsync()
    for item in items:
        if item.tag == "NetLiquidation" and item.currency == "USD":
            return float(item.value)
    raise RuntimeError("NetLiquidation not found")


async def _get_price(ib, ticker: str) -> float | None:
    ib.reqMarketDataType(3)  # 3 = delayed data（无需额外订阅）
    contract = Stock(ticker, "SMART", "USD")
    await ib.qualifyContractsAsync(contract)
    ticker_obj = ib.reqMktData(contract, snapshot=True)
    await asyncio.sleep(3)
    price = ticker_obj.marketPrice()
    ib.cancelMktData(contract)
    ib.reqMarketDataType(1)  # 恢复实时
    return float(price) if price and price == price else None


async def run_test() -> bool:
    mgr = IBClientManager()
    log.info("目标网关: %s:%s (clientId=%s)", mgr.host, mgr.port, mgr.client_id)

    async with mgr:
        ib = mgr.ib
        router = OrderRouter(ib)
        tracker = TradeTracker(ib)

        # ── 1. 账户快照 ──
        nlv = await _get_nlv(ib)
        log.info("账户 NLV = $%.2f", nlv)

        positions = {p.contract.symbol: int(p.position) for p in ib.positions()}
        log.info("当前持仓: %s", positions if positions else "(空)")

        # ── 2. 报价（仅供参考，市价单不依赖此步） ──
        price = await _get_price(ib, TICKER)
        if price:
            log.info("%s 当前价格: $%.2f", TICKER, price)
        else:
            log.warning("无法获取 %s 报价（不影响市价单提交）", TICKER)

        # ── 3. 买入 ──
        buy = OrderDelta(
            ticker=TICKER, action="BUY", quantity=QTY,
            target_weight=0.0, target_shares=QTY, current_shares=0,
        )
        log.info("提交买入: %s %d 股 MKT", TICKER, QTY)
        buy_trades = await router.route_orders([buy], order_type="MKT", dry_run=False)

        if not buy_trades:
            log.error("买入订单未提交")
            tracker.detach()
            return False

        await tracker.wait_for_fills(buy_trades, timeout=30.0)
        if buy_trades[0].orderStatus.status != "Filled":
            log.error("买入未成交，状态: %s", buy_trades[0].orderStatus.status)
            tracker.detach()
            return False

        fill_price = buy_trades[0].orderStatus.avgFillPrice
        log.info("买入成交 ✓  %d 股 %s @ $%.2f", QTY, TICKER, fill_price)

        # ── 4. 立即卖出平仓 ──
        sell = OrderDelta(
            ticker=TICKER, action="SELL", quantity=QTY,
            target_weight=0.0, target_shares=0, current_shares=QTY,
        )
        log.info("提交卖出: %s %d 股 MKT", TICKER, QTY)
        sell_trades = await router.route_orders([sell], order_type="MKT", dry_run=False)

        if not sell_trades:
            log.error("卖出订单未提交")
            tracker.detach()
            return False

        await tracker.wait_for_fills(sell_trades, timeout=30.0)
        if sell_trades[0].orderStatus.status != "Filled":
            log.error("卖出未成交，状态: %s", sell_trades[0].orderStatus.status)
            tracker.detach()
            return False

        sell_price = sell_trades[0].orderStatus.avgFillPrice
        log.info("卖出成交 ✓  %d 股 %s @ $%.2f", QTY, TICKER, sell_price)
        log.info("往返损益: $%.2f (不含佣金)", (sell_price - fill_price) * QTY)

        tracker.flush(tag="live_test")
        tracker.detach()

    return True


def main():
    log.info("=" * 50)
    log.info("实盘下单测试 — %s %d 股往返 (Part 5 执行引擎)", TICKER, QTY)
    log.info("=" * 50)

    confirm = input(f"\n即将在实盘买入 {QTY} 股 {TICKER} 并立即卖出，确认？(yes/no): ")
    if confirm.strip().lower() != "yes":
        log.info("用户取消")
        sys.exit(0)

    ok = asyncio.run(run_test())
    log.info("实盘下单测试%s", "通过 ✓" if ok else "失败 ✗")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
