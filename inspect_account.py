"""IB 账户查看 CLI — 持仓 / 账户摘要 / 挂单 / 成交日志 / 监控 / 绩效 / 滑点."""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

# ── .env 加载 ──
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        k, _, v = line.partition("=")
        os.environ[k.strip()] = v.strip()

from data_layer.ib_client import IBClientManager

TRADE_LOG_DIR = Path("./trade_logs")
SNAPSHOT_DIR = Path("./account_snapshots")
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

NLV_HISTORY = SNAPSHOT_DIR / "nlv_history.parquet"
POS_HISTORY = SNAPSHOT_DIR / "position_history.parquet"

TRADING_DAYS_PER_YEAR = 252


# ===================================================================
# 工具函数
# ===================================================================

async def _connect() -> IBClientManager:
    mgr = IBClientManager()
    await mgr.connect()
    return mgr


def _append_parquet(path: Path, new: pl.DataFrame) -> None:
    """追加写入 Parquet，如果文件已存在则合并去重."""
    if path.exists():
        old = pl.read_parquet(path)
        combined = pl.concat([old, new])
    else:
        combined = new
    combined.write_parquet(path)


def _load_all_trade_logs() -> pl.DataFrame:
    """加载 trade_logs/ 下所有 Parquet 成交日志并合并."""
    files = sorted(TRADE_LOG_DIR.glob("*.parquet"))
    if not files:
        return pl.DataFrame()
    frames = [pl.read_parquet(f) for f in files]
    return pl.concat(frames).sort("timestamp")


# ===================================================================
# cmd: positions
# ===================================================================

async def cmd_positions(args: argparse.Namespace) -> None:
    """显示当前持仓."""
    mgr = await _connect()
    ib = mgr.ib
    try:
        items = list(ib.portfolio())
        if not items:
            print("\n当前无持仓")
            return

        if args.ticker:
            tickers = {t.upper() for t in args.ticker}
            items = [p for p in items if p.contract.symbol in tickers]

        rows = []
        for p in items:
            rows.append({
                "ticker": p.contract.symbol,
                "qty": p.position,
                "avg_cost": p.averageCost,
                "market_price": p.marketPrice,
                "market_value": p.marketValue,
                "unrealized_pnl": p.unrealizedPNL,
                "realized_pnl": p.realizedPNL,
                "pnl_pct": (p.unrealizedPNL / (p.averageCost * p.position) * 100)
                if p.averageCost * p.position != 0 else 0.0,
            })

        if args.sort:
            key = args.sort
            valid_keys = {"ticker", "qty", "market_value", "unrealized_pnl", "pnl_pct"}
            if key not in valid_keys:
                print(f"无效排序字段 '{key}'，可选: {', '.join(sorted(valid_keys))}")
                return
            rows.sort(key=lambda r: r[key], reverse=not args.asc)

        total_mv = sum(r["market_value"] for r in rows)
        total_pnl = sum(r["unrealized_pnl"] for r in rows)

        print(f"\n{'─'*90}")
        print(f"  持仓明细  ({len(rows)} 只)")
        print(f"{'─'*90}")
        header = f"  {'Ticker':<8} {'持仓':>8} {'均价':>12} {'现价':>12} {'市值':>14} {'浮动盈亏':>12} {'盈亏%':>8}"
        print(header)
        print(f"  {'─'*8} {'─'*8} {'─'*12} {'─'*12} {'─'*14} {'─'*12} {'─'*8}")
        for r in rows:
            print(
                f"  {r['ticker']:<8} {r['qty']:>8.4g} "
                f"${r['avg_cost']:>10,.2f} ${r['market_price']:>10,.2f} "
                f"${r['market_value']:>12,.2f} ${r['unrealized_pnl']:>+10,.2f} "
                f"{r['pnl_pct']:>+7.2f}%"
            )
        print(f"  {'─'*8} {'─'*8} {'─'*12} {'─'*12} {'─'*14} {'─'*12} {'─'*8}")
        print(
            f"  {'合计':<8} {'':<8} {'':<12} {'':<12} "
            f"${total_mv:>12,.2f} ${total_pnl:>+10,.2f}"
        )

        if args.json:
            df = pl.DataFrame(rows)
            print(f"\n{df}")
    finally:
        await mgr.disconnect()


# ===================================================================
# cmd: summary
# ===================================================================

async def cmd_summary(args: argparse.Namespace) -> None:
    """显示账户摘要."""
    mgr = await _connect()
    ib = mgr.ib
    try:
        items = await ib.accountSummaryAsync()

        IMPORTANT_TAGS = {
            "NetLiquidation", "TotalCashValue", "GrossPositionValue",
            "MaintMarginReq", "AvailableFunds", "BuyingPower",
            "UnrealizedPnL", "RealizedPnL", "ExcessLiquidity",
        }

        if args.all:
            show = [(i.tag, i.value, i.currency) for i in items if i.currency == "USD"]
        else:
            show = [(i.tag, i.value, i.currency) for i in items
                    if i.tag in IMPORTANT_TAGS and i.currency == "USD"]

        show.sort(key=lambda x: x[0])

        TAG_CN = {
            "NetLiquidation": "净清算价值 (NLV)",
            "TotalCashValue": "现金余额",
            "GrossPositionValue": "持仓总市值",
            "MaintMarginReq": "维持保证金",
            "AvailableFunds": "可用资金",
            "BuyingPower": "购买力",
            "UnrealizedPnL": "未实现盈亏",
            "RealizedPnL": "已实现盈亏",
            "ExcessLiquidity": "超额流动性",
        }

        print(f"\n{'─'*60}")
        print(f"  账户摘要")
        print(f"{'─'*60}")
        for tag, val, cur in show:
            label = TAG_CN.get(tag, tag)
            try:
                formatted = f"${float(val):>14,.2f} {cur}"
            except ValueError:
                formatted = f"{val:>15} {cur}"
            print(f"  {label:<24} {formatted}")
    finally:
        await mgr.disconnect()


# ===================================================================
# cmd: orders
# ===================================================================

async def cmd_orders(args: argparse.Namespace) -> None:
    """显示当日挂单."""
    mgr = await _connect()
    ib = mgr.ib
    try:
        trades = ib.openTrades()
        if not trades:
            print("\n当前无活跃订单")
            return

        print(f"\n{'─'*80}")
        print(f"  活跃订单  ({len(trades)} 笔)")
        print(f"{'─'*80}")
        print(f"  {'OrderId':<10} {'Ticker':<8} {'Action':<6} {'Qty':>6} {'Type':<6} {'Status':<14}")
        print(f"  {'─'*10} {'─'*8} {'─'*6} {'─'*6} {'─'*6} {'─'*14}")
        for t in trades:
            print(
                f"  {t.order.orderId:<10} {t.contract.symbol:<8} "
                f"{t.order.action:<6} {t.order.totalQuantity:>6.0f} "
                f"{t.order.orderType:<6} {t.orderStatus.status:<14}"
            )
    finally:
        await mgr.disconnect()


# ===================================================================
# cmd: logs
# ===================================================================

def cmd_logs(args: argparse.Namespace) -> None:
    """查看成交日志（离线，无需连接 IB）."""
    files = sorted(TRADE_LOG_DIR.glob("*.parquet"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not files:
        print("\ntrade_logs/ 为空")
        return

    if args.list:
        print(f"\n{'─'*60}")
        print(f"  成交日志  ({len(files)} 个文件)")
        print(f"{'─'*60}")
        for f in files:
            df = pl.read_parquet(f)
            size = f"{f.stat().st_size / 1024:.1f} KB"
            print(f"  {f.name:<42} {len(df):>4} 笔  {size:>8}")
        return

    target = files[0]
    if args.file:
        match = [f for f in files if args.file in f.name]
        if not match:
            print(f"未找到匹配 '{args.file}' 的日志文件")
            return
        target = match[0]

    df = pl.read_parquet(target)
    if args.ticker:
        df = df.filter(pl.col("ticker") == args.ticker.upper())

    print(f"\n{'─'*60}")
    print(f"  {target.name}  ({len(df)} 笔成交)")
    print(f"{'─'*60}")
    print(df.head(args.top))

    if "price" in df.columns and "quantity" in df.columns:
        total_notional = (df["price"] * df["quantity"]).sum()
        total_comm = df["commission"].sum() if "commission" in df.columns else 0
        print(f"\n  成交金额合计: ${total_notional:,.2f}    佣金合计: ${total_comm:,.2f}")


# ===================================================================
# cmd: monitor  —— 账户快照采集
# ===================================================================

async def _take_snapshot(ib) -> tuple[pl.DataFrame, pl.DataFrame]:
    """采集一次快照，返回 (nlv_row, position_rows)."""
    now = datetime.now(timezone.utc)
    summary = await ib.accountSummaryAsync()

    acct: dict[str, float] = {}
    for item in summary:
        if item.currency == "USD":
            try:
                acct[item.tag] = float(item.value)
            except ValueError:
                pass

    nlv_row = pl.DataFrame([{
        "timestamp": now,
        "nlv": acct.get("NetLiquidation", 0.0),
        "cash": acct.get("TotalCashValue", 0.0),
        "gross_position_value": acct.get("GrossPositionValue", 0.0),
        "unrealized_pnl": acct.get("UnrealizedPnL", 0.0),
        "realized_pnl": acct.get("RealizedPnL", 0.0),
        "maint_margin": acct.get("MaintMarginReq", 0.0),
    }])

    pos_rows = []
    for p in ib.portfolio():
        pos_rows.append({
            "timestamp": now,
            "ticker": p.contract.symbol,
            "qty": float(p.position),
            "avg_cost": float(p.averageCost),
            "market_price": float(p.marketPrice),
            "market_value": float(p.marketValue),
            "unrealized_pnl": float(p.unrealizedPNL),
        })

    pos_df = pl.DataFrame(pos_rows) if pos_rows else pl.DataFrame(schema={
        "timestamp": pl.Datetime("us", "UTC"), "ticker": pl.Utf8,
        "qty": pl.Float64, "avg_cost": pl.Float64, "market_price": pl.Float64,
        "market_value": pl.Float64, "unrealized_pnl": pl.Float64,
    })

    return nlv_row, pos_df


async def cmd_monitor(args: argparse.Namespace) -> None:
    """定时采集账户快照."""
    mgr = await _connect()
    ib = mgr.ib

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        count = 0
        while True:
            nlv_row, pos_df = await _take_snapshot(ib)
            _append_parquet(NLV_HISTORY, nlv_row)
            if len(pos_df) > 0:
                _append_parquet(POS_HISTORY, pos_df)

            count += 1
            nlv = nlv_row["nlv"][0]
            n_pos = len(pos_df)
            ts = nlv_row["timestamp"][0]
            print(f"  [{ts}]  快照 #{count}  NLV=${nlv:,.2f}  持仓={n_pos}只  → {SNAPSHOT_DIR}/")

            if args.once:
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.interval)
                break  # stop signal received
            except asyncio.TimeoutError:
                pass  # interval elapsed, take next snapshot
    finally:
        await mgr.disconnect()

    total = len(pl.read_parquet(NLV_HISTORY)) if NLV_HISTORY.exists() else 0
    print(f"\n  累计 {total} 条 NLV 快照存储于 {NLV_HISTORY}")


# ===================================================================
# cmd: metrics  —— 账户绩效指标（离线）
# ===================================================================

def _calc_account_metrics(nlv_df: pl.DataFrame) -> dict:
    """从 NLV 时序计算账户级绩效指标."""
    daily = (
        nlv_df
        .with_columns(pl.col("timestamp").dt.date().alias("date"))
        .group_by("date").agg(pl.col("nlv").last())
        .sort("date")
    )

    if len(daily) < 2:
        return {"error": "快照不足 2 天，无法计算绩效"}

    returns = daily.with_columns(
        pl.col("nlv").pct_change().alias("daily_return")
    ).drop_nulls("daily_return")

    rets = returns["daily_return"]
    n_days = len(rets)
    mean_ret = rets.mean()
    std_ret = rets.std()

    ann_return = mean_ret * TRADING_DAYS_PER_YEAR
    ann_vol = std_ret * math.sqrt(TRADING_DAYS_PER_YEAR)
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0

    # 最大回撤
    equity = daily["nlv"]
    peak = equity.cum_max()
    dd = (equity - peak) / peak
    max_dd = dd.min()

    calmar = ann_return / abs(max_dd) if max_dd != 0 else 0.0

    # 胜率 & 盈亏比
    wins = rets.filter(rets > 0)
    losses = rets.filter(rets < 0)
    win_rate = len(wins) / n_days if n_days > 0 else 0.0
    avg_win = wins.mean() if len(wins) > 0 else 0.0
    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.0
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")

    # 期望值 = 胜率 × 平均盈利 - 败率 × 平均亏损
    expectancy = win_rate * avg_win - (1 - win_rate) * avg_loss

    first_nlv = daily["nlv"][0]
    last_nlv = daily["nlv"][-1]
    total_return = (last_nlv - first_nlv) / first_nlv

    return {
        "观测天数": n_days,
        "起始 NLV": first_nlv,
        "最新 NLV": last_nlv,
        "累计收益率": total_return,
        "年化收益率": ann_return,
        "年化波动率": ann_vol,
        "Sharpe Ratio": sharpe,
        "最大回撤": max_dd,
        "Calmar Ratio": calmar,
        "日胜率": win_rate,
        "平均盈利日": avg_win,
        "平均亏损日": avg_loss,
        "盈亏比": pl_ratio,
        "期望值 (E)": expectancy,
    }


def _calc_trade_metrics(trades_df: pl.DataFrame) -> dict:
    """从成交日志计算交易级指标（FIFO 配对）."""
    if trades_df.is_empty():
        return {"error": "无成交记录"}

    # FIFO 配对：按 ticker 分组，BUY 入队，SELL 出队
    queue: dict[str, list[tuple[float, int]]] = {}  # ticker → [(price, qty), ...]
    round_trips: list[dict] = []

    for row in trades_df.sort("timestamp").iter_rows(named=True):
        ticker = row["ticker"]
        price = row["price"]
        qty = row["quantity"]
        action = row["action"]
        commission = row.get("commission", 0.0) or 0.0

        if action == "BUY":
            queue.setdefault(ticker, []).append((price, qty, commission))
        elif action == "SELL":
            remaining = qty
            sell_notional = price * qty
            buy_notional = 0.0
            buy_comm = 0.0
            while remaining > 0 and queue.get(ticker):
                bp, bq, bc = queue[ticker][0]
                matched = min(remaining, bq)
                buy_notional += bp * matched
                buy_comm += bc * (matched / bq) if bq > 0 else 0
                remaining -= matched
                if matched == bq:
                    queue[ticker].pop(0)
                else:
                    queue[ticker][0] = (bp, bq - matched, bc * (1 - matched / bq))

            filled = qty - remaining
            if filled > 0:
                pnl = (price * filled) - buy_notional - commission - buy_comm
                round_trips.append({
                    "ticker": ticker,
                    "qty": filled,
                    "buy_avg": buy_notional / filled,
                    "sell_price": price,
                    "pnl": pnl,
                    "return_pct": pnl / buy_notional if buy_notional > 0 else 0.0,
                })

    if not round_trips:
        return {"info": "无完整往返交易（仅有单边）", "total_trades": len(trades_df)}

    rt_df = pl.DataFrame(round_trips)
    wins = rt_df.filter(pl.col("pnl") > 0)
    losses = rt_df.filter(pl.col("pnl") < 0)

    n = len(rt_df)
    win_rate = len(wins) / n
    avg_win = wins["pnl"].mean() if len(wins) > 0 else 0.0
    avg_loss = abs(losses["pnl"].mean()) if len(losses) > 0 else 0.0
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")
    total_pnl = rt_df["pnl"].sum()
    avg_return = rt_df["return_pct"].mean()

    return {
        "往返交易数": n,
        "盈利笔数": len(wins),
        "亏损笔数": len(losses),
        "交易胜率": win_rate,
        "平均盈利": avg_win,
        "平均亏损": avg_loss,
        "盈亏比": pl_ratio,
        "总盈亏": total_pnl,
        "平均收益率": avg_return,
    }


def cmd_metrics(args: argparse.Namespace) -> None:
    """计算账户绩效指标."""
    # ── 账户级指标 ──
    print(f"\n{'═'*60}")
    print(f"  账户绩效指标")
    print(f"{'═'*60}")

    if NLV_HISTORY.exists():
        nlv_df = pl.read_parquet(NLV_HISTORY)
        if args.days:
            cutoff = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            from datetime import timedelta
            cutoff -= timedelta(days=args.days)
            nlv_df = nlv_df.filter(pl.col("timestamp") >= cutoff)

        acct = _calc_account_metrics(nlv_df)
        if "error" in acct:
            print(f"\n  {acct['error']}")
        else:
            print(f"\n  ── NLV 时序分析 ({acct['观测天数']} 天) ──\n")
            fmt_map = {
                "起始 NLV": lambda v: f"${v:,.2f}",
                "最新 NLV": lambda v: f"${v:,.2f}",
                "累计收益率": lambda v: f"{v:+.2%}",
                "年化收益率": lambda v: f"{v:+.2%}",
                "年化波动率": lambda v: f"{v:.2%}",
                "Sharpe Ratio": lambda v: f"{v:.3f}",
                "最大回撤": lambda v: f"{v:.2%}",
                "Calmar Ratio": lambda v: f"{v:.3f}",
                "日胜率": lambda v: f"{v:.1%}",
                "平均盈利日": lambda v: f"{v:+.4%}",
                "平均亏损日": lambda v: f"{v:.4%}",
                "盈亏比": lambda v: f"{v:.2f}" if v < 1e6 else "∞",
                "期望值 (E)": lambda v: f"{v:+.4%}",
            }
            for k, v in acct.items():
                if k == "观测天数":
                    continue
                fmt = fmt_map.get(k, lambda x: f"{x}")
                print(f"  {k:<16} {fmt(v)}")
    else:
        print(f"\n  无 NLV 快照数据。请先运行: python inspect_account.py monitor --once")

    # ── 交易级指标 ──
    trades_df = _load_all_trade_logs()
    if args.ticker and not trades_df.is_empty():
        trades_df = trades_df.filter(pl.col("ticker") == args.ticker.upper())

    print(f"\n  ── 交易级分析 ──\n")
    if trades_df.is_empty():
        print("  无成交记录")
    else:
        scope = f" ({args.ticker.upper()})" if args.ticker else ""
        trade_m = _calc_trade_metrics(trades_df)
        if "error" in trade_m or "info" in trade_m:
            print(f"  {trade_m.get('error') or trade_m.get('info')}")
        else:
            fmt_map = {
                "交易胜率": lambda v: f"{v:.1%}",
                "平均盈利": lambda v: f"${v:+,.2f}",
                "平均亏损": lambda v: f"${v:,.2f}",
                "盈亏比": lambda v: f"{v:.2f}" if v < 1e6 else "∞",
                "总盈亏": lambda v: f"${v:+,.2f}",
                "平均收益率": lambda v: f"{v:+.4%}",
            }
            for k, v in trade_m.items():
                fmt = fmt_map.get(k, lambda x: f"{x}")
                print(f"  {k:<12}{scope} {fmt(v)}")


# ===================================================================
# cmd: slippage  —— 滑点分析（离线）
# ===================================================================

def cmd_slippage(args: argparse.Namespace) -> None:
    """滑点分析：成交价 vs 快照市价."""
    trades_df = _load_all_trade_logs()
    if trades_df.is_empty():
        print("\n无成交记录，无法分析滑点")
        return

    if args.ticker:
        trades_df = trades_df.filter(pl.col("ticker") == args.ticker.upper())

    # 尝试加载持仓快照以获取 reference price
    if not POS_HISTORY.exists():
        print("\n无持仓快照数据。请先运行: python inspect_account.py monitor --once")
        print("将使用成交日志自身数据分析...\n")
        _slippage_from_trades_only(trades_df)
        return

    pos_df = pl.read_parquet(POS_HISTORY)
    if pos_df.is_empty():
        _slippage_from_trades_only(trades_df)
        return

    # 对每笔成交，找最近一次快照中该 ticker 的 market_price 作为 reference
    results = []
    for row in trades_df.iter_rows(named=True):
        ticker = row["ticker"]
        fill_time = row["timestamp"]
        fill_price = row["price"]
        action = row["action"]
        qty = row["quantity"]

        snap = pos_df.filter(
            (pl.col("ticker") == ticker) & (pl.col("timestamp") <= fill_time)
        )
        if snap.is_empty():
            # 没有成交前的快照，尝试用最近的快照
            snap = pos_df.filter(pl.col("ticker") == ticker)

        if snap.is_empty():
            continue

        ref_row = snap.sort("timestamp").tail(1)
        ref_price = ref_row["market_price"][0]

        if ref_price <= 0:
            continue

        # 滑点: 买入时 fill > ref 为负滑点（付出更多），卖出时 fill < ref 为负滑点（收入更少）
        if action == "BUY":
            slip_dollar = fill_price - ref_price
        else:
            slip_dollar = ref_price - fill_price
        slip_bps = (slip_dollar / ref_price) * 10000

        results.append({
            "timestamp": fill_time,
            "ticker": ticker,
            "action": action,
            "qty": qty,
            "fill_price": fill_price,
            "ref_price": ref_price,
            "slippage_$": slip_dollar,
            "slippage_bps": slip_bps,
            "cost_$": slip_dollar * qty,
        })

    if not results:
        print("\n无法匹配成交与快照数据")
        return

    slip_df = pl.DataFrame(results)
    _print_slippage_report(slip_df)


def _slippage_from_trades_only(trades_df: pl.DataFrame) -> None:
    """无快照时，从成交日志自身做往返滑点分析."""
    print(f"{'═'*60}")
    print(f"  往返交易滑点分析（基于成交配对）")
    print(f"{'═'*60}\n")

    queue: dict[str, list[tuple[float, int]]] = {}
    results = []

    for row in trades_df.sort("timestamp").iter_rows(named=True):
        ticker = row["ticker"]
        price = row["price"]
        qty = row["quantity"]
        action = row["action"]

        if action == "BUY":
            queue.setdefault(ticker, []).append((price, qty))
        elif action == "SELL" and queue.get(ticker):
            bp, bq = queue[ticker][0]
            matched = min(qty, bq)
            spread = price - bp
            spread_bps = (spread / bp) * 10000

            results.append({
                "ticker": ticker,
                "buy_price": bp,
                "sell_price": price,
                "spread_$": spread,
                "spread_bps": spread_bps,
                "qty": matched,
            })

            if matched == bq:
                queue[ticker].pop(0)
            else:
                queue[ticker][0] = (bp, bq - matched)

    if not results:
        print("  无完整往返交易可分析")
        return

    rt_df = pl.DataFrame(results)
    print(rt_df)
    avg_spread = rt_df["spread_bps"].mean()
    print(f"\n  平均往返价差: {avg_spread:+.1f} bps")


def _print_slippage_report(slip_df: pl.DataFrame) -> None:
    """打印滑点分析报告."""
    print(f"\n{'═'*60}")
    print(f"  滑点分析报告  ({len(slip_df)} 笔成交)")
    print(f"{'═'*60}\n")

    # 总览
    avg_bps = slip_df["slippage_bps"].mean()
    median_bps = slip_df["slippage_bps"].median()
    total_cost = slip_df["cost_$"].sum()
    std_bps = slip_df["slippage_bps"].std()

    print(f"  总览:")
    print(f"    平均滑点          {avg_bps:+.2f} bps")
    print(f"    中位数滑点        {median_bps:+.2f} bps")
    print(f"    滑点标准差        {std_bps:.2f} bps")
    print(f"    滑点总成本        ${total_cost:+,.2f}")

    # 按方向分
    for action in ("BUY", "SELL"):
        sub = slip_df.filter(pl.col("action") == action)
        if sub.is_empty():
            continue
        label = "买入" if action == "BUY" else "卖出"
        print(f"\n  {label} ({len(sub)} 笔):")
        print(f"    平均滑点          {sub['slippage_bps'].mean():+.2f} bps")
        print(f"    滑点成本          ${sub['cost_$'].sum():+,.2f}")

    # 按 ticker 汇总
    by_ticker = (
        slip_df
        .group_by("ticker")
        .agg(
            pl.col("slippage_bps").mean().alias("avg_bps"),
            pl.col("cost_$").sum().alias("total_cost"),
            pl.len().alias("trades"),
        )
        .sort("avg_bps")
    )

    print(f"\n  按 Ticker 汇总:")
    print(f"  {'Ticker':<8} {'笔数':>6} {'平均滑点 bps':>14} {'滑点成本':>12}")
    print(f"  {'─'*8} {'─'*6} {'─'*14} {'─'*12}")
    for row in by_ticker.iter_rows(named=True):
        print(
            f"  {row['ticker']:<8} {row['trades']:>6} "
            f"{row['avg_bps']:>+13.2f} ${row['total_cost']:>+10,.2f}"
        )

    # 逐笔明细
    print(f"\n  逐笔明细:")
    detail = slip_df.select("timestamp", "ticker", "action", "fill_price", "ref_price", "slippage_bps", "cost_$")
    print(detail)


# ===================================================================
# CLI 入口
# ===================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IB 账户工具 — 持仓 / 摘要 / 挂单 / 日志 / 监控 / 绩效 / 滑点",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # ── positions ──
    pos = sub.add_parser("pos", aliases=["positions", "p"], help="查看当前持仓")
    pos.add_argument("-t", "--ticker", nargs="*", help="只看指定股票，如 -t AAPL GOOG")
    pos.add_argument("-s", "--sort", help="排序字段: ticker / qty / market_value / unrealized_pnl / pnl_pct")
    pos.add_argument("--asc", action="store_true", help="升序排列（默认降序）")
    pos.add_argument("--json", action="store_true", help="额外以 DataFrame 格式输出")

    # ── summary ──
    sm = sub.add_parser("summary", aliases=["s"], help="查看账户摘要")
    sm.add_argument("--all", action="store_true", help="显示所有账户字段")

    # ── orders ──
    sub.add_parser("orders", aliases=["o"], help="查看当前活跃挂单")

    # ── logs ──
    lg = sub.add_parser("logs", aliases=["l"], help="查看成交日志（离线）")
    lg.add_argument("--list", action="store_true", help="列出所有日志文件")
    lg.add_argument("-f", "--file", help="指定日志文件名（模糊匹配）")
    lg.add_argument("-t", "--ticker", help="按股票筛选")
    lg.add_argument("--top", type=int, default=50, help="最多显示行数（默认 50）")

    # ── monitor ──
    mon = sub.add_parser("monitor", aliases=["m"],
                         help="采集账户快照（NLV + 持仓），存储到 account_snapshots/")
    mon.add_argument("--once", action="store_true", help="仅采集一次后退出（默认持续采集）")
    mon.add_argument("-i", "--interval", type=float, default=60.0,
                     help="采集间隔秒数（默认 60）")

    # ── metrics ──
    mt = sub.add_parser("metrics", aliases=["mt"],
                        help="计算账户绩效（波动率/Sharpe/回撤/胜率/盈亏比）")
    mt.add_argument("-d", "--days", type=int, help="只分析最近 N 天")
    mt.add_argument("-t", "--ticker", help="交易级指标只看指定股票")

    # ── slippage ──
    sl = sub.add_parser("slippage", aliases=["sl"],
                        help="滑点分析（成交价 vs 快照市价）")
    sl.add_argument("-t", "--ticker", help="按股票筛选")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    cmd = args.command
    if cmd in ("pos", "positions", "p"):
        asyncio.run(cmd_positions(args))
    elif cmd in ("summary", "s"):
        asyncio.run(cmd_summary(args))
    elif cmd in ("orders", "o"):
        asyncio.run(cmd_orders(args))
    elif cmd in ("logs", "l"):
        cmd_logs(args)
    elif cmd in ("monitor", "m"):
        asyncio.run(cmd_monitor(args))
    elif cmd in ("metrics", "mt"):
        cmd_metrics(args)
    elif cmd in ("slippage", "sl"):
        cmd_slippage(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
