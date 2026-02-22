"""Performance metrics and BacktestResult container."""

from __future__ import annotations

import math
from dataclasses import dataclass

import polars as pl

TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class BacktestResult:
    """Immutable container holding back-test output.

    Can be serialised / handed off to a live execution platform directly.
    """

    equity_curve: pl.DataFrame  # datetime, equity, net_return
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: float
    max_drawdown: float
    total_trades_turnover: float

    def summary(self) -> str:
        return (
            f"AnnReturn={self.annualized_return:+.2%}  "
            f"AnnVol={self.annualized_volatility:.2%}  "
            f"Sharpe={self.sharpe_ratio:.3f}  "
            f"MaxDD={self.max_drawdown:.2%}  "
            f"TotalTurnover={self.total_trades_turnover:.2f}"
        )


def compute_metrics(
    portfolio: pl.DataFrame,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> BacktestResult:
    """Given ``datetime, net_return`` series, produce full performance stats."""
    ts = portfolio.sort("datetime")
    returns = ts["net_return"]

    # equity curve: E_t = E_{t-1} * (1 + r_t),  E_0 = 1
    equity = (1 + returns).cum_prod()
    ec = ts.select("datetime").with_columns(
        equity.alias("equity"),
        returns.alias("net_return"),
    )

    n = len(returns)
    if n < 2:
        return BacktestResult(
            equity_curve=ec,
            annualized_return=0.0,
            annualized_volatility=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0,
            total_trades_turnover=0.0,
        )

    total_return = equity[-1] - 1.0
    ann_factor = periods_per_year / n
    ann_return = (1 + total_return) ** ann_factor - 1

    std = returns.std()
    ann_vol = std * math.sqrt(periods_per_year) if std else 0.0

    sharpe = ann_return / ann_vol if ann_vol > 1e-12 else 0.0

    # max drawdown
    cum_max = equity.cum_max()
    drawdown = (equity - cum_max) / cum_max
    max_dd = drawdown.min()  # most negative value

    total_turnover = ts["turnover"].sum() if "turnover" in ts.columns else 0.0

    return BacktestResult(
        equity_curve=ec,
        annualized_return=ann_return,
        annualized_volatility=ann_vol,
        sharpe_ratio=sharpe,
        max_drawdown=max_dd,
        total_trades_turnover=total_turnover,
    )
