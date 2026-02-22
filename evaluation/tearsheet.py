"""Tear Sheet generator – assembles all evaluation charts into a single dashboard."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import polars as pl

from evaluation.bucket_stats import (
    compute_bucket_avg_return,
    compute_bucket_avg_return_demean,
    compute_bucket_returns,
    compute_cumulative_bucket_returns,
    compute_ls_spread,
)
from evaluation.data_prep import join_sector
from evaluation.ic_stats import compute_ic, compute_ic_summary
from evaluation.plotting import EvaluationPlotter

_plotter = EvaluationPlotter()


def _bucket_with_sector(df: pl.DataFrame, quantiles: int) -> pl.DataFrame:
    """Return *df* with bucket assignments (needed for sector exposure chart)."""
    from evaluation.bucket_stats import _assign_buckets

    return _assign_buckets(df, quantiles, ["datetime"])


def evaluate(
    aligned: pl.DataFrame,
    quantiles: int = 5,
    sector_map: dict[str, str] | pl.DataFrame | None = None,
) -> dict[str, object]:
    """Run all statistical computations, return a results dict.

    Keys
    ----
    ``ic_df``, ``ic_summary``, ``bucket_returns``, ``bucket_returns_neutral``,
    ``cum_bucket``, ``ls_spread``, ``bucketed_with_sector``.
    """
    df = join_sector(aligned, sector_map)

    ic_df = compute_ic(df)
    ic_summary = compute_ic_summary(ic_df)

    br = compute_bucket_returns(df, quantiles=quantiles, by_sector=False)
    br_neutral = compute_bucket_returns(df, quantiles=quantiles, by_sector=True)
    cum_br = compute_cumulative_bucket_returns(br)
    ls = compute_ls_spread(br, quantiles=quantiles)

    bucketed = _bucket_with_sector(df, quantiles)

    bucket_avg = compute_bucket_avg_return(df, quantiles=quantiles)
    bucket_avg_dm = compute_bucket_avg_return_demean(df, quantiles=quantiles)

    return {
        "ic_df": ic_df,
        "ic_summary": ic_summary,
        "bucket_returns": br,
        "bucket_returns_neutral": br_neutral,
        "cum_bucket": cum_br,
        "ls_spread": ls,
        "bucketed_with_sector": bucketed,
        "bucket_avg": bucket_avg,
        "bucket_avg_demean": bucket_avg_dm,
        "quantiles": quantiles,
    }


def _draw_metrics_table(ax: plt.Axes, ic_summary: dict, backtest_result=None) -> None:
    """Render key metrics as a text table on the given Axes."""
    ax.axis("off")
    lines: list[str] = ["─── Key Metrics ───"]

    lines.append(f"IC Mean:          {ic_summary['ic_mean']:+.4f}")
    lines.append(f"Rank IC Mean:     {ic_summary['rank_ic_mean']:+.4f}")
    lines.append(f"IC IR:            {ic_summary['ic_ir']:+.3f}")
    lines.append(f"Rank IC IR:       {ic_summary['rank_ic_ir']:+.3f}")
    lines.append(f"IC > 0:           {ic_summary['ic_positive_pct']:.1%}")
    lines.append(f"Rank IC > 0:      {ic_summary['rank_ic_positive_pct']:.1%}")

    if backtest_result is not None:
        lines.append("")
        lines.append(f"Ann. Return:      {backtest_result.annualized_return:+.2%}")
        lines.append(f"Sharpe Ratio:     {backtest_result.sharpe_ratio:.3f}")
        lines.append(f"Max Drawdown:     {backtest_result.max_drawdown:.2%}")
        lines.append(f"Avg Turnover:     {backtest_result.total_trades_turnover:.2f}")

    text = "\n".join(lines)
    ax.text(
        0.05, 0.95, text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment="top",
        fontfamily="monospace",
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "#f7f7f7", "edgecolor": "#cccccc"},
    )


def create_full_tearsheet(
    results_dict: dict[str, object],
    output_path: str,
    backtest_result=None,
    *,
    dpi: int = 150,
) -> Path:
    """Compose a multi-panel tear sheet and save to *output_path* (png/pdf).

    Parameters
    ----------
    results_dict : Output of ``evaluate()``.
    output_path : File path (extension determines format: .png or .pdf).
    backtest_result : Optional ``BacktestResult`` to display Sharpe / MaxDD.
    dpi : Resolution for raster output.

    Returns
    -------
    Resolved ``Path`` of the saved file.
    """
    ic_df = results_dict["ic_df"]
    ic_summary = results_dict["ic_summary"]
    cum_bucket = results_dict["cum_bucket"]
    bucketed = results_dict["bucketed_with_sector"]
    bucket_avg = results_dict["bucket_avg"]
    bucket_avg_dm = results_dict["bucket_avg_demean"]
    quantiles = results_dict["quantiles"]

    fig = plt.figure(figsize=(16, 18), constrained_layout=False)
    gs = gridspec.GridSpec(4, 2, figure=fig, hspace=0.45, wspace=0.30,
                           height_ratios=[1, 1, 1, 1])

    # Row 0, col 0-1: IC time-series (full width)
    ax_ic = fig.add_subplot(gs[0, :])
    _plotter.plot_ic_timeseries(ic_df, ax=ax_ic)

    # Row 1, col 0: Bucket cumulative returns
    ax_bucket = fig.add_subplot(gs[1, 0])
    _plotter.plot_bucket_returns(cum_bucket, quantiles=quantiles, ax=ax_bucket)

    # Row 1, col 1: Metrics table
    ax_metrics = fig.add_subplot(gs[1, 1])
    _draw_metrics_table(ax_metrics, ic_summary, backtest_result)

    # Row 2: Bucket avg return (raw) and demeaned
    ax_avg_raw = fig.add_subplot(gs[2, 0])
    ax_avg_dm = fig.add_subplot(gs[2, 1])
    _plotter.plot_bucket_avg_return(bucket_avg, bucket_avg_dm, axes=(ax_avg_raw, ax_avg_dm))

    # Row 3, col 0-1: Sector exposure (full width)
    ax_sector = fig.add_subplot(gs[3, :])
    _plotter.plot_sector_exposure(bucketed, quantiles=quantiles, ax=ax_sector)

    fig.suptitle("Factor Evaluation Tear Sheet", fontsize=14, fontweight="bold", y=0.98)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(out), dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return out.resolve()
