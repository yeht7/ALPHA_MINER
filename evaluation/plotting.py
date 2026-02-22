"""Plotting engine – decoupled chart renderers for the Tear Sheet."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

sns.set_theme(style="whitegrid", palette="muted", font_scale=0.9)


class EvaluationPlotter:
    """Stateless plotter: each method takes data and returns a ``Figure``."""

    # ------------------------------------------------------------------
    # IC time-series
    # ------------------------------------------------------------------
    @staticmethod
    def plot_ic_timeseries(
        ic_df: pl.DataFrame,
        rolling_window: int = 20,
        *,
        ax: plt.Axes | None = None,
    ) -> plt.Figure:
        """Bar chart of daily Rank IC with rolling MA overlay.

        Parameters
        ----------
        ic_df : Output of ``compute_ic`` with ``datetime`` and ``rank_ic``.
        rolling_window : Window for moving average line.
        ax : Optional pre-existing Axes to draw on.
        """
        data = ic_df.drop_nulls("rank_ic").sort("datetime")
        dates = data["datetime"].to_list()
        ric = data["rank_ic"].to_numpy()

        fig = None
        if ax is None:
            fig, ax = plt.subplots(figsize=(12, 4))
        else:
            fig = ax.get_figure()

        colors = np.where(ric >= 0, "#2ecc71", "#e74c3c")
        ax.bar(range(len(ric)), ric, color=colors, width=1.0, alpha=0.6, label="Daily Rank IC")

        if len(ric) >= rolling_window:
            ma = np.convolve(ric, np.ones(rolling_window) / rolling_window, mode="valid")
            offset = rolling_window - 1
            ax.plot(range(offset, offset + len(ma)), ma, color="#2c3e50", linewidth=1.5,
                    label=f"{rolling_window}d MA")

        ax.axhline(0, color="black", linewidth=0.5)
        ax.set_title("Daily Rank IC")
        ax.set_ylabel("Rank IC")
        _set_date_ticks(ax, dates)
        ax.legend(loc="upper right", fontsize=8)
        return fig

    # ------------------------------------------------------------------
    # Bucket cumulative returns
    # ------------------------------------------------------------------
    @staticmethod
    def plot_bucket_returns(
        cum_bucket: pl.DataFrame,
        quantiles: int = 5,
        *,
        ax: plt.Axes | None = None,
    ) -> plt.Figure:
        """Cumulative return lines for each quantile bucket.

        Parameters
        ----------
        cum_bucket : Output of ``compute_cumulative_bucket_returns`` with
            ``datetime``, ``bucket``, ``cumulative_return``.
        """
        fig = None
        if ax is None:
            fig, ax = plt.subplots(figsize=(12, 4))
        else:
            fig = ax.get_figure()

        cmap = plt.colormaps.get_cmap("RdYlGn").resampled(quantiles)
        for b in range(1, quantiles + 1):
            subset = cum_bucket.filter(pl.col("bucket") == b).sort("datetime")
            if subset.is_empty():
                continue
            label = f"Q{b}" + (" (Short)" if b == 1 else " (Long)" if b == quantiles else "")
            ax.plot(
                range(len(subset)),
                subset["cumulative_return"].to_numpy(),
                color=cmap(b - 1),
                linewidth=1.5 if b in (1, quantiles) else 0.8,
                label=label,
            )

        dates = cum_bucket.filter(pl.col("bucket") == 1).sort("datetime")["datetime"].to_list()
        ax.set_title("Cumulative Bucket Returns")
        ax.set_ylabel("Cumulative Return")
        _set_date_ticks(ax, dates)
        ax.legend(loc="upper left", fontsize=8)
        return fig

    # ------------------------------------------------------------------
    # Bucket average return histogram
    # ------------------------------------------------------------------
    @staticmethod
    def plot_bucket_avg_return(
        avg_df: pl.DataFrame,
        avg_demean_df: pl.DataFrame,
        *,
        axes: tuple[plt.Axes, plt.Axes] | None = None,
    ) -> plt.Figure:
        """Side-by-side bar charts: raw avg return and demeaned avg return per bucket.

        Parameters
        ----------
        avg_df : Output of ``compute_bucket_avg_return`` (``bucket``, ``avg_return``).
        avg_demean_df : Output of ``compute_bucket_avg_return_demean``
            (``bucket``, ``avg_return_demean``).
        axes : Optional pair of Axes to draw on.
        """
        fig = None
        if axes is None:
            fig, (ax_raw, ax_dm) = plt.subplots(1, 2, figsize=(12, 4))
        else:
            ax_raw, ax_dm = axes
            fig = ax_raw.get_figure()

        buckets_raw = avg_df.sort("bucket")["bucket"].to_list()
        vals_raw = avg_df.sort("bucket")["avg_return"].to_numpy()
        buckets_dm = avg_demean_df.sort("bucket")["bucket"].to_list()
        vals_dm = avg_demean_df.sort("bucket")["avg_return_demean"].to_numpy()

        n = len(buckets_raw)
        cmap = plt.colormaps.get_cmap("RdYlGn").resampled(n)
        colors = [cmap(i) for i in range(n)]

        # Raw
        bars = ax_raw.bar([f"Q{b}" for b in buckets_raw], vals_raw, color=colors, edgecolor="white")
        for bar, v in zip(bars, vals_raw):
            ax_raw.text(bar.get_x() + bar.get_width() / 2, v,
                        f"{v:+.4f}", ha="center", va="bottom" if v >= 0 else "top", fontsize=8)
        ax_raw.axhline(0, color="black", linewidth=0.5)
        ax_raw.set_title("Avg Forward Return by Bucket")
        ax_raw.set_ylabel("Mean Return")

        # Demeaned
        bars = ax_dm.bar([f"Q{b}" for b in buckets_dm], vals_dm, color=colors, edgecolor="white")
        for bar, v in zip(bars, vals_dm):
            ax_dm.text(bar.get_x() + bar.get_width() / 2, v,
                       f"{v:+.4f}", ha="center", va="bottom" if v >= 0 else "top", fontsize=8)
        ax_dm.axhline(0, color="black", linewidth=0.5)
        ax_dm.set_title("Avg Forward Return by Bucket (Demeaned)")
        ax_dm.set_ylabel("Mean Demeaned Return")

        return fig

    # ------------------------------------------------------------------
    # Sector exposure of top quantile
    # ------------------------------------------------------------------
    @staticmethod
    def plot_sector_exposure(
        df: pl.DataFrame,
        quantiles: int = 5,
        *,
        ax: plt.Axes | None = None,
    ) -> plt.Figure:
        """Stacked area chart of sector composition in the top quantile.

        Parameters
        ----------
        df : Full bucketed DataFrame with ``datetime``, ``bucket``, ``gics_sector``.
        """
        fig = None
        if ax is None:
            fig, ax = plt.subplots(figsize=(12, 4))
        else:
            fig = ax.get_figure()

        top = df.filter(pl.col("bucket") == quantiles)
        if top.is_empty():
            ax.set_title("Sector Exposure (Top Quantile) — no data")
            return fig

        counts = (
            top.group_by("datetime", "gics_sector")
            .len()
            .sort("datetime")
        )
        pivot = counts.pivot(on="gics_sector", index="datetime", values="len").fill_null(0)

        sector_cols = [c for c in pivot.columns if c != "datetime"]
        totals = pivot.select(sector_cols).sum_horizontal()
        pct = pivot.select(
            "datetime",
            *[(pl.col(c) / totals).alias(c) for c in sector_cols],
        )

        dates = pct["datetime"].to_list()
        x = np.arange(len(dates))
        bottom = np.zeros(len(dates))

        palette = sns.color_palette("husl", len(sector_cols))
        for i, sec in enumerate(sorted(sector_cols)):
            vals = pct[sec].to_numpy().astype(float)
            ax.fill_between(x, bottom, bottom + vals, label=sec, alpha=0.8, color=palette[i])
            bottom += vals

        ax.set_title("Sector Exposure — Top Quantile (Long Leg)")
        ax.set_ylabel("Weight %")
        ax.set_ylim(0, 1)
        _set_date_ticks(ax, dates)
        ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=7)
        return fig


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _set_date_ticks(ax: plt.Axes, dates: list[Any], max_ticks: int = 12) -> None:
    """Set readable x-axis date tick labels."""
    n = len(dates)
    if n == 0:
        return
    step = max(1, n // max_ticks)
    positions = list(range(0, n, step))
    labels = [str(dates[i])[:10] for i in positions]
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)
