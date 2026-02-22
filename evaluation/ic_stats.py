"""Information Coefficient (IC) computation engine."""

from __future__ import annotations

import polars as pl
from scipy import stats

TRADING_DAYS_PER_YEAR = 252


def _pearson_ic(factor: list[float], ret: list[float]) -> float | None:
    if len(factor) < 3:
        return None
    r, _ = stats.pearsonr(factor, ret)
    return float(r)


def _spearman_ic(factor: list[float], ret: list[float]) -> float | None:
    if len(factor) < 3:
        return None
    r, _ = stats.spearmanr(factor, ret)
    return float(r)


def compute_ic(df: pl.DataFrame) -> pl.DataFrame:
    """Compute daily Normal IC (Pearson) and Rank IC (Spearman).

    Parameters
    ----------
    df : DataFrame with ``datetime``, ``factor_value``, ``forward_return``.

    Returns
    -------
    DataFrame with columns: ``datetime``, ``ic``, ``rank_ic``, ``n_stocks``.
    """
    groups = df.group_by("datetime", maintain_order=True)

    rows: list[dict] = []
    for dt, group in groups:
        fv = group["factor_value"].to_list()
        fr = group["forward_return"].to_list()
        ic = _pearson_ic(fv, fr)
        ric = _spearman_ic(fv, fr)
        rows.append({
            "datetime": dt[0] if isinstance(dt, tuple) else dt,
            "ic": ic,
            "rank_ic": ric,
            "n_stocks": len(fv),
        })

    return pl.DataFrame(rows).sort("datetime")


def compute_ic_summary(ic_df: pl.DataFrame) -> dict[str, float]:
    """Aggregate IC statistics from the output of ``compute_ic``.

    Returns
    -------
    dict with keys: ``ic_mean``, ``ic_std``, ``rank_ic_mean``, ``rank_ic_std``,
    ``ic_ir``, ``rank_ic_ir``, ``ic_positive_pct``, ``rank_ic_positive_pct``,
    ``ic_mean_ann`` (annualized IC mean).
    """
    ic = ic_df.drop_nulls("ic")
    ric = ic_df.drop_nulls("rank_ic")

    ic_mean = ic["ic"].mean() or 0.0
    ic_std = ic["ic"].std() or 0.0
    ric_mean = ric["rank_ic"].mean() or 0.0
    ric_std = ric["rank_ic"].std() or 0.0

    return {
        "ic_mean": ic_mean,
        "ic_std": ic_std,
        "rank_ic_mean": ric_mean,
        "rank_ic_std": ric_std,
        "ic_ir": ic_mean / ic_std if ic_std > 1e-12 else 0.0,
        "rank_ic_ir": ric_mean / ric_std if ric_std > 1e-12 else 0.0,
        "ic_positive_pct": (ic["ic"] > 0).mean() if len(ic) else 0.0,
        "rank_ic_positive_pct": (ric["rank_ic"] > 0).mean() if len(ric) else 0.0,
        "ic_mean_ann": ic_mean * TRADING_DAYS_PER_YEAR,
    }
