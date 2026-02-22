"""Run reversal factor → backtest → evaluation → tearsheet."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from alpha_miner.factors.reversal import ShortTermReversal
from alpha_miner.pipeline import FactorPipeline
from backtester.engine import VectorizedBacktester
from evaluation.tearsheet import evaluate, create_full_tearsheet

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

CACHE_DIR = Path("./data_cache")
BAR_SIZE = "1 day"
SAFE_BAR = BAR_SIZE.replace(" ", "_")


def _discover_tickers() -> list[str]:
    suffix = f"_{SAFE_BAR}.parquet"
    return sorted(
        p.name.removesuffix(suffix)
        for p in CACHE_DIR.glob(f"*{suffix}")
    )


def main() -> None:
    tickers = _discover_tickers()
    logger.info("Discovered %d tickers in %s", len(tickers), CACHE_DIR)

    # 1. Compute reversal factor
    factor = ShortTermReversal(window=5)
    pipeline = FactorPipeline(
        factors=[factor],
        tickers=tickers,
        bar_size=BAR_SIZE,
        cache_dir=CACHE_DIR,
    )
    results = pipeline.run()
    signals = results["ShortTermReversal"]
    logger.info("Signal saved to ./signals/ShortTermReversal.parquet (%d rows)", len(signals))

    # 2. Build prices DataFrame from cache
    frames = []
    for t in tickers:
        path = CACHE_DIR / f"{t}_{SAFE_BAR}.parquet"
        if not path.exists():
            continue
        df = pl.read_parquet(path).select("datetime", "open", "high", "low", "close", "volume")
        df = df.with_columns(pl.lit(t).alias("ticker"))
        frames.append(df)
    prices = pl.concat(frames).sort("datetime", "ticker")

    # 3. Backtest
    bt = VectorizedBacktester(
        delay=1,
        quantiles=5,
        strategy="long_short",
        commission_rate=0.001,
        slippage_rate=0.0005,
    )
    bt_result = bt.run(signals, prices)
    logger.info("Backtest done: %s", bt_result.summary())

    # 4. Evaluation + Tearsheet
    from backtester.aligner import align_data
    aligned = align_data(signals, prices, delay=1)

    eval_results = evaluate(aligned, quantiles=10)
    ic_summary = eval_results["ic_summary"]
    logger.info(
        "IC Mean=%.4f  Rank IC Mean=%.4f  IC IR=%.3f",
        ic_summary["ic_mean"],
        ic_summary["rank_ic_mean"],
        ic_summary["ic_ir"],
    )

    out = create_full_tearsheet(
        eval_results,
        output_path="output/reversal_tearsheet.png",
        backtest_result=bt_result,
    )
    logger.info("Tearsheet saved → %s", out)


if __name__ == "__main__":
    main()
