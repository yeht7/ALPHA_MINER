"""Signal orchestrator – run a list of factors over a universe of tickers."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from alpha_miner.base import BaseFactor
from alpha_miner.feeder import load_data

logger = logging.getLogger(__name__)


class FactorPipeline:
    """Load data once, fan out to every registered factor, persist results."""

    def __init__(
        self,
        factors: list[BaseFactor],
        tickers: list[str],
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        bar_size: str = "1 day",
        cache_dir: Path | None = None,
    ) -> None:
        self.factors = factors
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.bar_size = bar_size
        self.cache_dir = cache_dir

    def run(self) -> dict[str, pl.DataFrame]:
        """Execute all factors and return ``{factor_name: DataFrame}``."""
        data = load_data(
            self.tickers,
            start_date=self.start_date,
            end_date=self.end_date,
            bar_size=self.bar_size,
            cache_dir=self.cache_dir,
        )
        logger.info("Loaded %d rows for %d tickers", len(data), len(self.tickers))

        results: dict[str, pl.DataFrame] = {}
        for factor in self.factors:
            name = type(factor).__name__
            logger.info("Computing factor: %s", name)
            signal = factor.compute(data)
            factor.save_signal(signal, name)
            results[name] = signal
            logger.info("Saved %s – %d rows", name, len(signal))

        return results
