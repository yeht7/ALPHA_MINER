"""Abstract base class for all alpha factors."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

SIGNAL_DIR = Path(os.getenv("SIGNAL_DIR", "./signals"))
OUTPUT_SCHEMA = ["datetime", "ticker", "factor_value"]


class BaseFactor(ABC):
    """Every factor must produce a DataFrame with exactly
    ``['datetime', 'ticker', 'factor_value']`` columns.

    The *compute* interface is intentionally kept minimal so that a subclass
    can later delegate heavy work to a native (C++ / CUDA) kernel while
    keeping the same Python API.
    """

    @abstractmethod
    def compute(self, data: pl.DataFrame) -> pl.DataFrame:
        """Return a DataFrame with columns ``OUTPUT_SCHEMA``.

        Implementations MUST be purely vectorized (no Python row-loops).
        """

    def save_signal(self, df: pl.DataFrame, factor_name: str) -> Path:
        """Validate schema, then persist to ``SIGNAL_DIR/<factor_name>.parquet``."""
        assert list(df.columns) == OUTPUT_SCHEMA, (
            f"Expected columns {OUTPUT_SCHEMA}, got {df.columns}"
        )
        SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
        path = SIGNAL_DIR / f"{factor_name}.parquet"
        df.write_parquet(path)
        return path
