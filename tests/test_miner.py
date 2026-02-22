"""Unit tests for alpha_miner – output contract & zero look-ahead bias."""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import pytest

from alpha_miner.base import OUTPUT_SCHEMA, BaseFactor, SIGNAL_DIR
from alpha_miner.factors.microstructure import VolumePriceTrend
from alpha_miner.factors.trend import IntradayVWAPDeviation
from alpha_miner.feeder import load_data
from alpha_miner.pipeline import FactorPipeline

TMP_CACHE = Path("./test_miner_cache")
TMP_SIGNAL = Path("./test_miner_signals")


def _synthetic_data(n_bars: int = 60, tickers: tuple[str, ...] = ("AAA", "BBB")) -> pl.DataFrame:
    """Deterministic OHLCV data: close = row_index per ticker, volume = 100."""
    rows: list[dict] = []
    for t in tickers:
        for i in range(n_bars):
            rows.append({
                "datetime": f"2025-01-{1 + i // 24:02d}T{i % 24:02d}:00:00",
                "ticker": t,
                "open": float(i),
                "high": float(i + 1),
                "low": float(max(i - 1, 0)),
                "close": float(i) + 0.5,
                "volume": 100.0,
            })
    return pl.DataFrame(rows).sort("datetime", "ticker")


@pytest.fixture(autouse=True)
def _tmp_dirs(monkeypatch):
    import alpha_miner.base as base_mod
    monkeypatch.setattr(base_mod, "SIGNAL_DIR", TMP_SIGNAL)
    TMP_CACHE.mkdir(exist_ok=True)
    TMP_SIGNAL.mkdir(exist_ok=True)
    yield
    shutil.rmtree(TMP_CACHE, ignore_errors=True)
    shutil.rmtree(TMP_SIGNAL, ignore_errors=True)


class TestOutputContract:
    """Every factor must emit exactly ['datetime', 'ticker', 'factor_value']."""

    @pytest.mark.parametrize("factor", [IntradayVWAPDeviation(window=5), VolumePriceTrend(span=5)])
    def test_columns_match_schema(self, factor: BaseFactor):
        df = factor.compute(_synthetic_data())
        assert list(df.columns) == OUTPUT_SCHEMA

    @pytest.mark.parametrize("factor", [IntradayVWAPDeviation(window=5), VolumePriceTrend(span=5)])
    def test_save_signal_creates_parquet(self, factor: BaseFactor):
        signal = factor.compute(_synthetic_data())
        path = factor.save_signal(signal, type(factor).__name__)
        assert path.exists()
        loaded = pl.read_parquet(path)
        assert list(loaded.columns) == OUTPUT_SCHEMA


class TestZeroLookaheadBias:
    """Factor value at time T must depend only on data where t <= T.

    Strategy: compute once on full data, then compute on data truncated at T.
    The value at T must be identical in both runs.
    """

    def _check_no_lookahead(self, factor: BaseFactor, data: pl.DataFrame):
        full_result = factor.compute(data)
        timestamps = data["datetime"].unique().sort().to_list()
        # pick a timestamp in the middle
        t_mid = timestamps[len(timestamps) // 2]

        truncated = data.filter(pl.col("datetime") <= t_mid)
        partial_result = factor.compute(truncated)

        full_at_t = full_result.filter(pl.col("datetime") == t_mid).sort("ticker")
        part_at_t = partial_result.filter(pl.col("datetime") == t_mid).sort("ticker")

        assert full_at_t.shape == part_at_t.shape, "Row count mismatch at truncation point"
        for col in OUTPUT_SCHEMA:
            if col == "factor_value":
                f_vals = full_at_t[col].to_list()
                p_vals = part_at_t[col].to_list()
                for fv, pv in zip(f_vals, p_vals):
                    if fv is None or pv is None:
                        assert fv is None and pv is None
                    else:
                        assert abs(fv - pv) < 1e-9, f"Look-ahead detected: {fv} != {pv}"
            else:
                assert full_at_t[col].to_list() == part_at_t[col].to_list()

    def test_vwap_no_lookahead(self):
        self._check_no_lookahead(IntradayVWAPDeviation(window=5), _synthetic_data())

    def test_vpt_no_lookahead(self):
        self._check_no_lookahead(VolumePriceTrend(span=5), _synthetic_data())


class TestFeeder:
    def test_load_from_cache_dir(self):
        data = _synthetic_data(n_bars=10, tickers=("XX",))
        ticker_df = data.drop("ticker")
        ticker_df.write_parquet(TMP_CACHE / "XX_1_day.parquet")

        loaded = load_data(["XX"], cache_dir=TMP_CACHE)
        assert "ticker" in loaded.columns
        assert loaded["ticker"].unique().to_list() == ["XX"]
        assert len(loaded) == 10

    def test_missing_ticker_returns_empty(self):
        loaded = load_data(["NONEXIST"], cache_dir=TMP_CACHE)
        assert len(loaded) == 0


class TestPipeline:
    def test_end_to_end(self):
        data = _synthetic_data(n_bars=30, tickers=("PP",))
        ticker_df = data.drop("ticker")
        ticker_df.write_parquet(TMP_CACHE / "PP_1_day.parquet")

        pipeline = FactorPipeline(
            factors=[IntradayVWAPDeviation(window=5), VolumePriceTrend(span=5)],
            tickers=["PP"],
            cache_dir=TMP_CACHE,
        )
        results = pipeline.run()
        assert len(results) == 2
        for name, signal in results.items():
            assert list(signal.columns) == OUTPUT_SCHEMA
            assert (TMP_SIGNAL / f"{name}.parquet").exists()
