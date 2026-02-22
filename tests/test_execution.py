"""Tests for execution engine (offline components – no IB connection needed)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from execution.target_translator import OrderDelta, calculate_order_delta
from execution.risk_manager import RiskController


# ── fixtures ──────────────────────────────────────────────────────────


def _make_weights(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema=["datetime", "ticker", "factor_value", "close", "forward_return", "target_weight"],
        orient="row",
    )


SAMPLE_WEIGHTS = _make_weights([
    (date(2026, 2, 20), "AAPL", 0.5, 180.0, 0.01, 0.25),
    (date(2026, 2, 20), "MSFT", -0.3, 400.0, -0.02, -0.25),
    (date(2026, 2, 20), "GOOG", 0.1, 160.0, 0.005, 0.0),
    (date(2026, 2, 21), "AAPL", 0.6, 182.0, 0.01, 0.30),
    (date(2026, 2, 21), "MSFT", -0.2, 405.0, -0.01, -0.30),
    (date(2026, 2, 21), "GOOG", 0.0, 162.0, 0.003, 0.0),
])

PRICES = {"AAPL": 182.0, "MSFT": 405.0, "GOOG": 162.0}
NLV = 100_000.0


# ── target_translator tests ──────────────────────────────────────────


class TestCalculateOrderDelta:
    def test_uses_latest_datetime(self):
        orders = calculate_order_delta(SAMPLE_WEIGHTS, {}, NLV, PRICES)
        # latest date is 2026-02-21, target weights: AAPL +0.30, MSFT -0.30
        tickers = {o.ticker for o in orders}
        assert "AAPL" in tickers
        assert "MSFT" in tickers
        assert "GOOG" not in tickers  # weight == 0 → no delta

    def test_buy_from_empty_portfolio(self):
        orders = calculate_order_delta(SAMPLE_WEIGHTS, {}, NLV, PRICES)
        aapl = next(o for o in orders if o.ticker == "AAPL")
        assert aapl.action == "BUY"
        # 0.30 * 100_000 / 182 ≈ 164
        assert aapl.quantity == 164

    def test_sell_order(self):
        orders = calculate_order_delta(SAMPLE_WEIGHTS, {}, NLV, PRICES)
        msft = next(o for o in orders if o.ticker == "MSFT")
        assert msft.action == "SELL"
        # -0.30 * 100_000 / 405 ≈ -74 → sell 74
        assert msft.quantity == 74

    def test_no_delta_when_already_at_target(self):
        existing = {"AAPL": 164, "MSFT": -74}
        orders = calculate_order_delta(SAMPLE_WEIGHTS, existing, NLV, PRICES)
        tickers = {o.ticker for o in orders}
        assert "AAPL" not in tickers
        assert "MSFT" not in tickers

    def test_skips_missing_price(self):
        orders = calculate_order_delta(SAMPLE_WEIGHTS, {}, NLV, {"AAPL": 182.0})
        tickers = {o.ticker for o in orders}
        assert "MSFT" not in tickers


# ── risk_manager tests ────────────────────────────────────────────────


class TestRiskController:
    def _sample_orders(self) -> list[OrderDelta]:
        return [
            OrderDelta("AAPL", "BUY", 164, 0.30, 164, 0),
            OrderDelta("MSFT", "SELL", 74, -0.30, -74, 0),
        ]

    def test_passes_within_limits(self):
        rc = RiskController(max_position_pct=0.50, max_gross_leverage=2.0)
        accepted = rc.validate(self._sample_orders(), NLV, PRICES)
        assert len(accepted) == 2

    def test_rejects_on_leverage(self):
        rc = RiskController(max_gross_leverage=0.10)
        accepted = rc.validate(self._sample_orders(), NLV, PRICES)
        assert accepted == []

    def test_rejects_restricted_ticker(self):
        rc = RiskController(max_position_pct=1.0, max_gross_leverage=2.0, restricted_list=["AAPL"])
        accepted = rc.validate(self._sample_orders(), NLV, PRICES)
        assert len(accepted) == 1
        assert accepted[0].ticker == "MSFT"

    def test_rejects_oversized_position(self):
        rc = RiskController(max_position_pct=0.01, max_gross_leverage=2.0)
        accepted = rc.validate(self._sample_orders(), NLV, PRICES)
        assert len(accepted) == 0
