"""Pre-trade risk guardrails – reject or clip dangerous orders."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from execution.target_translator import OrderDelta

logger = logging.getLogger(__name__)


@dataclass
class RiskController:
    """Intercept order deltas and enforce position / leverage / blacklist limits."""

    max_position_pct: float = 0.05       # single trade ≤ 5 % of NLV
    max_gross_leverage: float = 1.0       # Σ|weight| ≤ 1.0
    restricted_list: list[str] = field(default_factory=list)

    def validate(
        self,
        orders: list[OrderDelta],
        nlv: float,
        current_prices: dict[str, float],
    ) -> list[OrderDelta]:
        """Return only orders that pass all risk checks.  Rejected orders are logged."""
        accepted: list[OrderDelta] = []

        # --- leverage check (portfolio-level) ---
        gross_weight = sum(abs(o.target_weight) for o in orders)
        if gross_weight > self.max_gross_leverage:
            logger.error(
                "RISK REJECT (leverage): gross weight %.3f > limit %.3f – dropping ALL orders",
                gross_weight,
                self.max_gross_leverage,
            )
            return []

        for order in orders:
            # --- restricted list ---
            if order.ticker in self.restricted_list:
                logger.warning("RISK REJECT (restricted): %s is blacklisted", order.ticker)
                continue

            # --- single-position size ---
            price = current_prices.get(order.ticker, 0.0)
            trade_dollar = order.quantity * price
            if nlv > 0 and trade_dollar / nlv > self.max_position_pct:
                logger.warning(
                    "RISK REJECT (size): %s trade $%.0f = %.1f%% of NLV (limit %.1f%%)",
                    order.ticker,
                    trade_dollar,
                    trade_dollar / nlv * 100,
                    self.max_position_pct * 100,
                )
                continue

            accepted.append(order)

        logger.info(
            "Risk check: %d / %d orders passed", len(accepted), len(orders),
        )
        return accepted
