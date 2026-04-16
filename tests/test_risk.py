from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from bot_base import RiskState
from bot_risk import (
    apply_risk_state_on_close,
    calculate_order_size_for_balance,
    close_reason_kind,
    normalize_risk_state,
    openings_blocked_reason,
)
from tests.support import make_config, make_symbol


class RiskTests(unittest.TestCase):
    def test_calculate_order_size_for_balance_respects_margin_cap(self) -> None:
        config = make_config(leverage=2, max_margin_usdt=Decimal("100"), stop_loss_pct=1.5)
        symbol = make_symbol(step_size=Decimal("0.001"), min_qty=Decimal("0.001"))
        order_size = calculate_order_size_for_balance(Decimal("5000"), config, symbol, Decimal("100"))
        self.assertIsNotNone(order_size)
        assert order_size is not None
        self.assertEqual(order_size.notional, Decimal("200"))
        self.assertEqual(order_size.margin, Decimal("100"))
        self.assertEqual(order_size.quantity, Decimal("2"))

    def test_apply_risk_state_on_close_sets_cooldown_and_blocks_after_loss_streak(self) -> None:
        config = make_config(max_daily_loss_usdt=Decimal("20"), max_consecutive_losses=2, symbol_cooldown_minutes_after_stop=60)
        state = RiskState(day="2026-04-16", daily_realized_pnl=Decimal("0"), consecutive_losses=0, cooldowns={})
        now = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)

        apply_risk_state_on_close(config, state, "BTCUSDT", Decimal("-5"), "стоп-лосс LONG: test", now)
        self.assertEqual(state.consecutive_losses, 1)
        self.assertIn("BTCUSDT", state.cooldowns)
        self.assertIsNone(openings_blocked_reason(config, state, now))

        apply_risk_state_on_close(config, state, "ETHUSDT", Decimal("-6"), "стоп-лосс SHORT: test", now)
        self.assertEqual(state.consecutive_losses, 2)
        reason = openings_blocked_reason(config, state, now)
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("MAX_CONSECUTIVE_LOSSES", reason)

    def test_normalize_risk_state_resets_daily_pnl_and_loss_streak_on_new_day(self) -> None:
        state = RiskState(
            day="2026-04-15",
            daily_realized_pnl=Decimal("-11.5"),
            consecutive_losses=4,
            cooldowns={},
        )
        changed = normalize_risk_state(state, datetime(2026, 4, 16, 1, 0, tzinfo=timezone.utc))

        self.assertTrue(changed)
        self.assertEqual(state.day, "2026-04-16")
        self.assertEqual(state.daily_realized_pnl, Decimal("0"))
        self.assertEqual(state.consecutive_losses, 0)

    def test_close_reason_kind_detects_take_profit(self) -> None:
        self.assertEqual(close_reason_kind("тейк-профит LONG: вход=1"), "TAKE_PROFIT")


if __name__ == "__main__":
    unittest.main()
