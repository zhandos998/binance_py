from __future__ import annotations

import unittest
from decimal import Decimal

from bot_base import TradeSignal
from bot_execution import place_protection_orders, protection_prices
from tests.support import make_config, make_symbol


class ExecutionTests(unittest.TestCase):
    def test_protection_prices_for_long_stay_on_correct_sides(self) -> None:
        config = make_config(stop_loss_pct=1.0, take_profit_pct=2.0, protection_trigger_buffer_pct=0.2)
        symbol = make_symbol(tick_size=Decimal("0.1"))

        stop_price, take_price = protection_prices(
            config=config,
            symbol_meta=symbol,
            direction="LONG",
            entry_price=Decimal("100"),
            trigger_reference_price=Decimal("100"),
        )

        self.assertLess(stop_price, Decimal("100"))
        self.assertGreater(take_price, Decimal("100"))

    def test_place_protection_orders_returns_none_in_simulation(self) -> None:
        config = make_config(live_trading=False, place_protection_orders=True)
        symbol = make_symbol()
        signal = TradeSignal(
            symbol="BTCUSDT",
            action="OPEN_LONG",
            side="BUY",
            direction="LONG",
            score=1.0,
            price=Decimal("100"),
            reason="test",
        )

        stop_order_id, take_profit_order_id = place_protection_orders(
            client=object(),  # not used in simulation mode
            config=config,
            symbol_meta=symbol,
            signal=signal,
            entry_price=Decimal("100"),
        )

        self.assertIsNone(stop_order_id)
        self.assertIsNone(take_profit_order_id)


if __name__ == "__main__":
    unittest.main()
