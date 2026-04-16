from __future__ import annotations

import unittest
from decimal import Decimal

from bot_math import normalize_stop_price, round_step


class MathTests(unittest.TestCase):
    def test_round_step_rounds_down(self) -> None:
        self.assertEqual(round_step(Decimal("1.2345"), Decimal("0.01")), Decimal("1.23"))

    def test_normalize_stop_price_moves_invalid_long_take_above_entry(self) -> None:
        price = normalize_stop_price(
            value=Decimal("99.9"),
            tick_size=Decimal("0.1"),
            entry_price=Decimal("100"),
            direction="LONG",
            is_stop_loss=False,
        )
        self.assertEqual(price, Decimal("100.1"))


if __name__ == "__main__":
    unittest.main()
