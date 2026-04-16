from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from bot_base import Position
from bot_state import load_positions, save_positions
from tests.support import make_config


class StateTests(unittest.TestCase):
    def test_save_and_load_positions_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config = make_config(
                database_file=temp_path / "state.sqlite3",
                positions_file=temp_path / "positions.json",
            )
            positions = {
                "BTCUSDT": Position(
                    symbol="BTCUSDT",
                    direction="LONG",
                    entry_price=Decimal("64000"),
                    quantity=Decimal("0.01"),
                    margin_used=Decimal("320"),
                    leverage=2,
                    opened_at="2026-04-16T10:00:00+05:00",
                    entry_reference_price=Decimal("63990"),
                    entry_commission_usdt=Decimal("0.12"),
                    entry_slippage_usdt=Decimal("0.05"),
                    stop_order_id="sl_1",
                    take_profit_order_id="tp_1",
                )
            }

            save_positions(config, positions)
            restored = load_positions(config)

            self.assertIn("BTCUSDT", restored)
            self.assertEqual(restored["BTCUSDT"].entry_price, Decimal("64000"))
            self.assertEqual(restored["BTCUSDT"].entry_commission_usdt, Decimal("0.12"))
            self.assertEqual(restored["BTCUSDT"].stop_order_id, "sl_1")


if __name__ == "__main__":
    unittest.main()
