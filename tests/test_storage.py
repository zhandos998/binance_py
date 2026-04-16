from __future__ import annotations

import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from bot_base import Position, RiskState
from bot_storage import (
    append_trade_row_to_storage,
    load_positions_from_storage,
    load_risk_state_from_storage,
    load_trade_rows_from_storage,
    save_risk_state_to_storage,
)
from tests.support import make_config


class StorageTests(unittest.TestCase):
    def test_risk_state_roundtrip_uses_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config = make_config(database_file=temp_path / "bot.sqlite3")
            state = RiskState(
                day="2026-04-16",
                daily_realized_pnl=Decimal("-3.25"),
                consecutive_losses=2,
                cooldowns={"BTCUSDT": "2026-04-16T15:00:00+05:00"},
            )

            save_risk_state_to_storage(config, state)
            restored = load_risk_state_from_storage(config)

            self.assertIsNotNone(restored)
            assert restored is not None
            self.assertEqual(restored.daily_realized_pnl, Decimal("-3.25"))
            self.assertEqual(restored.consecutive_losses, 2)
            self.assertEqual(restored.cooldowns["BTCUSDT"], "2026-04-16T15:00:00+05:00")

    def test_trade_rows_roundtrip_uses_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config = make_config(database_file=temp_path / "bot.sqlite3")

            append_trade_row_to_storage(
                config,
                {
                    "timestamp": "2026-04-16T10:00:00+05:00",
                    "mode": "test",
                    "market": "USDT-M Futures",
                    "symbol": "BTCUSDT",
                    "action": "OPEN_LONG",
                    "side": "BUY",
                    "direction": "LONG",
                    "leverage": "2",
                    "price": "64000",
                    "quantity": "0.01",
                    "notional_usdt": "640",
                    "margin_usdt": "320",
                    "risk_at_stop_usdt": "9.60",
                    "reason": "test open",
                    "status": "SIMULATED",
                },
            )

            rows = load_trade_rows_from_storage(config)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "BTCUSDT")
            self.assertEqual(rows[0]["action"], "OPEN_LONG")
            self.assertEqual(rows[0]["status"], "SIMULATED")

    def test_legacy_files_are_migrated_once_into_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            positions_file = temp_path / "positions.json"
            risk_state_file = temp_path / "risk_state.json"
            trade_log_file = temp_path / "trades.csv"
            database_file = temp_path / "bot.sqlite3"

            positions_file.write_text(
                json.dumps(
                    {
                        "ETHUSDT": {
                            "direction": "SHORT",
                            "entry_price": "3200",
                            "quantity": "0.5",
                            "margin_used": "800",
                            "leverage": 2,
                            "opened_at": "2026-04-16T11:00:00+05:00",
                            "entry_reference_price": "3198",
                            "entry_commission_usdt": "0.4",
                            "entry_slippage_usdt": "0.1",
                            "stop_order_id": "sl_2",
                            "take_profit_order_id": "tp_2",
                        }
                    }
                ),
                encoding="utf-8",
            )
            risk_state_file.write_text(
                json.dumps(
                    {
                        "day": "2026-04-16",
                        "daily_realized_pnl": "-1.5",
                        "consecutive_losses": 1,
                        "cooldowns": {"ETHUSDT": "2026-04-16T14:00:00+05:00"},
                    }
                ),
                encoding="utf-8",
            )
            with trade_log_file.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=["timestamp", "symbol", "action", "side", "direction", "price", "quantity", "notional_usdt", "margin_usdt", "risk_at_stop_usdt", "reason", "status"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "timestamp": "2026-04-16T10:30:00+05:00",
                        "symbol": "ETHUSDT",
                        "action": "OPEN_SHORT",
                        "side": "SELL",
                        "direction": "SHORT",
                        "price": "3200",
                        "quantity": "0.5",
                        "notional_usdt": "1600",
                        "margin_usdt": "800",
                        "risk_at_stop_usdt": "24",
                        "reason": "legacy row",
                        "status": "SIMULATED",
                    }
                )

            config = make_config(
                database_file=database_file,
                positions_file=positions_file,
                risk_state_file=risk_state_file,
                trade_log_file=trade_log_file,
            )

            positions = load_positions_from_storage(config)
            risk_state = load_risk_state_from_storage(config)
            trades_first = load_trade_rows_from_storage(config)
            trades_second = load_trade_rows_from_storage(config)

            self.assertIn("ETHUSDT", positions)
            self.assertEqual(positions["ETHUSDT"], Position(
                symbol="ETHUSDT",
                direction="SHORT",
                entry_price=Decimal("3200"),
                quantity=Decimal("0.5"),
                margin_used=Decimal("800"),
                leverage=2,
                opened_at="2026-04-16T11:00:00+05:00",
                entry_reference_price=Decimal("3198"),
                entry_commission_usdt=Decimal("0.4"),
                entry_slippage_usdt=Decimal("0.1"),
                stop_order_id="sl_2",
                take_profit_order_id="tp_2",
            ))
            self.assertIsNotNone(risk_state)
            assert risk_state is not None
            self.assertEqual(risk_state.daily_realized_pnl, Decimal("-1.5"))
            self.assertEqual(risk_state.cooldowns["ETHUSDT"], "2026-04-16T14:00:00+05:00")
            self.assertEqual(len(trades_first), 1)
            self.assertEqual(len(trades_second), 1)
            self.assertEqual(trades_first[0]["reason"], "legacy row")


if __name__ == "__main__":
    unittest.main()
