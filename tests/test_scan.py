from __future__ import annotations

from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from bot_base import MarketSnapshot, Position, TradeSignal
from bot_scan import build_close_signal_from_opposite, scan_market
from tests.support import make_config, make_symbol


class ScanTests(TestCase):
    def test_build_close_signal_from_opposite_uses_existing_direction(self) -> None:
        position = Position(
            symbol="BTCUSDT",
            direction="LONG",
            entry_price=Decimal("100"),
            quantity=Decimal("1"),
            margin_used=Decimal("50"),
            leverage=2,
            opened_at="2026-04-16T10:00:00+05:00",
        )
        open_signal = TradeSignal(
            symbol="BTCUSDT",
            action="OPEN_SHORT",
            side="SELL",
            direction="SHORT",
            score=10.0,
            price=Decimal("99"),
            reason="short signal",
        )

        close_signal = build_close_signal_from_opposite(position, open_signal)

        self.assertEqual(close_signal.action, "CLOSE_LONG")
        self.assertEqual(close_signal.side, "SELL")
        self.assertEqual(close_signal.direction, "LONG")
        self.assertIn("противоположный сигнал", close_signal.reason)

    def test_scan_market_creates_open_signal_when_position_missing(self) -> None:
        config = make_config(request_sleep_seconds=0.0, log_symbol_decisions=False)
        symbols = {"BTCUSDT": make_symbol()}
        snapshot = MarketSnapshot(
            symbol="BTCUSDT",
            close=Decimal("100"),
            pct_change=1.2,
            rsi=60.0,
            ema_fast=99.0,
            ema_slow=98.0,
            volume_ratio=1.5,
        )

        with patch("bot_scan.analyze_symbol", return_value=snapshot):
            open_signals, close_signals, decisions = scan_market(object(), config, symbols, {})

        self.assertEqual(len(open_signals), 1)
        self.assertEqual(len(close_signals), 0)
        self.assertEqual(decisions[0].status, "OPEN_SIGNAL")
