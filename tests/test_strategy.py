from __future__ import annotations

import unittest
from decimal import Decimal

from bot_base import LONG, SHORT, MarketSnapshot, Position
from bot_strategy import build_open_signal, build_risk_exit_signal, ema, rsi, signal_blockers
from tests.support import make_config


class StrategyTests(unittest.TestCase):
    def test_ema_and_rsi_return_expected_shape(self) -> None:
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        ema_values = ema(values, 3)
        self.assertEqual(len(ema_values), len(values))
        self.assertGreater(ema_values[-1], ema_values[0])
        self.assertAlmostEqual(rsi(list(range(1, 20)), 14) or 0.0, 100.0, places=6)

    def test_build_open_signal_returns_long(self) -> None:
        config = make_config()
        snapshot = MarketSnapshot(
            symbol="BTCUSDT",
            close=Decimal("100"),
            pct_change=1.5,
            rsi=60.0,
            ema_fast=99.0,
            ema_slow=98.0,
            volume_ratio=1.8,
        )
        signal = build_open_signal(snapshot, config)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, LONG)
        self.assertEqual(signal.action, "OPEN_LONG")

    def test_build_open_signal_returns_short(self) -> None:
        config = make_config()
        snapshot = MarketSnapshot(
            symbol="ETHUSDT",
            close=Decimal("90"),
            pct_change=-1.4,
            rsi=35.0,
            ema_fast=91.0,
            ema_slow=92.0,
            volume_ratio=1.6,
        )
        signal = build_open_signal(snapshot, config)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, SHORT)
        self.assertEqual(signal.action, "OPEN_SHORT")

    def test_build_open_signal_blocks_long_when_funding_too_expensive(self) -> None:
        config = make_config(funding_filter_enabled=True, max_long_funding_rate_pct=0.03)
        snapshot = MarketSnapshot(
            symbol="BTCUSDT",
            close=Decimal("100"),
            pct_change=1.5,
            rsi=60.0,
            ema_fast=99.0,
            ema_slow=98.0,
            volume_ratio=1.8,
            funding_rate_pct=0.05,
        )
        signal = build_open_signal(snapshot, config)
        self.assertIsNone(signal)
        self.assertIn("funding", signal_blockers(snapshot, config, LONG))

    def test_build_open_signal_blocks_short_without_higher_timeframe_trend(self) -> None:
        config = make_config(higher_timeframe_enabled=True, higher_timeframe_interval="1h")
        snapshot = MarketSnapshot(
            symbol="ETHUSDT",
            close=Decimal("90"),
            pct_change=-1.4,
            rsi=35.0,
            ema_fast=91.0,
            ema_slow=92.0,
            volume_ratio=1.6,
            higher_timeframe_close=Decimal("101"),
            higher_timeframe_ema_fast=100.0,
            higher_timeframe_ema_slow=99.0,
        )
        signal = build_open_signal(snapshot, config)
        self.assertIsNone(signal)
        self.assertIn("HTF", signal_blockers(snapshot, config, SHORT))

    def test_build_open_signal_returns_pullback_long_for_riskier_mode(self) -> None:
        config = make_config(
            strategy_mode="trend_pullback",
            min_volume_ratio=1.2,
            higher_timeframe_enabled=True,
            funding_filter_enabled=True,
        )
        snapshot = MarketSnapshot(
            symbol="SOLUSDT",
            close=Decimal("105"),
            pct_change=0.25,
            rsi=54.0,
            ema_fast=104.0,
            ema_slow=103.0,
            volume_ratio=1.0,
            higher_timeframe_close=Decimal("110"),
            higher_timeframe_ema_fast=108.0,
            higher_timeframe_ema_slow=106.0,
            funding_rate_pct=0.01,
        )

        signal = build_open_signal(snapshot, config)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, LONG)
        self.assertEqual(signal.action, "OPEN_LONG")

    def test_signal_blockers_marks_pullback_window_as_movement_blocker(self) -> None:
        config = make_config(strategy_mode="trend_pullback")
        snapshot = MarketSnapshot(
            symbol="BTCUSDT",
            close=Decimal("110"),
            pct_change=1.6,
            rsi=55.0,
            ema_fast=109.0,
            ema_slow=108.0,
            volume_ratio=2.0,
        )

        signal = build_open_signal(snapshot, config)
        self.assertIsNone(signal)
        self.assertIn("движение", signal_blockers(snapshot, config, LONG))

    def test_build_risk_exit_signal_detects_take_profit_short(self) -> None:
        config = make_config(stop_loss_pct=1.5, take_profit_pct=2.5)
        position = Position(
            symbol="SOLUSDT",
            direction=SHORT,
            entry_price=Decimal("100"),
            quantity=Decimal("1"),
            margin_used=Decimal("50"),
            leverage=2,
            opened_at="2026-01-01T00:00:00+00:00",
        )
        snapshot = MarketSnapshot(
            symbol="SOLUSDT",
            close=Decimal("97.4"),
            pct_change=-2.0,
            rsi=30.0,
            ema_fast=98.0,
            ema_slow=99.0,
            volume_ratio=1.5,
        )
        signal = build_risk_exit_signal(position, snapshot, config)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.action, "CLOSE_SHORT")
        self.assertIn("тейк-профит", signal.reason)


if __name__ == "__main__":
    unittest.main()
