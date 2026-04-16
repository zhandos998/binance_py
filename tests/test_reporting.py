from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from bot_base import ScanDecision, TradeSignal
from bot_reporting import log_scan_summary
from tests.support import make_config


class ReportingTests(TestCase):
    def test_log_scan_summary_emits_summary_lines(self) -> None:
        config = make_config(log_scan_summary=True, scan_summary_top_n=3)
        decisions = [
            ScanDecision(
                symbol="BTCUSDT",
                status="NO_SIGNAL",
                best_direction="LONG",
                price=Decimal("100"),
                pct_change=0.5,
                rsi=55.0,
                volume_ratio=1.1,
                ema_state="UP",
                blockers=("движение",),
                details="движение",
                rank_score=1.0,
            )
        ]
        open_signals = [
            TradeSignal(
                symbol="ETHUSDT",
                action="OPEN_LONG",
                side="BUY",
                direction="LONG",
                score=5.0,
                price=Decimal("200"),
                reason="test",
            )
        ]

        with self.assertLogs(level="INFO") as captured:
            log_scan_summary(decisions, open_signals, [], config)

        joined = "\n".join(captured.output)
        self.assertIn("Итог скана", joined)
        self.assertIn("Ближайшие кандидаты", joined)
