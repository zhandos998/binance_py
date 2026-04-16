from __future__ import annotations

import unittest

from bot_market import analyze_symbol, load_current_funding_rates
from tests.support import make_config


class FakeMarketClient:
    def futures_klines(self, symbol: str, interval: str, limit: int) -> list[list[str]]:
        base = 100 if interval == "15m" else 1000
        closes = [base + idx for idx in range(limit)]
        rows: list[list[str]] = []
        for idx, close in enumerate(closes):
            rows.append(
                [
                    idx,
                    str(close - 1),
                    str(close + 1),
                    str(close - 2),
                    str(close),
                    "10" if idx < limit - 2 else "30",
                    idx + 1,
                ]
            )
        return rows

    def futures_mark_price(self) -> list[dict[str, str]]:
        return [
            {
                "symbol": "BTCUSDT",
                "markPrice": "100",
                "indexPrice": "100",
                "estimatedSettlePrice": "100",
                "lastFundingRate": "0.0002",
                "nextFundingTime": 0,
                "time": 0,
            }
        ]


class MarketTests(unittest.TestCase):
    def test_analyze_symbol_builds_snapshot_from_closed_klines(self) -> None:
        config = make_config(kline_limit=60, movement_lookback_candles=4, volume_avg_period=5)
        snapshot = analyze_symbol(FakeMarketClient(), "BTCUSDT", config)

        assert snapshot is not None
        self.assertEqual(snapshot.symbol, "BTCUSDT")
        self.assertGreater(snapshot.pct_change, 0)
        self.assertGreater(snapshot.volume_ratio, 1)

    def test_analyze_symbol_enriches_higher_timeframe_and_funding(self) -> None:
        config = make_config(
            kline_limit=60,
            movement_lookback_candles=4,
            volume_avg_period=5,
            higher_timeframe_enabled=True,
            higher_timeframe_interval="1h",
        )
        client = FakeMarketClient()
        funding_rates = load_current_funding_rates(client, ["BTCUSDT"])
        snapshot = analyze_symbol(client, "BTCUSDT", config, funding_rates.get("BTCUSDT"))

        assert snapshot is not None
        self.assertIsNotNone(snapshot.higher_timeframe_close)
        self.assertIsNotNone(snapshot.higher_timeframe_ema_fast)
        self.assertAlmostEqual(snapshot.funding_rate_pct or 0.0, 0.02, places=6)


if __name__ == "__main__":
    unittest.main()
