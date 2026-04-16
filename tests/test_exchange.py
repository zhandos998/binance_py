from __future__ import annotations

import unittest
from decimal import Decimal

from bot_exchange import futures_symbol_rejection_reason, get_usdt_futures_symbols
from tests.support import make_config


class FakeExchangeClient:
    def futures_exchange_info(self) -> dict[str, object]:
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.05", "multiplierDown": "0.95"},
                    ],
                },
                {
                    "symbol": "ETHUSDT",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.05", "multiplierDown": "0.95"},
                    ],
                },
                {
                    "symbol": "FUNUSDT",
                    "baseAsset": "FUN",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quantityPrecision": 0,
                    "filters": [
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "1", "stepSize": "1"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.05", "multiplierDown": "0.95"},
                    ],
                },
                {
                    "symbol": "USDCUSDT",
                    "baseAsset": "USDC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quantityPrecision": 0,
                    "filters": [],
                },
            ]
        }

    def futures_ticker(self) -> list[dict[str, str]]:
        return [
            {"symbol": "FUNUSDT", "quoteVolume": "500"},
            {"symbol": "ETHUSDT", "quoteVolume": "2000"},
            {"symbol": "BTCUSDT", "quoteVolume": "3000"},
            {"symbol": "USDCUSDT", "quoteVolume": "9000"},
        ]


class ExchangeTests(unittest.TestCase):
    def test_futures_symbol_rejection_reason_filters_stables(self) -> None:
        reason = futures_symbol_rejection_reason(
            {
                "symbol": "USDCUSDT",
                "baseAsset": "USDC",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "status": "TRADING",
                "contractType": "PERPETUAL",
            }
        )
        self.assertEqual(reason, "baseAsset стейбл/фиат")

    def test_get_usdt_futures_symbols_sorts_by_volume_and_applies_limit(self) -> None:
        config = make_config(max_symbols=2, symbol_selection="volume", log_scanned_symbols=False)
        symbols = get_usdt_futures_symbols(FakeExchangeClient(), config)

        self.assertEqual(list(symbols), ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(symbols["BTCUSDT"].quote_volume_24h, Decimal("3000"))
        self.assertNotIn("USDCUSDT", symbols)


if __name__ == "__main__":
    unittest.main()
