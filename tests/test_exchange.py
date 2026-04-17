from __future__ import annotations

import unittest
from decimal import Decimal

from bot_exchange import (
    futures_available_quote_balance,
    futures_symbol_rejection_reason,
    get_futures_symbols,
    get_usdt_futures_symbols,
    market_entry_passes_percent_filter,
)
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
                {
                    "symbol": "BTCUSDC",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "marginAsset": "USDC",
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
            ]
        }

    def futures_ticker(self) -> list[dict[str, str]]:
        return [
            {"symbol": "FUNUSDT", "quoteVolume": "500"},
            {"symbol": "ETHUSDT", "quoteVolume": "2000"},
            {"symbol": "BTCUSDT", "quoteVolume": "3000"},
            {"symbol": "USDCUSDT", "quoteVolume": "9000"},
            {"symbol": "BTCUSDC", "quoteVolume": "7000"},
        ]

    def futures_account_balance(self) -> list[dict[str, str]]:
        return [
            {"asset": "USDT", "availableBalance": "125.5"},
            {"asset": "USDC", "availableBalance": "88.25"},
        ]

    def futures_mark_price(self, symbol: str | None = None) -> dict[str, str]:
        return {"symbol": symbol or "BTCUSDT", "markPrice": "100"}

    def futures_orderbook_ticker(self, symbol: str | None = None) -> dict[str, str]:
        if symbol == "BTCUSDT":
            return {"symbol": "BTCUSDT", "bidPrice": "99.9", "askPrice": "100.1"}
        return {"symbol": symbol or "ETHUSDT", "bidPrice": "99", "askPrice": "101"}


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

    def test_get_usdt_futures_symbols_applies_whitelist_before_limit(self) -> None:
        config = make_config(
            max_symbols=5,
            symbol_selection="volume",
            log_scanned_symbols=False,
            symbol_whitelist=("ETHUSDT", "FUNUSDT"),
        )
        symbols = get_usdt_futures_symbols(FakeExchangeClient(), config)

        self.assertEqual(list(symbols), ["ETHUSDT", "FUNUSDT"])

    def test_get_futures_symbols_supports_usdc_profile(self) -> None:
        config = make_config(
            futures_quote_asset="USDC",
            max_symbols=5,
            symbol_selection="volume",
            log_scanned_symbols=False,
        )

        symbols = get_futures_symbols(FakeExchangeClient(), config)

        self.assertEqual(list(symbols), ["BTCUSDC"])
        self.assertEqual(symbols["BTCUSDC"].quote_asset, "USDC")

    def test_futures_available_quote_balance_reads_configured_asset(self) -> None:
        config = make_config(futures_quote_asset="USDC", live_trading=True)

        balance = futures_available_quote_balance(FakeExchangeClient(), config)

        self.assertEqual(balance, Decimal("88.25"))

    def test_market_entry_passes_percent_filter_rejects_wide_spread(self) -> None:
        client = FakeExchangeClient()
        config = make_config(live_trading=True, max_entry_spread_pct=0.15)
        symbol = get_usdt_futures_symbols(client, make_config(log_scanned_symbols=False))["ETHUSDT"]

        allowed = market_entry_passes_percent_filter(client, config, symbol, "BUY")

        self.assertFalse(allowed)


if __name__ == "__main__":
    unittest.main()
