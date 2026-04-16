from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from bot_base import Config, SymbolMeta


def make_config(**overrides: object) -> Config:
    data: dict[str, object] = {
        "env_profile_file": "",
        "env_profile_name": "test",
        "api_key": "",
        "api_secret": "",
        "live_trading": False,
        "use_test_order": False,
        "futures_demo": True,
        "futures_base_url": "",
        "ensure_one_way_mode": True,
        "scan_interval_minutes": 5,
        "kline_interval": "15m",
        "kline_limit": 150,
        "max_symbols": 20,
        "symbol_selection": "volume",
        "log_scanned_symbols": False,
        "log_scan_summary": False,
        "scan_summary_top_n": 8,
        "log_symbol_decisions": False,
        "request_sleep_seconds": 0.0,
        "dry_run_usdt_balance": Decimal("1000"),
        "leverage": 2,
        "margin_type": "ISOLATED",
        "ema_fast": 9,
        "ema_slow": 21,
        "rsi_period": 14,
        "volume_avg_period": 20,
        "movement_lookback_candles": 4,
        "movement_threshold_pct": 0.8,
        "min_volume_ratio": 1.2,
        "require_ema_trend": True,
        "buy_rsi_min": 50.0,
        "buy_rsi_max": 70.0,
        "sell_rsi_max": 45.0,
        "higher_timeframe_enabled": False,
        "higher_timeframe_interval": "1h",
        "higher_timeframe_ema_fast": 9,
        "higher_timeframe_ema_slow": 21,
        "funding_filter_enabled": False,
        "max_long_funding_rate_pct": 0.03,
        "min_short_funding_rate_pct": -0.03,
        "trade_risk_pct": 1.0,
        "max_open_positions": 2,
        "max_trades_per_cycle": 1,
        "min_margin_usdt": Decimal("2"),
        "max_margin_usdt": Decimal("100"),
        "min_notional_usdt": Decimal("5"),
        "stop_loss_pct": 1.5,
        "take_profit_pct": 2.5,
        "max_daily_loss_usdt": Decimal("20"),
        "max_consecutive_losses": 3,
        "symbol_cooldown_minutes_after_stop": 180,
        "symbol_cooldown_minutes_after_close": 30,
        "place_protection_orders": True,
        "protection_working_type": "MARK_PRICE",
        "protection_price_protect": False,
        "protection_trigger_buffer_pct": 0.10,
        "cancel_protection_on_close": True,
        "database_file": Path("test.sqlite3"),
        "trade_log_file": Path("test_trades.csv"),
        "app_log_file": Path("test.log"),
        "positions_file": Path("test_positions.json"),
        "risk_state_file": Path("test_risk.json"),
    }
    data.update(overrides)
    return Config(**data)


def make_symbol(**overrides: object) -> SymbolMeta:
    data: dict[str, object] = {
        "symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "min_qty": Decimal("0.001"),
        "step_size": Decimal("0.001"),
        "min_notional": Decimal("5"),
        "tick_size": Decimal("0.1"),
        "percent_price_up": Decimal("1.05"),
        "percent_price_down": Decimal("0.95"),
        "quantity_precision": 3,
        "quote_volume_24h": Decimal("1000000"),
        "selection_reason": "test",
    }
    data.update(overrides)
    return SymbolMeta(**data)
