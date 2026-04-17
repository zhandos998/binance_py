from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from binance.client import Client
from dotenv import load_dotenv


LONG = "LONG"
SHORT = "SHORT"
OPEN_LONG = "OPEN_LONG"
OPEN_SHORT = "OPEN_SHORT"
CLOSE_LONG = "CLOSE_LONG"
CLOSE_SHORT = "CLOSE_SHORT"
ORDER_TYPE_STOP_MARKET = "STOP_MARKET"
ORDER_TYPE_TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"

DEFAULT_DEMO_FUTURES_URL = "https://demo-fapi.binance.com/fapi"

STABLE_BASE_ASSETS = {
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "BUSD",
    "DAI",
    "USDP",
    "EUR",
    "TRY",
    "BRL",
    "UAH",
}

EXCLUDED_BASE_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)

ORDER_STATUS_RU = {
    "NEW": "НОВЫЙ",
    "PENDING_NEW": "ОЖИДАЕТ_СОЗДАНИЯ",
    "PARTIALLY_FILLED": "ЧАСТИЧНО_ИСПОЛНЕН",
    "FILLED": "ИСПОЛНЕН",
    "CANCELED": "ОТМЕНЕН",
    "PENDING_CANCEL": "ОЖИДАЕТ_ОТМЕНЫ",
    "REJECTED": "ОТКЛОНЕН",
    "EXPIRED": "ИСТЕК",
    "EXPIRED_IN_MATCH": "ИСТЕК_В_МАТЧИНГЕ",
    "SUBMITTED": "ОТПРАВЛЕН",
}


@dataclass(frozen=True)
class Config:
    env_profile_file: str
    env_profile_name: str
    api_key: str
    api_secret: str
    futures_quote_asset: str
    live_trading: bool
    use_test_order: bool
    futures_demo: bool
    futures_base_url: str
    ensure_one_way_mode: bool
    scan_interval_minutes: int
    kline_interval: str
    kline_limit: int
    max_symbols: int
    symbol_selection: str
    symbol_whitelist: tuple[str, ...]
    log_scanned_symbols: bool
    log_scan_summary: bool
    scan_summary_top_n: int
    log_symbol_decisions: bool
    request_sleep_seconds: float
    dry_run_usdt_balance: Decimal
    leverage: int
    margin_type: str
    strategy_mode: str
    ema_fast: int
    ema_slow: int
    rsi_period: int
    volume_avg_period: int
    movement_lookback_candles: int
    movement_threshold_pct: float
    min_volume_ratio: float
    require_ema_trend: bool
    buy_rsi_min: float
    buy_rsi_max: float
    sell_rsi_max: float
    higher_timeframe_enabled: bool
    higher_timeframe_interval: str
    higher_timeframe_ema_fast: int
    higher_timeframe_ema_slow: int
    funding_filter_enabled: bool
    max_long_funding_rate_pct: float
    min_short_funding_rate_pct: float
    max_entry_spread_pct: float
    trade_risk_pct: float
    max_open_positions: int
    max_trades_per_cycle: int
    min_margin_usdt: Decimal
    max_margin_usdt: Decimal
    min_notional_usdt: Decimal
    stop_loss_pct: float
    take_profit_pct: float
    max_daily_loss_usdt: Decimal
    max_consecutive_losses: int
    symbol_cooldown_minutes_after_stop: int
    symbol_cooldown_minutes_after_close: int
    place_protection_orders: bool
    protection_working_type: str
    protection_price_protect: bool
    protection_trigger_buffer_pct: float
    cancel_protection_on_close: bool
    database_file: Path
    trade_log_file: Path
    app_log_file: Path
    positions_file: Path
    risk_state_file: Path


@dataclass(frozen=True)
class SymbolMeta:
    symbol: str
    base_asset: str
    quote_asset: str
    min_qty: Decimal
    step_size: Decimal
    min_notional: Decimal
    tick_size: Decimal
    percent_price_up: Decimal
    percent_price_down: Decimal
    quantity_precision: int
    quote_volume_24h: Decimal = Decimal("0")
    selection_reason: str = ""


@dataclass
class Position:
    symbol: str
    direction: str
    entry_price: Decimal
    quantity: Decimal
    margin_used: Decimal
    leverage: int
    opened_at: str
    entry_reference_price: Decimal = Decimal("0")
    entry_commission_usdt: Decimal = Decimal("0")
    entry_slippage_usdt: Decimal = Decimal("0")
    stop_order_id: str | None = None
    take_profit_order_id: str | None = None


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    close: Decimal
    pct_change: float
    rsi: float
    ema_fast: float
    ema_slow: float
    volume_ratio: float
    higher_timeframe_close: Decimal | None = None
    higher_timeframe_ema_fast: float | None = None
    higher_timeframe_ema_slow: float | None = None
    funding_rate_pct: float | None = None


@dataclass(frozen=True)
class TradeSignal:
    symbol: str
    action: str
    side: str
    direction: str
    score: float
    price: Decimal
    reason: str


@dataclass(frozen=True)
class OrderSize:
    quantity: Decimal
    notional: Decimal
    margin: Decimal
    risk_at_stop: Decimal


@dataclass(frozen=True)
class ScanDecision:
    symbol: str
    status: str
    best_direction: str
    price: Decimal | None
    pct_change: float | None
    rsi: float | None
    volume_ratio: float | None
    ema_state: str
    blockers: tuple[str, ...]
    details: str
    rank_score: float


@dataclass
class RiskState:
    day: str
    daily_realized_pnl: Decimal
    consecutive_losses: int
    cooldowns: dict[str, str]


@dataclass(frozen=True)
class ExternalCloseEvent:
    signal: TradeSignal
    quantity: Decimal
    exit_price: Decimal
    close_commission_usdt: Decimal
    gross_realized_pnl: Decimal
    net_realized_pnl: Decimal
    slippage_usdt: Decimal
    slippage_pct: Decimal
    closed_at: datetime


@dataclass(frozen=True)
class ExecutionMetrics:
    quantity: Decimal
    avg_price: Decimal
    notional: Decimal
    commission_usdt: Decimal
    slippage_usdt: Decimal
    slippage_pct: Decimal
    realized_pnl: Decimal


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def local_now() -> datetime:
    return datetime.now().astimezone()


def current_local_day() -> str:
    return local_now().date().isoformat()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    raw = os.getenv(name)
    value = default if raw is None or raw.strip() == "" else int(raw)
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def env_float(name: str, default: float, min_value: float | None = None, max_value: float | None = None) -> float:
    raw = os.getenv(name)
    value = default if raw is None or raw.strip() == "" else float(raw)
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def env_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name)
    return Decimal(default if raw is None or raw.strip() == "" else raw.strip())


def normalize_futures_base_url(value: str) -> str:
    url = value.strip().rstrip("/")
    if not url:
        return DEFAULT_DEMO_FUTURES_URL
    if not url.endswith("/fapi"):
        url = f"{url}/fapi"
    return url


def load_environment() -> tuple[str, str]:
    load_dotenv()
    profile_file = os.getenv("BOT_PROFILE_FILE", "").strip()
    if profile_file:
        profile_path = Path(profile_file)
        if not profile_path.exists():
            raise RuntimeError(f"Файл профиля настроек не найден: {profile_file}")
        load_dotenv(profile_path, override=True)
    profile_name = os.getenv("BOT_PROFILE_NAME", "default").strip() or "default"
    return profile_file, profile_name


def load_config() -> Config:
    env_profile_file, env_profile_name = load_environment()
    futures_quote_asset = os.getenv("FUTURES_QUOTE_ASSET", "USDT").strip().upper() or "USDT"

    legacy_testnet = env_bool("SPOT_TESTNET", True)
    futures_demo = env_bool("FUTURES_DEMO", env_bool("FUTURES_TESTNET", legacy_testnet))
    futures_base_url_raw = os.getenv("FUTURES_BASE_URL")
    futures_base_url = ""
    if futures_base_url_raw is not None and futures_base_url_raw.strip():
        futures_base_url = normalize_futures_base_url(futures_base_url_raw)
    elif futures_demo:
        futures_base_url = DEFAULT_DEMO_FUTURES_URL

    margin_type = os.getenv("MARGIN_TYPE", "ISOLATED").strip().upper()
    if margin_type not in {"ISOLATED", "CROSSED"}:
        margin_type = "ISOLATED"
    protection_working_type = os.getenv("PROTECTION_WORKING_TYPE", "MARK_PRICE").strip().upper()
    if protection_working_type not in {"MARK_PRICE", "CONTRACT_PRICE"}:
        protection_working_type = "MARK_PRICE"
    symbol_selection = os.getenv("SYMBOL_SELECTION", "volume").strip().lower()
    if symbol_selection not in {"volume", "alphabetical"}:
        symbol_selection = "volume"
    strategy_mode = os.getenv("STRATEGY_MODE", "momentum").strip().lower()
    if strategy_mode not in {"momentum", "trend_pullback"}:
        strategy_mode = "momentum"
    higher_timeframe_interval = os.getenv("HIGHER_TIMEFRAME_INTERVAL", "1h").strip() or "1h"

    return Config(
        env_profile_file=env_profile_file,
        env_profile_name=env_profile_name,
        api_key=os.getenv("BINANCE_API_KEY", "").strip(),
        api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
        futures_quote_asset=futures_quote_asset,
        live_trading=env_bool("LIVE_TRADING", False),
        use_test_order=env_bool("USE_TEST_ORDER", False),
        futures_demo=futures_demo,
        futures_base_url=futures_base_url,
        ensure_one_way_mode=env_bool("ENSURE_ONE_WAY_MODE", True),
        scan_interval_minutes=env_int("SCAN_INTERVAL_MINUTES", 3, 1, 5),
        kline_interval=os.getenv("KLINE_INTERVAL", "1m").strip(),
        kline_limit=env_int("KLINE_LIMIT", 90, 50, 500),
        max_symbols=env_int("MAX_SYMBOLS", 0, 0, None),
        symbol_selection=symbol_selection,
        symbol_whitelist=tuple(
            item.strip().upper()
            for item in os.getenv("SYMBOL_WHITELIST", "").split(",")
            if item.strip()
        ),
        log_scanned_symbols=env_bool("LOG_SCANNED_SYMBOLS", True),
        log_scan_summary=env_bool("LOG_SCAN_SUMMARY", True),
        scan_summary_top_n=env_int("SCAN_SUMMARY_TOP_N", 8, 1, 30),
        log_symbol_decisions=env_bool("LOG_SYMBOL_DECISIONS", False),
        request_sleep_seconds=env_float("REQUEST_SLEEP_SECONDS", 0.08, 0.0, None),
        dry_run_usdt_balance=env_decimal("DRY_RUN_USDT_BALANCE", "1000"),
        leverage=env_int("LEVERAGE", 3, 1, 20),
        margin_type=margin_type,
        strategy_mode=strategy_mode,
        ema_fast=env_int("EMA_FAST", 9, 2, None),
        ema_slow=env_int("EMA_SLOW", 21, 3, None),
        rsi_period=env_int("RSI_PERIOD", 14, 2, None),
        volume_avg_period=env_int("VOLUME_AVG_PERIOD", 20, 2, None),
        movement_lookback_candles=env_int("MOVEMENT_LOOKBACK_CANDLES", 5, 1, None),
        movement_threshold_pct=env_float("MOVEMENT_THRESHOLD_PCT", 0.8, 0.0, None),
        min_volume_ratio=env_float("MIN_VOLUME_RATIO", 1.5, 0.0, None),
        require_ema_trend=env_bool("REQUIRE_EMA_TREND", True),
        buy_rsi_min=env_float("BUY_RSI_MIN", 50.0, 0.0, 100.0),
        buy_rsi_max=env_float("BUY_RSI_MAX", 72.0, 0.0, 100.0),
        sell_rsi_max=env_float("SELL_RSI_MAX", 45.0, 0.0, 100.0),
        higher_timeframe_enabled=env_bool("HIGHER_TIMEFRAME_ENABLED", False),
        higher_timeframe_interval=higher_timeframe_interval,
        higher_timeframe_ema_fast=env_int("HIGHER_TIMEFRAME_EMA_FAST", 9, 2, None),
        higher_timeframe_ema_slow=env_int("HIGHER_TIMEFRAME_EMA_SLOW", 21, 3, None),
        funding_filter_enabled=env_bool("FUNDING_FILTER_ENABLED", False),
        max_long_funding_rate_pct=env_float("MAX_LONG_FUNDING_RATE_PCT", 0.03, 0.0, None),
        min_short_funding_rate_pct=env_float("MIN_SHORT_FUNDING_RATE_PCT", -0.03, None, 0.0),
        max_entry_spread_pct=env_float("MAX_ENTRY_SPREAD_PCT", 0.0, 0.0, None),
        trade_risk_pct=env_float("TRADE_RISK_PCT", 1.0, 1.0, 5.0),
        max_open_positions=env_int("MAX_OPEN_POSITIONS", 5, 1, None),
        max_trades_per_cycle=env_int("MAX_TRADES_PER_CYCLE", 3, 1, None),
        min_margin_usdt=env_decimal("MIN_MARGIN_USDT", os.getenv("MIN_USDT_ORDER", "2")),
        max_margin_usdt=env_decimal("MAX_MARGIN_USDT", os.getenv("MAX_USDT_ORDER", "20")),
        min_notional_usdt=env_decimal("MIN_NOTIONAL_USDT", "5"),
        stop_loss_pct=env_float("STOP_LOSS_PCT", 2.0, 0.1, None),
        take_profit_pct=env_float("TAKE_PROFIT_PCT", 3.0, 0.0, None),
        max_daily_loss_usdt=env_decimal("MAX_DAILY_LOSS_USDT", "0"),
        max_consecutive_losses=env_int("MAX_CONSECUTIVE_LOSSES", 0, 0, None),
        symbol_cooldown_minutes_after_stop=env_int("SYMBOL_COOLDOWN_MINUTES_AFTER_STOP", 0, 0, None),
        symbol_cooldown_minutes_after_close=env_int("SYMBOL_COOLDOWN_MINUTES_AFTER_CLOSE", 0, 0, None),
        place_protection_orders=env_bool("PLACE_PROTECTION_ORDERS", True),
        protection_working_type=protection_working_type,
        protection_price_protect=env_bool("PROTECTION_PRICE_PROTECT", False),
        protection_trigger_buffer_pct=env_float("PROTECTION_TRIGGER_BUFFER_PCT", 0.10, 0.0, None),
        cancel_protection_on_close=env_bool("CANCEL_PROTECTION_ON_CLOSE", True),
        database_file=Path(os.getenv("DATABASE_FILE", "bot.sqlite3")),
        trade_log_file=Path(os.getenv("TRADE_LOG_FILE", "futures_trades.csv")),
        app_log_file=Path(os.getenv("APP_LOG_FILE", "bot.log")),
        positions_file=Path(os.getenv("POSITIONS_FILE", "futures_positions.json")),
        risk_state_file=Path(os.getenv("RISK_STATE_FILE", "risk_state.json")),
    )


def configure_logging(config: Config) -> None:
    logging.addLevelName(logging.DEBUG, "ОТЛАДКА")
    logging.addLevelName(logging.INFO, "ИНФО")
    logging.addLevelName(logging.WARNING, "ПРЕДУПРЕЖДЕНИЕ")
    logging.addLevelName(logging.ERROR, "ОШИБКА")
    logging.addLevelName(logging.CRITICAL, "КРИТИЧНО")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(config.app_log_file, encoding="utf-8"),
        ],
    )


def create_client(config: Config) -> Client:
    if config.live_trading and (not config.api_key or not config.api_secret):
        raise RuntimeError("BINANCE_API_KEY и BINANCE_API_SECRET обязательны при LIVE_TRADING=true.")

    client = Client(config.api_key, config.api_secret, testnet=config.futures_demo)
    if config.futures_demo and config.futures_base_url:
        client.FUTURES_TESTNET_URL = config.futures_base_url
    elif config.futures_base_url:
        client.FUTURES_URL = config.futures_base_url
    return client


def translate_order_status(status: str) -> str:
    return ORDER_STATUS_RU.get(status, status)


def decimal_to_str(value: Decimal) -> str:
    return format(value.normalize(), "f")
