from __future__ import annotations

import csv
import json
import logging
import os
import signal
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

from binance.client import Client
from binance.enums import ORDER_TYPE_MARKET, SIDE_BUY, SIDE_SELL
from binance.exceptions import BinanceAPIException, BinanceRequestException
from dotenv import load_dotenv


LONG = "LONG"
SHORT = "SHORT"
OPEN_LONG = "OPEN_LONG"
OPEN_SHORT = "OPEN_SHORT"
CLOSE_LONG = "CLOSE_LONG"
CLOSE_SHORT = "CLOSE_SHORT"

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
    api_key: str
    api_secret: str
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
    log_scanned_symbols: bool
    log_scan_summary: bool
    scan_summary_top_n: int
    log_symbol_decisions: bool
    request_sleep_seconds: float
    dry_run_usdt_balance: Decimal
    leverage: int
    margin_type: str
    ema_fast: int
    ema_slow: int
    rsi_period: int
    volume_avg_period: int
    movement_lookback_candles: int
    movement_threshold_pct: float
    min_volume_ratio: float
    buy_rsi_min: float
    buy_rsi_max: float
    sell_rsi_max: float
    trade_risk_pct: float
    max_open_positions: int
    max_trades_per_cycle: int
    min_margin_usdt: Decimal
    max_margin_usdt: Decimal
    min_notional_usdt: Decimal
    stop_loss_pct: float
    take_profit_pct: float
    trade_log_file: Path
    app_log_file: Path
    positions_file: Path


@dataclass(frozen=True)
class SymbolMeta:
    symbol: str
    base_asset: str
    quote_asset: str
    min_qty: Decimal
    step_size: Decimal
    min_notional: Decimal
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


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    close: Decimal
    pct_change: float
    rsi: float
    ema_fast: float
    ema_slow: float
    volume_ratio: float


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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def load_config() -> Config:
    load_dotenv()

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
    symbol_selection = os.getenv("SYMBOL_SELECTION", "volume").strip().lower()
    if symbol_selection not in {"volume", "alphabetical"}:
        symbol_selection = "volume"

    return Config(
        api_key=os.getenv("BINANCE_API_KEY", "").strip(),
        api_secret=os.getenv("BINANCE_API_SECRET", "").strip(),
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
        log_scanned_symbols=env_bool("LOG_SCANNED_SYMBOLS", True),
        log_scan_summary=env_bool("LOG_SCAN_SUMMARY", True),
        scan_summary_top_n=env_int("SCAN_SUMMARY_TOP_N", 8, 1, 30),
        log_symbol_decisions=env_bool("LOG_SYMBOL_DECISIONS", False),
        request_sleep_seconds=env_float("REQUEST_SLEEP_SECONDS", 0.08, 0.0, None),
        dry_run_usdt_balance=env_decimal("DRY_RUN_USDT_BALANCE", "1000"),
        leverage=env_int("LEVERAGE", 3, 1, 20),
        margin_type=margin_type,
        ema_fast=env_int("EMA_FAST", 9, 2, None),
        ema_slow=env_int("EMA_SLOW", 21, 3, None),
        rsi_period=env_int("RSI_PERIOD", 14, 2, None),
        volume_avg_period=env_int("VOLUME_AVG_PERIOD", 20, 2, None),
        movement_lookback_candles=env_int("MOVEMENT_LOOKBACK_CANDLES", 5, 1, None),
        movement_threshold_pct=env_float("MOVEMENT_THRESHOLD_PCT", 0.8, 0.0, None),
        min_volume_ratio=env_float("MIN_VOLUME_RATIO", 1.5, 0.0, None),
        buy_rsi_min=env_float("BUY_RSI_MIN", 50.0, 0.0, 100.0),
        buy_rsi_max=env_float("BUY_RSI_MAX", 72.0, 0.0, 100.0),
        sell_rsi_max=env_float("SELL_RSI_MAX", 45.0, 0.0, 100.0),
        trade_risk_pct=env_float("TRADE_RISK_PCT", 1.0, 1.0, 5.0),
        max_open_positions=env_int("MAX_OPEN_POSITIONS", 5, 1, None),
        max_trades_per_cycle=env_int("MAX_TRADES_PER_CYCLE", 3, 1, None),
        min_margin_usdt=env_decimal("MIN_MARGIN_USDT", os.getenv("MIN_USDT_ORDER", "2")),
        max_margin_usdt=env_decimal("MAX_MARGIN_USDT", os.getenv("MAX_USDT_ORDER", "20")),
        min_notional_usdt=env_decimal("MIN_NOTIONAL_USDT", "5"),
        stop_loss_pct=env_float("STOP_LOSS_PCT", 2.0, 0.1, None),
        take_profit_pct=env_float("TAKE_PROFIT_PCT", 3.0, 0.0, None),
        trade_log_file=Path(os.getenv("TRADE_LOG_FILE", "futures_trades.csv")),
        app_log_file=Path(os.getenv("APP_LOG_FILE", "bot.log")),
        positions_file=Path(os.getenv("POSITIONS_FILE", "futures_positions.json")),
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


def decimal_from_filter(filters: list[dict[str, Any]], filter_type: str, field: str, default: str) -> Decimal:
    for item in filters:
        if item.get("filterType") == filter_type and item.get(field) is not None:
            return Decimal(str(item[field]))
    return Decimal(default)


def get_market_lot_values(filters: list[dict[str, Any]]) -> tuple[Decimal, Decimal]:
    market_min_qty = decimal_from_filter(filters, "MARKET_LOT_SIZE", "minQty", "0")
    market_step_size = decimal_from_filter(filters, "MARKET_LOT_SIZE", "stepSize", "0")
    lot_min_qty = decimal_from_filter(filters, "LOT_SIZE", "minQty", "0")
    lot_step_size = decimal_from_filter(filters, "LOT_SIZE", "stepSize", "0")

    min_qty = market_min_qty if market_min_qty > 0 else lot_min_qty
    step_size = market_step_size if market_step_size > 0 else lot_step_size
    return min_qty, step_size


def get_min_notional(filters: list[dict[str, Any]]) -> Decimal:
    value = decimal_from_filter(filters, "MIN_NOTIONAL", "notional", "0")
    if value == 0:
        value = decimal_from_filter(filters, "MIN_NOTIONAL", "minNotional", "0")
    if value == 0:
        value = decimal_from_filter(filters, "NOTIONAL", "minNotional", "0")
    return value


def futures_symbol_rejection_reason(symbol_info: dict[str, Any]) -> str | None:
    symbol = str(symbol_info.get("symbol", ""))
    base_asset = str(symbol_info.get("baseAsset", ""))

    if symbol_info.get("status") != "TRADING":
        return "status не TRADING"
    if symbol_info.get("quoteAsset") != "USDT":
        return "quoteAsset не USDT"
    if symbol_info.get("marginAsset") not in {None, "USDT"}:
        return "marginAsset не USDT"
    if symbol_info.get("contractType") != "PERPETUAL":
        return "contractType не PERPETUAL"
    if base_asset in STABLE_BASE_ASSETS:
        return "baseAsset стейбл/фиат"
    if base_asset.endswith(EXCLUDED_BASE_SUFFIXES):
        return "токен с суффиксом UP/DOWN/BULL/BEAR"
    if not symbol.endswith("USDT"):
        return "symbol не заканчивается на USDT"
    return None


def is_supported_usdt_futures_symbol(symbol_info: dict[str, Any]) -> bool:
    return futures_symbol_rejection_reason(symbol_info) is None


def get_futures_quote_volumes(client: Client) -> dict[str, Decimal]:
    try:
        tickers = client.futures_ticker()
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.warning("Не удалось получить 24h volume для сортировки символов: %s", exc)
        return {}

    if isinstance(tickers, dict):
        tickers = [tickers]

    volumes: dict[str, Decimal] = {}
    for item in tickers:
        symbol = item.get("symbol")
        if symbol:
            volumes[str(symbol)] = Decimal(str(item.get("quoteVolume", "0")))
    return volumes


def format_symbol_list(symbols: list[str] | tuple[str, ...] | dict[str, Any], limit: int = 80) -> str:
    symbol_list = list(symbols)
    if len(symbol_list) <= limit:
        return ", ".join(symbol_list)
    visible = ", ".join(symbol_list[:limit])
    return f"{visible}, ... еще {len(symbol_list) - limit}"


def log_symbol_chunks(title: str, symbols: list[str] | tuple[str, ...] | dict[str, Any], chunk_size: int = 10) -> None:
    symbol_list = list(symbols)
    logging.info("%s (%s):", title, len(symbol_list))
    for start in range(0, len(symbol_list), chunk_size):
        chunk = symbol_list[start : start + chunk_size]
        logging.info("  %02d-%02d: %s", start + 1, start + len(chunk), ", ".join(chunk))


def format_rejection_counts(rejection_counts: dict[str, int]) -> str:
    return "; ".join(f"{reason}: {count}" for reason, count in sorted(rejection_counts.items()))


def get_usdt_futures_symbols(client: Client, config: Config) -> dict[str, SymbolMeta]:
    exchange_info = client.futures_exchange_info()
    symbols: dict[str, SymbolMeta] = {}
    rejection_counts: dict[str, int] = {}
    quote_volumes = get_futures_quote_volumes(client) if config.symbol_selection == "volume" else {}

    for item in exchange_info.get("symbols", []):
        rejection_reason = futures_symbol_rejection_reason(item)
        if rejection_reason is not None:
            rejection_counts[rejection_reason] = rejection_counts.get(rejection_reason, 0) + 1
            continue

        filters = item.get("filters", [])
        min_qty, step_size = get_market_lot_values(filters)
        min_notional = get_min_notional(filters)
        symbol = item["symbol"]
        quote_volume = quote_volumes.get(symbol, Decimal("0"))
        selection_reason = (
            "активный USDT-M perpetual: status=TRADING, quoteAsset=USDT, "
            "marginAsset=USDT, contractType=PERPETUAL"
        )
        symbols[symbol] = SymbolMeta(
            symbol=symbol,
            base_asset=item["baseAsset"],
            quote_asset=item["quoteAsset"],
            min_qty=min_qty,
            step_size=step_size,
            min_notional=min_notional,
            quantity_precision=int(item.get("quantityPrecision", 8)),
            quote_volume_24h=quote_volume,
            selection_reason=selection_reason,
        )

    if config.symbol_selection == "volume":
        ordered_items = sorted(symbols.items(), key=lambda item: (-item[1].quote_volume_24h, item[0]))
    else:
        ordered_items = sorted(symbols.items())

    if config.max_symbols > 0:
        ordered_items = ordered_items[: config.max_symbols]

    ordered_symbols = dict(ordered_items)
    if config.log_scanned_symbols:
        limit_text = "без лимита" if config.max_symbols == 0 else str(config.max_symbols)
        logging.info(
            "Отбор futures-символов: найдено подходящих=%s, сканируется=%s, MAX_SYMBOLS=%s, SYMBOL_SELECTION=%s.",
            len(symbols),
            len(ordered_symbols),
            limit_text,
            config.symbol_selection,
        )
        logging.info(
            "Почему они сканируются: %s",
            "status=TRADING, contractType=PERPETUAL, quoteAsset=USDT, marginAsset=USDT; "
            "исключены стейблкоины/фиат и UP/DOWN/BULL/BEAR токены.",
        )
        if config.symbol_selection == "volume":
            logging.info("Если MAX_SYMBOLS > 0, выбираются самые ликвидные по 24h quoteVolume.")
        log_symbol_chunks("Список символов для сканирования", list(ordered_symbols))
        if rejection_counts:
            logging.info("Почему остальные символы не попали в скан: %s", format_rejection_counts(rejection_counts))

    return ordered_symbols


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []

    alpha = 2 / (period + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append((value * alpha) + (result[-1] * (1 - alpha)))
    return result


def rsi(values: list[float], period: int) -> float | None:
    if len(values) <= period:
        return None

    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [abs(min(delta, 0.0)) for delta in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    relative_strength = avg_gain / avg_loss
    return 100 - (100 / (1 + relative_strength))


def get_closed_klines(client: Client, symbol: str, config: Config) -> list[list[Any]]:
    klines = client.futures_klines(symbol=symbol, interval=config.kline_interval, limit=config.kline_limit)
    if len(klines) > 1:
        return klines[:-1]
    return klines


def analyze_symbol(client: Client, symbol: str, config: Config) -> MarketSnapshot | None:
    klines = get_closed_klines(client, symbol, config)
    min_required = max(
        config.ema_slow + 2,
        config.rsi_period + 2,
        config.volume_avg_period + 2,
        config.movement_lookback_candles + 2,
    )
    if len(klines) < min_required:
        return None

    closes = [float(kline[4]) for kline in klines]
    volumes = [float(kline[5]) for kline in klines]

    fast_ema_series = ema(closes, config.ema_fast)
    slow_ema_series = ema(closes, config.ema_slow)
    latest_rsi = rsi(closes, config.rsi_period)
    if latest_rsi is None:
        return None

    old_close = closes[-1 - config.movement_lookback_candles]
    if old_close <= 0:
        return None

    pct_change = ((closes[-1] - old_close) / old_close) * 100
    avg_volume = sum(volumes[-1 - config.volume_avg_period : -1]) / config.volume_avg_period
    volume_ratio = 0.0 if avg_volume <= 0 else volumes[-1] / avg_volume

    return MarketSnapshot(
        symbol=symbol,
        close=Decimal(str(closes[-1])),
        pct_change=pct_change,
        rsi=latest_rsi,
        ema_fast=fast_ema_series[-1],
        ema_slow=slow_ema_series[-1],
        volume_ratio=volume_ratio,
    )


def build_open_signal(snapshot: MarketSnapshot, config: Config) -> TradeSignal | None:
    price = float(snapshot.close)
    trend_up = price > snapshot.ema_fast > snapshot.ema_slow
    trend_down = price < snapshot.ema_fast < snapshot.ema_slow
    volume_ok = snapshot.volume_ratio >= config.min_volume_ratio

    if (
        snapshot.pct_change >= config.movement_threshold_pct
        and trend_up
        and volume_ok
        and config.buy_rsi_min <= snapshot.rsi <= config.buy_rsi_max
    ):
        score = snapshot.pct_change + snapshot.volume_ratio + ((snapshot.rsi - 50) / 10)
        reason = (
            f"импульс вверх {snapshot.pct_change:.2f}%, "
            f"RSI={snapshot.rsi:.1f}, объем/средний={snapshot.volume_ratio:.2f}, EMA тренд вверх"
        )
        return TradeSignal(snapshot.symbol, OPEN_LONG, SIDE_BUY, LONG, score, snapshot.close, reason)

    if (
        snapshot.pct_change <= -config.movement_threshold_pct
        and trend_down
        and volume_ok
        and snapshot.rsi <= config.sell_rsi_max
    ):
        score = abs(snapshot.pct_change) + snapshot.volume_ratio + ((50 - snapshot.rsi) / 10)
        reason = (
            f"импульс вниз {snapshot.pct_change:.2f}%, "
            f"RSI={snapshot.rsi:.1f}, объем/средний={snapshot.volume_ratio:.2f}, EMA тренд вниз"
        )
        return TradeSignal(snapshot.symbol, OPEN_SHORT, SIDE_SELL, SHORT, score, snapshot.close, reason)

    return None


def format_market_metrics(snapshot: MarketSnapshot) -> str:
    return (
        f"цена={snapshot.close}, change={snapshot.pct_change:.2f}%, RSI={snapshot.rsi:.1f}, "
        f"EMA_FAST={snapshot.ema_fast:.8f}, EMA_SLOW={snapshot.ema_slow:.8f}, "
        f"volume/avg={snapshot.volume_ratio:.2f}"
    )


def explain_no_open_signal(snapshot: MarketSnapshot, config: Config) -> str:
    price = float(snapshot.close)
    trend_up = price > snapshot.ema_fast > snapshot.ema_slow
    trend_down = price < snapshot.ema_fast < snapshot.ema_slow
    volume_ok = snapshot.volume_ratio >= config.min_volume_ratio

    long_blockers: list[str] = []
    if snapshot.pct_change < config.movement_threshold_pct:
        long_blockers.append(f"рост {snapshot.pct_change:.2f}% < порога {config.movement_threshold_pct:.2f}%")
    if not trend_up:
        long_blockers.append("нет EMA-тренда вверх")
    if not volume_ok:
        long_blockers.append(f"объем {snapshot.volume_ratio:.2f} < порога {config.min_volume_ratio:.2f}")
    if not (config.buy_rsi_min <= snapshot.rsi <= config.buy_rsi_max):
        long_blockers.append(
            f"RSI {snapshot.rsi:.1f} вне диапазона LONG {config.buy_rsi_min:.1f}-{config.buy_rsi_max:.1f}"
        )

    short_blockers: list[str] = []
    if snapshot.pct_change > -config.movement_threshold_pct:
        short_blockers.append(f"падение {snapshot.pct_change:.2f}% слабее порога -{config.movement_threshold_pct:.2f}%")
    if not trend_down:
        short_blockers.append("нет EMA-тренда вниз")
    if not volume_ok:
        short_blockers.append(f"объем {snapshot.volume_ratio:.2f} < порога {config.min_volume_ratio:.2f}")
    if snapshot.rsi > config.sell_rsi_max:
        short_blockers.append(f"RSI {snapshot.rsi:.1f} выше SHORT-порога {config.sell_rsi_max:.1f}")

    long_text = "; ".join(long_blockers) if long_blockers else "условия LONG выполнены"
    short_text = "; ".join(short_blockers) if short_blockers else "условия SHORT выполнены"
    return f"LONG нет: {long_text}. SHORT нет: {short_text}"


def close_action_for_direction(direction: str) -> str:
    return CLOSE_LONG if direction == LONG else CLOSE_SHORT


def close_side_for_direction(direction: str) -> str:
    return SIDE_SELL if direction == LONG else SIDE_BUY


def ema_state(snapshot: MarketSnapshot) -> str:
    price = float(snapshot.close)
    if price > snapshot.ema_fast > snapshot.ema_slow:
        return "UP"
    if price < snapshot.ema_fast < snapshot.ema_slow:
        return "DOWN"
    return "MIXED"


def signal_blockers(snapshot: MarketSnapshot, config: Config, direction: str) -> tuple[str, ...]:
    price = float(snapshot.close)
    trend_up = price > snapshot.ema_fast > snapshot.ema_slow
    trend_down = price < snapshot.ema_fast < snapshot.ema_slow
    volume_ok = snapshot.volume_ratio >= config.min_volume_ratio
    blockers: list[str] = []

    if direction == LONG:
        if snapshot.pct_change < config.movement_threshold_pct:
            blockers.append("движение")
        if not trend_up:
            blockers.append("EMA")
        if not volume_ok:
            blockers.append("объем")
        if not (config.buy_rsi_min <= snapshot.rsi <= config.buy_rsi_max):
            blockers.append("RSI")
    else:
        if snapshot.pct_change > -config.movement_threshold_pct:
            blockers.append("движение")
        if not trend_down:
            blockers.append("EMA")
        if not volume_ok:
            blockers.append("объем")
        if snapshot.rsi > config.sell_rsi_max:
            blockers.append("RSI")

    return tuple(blockers)


def choose_best_direction(snapshot: MarketSnapshot, config: Config) -> tuple[str, tuple[str, ...]]:
    long_blockers = signal_blockers(snapshot, config, LONG)
    short_blockers = signal_blockers(snapshot, config, SHORT)
    if len(long_blockers) < len(short_blockers):
        return LONG, long_blockers
    if len(short_blockers) < len(long_blockers):
        return SHORT, short_blockers
    if snapshot.pct_change >= 0:
        return LONG, long_blockers
    return SHORT, short_blockers


def decision_rank(snapshot: MarketSnapshot, blockers: tuple[str, ...]) -> float:
    passed_checks = 4 - len(blockers)
    return (passed_checks * 1000) + abs(snapshot.pct_change) + min(snapshot.volume_ratio, 10)


def no_signal_decision(symbol: str, snapshot: MarketSnapshot, config: Config, status: str = "NO_SIGNAL") -> ScanDecision:
    direction, blockers = choose_best_direction(snapshot, config)
    details = ", ".join(blockers) if blockers else "условия выполнены"
    return ScanDecision(
        symbol=symbol,
        status=status,
        best_direction=direction,
        price=snapshot.close,
        pct_change=snapshot.pct_change,
        rsi=snapshot.rsi,
        volume_ratio=snapshot.volume_ratio,
        ema_state=ema_state(snapshot),
        blockers=blockers,
        details=details,
        rank_score=decision_rank(snapshot, blockers),
    )


def signal_decision(signal: TradeSignal, snapshot: MarketSnapshot, status: str) -> ScanDecision:
    return ScanDecision(
        symbol=signal.symbol,
        status=status,
        best_direction=signal.direction,
        price=snapshot.close,
        pct_change=snapshot.pct_change,
        rsi=snapshot.rsi,
        volume_ratio=snapshot.volume_ratio,
        ema_state=ema_state(snapshot),
        blockers=(),
        details=signal.reason,
        rank_score=10_000 + signal.score,
    )


def skipped_decision(symbol: str, details: str) -> ScanDecision:
    return ScanDecision(
        symbol=symbol,
        status="SKIPPED",
        best_direction="-",
        price=None,
        pct_change=None,
        rsi=None,
        volume_ratio=None,
        ema_state="-",
        blockers=("данные",),
        details=details,
        rank_score=-1,
    )


def build_risk_exit_signal(position: Position, snapshot: MarketSnapshot, config: Config) -> TradeSignal | None:
    stop_fraction = Decimal(str(config.stop_loss_pct)) / Decimal("100")
    take_fraction = Decimal(str(config.take_profit_pct)) / Decimal("100")

    if position.direction == LONG:
        stop_price = position.entry_price * (Decimal("1") - stop_fraction)
        take_price = position.entry_price * (Decimal("1") + take_fraction)
        if snapshot.close <= stop_price:
            reason = f"стоп-лосс LONG: вход={position.entry_price}, текущая={snapshot.close}, стоп={stop_price}"
            return TradeSignal(position.symbol, CLOSE_LONG, SIDE_SELL, LONG, 10_000.0, snapshot.close, reason)
        if config.take_profit_pct > 0 and snapshot.close >= take_price:
            reason = f"тейк-профит LONG: вход={position.entry_price}, текущая={snapshot.close}, цель={take_price}"
            return TradeSignal(position.symbol, CLOSE_LONG, SIDE_SELL, LONG, 9_000.0, snapshot.close, reason)

    if position.direction == SHORT:
        stop_price = position.entry_price * (Decimal("1") + stop_fraction)
        take_price = position.entry_price * (Decimal("1") - take_fraction)
        if snapshot.close >= stop_price:
            reason = f"стоп-лосс SHORT: вход={position.entry_price}, текущая={snapshot.close}, стоп={stop_price}"
            return TradeSignal(position.symbol, CLOSE_SHORT, SIDE_BUY, SHORT, 10_000.0, snapshot.close, reason)
        if config.take_profit_pct > 0 and snapshot.close <= take_price:
            reason = f"тейк-профит SHORT: вход={position.entry_price}, текущая={snapshot.close}, цель={take_price}"
            return TradeSignal(position.symbol, CLOSE_SHORT, SIDE_BUY, SHORT, 9_000.0, snapshot.close, reason)

    return None


def load_positions(config: Config) -> dict[str, Position]:
    if not config.positions_file.exists():
        return {}

    try:
        with config.positions_file.open("r", encoding="utf-8") as file:
            raw_positions = json.load(file)
    except (json.JSONDecodeError, OSError) as exc:
        logging.warning("Не удалось прочитать файл позиций %s: %s", config.positions_file, exc)
        return {}

    positions: dict[str, Position] = {}
    for symbol, item in raw_positions.items():
        if "direction" not in item:
            logging.warning("Пропуск старой spot-позиции %s: формат несовместим с futures.", symbol)
            continue

        positions[symbol] = Position(
            symbol=symbol,
            direction=item["direction"],
            entry_price=Decimal(str(item["entry_price"])),
            quantity=Decimal(str(item["quantity"])),
            margin_used=Decimal(str(item.get("margin_used", "0"))),
            leverage=int(item.get("leverage", config.leverage)),
            opened_at=item.get("opened_at", utc_now()),
        )
    return positions


def save_positions(config: Config, positions: dict[str, Position]) -> None:
    serializable = {}
    for symbol, position in positions.items():
        item = asdict(position)
        item["entry_price"] = str(position.entry_price)
        item["quantity"] = str(position.quantity)
        item["margin_used"] = str(position.margin_used)
        serializable[symbol] = item

    with config.positions_file.open("w", encoding="utf-8") as file:
        json.dump(serializable, file, indent=2, ensure_ascii=False)


def append_trade_log(
    config: Config,
    signal: TradeSignal,
    quantity: Decimal,
    notional: Decimal,
    margin: Decimal,
    risk_at_stop: Decimal,
    status: str,
    order_response: dict[str, Any] | None,
) -> None:
    file_exists = config.trade_log_file.exists() and config.trade_log_file.stat().st_size > 0
    fieldnames = [
        "timestamp",
        "mode",
        "market",
        "symbol",
        "action",
        "side",
        "direction",
        "leverage",
        "price",
        "quantity",
        "notional_usdt",
        "margin_usdt",
        "risk_at_stop_usdt",
        "reason",
        "status",
        "order_id",
        "raw_response",
    ]
    order_response = order_response or {}

    with config.trade_log_file.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": utc_now(),
                "mode": "реальный" if config.live_trading else "симуляция",
                "market": "USDT-M Futures",
                "symbol": signal.symbol,
                "action": signal.action,
                "side": signal.side,
                "direction": signal.direction,
                "leverage": config.leverage,
                "price": str(signal.price),
                "quantity": str(quantity),
                "notional_usdt": str(notional),
                "margin_usdt": str(margin),
                "risk_at_stop_usdt": str(risk_at_stop),
                "reason": signal.reason,
                "status": status,
                "order_id": order_response.get("orderId", ""),
                "raw_response": json.dumps(order_response, ensure_ascii=False),
            }
        )


def round_step(value: Decimal, step_size: Decimal) -> Decimal:
    if step_size <= 0:
        return value
    return (value / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size


def futures_available_usdt(client: Client, config: Config) -> Decimal:
    if not config.live_trading:
        return config.dry_run_usdt_balance

    balances = client.futures_account_balance()
    for item in balances:
        if item.get("asset") == "USDT":
            return Decimal(str(item.get("availableBalance", item.get("balance", "0"))))
    return Decimal("0")


def calculate_order_size(
    client: Client,
    config: Config,
    symbol_meta: SymbolMeta,
    price: Decimal,
) -> OrderSize | None:
    available_usdt = futures_available_usdt(client, config)
    risk_budget = available_usdt * Decimal(str(config.trade_risk_pct)) / Decimal("100")
    stop_fraction = Decimal(str(config.stop_loss_pct)) / Decimal("100")
    if stop_fraction <= 0:
        logging.info("Пропуск %s: STOP_LOSS_PCT должен быть больше 0.", symbol_meta.symbol)
        return None

    max_notional_by_risk = risk_budget / stop_fraction
    max_notional_by_margin = config.max_margin_usdt * Decimal(config.leverage)
    notional = min(max_notional_by_risk, max_notional_by_margin)

    effective_min_notional = max(config.min_notional_usdt, symbol_meta.min_notional)
    if notional < effective_min_notional:
        logging.info(
            "Пропуск %s: расчетный notional %s USDT ниже минимума %s USDT.",
            symbol_meta.symbol,
            notional.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            effective_min_notional,
        )
        return None

    margin = notional / Decimal(config.leverage)
    if margin < config.min_margin_usdt:
        logging.info(
            "Пропуск %s: маржа %s USDT ниже MIN_MARGIN_USDT=%s.",
            symbol_meta.symbol,
            margin.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            config.min_margin_usdt,
        )
        return None

    quantity = round_step(notional / price, symbol_meta.step_size)
    if quantity < symbol_meta.min_qty:
        logging.info(
            "Пропуск %s: количество %s ниже минимального %s.",
            symbol_meta.symbol,
            quantity,
            symbol_meta.min_qty,
        )
        return None

    actual_notional = quantity * price
    if actual_notional < effective_min_notional:
        logging.info(
            "Пропуск %s: notional после округления %s USDT ниже минимума %s USDT.",
            symbol_meta.symbol,
            actual_notional.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            effective_min_notional,
        )
        return None

    actual_margin = actual_notional / Decimal(config.leverage)
    risk_at_stop = actual_notional * stop_fraction
    return OrderSize(quantity, actual_notional, actual_margin, risk_at_stop)


def sync_live_positions(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
) -> dict[str, Position]:
    if not config.live_trading or config.use_test_order:
        return load_positions(config)

    live_positions: dict[str, Position] = {}
    raw_positions = client.futures_position_information()
    for item in raw_positions:
        symbol = item.get("symbol", "")
        if symbol not in symbols:
            continue

        amount = Decimal(str(item.get("positionAmt", "0")))
        if amount == 0:
            continue

        entry_price = Decimal(str(item.get("entryPrice", "0")))
        if entry_price <= 0:
            entry_price = Decimal(str(item.get("markPrice", "0")))

        quantity = abs(amount)
        leverage = int(item.get("leverage", config.leverage))
        direction = LONG if amount > 0 else SHORT
        margin_used = Decimal("0") if leverage <= 0 else (quantity * entry_price) / Decimal(leverage)

        live_positions[symbol] = Position(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            quantity=quantity,
            margin_used=margin_used,
            leverage=leverage,
            opened_at=utc_now(),
        )

    save_positions(config, live_positions)
    return live_positions


def ensure_one_way_position_mode(client: Client, config: Config) -> None:
    if not config.live_trading or not config.ensure_one_way_mode:
        return

    try:
        mode = client.futures_get_position_mode()
        is_hedge_mode = str(mode.get("dualSidePosition", "false")).lower() == "true"
        if is_hedge_mode:
            client.futures_change_position_mode(dualSidePosition="false")
            logging.info("Режим позиций переключен в One-way.")
    except BinanceAPIException as exc:
        raise RuntimeError(
            "Не удалось включить One-way mode. Закрой открытые futures-позиции/ордера "
            "или выставь ENSURE_ONE_WAY_MODE=false, если понимаешь последствия hedge mode."
        ) from exc


def prepare_symbol_for_open_order(client: Client, config: Config, symbol: str) -> None:
    if not config.live_trading or config.use_test_order:
        return

    try:
        client.futures_change_margin_type(symbol=symbol, marginType=config.margin_type)
        logging.info("%s: тип маржи установлен %s.", symbol, config.margin_type)
    except BinanceAPIException as exc:
        if exc.code == -4046:
            logging.info("%s: тип маржи уже %s.", symbol, config.margin_type)
        else:
            logging.warning("%s: не удалось изменить тип маржи: %s", symbol, exc)

    try:
        client.futures_change_leverage(symbol=symbol, leverage=config.leverage)
        logging.info("%s: плечо установлено x%s.", symbol, config.leverage)
    except BinanceAPIException as exc:
        logging.warning("%s: не удалось изменить плечо: %s", symbol, exc)


def build_close_signal_from_opposite(position: Position, open_signal: TradeSignal) -> TradeSignal:
    reason = f"закрытие {position.direction}: противоположный сигнал. {open_signal.reason}"
    return TradeSignal(
        symbol=position.symbol,
        action=close_action_for_direction(position.direction),
        side=close_side_for_direction(position.direction),
        direction=position.direction,
        score=open_signal.score + 5_000.0,
        price=open_signal.price,
        reason=reason,
    )


def fmt_optional_float(value: float | None, width: int, precision: int, suffix: str = "") -> str:
    if value is None:
        return "-".rjust(width)
    return f"{value:{width}.{precision}f}{suffix}"


def fmt_optional_decimal(value: Decimal | None, width: int) -> str:
    if value is None:
        return "-".rjust(width)
    return f"{str(value):>{width}}"


def log_scan_summary(
    decisions: list[ScanDecision],
    open_signals: list[TradeSignal],
    close_signals: list[TradeSignal],
    config: Config,
) -> None:
    if not config.log_scan_summary:
        return

    total = len(decisions)
    skipped = sum(1 for item in decisions if item.status == "SKIPPED")
    no_signal = sum(1 for item in decisions if item.status in {"NO_SIGNAL", "HOLD"})
    open_long = sum(1 for item in open_signals if item.action == OPEN_LONG)
    open_short = sum(1 for item in open_signals if item.action == OPEN_SHORT)

    blocker_counts = {"движение": 0, "EMA": 0, "объем": 0, "RSI": 0, "данные": 0}
    for decision in decisions:
        if decision.status not in {"NO_SIGNAL", "HOLD", "SKIPPED"}:
            continue
        for blocker in decision.blockers:
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1

    logging.info(
        "Итог скана: символов=%s | LONG=%s | SHORT=%s | закрыть=%s | без входа=%s | пропуск=%s",
        total,
        open_long,
        open_short,
        len(close_signals),
        no_signal,
        skipped,
    )
    logging.info(
        "Главные причины без входа: движение=%s, объем=%s, EMA=%s, RSI=%s, данные=%s",
        blocker_counts.get("движение", 0),
        blocker_counts.get("объем", 0),
        blocker_counts.get("EMA", 0),
        blocker_counts.get("RSI", 0),
        blocker_counts.get("данные", 0),
    )

    candidates = [
        item
        for item in decisions
        if item.status in {"NO_SIGNAL", "HOLD", "OPEN_SIGNAL", "CLOSE_SIGNAL", "CLOSE_RISK"}
        and item.price is not None
    ]
    candidates.sort(key=lambda item: item.rank_score, reverse=True)
    if not candidates:
        return

    logging.info("Ближайшие кандидаты:")
    logging.info("  %-14s %-6s %9s %7s %7s %-6s %-18s %s", "SYMBOL", "DIR", "CHANGE", "RSI", "VOL", "EMA", "STATUS", "НЕ ХВАТИЛО / ПРИЧИНА")
    for item in candidates[: config.scan_summary_top_n]:
        details = item.details
        if len(details) > 72:
            details = f"{details[:69]}..."
        logging.info(
            "  %-14s %-6s %8s%% %7s %7s %-6s %-18s %s",
            item.symbol,
            item.best_direction,
            fmt_optional_float(item.pct_change, 7, 2),
            fmt_optional_float(item.rsi, 7, 1),
            fmt_optional_float(item.volume_ratio, 7, 2),
            item.ema_state,
            item.status,
            details,
        )


def scan_market(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
    positions: dict[str, Position],
) -> tuple[list[TradeSignal], list[TradeSignal], list[ScanDecision]]:
    open_signals: list[TradeSignal] = []
    close_signals: list[TradeSignal] = []
    decisions: list[ScanDecision] = []

    for symbol in symbols:
        try:
            snapshot = analyze_symbol(client, symbol, config)
            if snapshot is None:
                decisions.append(skipped_decision(symbol, "недостаточно закрытых свечей или некорректные данные"))
                if config.log_symbol_decisions:
                    logging.info("%s: пропуск анализа - недостаточно закрытых свечей или некорректные данные.", symbol)
                continue

            position = positions.get(symbol)
            if position is not None:
                risk_exit = build_risk_exit_signal(position, snapshot, config)
                if risk_exit is not None:
                    close_signals.append(risk_exit)
                    decisions.append(signal_decision(risk_exit, snapshot, "CLOSE_RISK"))
                    if config.log_symbol_decisions:
                        logging.info("%s: найден сигнал закрытия %s. %s", symbol, position.direction, risk_exit.reason)
                    continue

            signal = build_open_signal(snapshot, config)
            if signal is None:
                decisions.append(no_signal_decision(symbol, snapshot, config, "HOLD" if position is not None else "NO_SIGNAL"))
                if config.log_symbol_decisions:
                    if position is None:
                        logging.info(
                            "%s: позиция не открыта. %s. %s",
                            symbol,
                            format_market_metrics(snapshot),
                            explain_no_open_signal(snapshot, config),
                        )
                    else:
                        logging.info(
                            "%s: позиция %s удерживается. Stop/take не сработал, противоположного сигнала нет. %s. %s",
                            symbol,
                            position.direction,
                            format_market_metrics(snapshot),
                            explain_no_open_signal(snapshot, config),
                        )
                continue

            if position is None:
                open_signals.append(signal)
                decisions.append(signal_decision(signal, snapshot, "OPEN_SIGNAL"))
                if config.log_symbol_decisions:
                    logging.info(
                        "%s: найден сигнал %s. %s. Причина: %s",
                        symbol,
                        signal.action,
                        format_market_metrics(snapshot),
                        signal.reason,
                    )
            elif signal.direction != position.direction:
                close_signal = build_close_signal_from_opposite(position, signal)
                close_signals.append(close_signal)
                decisions.append(signal_decision(close_signal, snapshot, "CLOSE_SIGNAL"))
                if config.log_symbol_decisions:
                    logging.info(
                        "%s: найден противоположный сигнал, будет закрытие %s. Причина: %s",
                        symbol,
                        position.direction,
                        close_signal.reason,
                    )
            else:
                decisions.append(no_signal_decision(symbol, snapshot, config, "HOLD"))
                if config.log_symbol_decisions:
                    logging.info(
                    "%s: сигнал %s совпадает с уже открытой позицией %s, новая позиция не открывается.",
                    symbol,
                    signal.action,
                    position.direction,
                )

        except (BinanceAPIException, BinanceRequestException) as exc:
            decisions.append(skipped_decision(symbol, f"ошибка Binance: {exc}"))
            logging.warning("Ошибка Binance при сканировании %s: %s", symbol, exc)
        except Exception:
            decisions.append(skipped_decision(symbol, "неожиданная ошибка анализа"))
            logging.exception("Неожиданная ошибка при сканировании %s", symbol)
        finally:
            if config.request_sleep_seconds > 0:
                time.sleep(config.request_sleep_seconds)

    open_signals.sort(key=lambda item: item.score, reverse=True)
    close_signals.sort(key=lambda item: item.score, reverse=True)
    return open_signals, close_signals, decisions


def place_open_order(
    client: Client,
    config: Config,
    symbol_meta: SymbolMeta,
    signal: TradeSignal,
    positions: dict[str, Position],
) -> bool:
    order_size = calculate_order_size(client, config, symbol_meta, signal.price)
    if order_size is None:
        return False

    status = "СИМУЛЯЦИЯ"
    order_response: dict[str, Any] | None = None

    if config.live_trading:
        prepare_symbol_for_open_order(client, config, signal.symbol)
        params = {
            "symbol": signal.symbol,
            "side": signal.side,
            "type": ORDER_TYPE_MARKET,
            "quantity": decimal_to_str(order_size.quantity),
        }
        if config.use_test_order:
            order_response = client.futures_create_test_order(**params)
            status = "ТЕСТОВЫЙ_ОРДЕР_ПРИНЯТ_БЕЗ_ИСПОЛНЕНИЯ"
        else:
            order_response = client.futures_create_order(**params)
            status = translate_order_status(str(order_response.get("status", "SUBMITTED")))

    if not config.live_trading or not config.use_test_order:
        positions[signal.symbol] = Position(
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=signal.price,
            quantity=order_size.quantity,
            margin_used=order_size.margin,
            leverage=config.leverage,
            opened_at=utc_now(),
        )
        save_positions(config, positions)

    append_trade_log(
        config=config,
        signal=signal,
        quantity=order_size.quantity,
        notional=order_size.notional,
        margin=order_size.margin,
        risk_at_stop=order_size.risk_at_stop,
        status=status,
        order_response=order_response,
    )
    logging.info(
        "%s %s: qty=%s, notional=%s USDT, маржа=%s USDT, x%s. Причина: %s",
        signal.action,
        signal.symbol,
        order_size.quantity,
        order_size.notional.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        order_size.margin.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        config.leverage,
        signal.reason,
    )
    return True


def place_close_order(
    client: Client,
    config: Config,
    symbol_meta: SymbolMeta,
    signal: TradeSignal,
    positions: dict[str, Position],
) -> bool:
    position = positions.get(signal.symbol)
    if position is None:
        return False

    quantity = round_step(position.quantity, symbol_meta.step_size)
    if quantity < symbol_meta.min_qty:
        logging.info("Пропуск закрытия %s: количество %s ниже минимума %s.", signal.symbol, quantity, symbol_meta.min_qty)
        return False

    notional = quantity * signal.price
    margin = notional / Decimal(max(position.leverage, 1))
    risk_at_stop = Decimal("0")
    status = "СИМУЛЯЦИЯ"
    order_response: dict[str, Any] | None = None

    if config.live_trading:
        params = {
            "symbol": signal.symbol,
            "side": signal.side,
            "type": ORDER_TYPE_MARKET,
            "quantity": decimal_to_str(quantity),
            "reduceOnly": "true",
        }
        if config.use_test_order:
            order_response = client.futures_create_test_order(**params)
            status = "ТЕСТОВЫЙ_ОРДЕР_ПРИНЯТ_БЕЗ_ИСПОЛНЕНИЯ"
        else:
            order_response = client.futures_create_order(**params)
            status = translate_order_status(str(order_response.get("status", "SUBMITTED")))

    if not config.live_trading or not config.use_test_order:
        positions.pop(signal.symbol, None)
        save_positions(config, positions)

    append_trade_log(
        config=config,
        signal=signal,
        quantity=quantity,
        notional=notional,
        margin=margin,
        risk_at_stop=risk_at_stop,
        status=status,
        order_response=order_response,
    )
    logging.info(
        "%s %s: qty=%s по цене %s. Причина: %s",
        signal.action,
        signal.symbol,
        quantity,
        signal.price,
        signal.reason,
    )
    return True


def execute_cycle(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
    positions: dict[str, Position],
) -> None:
    if config.live_trading and not config.use_test_order:
        positions.clear()
        positions.update(sync_live_positions(client, config, symbols))

    logging.info("Сканирование futures-символов: %s. Открытых позиций: %s", len(symbols), len(positions))
    if config.log_scanned_symbols:
        log_symbol_chunks("Монеты текущего скана", list(symbols))
    open_signals, close_signals, decisions = scan_market(client, config, symbols, positions)
    logging.info("Найдено сигналов: открыть=%s закрыть=%s", len(open_signals), len(close_signals))
    log_scan_summary(decisions, open_signals, close_signals, config)
    if not open_signals and not close_signals:
        logging.info("Позиция не открыта: ни один символ не прошел полный набор условий стратегии.")

    trades_count = 0
    for signal in close_signals:
        if trades_count >= config.max_trades_per_cycle:
            break
        symbol_meta = symbols.get(signal.symbol)
        if symbol_meta and place_close_order(client, config, symbol_meta, signal, positions):
            trades_count += 1

    for signal in open_signals:
        if trades_count >= config.max_trades_per_cycle:
            logging.info("Остановка открытия позиций: достигнут MAX_TRADES_PER_CYCLE=%s.", config.max_trades_per_cycle)
            break
        if signal.symbol in positions:
            logging.info("Пропуск %s: позиция уже открыта.", signal.symbol)
            continue
        if len(positions) >= config.max_open_positions:
            logging.info("Остановка открытия позиций: достигнут MAX_OPEN_POSITIONS=%s.", config.max_open_positions)
            break
        symbol_meta = symbols.get(signal.symbol)
        if symbol_meta and place_open_order(client, config, symbol_meta, signal, positions):
            trades_count += 1


def sleep_until_next_cycle(minutes: int, stop_requested: dict[str, bool]) -> None:
    total_seconds = minutes * 60
    for _ in range(total_seconds):
        if stop_requested["value"]:
            return
        time.sleep(1)


def main() -> None:
    config = load_config()
    configure_logging(config)

    mode = "РЕАЛЬНЫЙ" if config.live_trading else "СИМУЛЯЦИЯ"
    endpoint = config.futures_base_url if config.futures_base_url else "live futures endpoint"
    logging.info("Запуск Binance USDT-M Futures бота. Режим: %s. Endpoint: %s", mode, endpoint)
    if config.use_test_order:
        logging.info("USE_TEST_ORDER=true: ордера будут только проверяться, без исполнения.")

    client = create_client(config)
    ensure_one_way_position_mode(client, config)

    symbols = get_usdt_futures_symbols(client, config)
    positions = sync_live_positions(client, config, symbols) if config.live_trading else load_positions(config)
    logging.info("Загружено активных USDT-M perpetual символов: %s.", len(symbols))

    stop_requested = {"value": False}

    def handle_stop(signum: int, frame: Any) -> None:
        del signum, frame
        stop_requested["value"] = True
        logging.info("Получен сигнал остановки. Завершаю текущий шаг.")

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    cycle = 0
    while not stop_requested["value"]:
        cycle += 1
        if cycle % 30 == 0:
            symbols = get_usdt_futures_symbols(client, config)
            logging.info("Обновлен список futures-символов USDT: %s.", len(symbols))

        try:
            execute_cycle(client, config, symbols, positions)
        except (BinanceAPIException, BinanceRequestException) as exc:
            logging.error("Ошибка Binance в цикле: %s", exc)
        except Exception:
            logging.exception("Неожиданная ошибка в цикле.")

        sleep_until_next_cycle(config.scan_interval_minutes, stop_requested)

    save_positions(config, positions)
    logging.info("Бот остановлен.")


if __name__ == "__main__":
    main()
