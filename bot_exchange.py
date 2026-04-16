from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from binance.client import Client
from binance.enums import SIDE_BUY
from binance.exceptions import BinanceAPIException, BinanceRequestException

from bot_base import Config, EXCLUDED_BASE_SUFFIXES, STABLE_BASE_ASSETS, SymbolMeta


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


def get_tick_size(filters: list[dict[str, Any]]) -> Decimal:
    return decimal_from_filter(filters, "PRICE_FILTER", "tickSize", "0")


def get_percent_price_values(filters: list[dict[str, Any]]) -> tuple[Decimal, Decimal]:
    multiplier_up = decimal_from_filter(filters, "PERCENT_PRICE", "multiplierUp", "0")
    multiplier_down = decimal_from_filter(filters, "PERCENT_PRICE", "multiplierDown", "0")
    if multiplier_up <= 0:
        multiplier_up = Decimal("1.05")
    if multiplier_down <= 0:
        multiplier_down = Decimal("0.95")
    return multiplier_up, multiplier_down


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
        tick_size = get_tick_size(filters)
        percent_price_up, percent_price_down = get_percent_price_values(filters)
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
            tick_size=tick_size,
            percent_price_up=percent_price_up,
            percent_price_down=percent_price_down,
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


def futures_available_usdt(client: Client, config: Config) -> Decimal:
    if not config.live_trading:
        return config.dry_run_usdt_balance

    balances = client.futures_account_balance()
    for item in balances:
        if item.get("asset") == "USDT":
            return Decimal(str(item.get("availableBalance", item.get("balance", "0"))))
    return Decimal("0")


def get_entry_reference_prices(client: Client, symbol: str) -> tuple[Decimal, Decimal, Decimal]:
    mark_data = client.futures_mark_price(symbol=symbol)
    orderbook = client.futures_orderbook_ticker(symbol=symbol)
    mark_price = Decimal(str(mark_data.get("markPrice", "0")))
    bid_price = Decimal(str(orderbook.get("bidPrice", "0")))
    ask_price = Decimal(str(orderbook.get("askPrice", "0")))
    return mark_price, bid_price, ask_price


def market_entry_passes_percent_filter(client: Client, config: Config, symbol_meta: SymbolMeta, side: str) -> bool:
    if not config.live_trading:
        return True

    try:
        mark_price, bid_price, ask_price = get_entry_reference_prices(client, symbol_meta.symbol)
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.warning("%s: Не удалось проверить PERCENT_PRICE перед входом: %s", symbol_meta.symbol, exc)
        return False

    if mark_price <= 0 or bid_price <= 0 or ask_price <= 0:
        logging.info("%s: пропуск входа - некорректные bid/ask/mark цены.", symbol_meta.symbol)
        return False

    if side == SIDE_BUY:
        limit_price = mark_price * symbol_meta.percent_price_up
        if ask_price > limit_price:
            logging.info(
                "%s: пропуск BUY - ask=%s выше PERCENT_PRICE лимита %s от mark=%s.",
                symbol_meta.symbol,
                ask_price,
                limit_price,
                mark_price,
            )
            return False
    else:
        limit_price = mark_price * symbol_meta.percent_price_down
        if bid_price < limit_price:
            logging.info(
                "%s: пропуск SELL - bid=%s ниже PERCENT_PRICE лимита %s от mark=%s.",
                symbol_meta.symbol,
                bid_price,
                limit_price,
                mark_price,
            )
            return False

    return True
