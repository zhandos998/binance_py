from __future__ import annotations

import logging
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

from binance.client import Client
from binance.enums import ORDER_TYPE_MARKET, SIDE_BUY, SIDE_SELL
from binance.exceptions import BinanceAPIException, BinanceRequestException

from bot_base import (
    LONG,
    OPEN_LONG,
    OPEN_SHORT,
    ORDER_TYPE_STOP_MARKET,
    ORDER_TYPE_TAKE_PROFIT_MARKET,
    OrderSize,
    Position,
    RiskState,
    SHORT,
    Config,
    SymbolMeta,
    TradeSignal,
    decimal_to_str,
    translate_order_status,
    utc_now,
)
from bot_exchange import futures_available_usdt, market_entry_passes_percent_filter
from bot_math import extract_decimal_from_order, normalize_stop_price, round_price, round_price_up, round_step
from bot_risk import apply_risk_state_on_close, calculate_realized_pnl, close_reason_kind, save_risk_state
from bot_state import (
    append_trade_log,
    fetch_external_close_event,
    fetch_order_execution_metrics,
    load_positions,
    record_external_close,
    save_positions,
)
from bot_strategy import close_side_for_direction


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
        logging.info("%s: пропуск, STOP_LOSS_PCT должен быть больше 0.", symbol_meta.symbol)
        return None

    max_notional_by_risk = risk_budget / stop_fraction
    max_notional_by_margin = config.max_margin_usdt * Decimal(config.leverage)
    notional = min(max_notional_by_risk, max_notional_by_margin)

    effective_min_notional = max(config.min_notional_usdt, symbol_meta.min_notional)
    if max_notional_by_margin < effective_min_notional:
        logging.info(
            "%s: MAX_MARGIN_USDT=%s * LEVERAGE=%s дает максимум %s USDT notional, а минимум для сделки %s USDT.",
            symbol_meta.symbol,
            config.max_margin_usdt,
            config.leverage,
            max_notional_by_margin.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            effective_min_notional,
        )
        return None

    if notional < effective_min_notional:
        logging.info(
            "%s: расчетный notional %s USDT ниже минимума %s USDT.",
            symbol_meta.symbol,
            notional.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            effective_min_notional,
        )
        return None

    margin = notional / Decimal(config.leverage)
    if margin < config.min_margin_usdt:
        logging.info(
            "%s: маржа %s USDT ниже MIN_MARGIN_USDT=%s.",
            symbol_meta.symbol,
            margin.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            config.min_margin_usdt,
        )
        return None

    quantity = round_step(notional / price, symbol_meta.step_size)
    if quantity < symbol_meta.min_qty:
        logging.info(
            "%s: количество %s ниже минимального %s.",
            symbol_meta.symbol,
            quantity,
            symbol_meta.min_qty,
        )
        return None

    actual_notional = quantity * price
    if actual_notional < effective_min_notional:
        logging.info(
            "%s: notional после округления %s USDT ниже минимума %s USDT.",
            symbol_meta.symbol,
            actual_notional.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            effective_min_notional,
        )
        return None

    actual_margin = actual_notional / Decimal(config.leverage)
    risk_at_stop = actual_notional * stop_fraction
    return OrderSize(quantity, actual_notional, actual_margin, risk_at_stop)


def protection_prices(
    config: Config,
    symbol_meta: SymbolMeta,
    direction: str,
    entry_price: Decimal,
    trigger_reference_price: Decimal | None = None,
) -> tuple[Decimal, Decimal]:
    stop_fraction = Decimal(str(config.stop_loss_pct)) / Decimal("100")
    take_fraction = Decimal(str(config.take_profit_pct)) / Decimal("100")
    buffer_fraction = Decimal(str(config.protection_trigger_buffer_pct)) / Decimal("100")

    if direction == LONG:
        raw_stop = entry_price * (Decimal("1") - stop_fraction)
        raw_take = entry_price * (Decimal("1") + take_fraction)
    else:
        raw_stop = entry_price * (Decimal("1") + stop_fraction)
        raw_take = entry_price * (Decimal("1") - take_fraction)

    stop_price = normalize_stop_price(raw_stop, symbol_meta.tick_size, entry_price, direction, True)
    take_price = normalize_stop_price(raw_take, symbol_meta.tick_size, entry_price, direction, False)

    if trigger_reference_price and trigger_reference_price > 0:
        if direction == LONG:
            max_stop = trigger_reference_price * (Decimal("1") - buffer_fraction)
            min_take = trigger_reference_price * (Decimal("1") + buffer_fraction)
            if stop_price > max_stop:
                stop_price = round_price(max_stop, symbol_meta.tick_size)
            if take_price < min_take:
                take_price = round_price_up(min_take, symbol_meta.tick_size)
        else:
            min_stop = trigger_reference_price * (Decimal("1") + buffer_fraction)
            max_take = trigger_reference_price * (Decimal("1") - buffer_fraction)
            if stop_price < min_stop:
                stop_price = round_price_up(min_stop, symbol_meta.tick_size)
            if take_price > max_take:
                take_price = round_price(max_take, symbol_meta.tick_size)

        stop_price = normalize_stop_price(stop_price, symbol_meta.tick_size, trigger_reference_price, direction, True)
        take_price = normalize_stop_price(take_price, symbol_meta.tick_size, trigger_reference_price, direction, False)

    return stop_price, take_price


def current_protection_trigger_price(client: Client, config: Config, symbol: str, fallback: Decimal) -> Decimal:
    if not config.live_trading:
        return fallback
    try:
        if config.protection_working_type == "MARK_PRICE":
            mark_data = client.futures_mark_price(symbol=symbol)
            mark_price = Decimal(str(mark_data.get("markPrice", "0")))
            if mark_price > 0:
                return mark_price
        ticker = client.futures_symbol_ticker(symbol=symbol)
        last_price = Decimal(str(ticker.get("price", "0")))
        if last_price > 0:
            return last_price
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.warning("%s: не удалось получить trigger reference price для проверки SL/TP: %s", symbol, exc)
    return fallback


def adjusted_trigger_price(
    reference_price: Decimal,
    symbol_meta: SymbolMeta,
    direction: str,
    is_stop_loss: bool,
    buffer_pct: float,
) -> Decimal:
    buffer_fraction = Decimal(str(buffer_pct)) / Decimal("100")
    if direction == LONG:
        if is_stop_loss:
            return round_price(reference_price * (Decimal("1") - buffer_fraction), symbol_meta.tick_size)
        return round_price_up(reference_price * (Decimal("1") + buffer_fraction), symbol_meta.tick_size)

    if is_stop_loss:
        return round_price_up(reference_price * (Decimal("1") + buffer_fraction), symbol_meta.tick_size)
    return round_price(reference_price * (Decimal("1") - buffer_fraction), symbol_meta.tick_size)


def protection_client_order_id(prefix: str, symbol: str) -> str:
    millis = int(time.time() * 1000) % 1_000_000_000
    return f"bot_{prefix}_{symbol.lower()}_{millis}"[:36]


def build_protection_order_params(
    config: Config,
    signal: TradeSignal,
    order_type: str,
    stop_price: Decimal,
    client_order_id: str,
) -> dict[str, str]:
    params = {
        "algoType": "CONDITIONAL",
        "symbol": signal.symbol,
        "side": close_side_for_direction(signal.direction),
        "type": order_type,
        "triggerPrice": decimal_to_str(stop_price),
        "closePosition": "true",
        "workingType": config.protection_working_type,
        "clientAlgoId": client_order_id,
    }
    if config.protection_price_protect:
        params["priceProtect"] = "TRUE"
    return params


def futures_create_algo_order(client: Client, **params: str) -> dict[str, Any]:
    return client._request_futures_api("post", "algoOrder", True, data=params)


def futures_cancel_algo_order(client: Client, algo_id: str) -> dict[str, Any]:
    return client._request_futures_api("delete", "algoOrder", True, data={"algoId": algo_id})


def futures_open_algo_orders(client: Client, symbol: str) -> list[dict[str, Any]]:
    response = client._request_futures_api("get", "openAlgoOrders", True, data={"symbol": symbol})
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ("orders", "data", "list"):
            value = response.get(key)
            if isinstance(value, list):
                return value
    return []


def find_existing_protection_algo_ids(
    client: Client,
    symbol: str,
    close_side: str,
) -> tuple[str | None, str | None]:
    stop_algo_id: str | None = None
    take_profit_algo_id: str | None = None
    try:
        open_orders = futures_open_algo_orders(client, symbol)
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.warning("%s: не удалось получить открытые algo-ордера: %s", symbol, exc)
        return None, None

    for order in open_orders:
        order_side = str(order.get("side", ""))
        order_type = str(order.get("type", ""))
        close_position = str(order.get("closePosition", "")).lower()
        reduce_only = str(order.get("reduceOnly", "")).lower()
        if order_side != close_side:
            continue
        if close_position != "true" and reduce_only != "true":
            continue

        algo_id = str(order.get("algoId", order.get("orderId", "")))
        if not algo_id:
            continue
        if order_type == ORDER_TYPE_STOP_MARKET and stop_algo_id is None:
            stop_algo_id = algo_id
        elif order_type == ORDER_TYPE_TAKE_PROFIT_MARKET and take_profit_algo_id is None:
            take_profit_algo_id = algo_id

    return stop_algo_id, take_profit_algo_id


def create_protection_algo_order_with_retry(
    client: Client,
    config: Config,
    symbol_meta: SymbolMeta,
    signal: TradeSignal,
    params: dict[str, str],
    is_stop_loss: bool,
) -> dict[str, Any]:
    try:
        return futures_create_algo_order(client, **params)
    except BinanceAPIException as exc:
        if exc.code != -2021:
            raise

        reference_price = current_protection_trigger_price(client, config, signal.symbol, signal.price)
        retry_buffer_pct = max(config.protection_trigger_buffer_pct * 3, 0.50)
        retry_trigger_price = adjusted_trigger_price(
            reference_price=reference_price,
            symbol_meta=symbol_meta,
            direction=signal.direction,
            is_stop_loss=is_stop_loss,
            buffer_pct=retry_buffer_pct,
        )
        retry_params = dict(params)
        retry_params["triggerPrice"] = decimal_to_str(retry_trigger_price)
        retry_params["clientAlgoId"] = protection_client_order_id(
            "slr" if is_stop_loss else "tpr",
            signal.symbol,
        )
        logging.warning(
            "%s: защитный algo-ордер %s с trigger=%s сработал бы сразу. Повтор: trigger=%s, reference=%s, buffer=%.2f%%.",
            signal.symbol,
            "SL" if is_stop_loss else "TP",
            params.get("triggerPrice"),
            retry_trigger_price,
            reference_price,
            retry_buffer_pct,
        )
        return futures_create_algo_order(client, **retry_params)


def place_protection_orders(
    client: Client,
    config: Config,
    symbol_meta: SymbolMeta,
    signal: TradeSignal,
    entry_price: Decimal,
) -> tuple[str | None, str | None]:
    if not config.place_protection_orders:
        return None, None

    trigger_reference_price = current_protection_trigger_price(client, config, signal.symbol, entry_price)
    stop_price, take_price = protection_prices(
        config,
        symbol_meta,
        signal.direction,
        entry_price,
        trigger_reference_price,
    )
    if trigger_reference_price != entry_price:
        logging.info(
            "%s: расчет защиты от entry=%s проверен по MARK_PRICE=%s. SL=%s, TP=%s.",
            signal.symbol,
            entry_price,
            trigger_reference_price,
            stop_price,
            take_price if config.take_profit_pct > 0 else "выключен",
        )
    stop_params = build_protection_order_params(
        config,
        signal,
        ORDER_TYPE_STOP_MARKET,
        stop_price,
        protection_client_order_id("sl", signal.symbol),
    )
    take_params = None
    if config.take_profit_pct > 0:
        take_params = build_protection_order_params(
            config,
            signal,
            ORDER_TYPE_TAKE_PROFIT_MARKET,
            take_price,
            protection_client_order_id("tp", signal.symbol),
        )

    if not config.live_trading:
        logging.info(
            "Защитные ордера %s (симуляция): SL=%s, TP=%s, workingType=%s.",
            signal.symbol,
            stop_price,
            take_price if take_params else "выключен",
            config.protection_working_type,
        )
        return None, None

    stop_order_id: str | None = None
    take_profit_order_id: str | None = None

    existing_stop_id, existing_take_profit_id = find_existing_protection_algo_ids(
        client,
        signal.symbol,
        close_side_for_direction(signal.direction),
    )
    if existing_stop_id and (existing_take_profit_id or config.take_profit_pct <= 0):
        logging.info(
            "%s: защитные algo-ордера уже есть на Binance: SL=%s, TP=%s.",
            signal.symbol,
            existing_stop_id,
            existing_take_profit_id or "выключен",
        )
        return existing_stop_id, existing_take_profit_id

    if config.use_test_order:
        logging.info(
            "Защитные algo-ордера %s не размещаются при USE_TEST_ORDER=true. Расчет: SL=%s, TP=%s.",
            signal.symbol,
            stop_price,
            take_price if take_params else "выключен",
        )
        return None, None

    if existing_stop_id:
        stop_order_id = existing_stop_id
    else:
        try:
            stop_response = create_protection_algo_order_with_retry(
                client,
                config,
                symbol_meta,
                signal,
                stop_params,
                True,
            )
            stop_order_id = str(stop_response.get("algoId", ""))
        except BinanceAPIException as exc:
            if exc.code != -4130:
                raise
            stop_order_id, existing_take_profit_id = find_existing_protection_algo_ids(
                client,
                signal.symbol,
                close_side_for_direction(signal.direction),
            )
            if not stop_order_id:
                raise
            logging.info("%s: SL уже существовал на Binance, привязан algoId=%s.", signal.symbol, stop_order_id)

    if take_params:
        if existing_take_profit_id:
            take_profit_order_id = existing_take_profit_id
        else:
            try:
                take_response = create_protection_algo_order_with_retry(
                    client,
                    config,
                    symbol_meta,
                    signal,
                    take_params,
                    False,
                )
                take_profit_order_id = str(take_response.get("algoId", ""))
            except BinanceAPIException as exc:
                if exc.code == -4130:
                    _, take_profit_order_id = find_existing_protection_algo_ids(
                        client,
                        signal.symbol,
                        close_side_for_direction(signal.direction),
                    )
                    if take_profit_order_id:
                        logging.info("%s: TP уже существовал на Binance, привязан algoId=%s.", signal.symbol, take_profit_order_id)
                    else:
                        raise
                else:
                    if stop_order_id and stop_order_id != existing_stop_id:
                        try:
                            futures_cancel_algo_order(client, stop_order_id)
                            logging.warning("%s: SL algoId=%s отменен, потому что TP не был поставлен.", signal.symbol, stop_order_id)
                        except BinanceAPIException as cancel_exc:
                            logging.warning("%s: не удалось отменить SL algoId=%s после ошибки TP: %s", signal.symbol, stop_order_id, cancel_exc)
                    raise
            except Exception:
                if stop_order_id and stop_order_id != existing_stop_id:
                    try:
                        futures_cancel_algo_order(client, stop_order_id)
                        logging.warning("%s: SL algoId=%s отменен, потому что TP не был поставлен.", signal.symbol, stop_order_id)
                    except BinanceAPIException as cancel_exc:
                        logging.warning("%s: не удалось отменить SL algoId=%s после ошибки TP: %s", signal.symbol, stop_order_id, cancel_exc)
                raise
    logging.info(
        "Поставлены биржевые защитные algo-ордера %s: SL=%s algoId=%s, TP=%s algoId=%s.",
        signal.symbol,
        stop_price,
        stop_order_id,
        take_price if take_params else "выключен",
        take_profit_order_id or "-",
    )
    return stop_order_id or None, take_profit_order_id or None


def cancel_protection_orders(client: Client, config: Config, position: Position) -> None:
    if not config.live_trading or config.use_test_order or not config.cancel_protection_on_close:
        return

    order_ids = [position.stop_order_id, position.take_profit_order_id]
    for order_id in order_ids:
        if not order_id:
            continue
        try:
            futures_cancel_algo_order(client, order_id)
            logging.info("%s: защитный algo-ордер %s отменен.", position.symbol, order_id)
        except BinanceAPIException as exc:
            if exc.code in {-2011, -2013}:
                logging.info("%s: защитный algo-ордер %s уже не активен.", position.symbol, order_id)
            else:
                logging.warning("%s: не удалось отменить защитный algo-ордер %s: %s", position.symbol, order_id, exc)


def ensure_position_has_protection(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
    position: Position,
) -> Position:
    if (
        not config.live_trading
        or config.use_test_order
        or not config.place_protection_orders
        or (position.stop_order_id and (position.take_profit_order_id or config.take_profit_pct <= 0))
    ):
        return position

    symbol_meta = symbols.get(position.symbol)
    if symbol_meta is None:
        return position

    signal = TradeSignal(
        symbol=position.symbol,
        action=OPEN_LONG if position.direction == LONG else OPEN_SHORT,
        side=SIDE_BUY if position.direction == LONG else SIDE_SELL,
        direction=position.direction,
        score=0,
        price=position.entry_price,
        reason="восстановление защитных ордеров для уже открытой позиции",
    )
    try:
        stop_order_id, take_profit_order_id = place_protection_orders(
            client=client,
            config=config,
            symbol_meta=symbol_meta,
            signal=signal,
            entry_price=position.entry_price,
        )
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.error("%s: не удалось восстановить защитные algo-ордера: %s", position.symbol, exc)
        return position
    except Exception:
        logging.exception("%s: неожиданная ошибка при восстановлении защитных algo-ордеров.", position.symbol)
        return position

    return Position(
        symbol=position.symbol,
        direction=position.direction,
        entry_price=position.entry_price,
        quantity=position.quantity,
        margin_used=position.margin_used,
        leverage=position.leverage,
        opened_at=position.opened_at,
        entry_reference_price=position.entry_reference_price,
        entry_commission_usdt=position.entry_commission_usdt,
        entry_slippage_usdt=position.entry_slippage_usdt,
        stop_order_id=stop_order_id,
        take_profit_order_id=take_profit_order_id,
    )


def sync_live_positions(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
    risk_state: RiskState,
) -> dict[str, Position]:
    if not config.live_trading or config.use_test_order:
        return load_positions(config)

    known_positions = load_positions(config)
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
        known_position = known_positions.get(symbol)

        position = Position(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            quantity=quantity,
            margin_used=margin_used,
            leverage=leverage,
            opened_at=known_position.opened_at if known_position else utc_now(),
            entry_reference_price=known_position.entry_reference_price if known_position else entry_price,
            entry_commission_usdt=known_position.entry_commission_usdt if known_position else Decimal("0"),
            entry_slippage_usdt=known_position.entry_slippage_usdt if known_position else Decimal("0"),
            stop_order_id=known_position.stop_order_id if known_position else None,
            take_profit_order_id=known_position.take_profit_order_id if known_position else None,
        )
        live_positions[symbol] = ensure_position_has_protection(client, config, symbols, position)

    for symbol, known_position in known_positions.items():
        if symbol not in live_positions:
            external_close_event = fetch_external_close_event(client, config, known_position)
            record_external_close(config, risk_state, known_position, external_close_event)
            cancel_protection_orders(client, config, known_position)

    save_positions(config, live_positions)
    save_risk_state(config, risk_state)
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

    if not market_entry_passes_percent_filter(client, config, symbol_meta, signal.side):
        return False

    status = "СИМУЛЯЦИЯ"
    order_response: dict[str, Any] | None = None
    order_start_time_ms = int(time.time() * 1000)

    if config.live_trading:
        prepare_symbol_for_open_order(client, config, signal.symbol)
        params = {
            "symbol": signal.symbol,
            "side": signal.side,
            "type": ORDER_TYPE_MARKET,
            "quantity": decimal_to_str(order_size.quantity),
            "newOrderRespType": "RESULT",
        }
        if config.use_test_order:
            order_response = client.futures_create_test_order(**params)
            status = "ТЕСТОВЫЙ_ОРДЕР_ПРИНЯТ_БЕЗ_ИСПОЛНЕНИЯ"
        else:
            try:
                order_response = client.futures_create_order(**params)
                status = translate_order_status(str(order_response.get("status", "SUBMITTED")))
            except BinanceAPIException as exc:
                if exc.code == -4131:
                    logging.warning(
                        "%s: market-вход пропущен: counterparty best price не прошел PERCENT_PRICE фильтр Binance.",
                        signal.symbol,
                    )
                    return False
                raise

    entry_price = extract_decimal_from_order(order_response, ("avgPrice", "price"), signal.price)
    executed_qty = extract_decimal_from_order(order_response, ("executedQty", "origQty"), order_size.quantity)
    execution_metrics = None
    if config.live_trading and not config.use_test_order:
        execution_metrics = fetch_order_execution_metrics(
            client=client,
            symbol_meta=symbol_meta,
            order_id=(order_response or {}).get("orderId"),
            reference_price=signal.price,
            side=signal.side,
            start_time_ms=order_start_time_ms,
        )
        if execution_metrics is not None:
            entry_price = execution_metrics.avg_price
            executed_qty = execution_metrics.quantity
    stop_order_id: str | None = None
    take_profit_order_id: str | None = None
    try:
        stop_order_id, take_profit_order_id = place_protection_orders(
            client=client,
            config=config,
            symbol_meta=symbol_meta,
            signal=signal,
            entry_price=entry_price,
        )
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.error("%s: позиция открыта, но защитные ордера не поставлены: %s", signal.symbol, exc)
    except Exception:
        logging.exception("%s: позиция открыта, но произошла ошибка при постановке защитных ордеров.", signal.symbol)

    if not config.live_trading or not config.use_test_order:
        actual_notional = executed_qty * entry_price
        positions[signal.symbol] = Position(
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=entry_price,
            quantity=executed_qty,
            margin_used=actual_notional / Decimal(config.leverage),
            leverage=config.leverage,
            opened_at=utc_now(),
            entry_reference_price=signal.price,
            entry_commission_usdt=execution_metrics.commission_usdt if execution_metrics is not None else Decimal("0"),
            entry_slippage_usdt=execution_metrics.slippage_usdt if execution_metrics is not None else Decimal("0"),
            stop_order_id=stop_order_id,
            take_profit_order_id=take_profit_order_id,
        )
        save_positions(config, positions)

    append_trade_log(
        config=config,
        signal=signal,
        quantity=executed_qty,
        notional=executed_qty * entry_price,
        margin=(executed_qty * entry_price) / Decimal(config.leverage),
        risk_at_stop=(executed_qty * entry_price) * (Decimal(str(config.stop_loss_pct)) / Decimal("100")),
        status=status,
        order_response=order_response,
        leverage_value=config.leverage,
        entry_price=entry_price,
        reference_price=signal.price,
        fill_price=entry_price,
        commission_usdt=execution_metrics.commission_usdt if execution_metrics is not None else Decimal("0"),
        slippage_usdt=execution_metrics.slippage_usdt if execution_metrics is not None else Decimal("0"),
        slippage_pct=execution_metrics.slippage_pct if execution_metrics is not None else Decimal("0"),
    )
    logging.info(
        "%s %s: qty=%s, notional=%s USDT, маржа=%s USDT, x%s, комиссия=%s USDT, slippage=%s USDT. Причина: %s",
        signal.action,
        signal.symbol,
        executed_qty,
        (executed_qty * entry_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        ((executed_qty * entry_price) / Decimal(config.leverage)).quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        config.leverage,
        (execution_metrics.commission_usdt if execution_metrics is not None else Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        (execution_metrics.slippage_usdt if execution_metrics is not None else Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        signal.reason,
    )
    return True


def place_close_order(
    client: Client,
    config: Config,
    symbol_meta: SymbolMeta,
    signal: TradeSignal,
    positions: dict[str, Position],
    risk_state: RiskState,
) -> bool:
    position = positions.get(signal.symbol)
    if position is None:
        return False

    quantity = round_step(position.quantity, symbol_meta.step_size)
    if quantity < symbol_meta.min_qty:
        logging.info("%s: пропуск закрытия, количество %s ниже минимума %s.", signal.symbol, quantity, symbol_meta.min_qty)
        return False

    notional = quantity * signal.price
    margin = notional / Decimal(max(position.leverage, 1))
    risk_at_stop = Decimal("0")
    status = "СИМУЛЯЦИЯ"
    order_response: dict[str, Any] | None = None
    order_start_time_ms = int(time.time() * 1000)

    if config.live_trading:
        cancel_protection_orders(client, config, position)
        params = {
            "symbol": signal.symbol,
            "side": signal.side,
            "type": ORDER_TYPE_MARKET,
            "quantity": decimal_to_str(quantity),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        }
        if config.use_test_order:
            order_response = client.futures_create_test_order(**params)
            status = "ТЕСТОВЫЙ_ОРДЕР_ПРИНЯТ_БЕЗ_ИСПОЛНЕНИЯ"
        else:
            order_response = client.futures_create_order(**params)
            status = translate_order_status(str(order_response.get("status", "SUBMITTED")))

    exit_price = extract_decimal_from_order(order_response, ("avgPrice", "price"), signal.price)
    execution_metrics = None
    if config.live_trading and not config.use_test_order:
        execution_metrics = fetch_order_execution_metrics(
            client=client,
            symbol_meta=symbol_meta,
            order_id=(order_response or {}).get("orderId"),
            reference_price=signal.price,
            side=signal.side,
            start_time_ms=order_start_time_ms,
        )
        if execution_metrics is not None:
            exit_price = execution_metrics.avg_price
            quantity = execution_metrics.quantity

    gross_realized_pnl = execution_metrics.realized_pnl if execution_metrics is not None else calculate_realized_pnl(position, exit_price, quantity)
    close_commission_usdt = execution_metrics.commission_usdt if execution_metrics is not None else Decimal("0")
    total_commission_usdt = position.entry_commission_usdt + close_commission_usdt
    net_realized_pnl = gross_realized_pnl - total_commission_usdt
    notional = quantity * exit_price
    margin = notional / Decimal(max(position.leverage, 1))
    if not config.live_trading or not config.use_test_order:
        positions.pop(signal.symbol, None)
        save_positions(config, positions)
        apply_risk_state_on_close(
            config=config,
            state=risk_state,
            symbol=signal.symbol,
            realized_pnl=net_realized_pnl,
            close_reason=signal.reason,
        )
        save_risk_state(config, risk_state)

    append_trade_log(
        config=config,
        signal=signal,
        quantity=quantity,
        notional=notional,
        margin=margin,
        risk_at_stop=risk_at_stop,
        status=status,
        order_response=order_response,
        leverage_value=position.leverage,
        entry_price=position.entry_price,
        reference_price=signal.price,
        fill_price=exit_price,
        commission_usdt=total_commission_usdt,
        slippage_usdt=execution_metrics.slippage_usdt if execution_metrics is not None else Decimal("0"),
        slippage_pct=execution_metrics.slippage_pct if execution_metrics is not None else Decimal("0"),
        gross_realized_pnl=gross_realized_pnl,
        realized_pnl=net_realized_pnl,
        close_kind=close_reason_kind(signal.reason),
    )
    logging.info(
        "%s %s: qty=%s по цене %s, gross PnL=%s USDT, net PnL=%s USDT, комиссия=%s USDT, slippage=%s USDT. Причина: %s",
        signal.action,
        signal.symbol,
        quantity,
        exit_price,
        gross_realized_pnl.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        net_realized_pnl.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        total_commission_usdt.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        (execution_metrics.slippage_usdt if execution_metrics is not None else Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        signal.reason,
    )
    return True
