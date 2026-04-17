from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any

from binance.client import Client
from binance.enums import SIDE_BUY
from binance.exceptions import BinanceAPIException, BinanceRequestException

from bot_base import (
    Config,
    ExecutionMetrics,
    ExternalCloseEvent,
    LONG,
    Position,
    RiskState,
    STABLE_BASE_ASSETS,
    SymbolMeta,
    TradeSignal,
    local_now,
    parse_iso_datetime,
    utc_now,
)
from bot_risk import apply_risk_state_on_close, close_reason_kind, set_symbol_cooldown
from bot_storage import append_trade_row_to_storage, load_positions_from_storage, save_positions_to_storage
from bot_strategy import close_action_for_direction, close_side_for_direction


def load_positions(config: Config) -> dict[str, Position]:
    return load_positions_from_storage(config)


def save_positions(config: Config, positions: dict[str, Position]) -> None:
    save_positions_to_storage(config, positions)


def append_trade_log(
    config: Config,
    signal: TradeSignal,
    quantity: Decimal,
    notional: Decimal,
    margin: Decimal,
    risk_at_stop: Decimal,
    status: str,
    order_response: dict[str, Any] | None,
    leverage_value: int | None = None,
    entry_price: Decimal | None = None,
    reference_price: Decimal | None = None,
    fill_price: Decimal | None = None,
    commission_usdt: Decimal | None = None,
    slippage_usdt: Decimal | None = None,
    slippage_pct: Decimal | None = None,
    gross_realized_pnl: Decimal | None = None,
    realized_pnl: Decimal | None = None,
    close_kind: str = "",
) -> None:
    order_response = order_response or {}
    append_trade_row_to_storage(
        config,
        {
            "timestamp": utc_now(),
            "mode": "реальный" if config.live_trading else "симуляция",
            "market": f"{config.futures_quote_asset}-M Futures",
            "symbol": signal.symbol,
            "action": signal.action,
            "side": signal.side,
            "direction": signal.direction,
            "leverage": leverage_value if leverage_value is not None else config.leverage,
            "entry_price": "" if entry_price is None else str(entry_price),
            "reference_price": "" if reference_price is None else str(reference_price),
            "price": str(signal.price),
            "fill_price": "" if fill_price is None else str(fill_price),
            "quantity": str(quantity),
            "notional_usdt": str(notional),
            "margin_usdt": str(margin),
            "risk_at_stop_usdt": str(risk_at_stop),
            "commission_usdt": "" if commission_usdt is None else str(commission_usdt),
            "slippage_usdt": "" if slippage_usdt is None else str(slippage_usdt),
            "slippage_pct": "" if slippage_pct is None else str(slippage_pct),
            "gross_realized_pnl_usdt": "" if gross_realized_pnl is None else str(gross_realized_pnl),
            "realized_pnl_usdt": "" if realized_pnl is None else str(realized_pnl),
            "close_kind": close_kind,
            "reason": signal.reason,
            "status": status,
            "order_id": order_response.get("orderId", ""),
            "raw_response": json.dumps(order_response, ensure_ascii=False),
        },
    )


def asset_price_in_usdt(client: Client, asset: str) -> Decimal | None:
    normalized_asset = asset.strip().upper()
    if not normalized_asset or normalized_asset in STABLE_BASE_ASSETS:
        return Decimal("1")

    direct_symbol = f"{normalized_asset}USDT"
    inverse_symbol = f"USDT{normalized_asset}"
    for symbol, inverse in ((direct_symbol, False), (inverse_symbol, True)):
        try:
            price_data = client.futures_symbol_ticker(symbol=symbol)
            raw_price = Decimal(str(price_data.get("price", "0")))
            if raw_price <= 0:
                continue
            return (Decimal("1") / raw_price) if inverse else raw_price
        except (BinanceAPIException, BinanceRequestException):
            continue
    return None


def commission_to_usdt(
    client: Client,
    symbol_meta: SymbolMeta,
    commission: Decimal,
    commission_asset: str,
    fill_price: Decimal,
) -> Decimal | None:
    asset = commission_asset.strip().upper()
    if commission <= 0:
        return Decimal("0")
    if asset in {"", "USDT", symbol_meta.quote_asset.upper()}:
        return commission
    if asset == symbol_meta.base_asset.upper():
        return commission * fill_price

    asset_price = asset_price_in_usdt(client, asset)
    if asset_price is None:
        return None
    return commission * asset_price


def fill_slippage(reference_price: Decimal, avg_price: Decimal, quantity: Decimal, side: str) -> tuple[Decimal, Decimal]:
    if reference_price <= 0 or avg_price <= 0 or quantity <= 0:
        return Decimal("0"), Decimal("0")

    if side == SIDE_BUY:
        slippage_usdt = (avg_price - reference_price) * quantity
        slippage_pct = ((avg_price / reference_price) - Decimal("1")) * Decimal("100")
    else:
        slippage_usdt = (reference_price - avg_price) * quantity
        slippage_pct = ((reference_price / avg_price) - Decimal("1")) * Decimal("100")
    return slippage_usdt, slippage_pct


def build_execution_metrics(
    client: Client,
    symbol_meta: SymbolMeta,
    trades: list[dict[str, Any]],
    reference_price: Decimal,
    side: str,
) -> ExecutionMetrics | None:
    total_qty = Decimal("0")
    total_quote = Decimal("0")
    total_commission_usdt = Decimal("0")
    total_realized_pnl = Decimal("0")

    for item in trades:
        qty = Decimal(str(item.get("qty", "0")))
        price = Decimal(str(item.get("price", "0")))
        commission = Decimal(str(item.get("commission", "0")))
        commission_asset = str(item.get("commissionAsset", ""))
        total_qty += qty
        total_quote += qty * price
        total_realized_pnl += Decimal(str(item.get("realizedPnl", "0")))
        commission_usdt = commission_to_usdt(client, symbol_meta, commission, commission_asset, price)
        if commission_usdt is None:
            logging.warning(
                "%s: не удалось перевести комиссию %s %s в USDT, комиссия не будет учтена в net PnL.",
                symbol_meta.symbol,
                commission,
                commission_asset or "?",
            )
        else:
            total_commission_usdt += commission_usdt

    if total_qty <= 0:
        return None

    avg_price = total_quote / total_qty
    slippage_usdt, slippage_pct = fill_slippage(reference_price, avg_price, total_qty, side)
    return ExecutionMetrics(
        quantity=total_qty,
        avg_price=avg_price,
        notional=total_qty * avg_price,
        commission_usdt=total_commission_usdt,
        slippage_usdt=slippage_usdt,
        slippage_pct=slippage_pct,
        realized_pnl=total_realized_pnl,
    )


def fetch_order_execution_metrics(
    client: Client,
    symbol_meta: SymbolMeta,
    order_id: int | str | None,
    reference_price: Decimal,
    side: str,
    start_time_ms: int | None = None,
) -> ExecutionMetrics | None:
    if not order_id:
        return None
    params: dict[str, Any] = {"symbol": symbol_meta.symbol, "orderId": order_id}
    if start_time_ms is not None and start_time_ms > 0:
        params["startTime"] = start_time_ms
    try:
        trades = client.futures_account_trades(**params)
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.warning("%s: не удалось получить fills по orderId=%s: %s", symbol_meta.symbol, order_id, exc)
        return None
    return build_execution_metrics(client, symbol_meta, trades, reference_price, side)


def infer_close_kind_from_price(position: Position, exit_price: Decimal, config: Config) -> str:
    stop_fraction = Decimal(str(config.stop_loss_pct)) / Decimal("100")
    take_fraction = Decimal(str(config.take_profit_pct)) / Decimal("100")
    if position.direction == LONG:
        stop_price = position.entry_price * (Decimal("1") - stop_fraction)
        take_price = position.entry_price * (Decimal("1") + take_fraction)
        if exit_price <= stop_price:
            return "STOP_LOSS"
        if config.take_profit_pct > 0 and exit_price >= take_price:
            return "TAKE_PROFIT"
    else:
        stop_price = position.entry_price * (Decimal("1") + stop_fraction)
        take_price = position.entry_price * (Decimal("1") - take_fraction)
        if exit_price >= stop_price:
            return "STOP_LOSS"
        if config.take_profit_pct > 0 and exit_price <= take_price:
            return "TAKE_PROFIT"
    return "EXTERNAL_CLOSE"


def external_close_reason(position: Position, exit_price: Decimal, realized_pnl: Decimal, config: Config) -> str:
    close_kind = infer_close_kind_from_price(position, exit_price, config)
    if close_kind == "STOP_LOSS":
        prefix = "внешнее закрытие позиции: вероятно stop-loss на бирже"
    elif close_kind == "TAKE_PROFIT":
        prefix = "внешнее закрытие позиции: вероятно take-profit на бирже"
    else:
        prefix = "внешнее закрытие позиции вне бота"
    return (
        f"{prefix}. направление={position.direction}, вход={position.entry_price}, "
        f"выход={exit_price}, pnl={realized_pnl.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"
    )


def fetch_external_close_event(client: Client, config: Config, position: Position) -> ExternalCloseEvent | None:
    opened_at = parse_iso_datetime(position.opened_at)
    start_time_ms = 0
    if opened_at is not None:
        start_time_ms = int(opened_at.timestamp() * 1000)

    try:
        raw_trades = client.futures_account_trades(symbol=position.symbol, startTime=start_time_ms, limit=100)
    except (BinanceAPIException, BinanceRequestException) as exc:
        logging.warning("%s: не удалось получить account trades для внешнего закрытия: %s", position.symbol, exc)
        return None

    close_side = close_side_for_direction(position.direction)
    closing_trades = []
    for item in raw_trades:
        trade_side = str(item.get("side", "")).upper()
        if trade_side != close_side:
            continue
        trade_time = int(item.get("time", 0) or 0)
        if start_time_ms and trade_time < start_time_ms:
            continue
        closing_trades.append(item)

    if not closing_trades:
        return None

    remaining_qty = position.quantity
    selected_trades: list[dict[str, Any]] = []
    close_time_ms = 0
    for item in sorted(closing_trades, key=lambda trade: int(trade.get("time", 0) or 0), reverse=True):
        qty = Decimal(str(item.get("qty", "0")))
        if qty <= 0:
            continue
        selected_trades.append(item)
        remaining_qty -= qty
        close_time_ms = max(close_time_ms, int(item.get("time", 0) or 0))
        if remaining_qty <= Decimal("0.00000001"):
            break

    if not selected_trades:
        return None
    if remaining_qty > Decimal("0.00000001"):
        logging.warning(
            "%s: не удалось полностью восстановить fills внешнего закрытия. Не хватает qty=%s.",
            position.symbol,
            remaining_qty,
        )

    quote_asset = config.futures_quote_asset.strip().upper() or "USDT"
    base_asset = position.symbol.removesuffix(quote_asset) if position.symbol.endswith(quote_asset) else position.symbol
    symbol_meta = SymbolMeta(
        symbol=position.symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        min_qty=Decimal("0"),
        step_size=Decimal("0"),
        min_notional=Decimal("0"),
        tick_size=Decimal("0"),
        percent_price_up=Decimal("0"),
        percent_price_down=Decimal("0"),
        quantity_precision=0,
    )
    metrics = build_execution_metrics(client, symbol_meta, selected_trades, position.entry_price, close_side)
    if metrics is None:
        return None

    exit_price = metrics.avg_price
    closed_at = datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc).astimezone() if close_time_ms else local_now()
    net_realized_pnl = metrics.realized_pnl - position.entry_commission_usdt - metrics.commission_usdt
    reason = external_close_reason(position, exit_price, net_realized_pnl, config)
    signal = TradeSignal(
        symbol=position.symbol,
        action=close_action_for_direction(position.direction),
        side=close_side_for_direction(position.direction),
        direction=position.direction,
        score=0,
        price=exit_price,
        reason=reason,
    )
    return ExternalCloseEvent(
        signal=signal,
        quantity=metrics.quantity,
        exit_price=exit_price,
        close_commission_usdt=metrics.commission_usdt,
        gross_realized_pnl=metrics.realized_pnl,
        net_realized_pnl=net_realized_pnl,
        slippage_usdt=metrics.slippage_usdt,
        slippage_pct=metrics.slippage_pct,
        closed_at=closed_at,
    )


def record_external_close(
    config: Config,
    state: RiskState,
    position: Position,
    event: ExternalCloseEvent | None,
) -> None:
    if event is None:
        logging.warning(
            "%s: позиция исчезла на Binance, но не удалось восстановить детали закрытия. "
            "Дневной PnL и серия убытков не обновлены.",
            position.symbol,
        )
        if config.symbol_cooldown_minutes_after_close > 0:
            cooldown_until = set_symbol_cooldown(state, position.symbol, config.symbol_cooldown_minutes_after_close)
            logging.info("%s: установлен fallback cooldown до %s.", position.symbol, cooldown_until)
        return

    notional = event.quantity * event.exit_price
    margin = notional / Decimal(max(position.leverage, 1))
    close_kind = close_reason_kind(event.signal.reason)
    append_trade_log(
        config=config,
        signal=event.signal,
        quantity=event.quantity,
        notional=notional,
        margin=margin,
        risk_at_stop=Decimal("0"),
        status="ВНЕШНЕЕ_ЗАКРЫТИЕ",
        order_response=None,
        leverage_value=position.leverage,
        entry_price=position.entry_price,
        reference_price=position.entry_reference_price if position.entry_reference_price > 0 else position.entry_price,
        fill_price=event.exit_price,
        commission_usdt=event.close_commission_usdt + position.entry_commission_usdt,
        slippage_usdt=event.slippage_usdt,
        slippage_pct=event.slippage_pct,
        gross_realized_pnl=event.gross_realized_pnl,
        realized_pnl=event.net_realized_pnl,
        close_kind=close_kind,
    )
    apply_risk_state_on_close(
        config=config,
        state=state,
        symbol=position.symbol,
        realized_pnl=event.net_realized_pnl,
        close_reason=event.signal.reason,
        closed_at=event.closed_at,
    )
    logging.info(
        "%s: обнаружено внешнее закрытие. Выход=%s, qty=%s, gross PnL=%s USDT, net PnL=%s USDT, комиссия=%s USDT.",
        position.symbol,
        event.exit_price,
        event.quantity,
        event.gross_realized_pnl.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        event.net_realized_pnl.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        (event.close_commission_usdt + position.entry_commission_usdt).quantize(Decimal("0.01"), rounding=ROUND_DOWN),
    )
