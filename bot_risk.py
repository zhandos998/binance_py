from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

from bot_base import Config, OrderSize, Position, RiskState, SymbolMeta, current_local_day, local_now, parse_iso_datetime
from bot_math import round_step
from bot_storage import load_risk_state_from_storage, save_risk_state_to_storage


def default_risk_state() -> RiskState:
    return RiskState(
        day=current_local_day(),
        daily_realized_pnl=Decimal("0"),
        consecutive_losses=0,
        cooldowns={},
    )


def normalize_risk_state(state: RiskState, now: datetime | None = None) -> bool:
    changed = False
    current_time = now or local_now()
    current_day = current_time.date().isoformat()
    if state.day != current_day:
        state.day = current_day
        state.daily_realized_pnl = Decimal("0")
        changed = True

    active_cooldowns: dict[str, str] = {}
    for symbol, until_value in state.cooldowns.items():
        until_dt = parse_iso_datetime(until_value)
        if until_dt is None:
            changed = True
            continue
        if until_dt <= current_time:
            changed = True
            continue
        active_cooldowns[symbol] = until_dt.isoformat(timespec="seconds")

    if active_cooldowns != state.cooldowns:
        state.cooldowns = active_cooldowns
        changed = True
    return changed


def load_risk_state(config: Config) -> RiskState:
    state = load_risk_state_from_storage(config)
    if state is None:
        return default_risk_state()
    normalize_risk_state(state)
    return state


def save_risk_state(config: Config, state: RiskState) -> None:
    normalize_risk_state(state)
    save_risk_state_to_storage(config, state)


def format_remaining_time(seconds: int) -> str:
    if seconds <= 0:
        return "0с"
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours}ч")
    if minutes > 0:
        parts.append(f"{minutes}м")
    if secs > 0 and not parts:
        parts.append(f"{secs}с")
    return " ".join(parts)


def cooldown_remaining_text(state: RiskState, symbol: str, now: datetime | None = None) -> str | None:
    normalize_risk_state(state, now)
    until_value = state.cooldowns.get(symbol)
    until_dt = parse_iso_datetime(until_value)
    current_time = now or local_now()
    if until_dt is None or until_dt <= current_time:
        return None
    remaining_seconds = int((until_dt - current_time).total_seconds())
    return format_remaining_time(remaining_seconds)


def set_symbol_cooldown(state: RiskState, symbol: str, minutes: int, now: datetime | None = None) -> str | None:
    if minutes <= 0:
        return None
    current_time = now or local_now()
    until_dt = current_time + timedelta(minutes=minutes)
    state.cooldowns[symbol] = until_dt.isoformat(timespec="seconds")
    return state.cooldowns[symbol]


def openings_blocked_reason(config: Config, state: RiskState, now: datetime | None = None) -> str | None:
    normalize_risk_state(state, now)
    if config.max_daily_loss_usdt > 0 and state.daily_realized_pnl <= -config.max_daily_loss_usdt:
        return (
            f"достигнут MAX_DAILY_LOSS_USDT={config.max_daily_loss_usdt}. "
            f"Реализованный PnL за {state.day}: {state.daily_realized_pnl.quantize(Decimal('0.01'), rounding=ROUND_DOWN)} USDT."
        )
    if config.max_consecutive_losses > 0 and state.consecutive_losses >= config.max_consecutive_losses:
        return (
            f"достигнут MAX_CONSECUTIVE_LOSSES={config.max_consecutive_losses}. "
            f"Текущая серия убытков: {state.consecutive_losses}."
        )
    return None


def close_reason_kind(reason: str) -> str:
    text = reason.strip().lower()
    if "стоп-лосс" in text or "stop-loss" in text:
        return "STOP_LOSS"
    if "тейк-профит" in text or "take-profit" in text:
        return "TAKE_PROFIT"
    if text.startswith("закрытие"):
        return "SIGNAL_CLOSE"
    if text.startswith("внешнее закрытие"):
        return "EXTERNAL_CLOSE"
    return "CLOSE"


def calculate_realized_pnl(position: Position, exit_price: Decimal, quantity: Decimal) -> Decimal:
    if position.direction == "LONG":
        return (exit_price - position.entry_price) * quantity
    return (position.entry_price - exit_price) * quantity


def apply_risk_state_on_close(
    config: Config,
    state: RiskState,
    symbol: str,
    realized_pnl: Decimal,
    close_reason: str,
    closed_at: datetime | None = None,
) -> None:
    current_time = closed_at or local_now()
    normalize_risk_state(state, current_time)
    state.daily_realized_pnl += realized_pnl
    if realized_pnl < 0:
        state.consecutive_losses += 1
    elif realized_pnl > 0:
        state.consecutive_losses = 0

    reason_kind = close_reason_kind(close_reason)
    cooldown_minutes = (
        config.symbol_cooldown_minutes_after_stop
        if reason_kind == "STOP_LOSS"
        else config.symbol_cooldown_minutes_after_close
    )
    cooldown_until = set_symbol_cooldown(state, symbol, cooldown_minutes, current_time)
    pnl_text = realized_pnl.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if cooldown_until:
        logging.info(
            "%s: обновлен риск-контур. Реализованный PnL=%s USDT, серия убытков=%s, cooldown до %s.",
            symbol,
            pnl_text,
            state.consecutive_losses,
            cooldown_until,
        )
    else:
        logging.info(
            "%s: обновлен риск-контур. Реализованный PnL=%s USDT, серия убытков=%s.",
            symbol,
            pnl_text,
            state.consecutive_losses,
        )


def calculate_order_size_for_balance(
    available_usdt: Decimal,
    config: Config,
    symbol_meta: SymbolMeta,
    price: Decimal,
) -> OrderSize | None:
    risk_budget = available_usdt * Decimal(str(config.trade_risk_pct)) / Decimal("100")
    stop_fraction = Decimal(str(config.stop_loss_pct)) / Decimal("100")
    if stop_fraction <= 0:
        return None

    max_notional_by_risk = risk_budget / stop_fraction
    max_notional_by_margin = config.max_margin_usdt * Decimal(config.leverage)
    notional = min(max_notional_by_risk, max_notional_by_margin)

    effective_min_notional = max(config.min_notional_usdt, symbol_meta.min_notional)
    if max_notional_by_margin < effective_min_notional:
        return None
    if notional < effective_min_notional:
        return None

    margin = notional / Decimal(config.leverage)
    if margin < config.min_margin_usdt:
        return None

    quantity = round_step(notional / price, symbol_meta.step_size)
    if quantity < symbol_meta.min_qty:
        return None

    actual_notional = quantity * price
    if actual_notional < effective_min_notional:
        return None

    actual_margin = actual_notional / Decimal(config.leverage)
    risk_at_stop = actual_notional * stop_fraction
    return OrderSize(quantity, actual_notional, actual_margin, risk_at_stop)
