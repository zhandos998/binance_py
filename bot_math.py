from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any


def round_step(value: Decimal, step_size: Decimal) -> Decimal:
    if step_size <= 0:
        return value
    return (value / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size


def round_price(value: Decimal, tick_size: Decimal) -> Decimal:
    if tick_size <= 0:
        return value
    return (value / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size


def round_price_up(value: Decimal, tick_size: Decimal) -> Decimal:
    if tick_size <= 0:
        return value
    return (value / tick_size).to_integral_value(rounding=ROUND_UP) * tick_size


def normalize_stop_price(value: Decimal, tick_size: Decimal, entry_price: Decimal, direction: str, is_stop_loss: bool) -> Decimal:
    price = round_price(value, tick_size)
    if tick_size <= 0:
        return price

    if direction == "LONG" and not is_stop_loss and price <= entry_price:
        price = round_price(entry_price + tick_size, tick_size)
    elif direction == "SHORT" and is_stop_loss and price <= entry_price:
        price = round_price(entry_price + tick_size, tick_size)
    elif direction == "SHORT" and not is_stop_loss and price >= entry_price:
        price = round_price(entry_price - tick_size, tick_size)
    elif direction == "LONG" and is_stop_loss and price >= entry_price:
        price = round_price(entry_price - tick_size, tick_size)

    return max(price, tick_size)


def extract_decimal_from_order(order_response: dict[str, Any] | None, keys: tuple[str, ...], fallback: Decimal) -> Decimal:
    if not order_response:
        return fallback
    for key in keys:
        raw_value = order_response.get(key)
        if raw_value is None:
            continue
        value = Decimal(str(raw_value))
        if value > 0:
            return value
    return fallback
