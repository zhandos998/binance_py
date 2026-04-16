from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable

from binance.client import Client

from bot_base import Config, MarketSnapshot
from bot_strategy import ema, rsi


def get_closed_klines(
    client: Client,
    symbol: str,
    config: Config,
    interval: str | None = None,
    limit: int | None = None,
) -> list[list[Any]]:
    klines = client.futures_klines(
        symbol=symbol,
        interval=interval or config.kline_interval,
        limit=limit or config.kline_limit,
    )
    if len(klines) > 1:
        return klines[:-1]
    return klines


def minimum_required_bars(
    ema_slow_period: int,
    rsi_period: int | None = None,
    volume_avg_period: int | None = None,
    movement_lookback_candles: int | None = None,
) -> int:
    values = [ema_slow_period + 2]
    if rsi_period is not None:
        values.append(rsi_period + 2)
    if volume_avg_period is not None:
        values.append(volume_avg_period + 2)
    if movement_lookback_candles is not None:
        values.append(movement_lookback_candles + 2)
    return max(values)


def build_snapshot(
    symbol: str,
    closes: list[float],
    volumes: list[float],
    config: Config,
    higher_timeframe_closes: list[float] | None = None,
    funding_rate_pct: float | None = None,
) -> MarketSnapshot | None:
    min_required = minimum_required_bars(
        config.ema_slow,
        config.rsi_period,
        config.volume_avg_period,
        config.movement_lookback_candles,
    )
    if len(closes) < min_required:
        return None

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

    higher_close: Decimal | None = None
    higher_fast: float | None = None
    higher_slow: float | None = None
    if higher_timeframe_closes is not None:
        higher_min_required = minimum_required_bars(config.higher_timeframe_ema_slow)
        if len(higher_timeframe_closes) >= higher_min_required:
            higher_fast = ema(higher_timeframe_closes, config.higher_timeframe_ema_fast)[-1]
            higher_slow = ema(higher_timeframe_closes, config.higher_timeframe_ema_slow)[-1]
            higher_close = Decimal(str(higher_timeframe_closes[-1]))

    return MarketSnapshot(
        symbol=symbol,
        close=Decimal(str(closes[-1])),
        pct_change=pct_change,
        rsi=latest_rsi,
        ema_fast=fast_ema_series[-1],
        ema_slow=slow_ema_series[-1],
        volume_ratio=volume_ratio,
        higher_timeframe_close=higher_close,
        higher_timeframe_ema_fast=higher_fast,
        higher_timeframe_ema_slow=higher_slow,
        funding_rate_pct=funding_rate_pct,
    )


def load_current_funding_rates(client: Client, symbols: Iterable[str]) -> dict[str, float]:
    requested = {symbol.upper() for symbol in symbols}
    if not requested:
        return {}

    raw_rows = client.futures_mark_price()
    if isinstance(raw_rows, dict):
        raw_rows = [raw_rows]

    rates: dict[str, float] = {}
    for row in raw_rows:
        symbol = str(row.get("symbol", "")).upper()
        if symbol not in requested:
            continue
        raw_rate = row.get("lastFundingRate")
        try:
            rates[symbol] = float(raw_rate) * 100
        except (TypeError, ValueError):
            continue
    return rates


def analyze_symbol(
    client: Client,
    symbol: str,
    config: Config,
    funding_rate_pct: float | None = None,
) -> MarketSnapshot | None:
    klines = get_closed_klines(client, symbol, config)
    closes = [float(kline[4]) for kline in klines]
    volumes = [float(kline[5]) for kline in klines]

    higher_timeframe_closes: list[float] | None = None
    if config.higher_timeframe_enabled:
        higher_limit = max(config.kline_limit, minimum_required_bars(config.higher_timeframe_ema_slow))
        higher_klines = get_closed_klines(
            client,
            symbol,
            config,
            interval=config.higher_timeframe_interval,
            limit=higher_limit,
        )
        higher_timeframe_closes = [float(kline[4]) for kline in higher_klines]

    return build_snapshot(
        symbol=symbol,
        closes=closes,
        volumes=volumes,
        config=config,
        higher_timeframe_closes=higher_timeframe_closes,
        funding_rate_pct=funding_rate_pct,
    )
