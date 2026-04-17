from __future__ import annotations

import argparse
import csv
import logging
import time
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean
from typing import Any

from binance.client import Client

from bot_base import CLOSE_LONG, CLOSE_SHORT, LONG, Config, MarketSnapshot, OrderSize, Position, SymbolMeta, TradeSignal, load_config
from bot_exchange import get_futures_symbols
from bot_market import build_snapshot, minimum_required_bars
from bot_math import round_step
from bot_risk import (
    apply_risk_state_on_close,
    calculate_order_size_for_balance,
    close_reason_kind,
    cooldown_remaining_text,
    default_risk_state,
    normalize_risk_state,
    openings_blocked_reason,
)
from bot_strategy import build_open_signal, build_risk_exit_signal


@dataclass(frozen=True)
class Candle:
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass(frozen=True)
class SimTrade:
    symbol: str
    direction: str
    entry_time: datetime
    exit_time: datetime
    entry_reference_price: Decimal
    entry_price: Decimal
    exit_reference_price: Decimal
    exit_price: Decimal
    quantity: Decimal
    notional_entry: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    entry_fee: Decimal
    exit_fee: Decimal
    total_fee: Decimal
    entry_slippage_usdt: Decimal
    exit_slippage_usdt: Decimal
    total_slippage_usdt: Decimal
    close_kind: str
    reason: str
    hold_minutes: float


@dataclass
class SimPosition:
    symbol: str
    direction: str
    entry_time: datetime
    entry_reference_price: Decimal
    entry_price: Decimal
    quantity: Decimal
    notional_entry: Decimal
    margin_used: Decimal
    leverage: int
    entry_fee: Decimal
    entry_slippage_usdt: Decimal


@dataclass(frozen=True)
class FundingRatePoint:
    funding_time: datetime
    rate_pct: float


@dataclass(frozen=True)
class BacktestResult:
    trades: list[SimTrade]
    start_balance: Decimal
    final_balance: Decimal
    realized_pnl: Decimal
    gross_pnl: Decimal
    total_fees: Decimal
    total_slippage: Decimal
    max_drawdown: Decimal
    symbols: list[str]
    start_time: datetime | None
    end_time: datetime | None
    interval: str
    walk_forward_rows: list[tuple[str, str, int, Decimal, float]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Простой backtest/walk-forward для текущей логики Binance futures-бота.")
    parser.add_argument("--symbols", type=str, default="", help="Список символов через запятую, например BTCUSDT,ETHUSDT")
    parser.add_argument("--days", type=int, default=30, help="Сколько последних дней истории брать.")
    parser.add_argument("--start-balance", type=Decimal, default=Decimal("5000"), help="Стартовый баланс USDT.")
    parser.add_argument("--taker-fee-pct", type=Decimal, default=Decimal("0.04"), help="Комиссия taker в процентах на одну сторону.")
    parser.add_argument("--slippage-bps", type=Decimal, default=Decimal("2"), help="Проскальзывание в bps на одну сторону.")
    parser.add_argument("--walk-forward", action="store_true", help="Показать rolling walk-forward сводку.")
    parser.add_argument("--train-bars", type=int, default=300, help="Размер train/warm-up окна в барах для walk-forward.")
    parser.add_argument("--test-bars", type=int, default=100, help="Размер тестового окна в барах для walk-forward.")
    parser.add_argument("--save-trades", type=Path, help="Сохранить сделки backtest в CSV.")
    return parser.parse_args()


def live_market_data_client() -> Client:
    return Client()


def interval_to_minutes(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    if unit == "m":
        return value
    if unit == "h":
        return value * 60
    if unit == "d":
        return value * 1440
    if unit == "w":
        return value * 10080
    raise ValueError(f"Неподдерживаемый interval: {interval}")


def warmup_minutes(config: Config) -> int:
    main_minutes = minimum_required_bars(
        config.ema_slow,
        config.rsi_period,
        config.volume_avg_period,
        config.movement_lookback_candles,
    ) * interval_to_minutes(config.kline_interval)
    if not config.higher_timeframe_enabled:
        return main_minutes

    higher_minutes = minimum_required_bars(config.higher_timeframe_ema_slow) * interval_to_minutes(
        config.higher_timeframe_interval
    )
    return max(main_minutes, higher_minutes)


def fetch_symbol_candles(
    client: Client,
    symbol: str,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
) -> list[Candle]:
    raw_klines = client.futures_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=str(int(start_dt.timestamp() * 1000)),
        end_str=str(int(end_dt.timestamp() * 1000)),
        limit=1500,
    )
    candles: list[Candle] = []
    for item in raw_klines:
        candles.append(
            Candle(
                open_time=datetime.fromtimestamp(int(item[0]) / 1000, tz=timezone.utc),
                close_time=datetime.fromtimestamp(int(item[6]) / 1000, tz=timezone.utc),
                open=Decimal(str(item[1])),
                high=Decimal(str(item[2])),
                low=Decimal(str(item[3])),
                close=Decimal(str(item[4])),
                volume=Decimal(str(item[5])),
            )
        )
    return candles


def fetch_funding_history(
    client: Client,
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
) -> list[FundingRatePoint]:
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    cursor = start_ms
    points: list[FundingRatePoint] = []

    while cursor <= end_ms:
        rows = client.futures_funding_rate(symbol=symbol, startTime=cursor, endTime=end_ms, limit=1000)
        if not rows:
            break

        last_time = cursor
        for row in rows:
            funding_time = int(row.get("fundingTime", 0) or 0)
            if funding_time <= 0:
                continue
            rate_pct = float(row.get("fundingRate", 0.0) or 0.0) * 100
            points.append(
                FundingRatePoint(
                    funding_time=datetime.fromtimestamp(funding_time / 1000, tz=timezone.utc),
                    rate_pct=rate_pct,
                )
            )
            last_time = max(last_time, funding_time)

        if len(rows) < 1000 or last_time <= cursor:
            break
        cursor = last_time + 1

    return points


def find_latest_candle_index(candles: list[Candle], current_time: datetime) -> int | None:
    if not candles:
        return None
    open_times = [candle.open_time for candle in candles]
    idx = bisect_right(open_times, current_time) - 1
    if idx < 0:
        return None
    return idx


def funding_rate_at_or_before(points: list[FundingRatePoint], current_time: datetime) -> float | None:
    if not points:
        return None
    funding_times = [point.funding_time for point in points]
    idx = bisect_right(funding_times, current_time) - 1
    if idx < 0:
        return None
    return points[idx].rate_pct


def snapshot_from_candles(
    candles: list[Candle],
    end_index: int,
    config: Config,
    symbol: str,
    higher_timeframe_candles: list[Candle] | None = None,
    funding_history: list[FundingRatePoint] | None = None,
) -> MarketSnapshot | None:
    history = candles[: end_index + 1]
    closes = [float(candle.close) for candle in history]
    volumes = [float(candle.volume) for candle in history]

    higher_closes: list[float] | None = None
    if config.higher_timeframe_enabled and higher_timeframe_candles is not None:
        higher_idx = find_latest_candle_index(higher_timeframe_candles, history[-1].open_time)
        if higher_idx is not None:
            higher_history = higher_timeframe_candles[: higher_idx + 1]
            higher_closes = [float(candle.close) for candle in higher_history]

    funding_rate_pct = None
    if config.funding_filter_enabled and funding_history is not None:
        funding_rate_pct = funding_rate_at_or_before(funding_history, history[-1].open_time)

    return build_snapshot(
        symbol=symbol,
        closes=closes,
        volumes=volumes,
        config=config,
        higher_timeframe_closes=higher_closes,
        funding_rate_pct=funding_rate_pct,
    )


def backtest_available_usdt(balance: Decimal, positions: dict[str, SimPosition]) -> Decimal:
    used_margin = sum(position.margin_used for position in positions.values())
    return max(balance - used_margin, Decimal("0"))


def slippage_fill_price(reference_price: Decimal, side: str, slippage_bps: Decimal) -> Decimal:
    factor = slippage_bps / Decimal("10000")
    if side == "BUY":
        return reference_price * (Decimal("1") + factor)
    return reference_price * (Decimal("1") - factor)


def slippage_cost(reference_price: Decimal, fill_price: Decimal, quantity: Decimal, side: str) -> Decimal:
    if side == "BUY":
        return (fill_price - reference_price) * quantity
    return (reference_price - fill_price) * quantity


def commission_usdt(notional: Decimal, taker_fee_pct: Decimal) -> Decimal:
    return notional * (taker_fee_pct / Decimal("100"))


def intrabar_protection_exit(position: SimPosition, candle: Candle, config: Config) -> tuple[str, Decimal] | None:
    stop_fraction = Decimal(str(config.stop_loss_pct)) / Decimal("100")
    take_fraction = Decimal(str(config.take_profit_pct)) / Decimal("100")

    if position.direction == LONG:
        stop_price = position.entry_price * (Decimal("1") - stop_fraction)
        take_price = position.entry_price * (Decimal("1") + take_fraction)
        if candle.low <= stop_price:
            return "STOP_LOSS", stop_price
        if config.take_profit_pct > 0 and candle.high >= take_price:
            return "TAKE_PROFIT", take_price
    else:
        stop_price = position.entry_price * (Decimal("1") + stop_fraction)
        take_price = position.entry_price * (Decimal("1") - take_fraction)
        if candle.high >= stop_price:
            return "STOP_LOSS", stop_price
        if config.take_profit_pct > 0 and candle.low <= take_price:
            return "TAKE_PROFIT", take_price
    return None


def close_side_for_direction(direction: str) -> str:
    return "SELL" if direction == LONG else "BUY"


def close_action_for_direction(direction: str) -> str:
    return CLOSE_LONG if direction == LONG else CLOSE_SHORT


def signal_close_kind(signal: TradeSignal) -> str:
    return close_reason_kind(signal.reason)


def close_position(
    position: SimPosition,
    exit_time: datetime,
    exit_reference_price: Decimal,
    fill_price: Decimal,
    reason: str,
    close_kind: str,
    taker_fee_pct: Decimal,
) -> SimTrade:
    quantity = position.quantity
    exit_notional = quantity * fill_price
    exit_fee = commission_usdt(exit_notional, taker_fee_pct)
    if position.direction == LONG:
        gross_pnl = (fill_price - position.entry_price) * quantity
    else:
        gross_pnl = (position.entry_price - fill_price) * quantity
    net_pnl = gross_pnl - position.entry_fee - exit_fee
    exit_slippage = slippage_cost(exit_reference_price, fill_price, quantity, close_side_for_direction(position.direction))
    hold_minutes = max((exit_time - position.entry_time).total_seconds() / 60, 0.0)
    return SimTrade(
        symbol=position.symbol,
        direction=position.direction,
        entry_time=position.entry_time,
        exit_time=exit_time,
        entry_reference_price=position.entry_reference_price,
        entry_price=position.entry_price,
        exit_reference_price=exit_reference_price,
        exit_price=fill_price,
        quantity=quantity,
        notional_entry=position.notional_entry,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        entry_fee=position.entry_fee,
        exit_fee=exit_fee,
        total_fee=position.entry_fee + exit_fee,
        entry_slippage_usdt=position.entry_slippage_usdt,
        exit_slippage_usdt=exit_slippage,
        total_slippage_usdt=position.entry_slippage_usdt + exit_slippage,
        close_kind=close_kind,
        reason=reason,
        hold_minutes=hold_minutes,
    )


def max_drawdown(trades: list[SimTrade]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    for trade in trades:
        equity += trade.net_pnl
        if equity > peak:
            peak = equity
        current_drawdown = peak - equity
        if current_drawdown > drawdown:
            drawdown = current_drawdown
    return drawdown


def walk_forward_rows(
    trades: list[SimTrade],
    timeline: list[datetime],
    train_bars: int,
    test_bars: int,
) -> list[tuple[str, str, int, Decimal, float]]:
    if not timeline or test_bars <= 0 or len(timeline) <= train_bars:
        return []

    rows: list[tuple[str, str, int, Decimal, float]] = []
    start_index = train_bars
    while start_index < len(timeline):
        end_index = min(start_index + test_bars - 1, len(timeline) - 1)
        start_dt = timeline[start_index]
        end_dt = timeline[end_index]
        window_trades = [trade for trade in trades if start_dt <= trade.exit_time <= end_dt]
        if not window_trades:
            start_index += test_bars
            continue
        window_pnl = sum((trade.net_pnl for trade in window_trades), Decimal("0"))
        wins = sum(1 for trade in window_trades if trade.net_pnl > 0)
        winrate = (wins / len(window_trades) * 100) if window_trades else 0.0
        rows.append((start_dt.date().isoformat(), end_dt.date().isoformat(), len(window_trades), window_pnl, winrate))
        start_index += test_bars
    return rows


def simulate(
    config: Config,
    symbol_meta_map: dict[str, SymbolMeta],
    candles_by_symbol: dict[str, list[Candle]],
    higher_candles_by_symbol: dict[str, list[Candle]],
    funding_rates_by_symbol: dict[str, list[FundingRatePoint]],
    simulation_start_dt: datetime,
    start_balance: Decimal,
    taker_fee_pct: Decimal,
    slippage_bps: Decimal,
    walk_forward_enabled: bool,
    train_bars: int,
    test_bars: int,
) -> BacktestResult:
    positions: dict[str, SimPosition] = {}
    realized_balance = start_balance
    trades: list[SimTrade] = []
    risk_state = default_risk_state()

    index_by_time: dict[str, dict[datetime, int]] = {
        symbol: {candle.open_time: idx for idx, candle in enumerate(candles)}
        for symbol, candles in candles_by_symbol.items()
    }
    timeline = sorted({candle.open_time for candles in candles_by_symbol.values() for candle in candles})
    if len(timeline) < 2:
        return BacktestResult([], start_balance, start_balance, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), list(symbol_meta_map), None, None, config.kline_interval, [])
    active_timeline = [item for item in timeline if item >= simulation_start_dt]

    pending_closes: list[TradeSignal] = []
    pending_opens: list[TradeSignal] = []
    start_time: datetime | None = None

    for current_time in timeline:
        if current_time < simulation_start_dt:
            continue
        bar_candles: dict[str, Candle] = {}
        bar_indices: dict[str, int] = {}
        for symbol, candles in candles_by_symbol.items():
            idx = index_by_time[symbol].get(current_time)
            if idx is None:
                continue
            bar_indices[symbol] = idx
            bar_candles[symbol] = candles[idx]
        if not bar_candles:
            continue

        if start_time is None:
            start_time = current_time
            risk_state.day = current_time.astimezone().date().isoformat()

        normalize_risk_state(risk_state, current_time.astimezone())

        for signal in pending_closes:
            position = positions.get(signal.symbol)
            candle = bar_candles.get(signal.symbol)
            if position is None or candle is None:
                continue
            exit_reference_price = candle.open
            fill_price = slippage_fill_price(exit_reference_price, signal.side, slippage_bps)
            trade = close_position(
                position=position,
                exit_time=candle.open_time,
                exit_reference_price=exit_reference_price,
                fill_price=fill_price,
                reason=signal.reason,
                close_kind=signal_close_kind(signal),
                taker_fee_pct=taker_fee_pct,
            )
            positions.pop(signal.symbol, None)
            realized_balance += trade.gross_pnl - trade.exit_fee
            apply_risk_state_on_close(
                config=config,
                state=risk_state,
                symbol=signal.symbol,
                realized_pnl=trade.net_pnl,
                close_reason=signal.reason,
                closed_at=candle.open_time.astimezone(),
            )
            trades.append(trade)
        pending_closes = []

        for signal in pending_opens:
            if signal.symbol in positions:
                continue
            candle = bar_candles.get(signal.symbol)
            symbol_meta = symbol_meta_map.get(signal.symbol)
            if candle is None or symbol_meta is None:
                continue
            available_usdt = backtest_available_usdt(realized_balance, positions)
            order_size = calculate_order_size_for_balance(available_usdt, config, symbol_meta, candle.open)
            if order_size is None:
                continue

            entry_reference_price = candle.open
            entry_price = slippage_fill_price(entry_reference_price, signal.side, slippage_bps)
            notional = order_size.quantity * entry_price
            margin_used = notional / Decimal(config.leverage)
            entry_fee = commission_usdt(notional, taker_fee_pct)
            entry_slippage = slippage_cost(entry_reference_price, entry_price, order_size.quantity, signal.side)
            realized_balance -= entry_fee
            positions[signal.symbol] = SimPosition(
                symbol=signal.symbol,
                direction=signal.direction,
                entry_time=candle.open_time,
                entry_reference_price=entry_reference_price,
                entry_price=entry_price,
                quantity=order_size.quantity,
                notional_entry=notional,
                margin_used=margin_used,
                leverage=config.leverage,
                entry_fee=entry_fee,
                entry_slippage_usdt=entry_slippage,
            )
        pending_opens = []

        intrabar_closures: list[tuple[str, SimTrade]] = []
        for symbol, position in list(positions.items()):
            candle = bar_candles.get(symbol)
            if candle is None:
                continue
            protection = intrabar_protection_exit(position, candle, config)
            if protection is None:
                continue
            close_kind, reference_price = protection
            fill_price = slippage_fill_price(reference_price, close_side_for_direction(position.direction), slippage_bps)
            reason = (
                f"стоп-лосс {position.direction}: вход={position.entry_price}, свеча={candle.close}, стоп={reference_price}"
                if close_kind == "STOP_LOSS"
                else f"тейк-профит {position.direction}: вход={position.entry_price}, свеча={candle.close}, цель={reference_price}"
            )
            intrabar_closures.append(
                (
                    symbol,
                    close_position(
                        position=position,
                        exit_time=candle.close_time,
                        exit_reference_price=reference_price,
                        fill_price=fill_price,
                        reason=reason,
                        close_kind=close_kind,
                        taker_fee_pct=taker_fee_pct,
                    ),
                )
            )

        for symbol, trade in intrabar_closures:
            positions.pop(symbol, None)
            realized_balance += trade.gross_pnl - trade.exit_fee
            apply_risk_state_on_close(
                config=config,
                state=risk_state,
                symbol=symbol,
                realized_pnl=trade.net_pnl,
                close_reason=trade.reason,
                closed_at=trade.exit_time.astimezone(),
            )
            trades.append(trade)

        open_signals: list[TradeSignal] = []
        close_signals: list[TradeSignal] = []
        for symbol, idx in bar_indices.items():
            snapshot = snapshot_from_candles(
                candles_by_symbol[symbol],
                idx,
                config,
                symbol,
                higher_timeframe_candles=higher_candles_by_symbol.get(symbol),
                funding_history=funding_rates_by_symbol.get(symbol),
            )
            if snapshot is None:
                continue
            position = positions.get(symbol)
            if position is not None:
                bot_position = Position(
                    symbol=position.symbol,
                    direction=position.direction,
                    entry_price=position.entry_price,
                    quantity=position.quantity,
                    margin_used=position.margin_used,
                    leverage=position.leverage,
                    opened_at=position.entry_time.isoformat(),
                    entry_reference_price=position.entry_reference_price,
                    entry_commission_usdt=position.entry_fee,
                    entry_slippage_usdt=position.entry_slippage_usdt,
                )
                risk_exit = build_risk_exit_signal(bot_position, snapshot, config)
                if risk_exit is not None:
                    close_signals.append(risk_exit)
                    continue

            signal = build_open_signal(snapshot, config)
            if signal is None:
                continue

            if position is None:
                open_signals.append(signal)
            elif signal.direction != position.direction:
                close_signals.append(
                    TradeSignal(
                        symbol=symbol,
                        action=close_action_for_direction(position.direction),
                        side=close_side_for_direction(position.direction),
                        direction=position.direction,
                        score=signal.score + 5000.0,
                        price=signal.price,
                        reason=f"закрытие {position.direction}: противоположный сигнал. {signal.reason}",
                    )
                )

        open_signals.sort(key=lambda item: item.score, reverse=True)
        close_signals.sort(key=lambda item: item.score, reverse=True)
        selected_closes: list[TradeSignal] = []
        selected_opens: list[TradeSignal] = []
        trades_count = 0

        for signal in close_signals:
            if trades_count >= config.max_trades_per_cycle:
                break
            if signal.symbol not in positions:
                continue
            selected_closes.append(signal)
            trades_count += 1

        block_reason = openings_blocked_reason(config, risk_state, current_time.astimezone())
        if block_reason is None:
            open_slots = max(config.max_open_positions - len(positions), 0)
            for signal in open_signals:
                if trades_count >= config.max_trades_per_cycle or open_slots <= 0:
                    break
                if signal.symbol in positions or any(item.symbol == signal.symbol for item in selected_opens):
                    continue
                if cooldown_remaining_text(risk_state, signal.symbol, current_time.astimezone()):
                    continue
                selected_opens.append(signal)
                trades_count += 1
                open_slots -= 1

        pending_closes = selected_closes
        pending_opens = selected_opens

    if timeline:
        for symbol, position in list(positions.items()):
            last_candle = candles_by_symbol[symbol][-1]
            exit_reference_price = last_candle.close
            fill_price = slippage_fill_price(exit_reference_price, close_side_for_direction(position.direction), slippage_bps)
            trade = close_position(
                position=position,
                exit_time=last_candle.close_time,
                exit_reference_price=exit_reference_price,
                fill_price=fill_price,
                reason="финальная переоценка в конце backtest",
                close_kind="FINAL_MARK",
                taker_fee_pct=taker_fee_pct,
            )
            realized_balance += trade.gross_pnl - trade.exit_fee
            trades.append(trade)
            positions.pop(symbol, None)

    trades.sort(key=lambda trade: trade.exit_time)
    gross_pnl = sum((trade.gross_pnl for trade in trades), Decimal("0"))
    net_pnl = sum((trade.net_pnl for trade in trades), Decimal("0"))
    total_fees = sum((trade.total_fee for trade in trades), Decimal("0"))
    total_slippage = sum((trade.total_slippage_usdt for trade in trades), Decimal("0"))
    walk_rows = walk_forward_rows(trades, active_timeline, train_bars, test_bars) if walk_forward_enabled else []
    return BacktestResult(
        trades=trades,
        start_balance=start_balance,
        final_balance=realized_balance,
        realized_pnl=net_pnl,
        gross_pnl=gross_pnl,
        total_fees=total_fees,
        total_slippage=total_slippage,
        max_drawdown=max_drawdown(trades),
        symbols=list(symbol_meta_map),
        start_time=simulation_start_dt,
        end_time=timeline[-1] if timeline else None,
        interval=config.kline_interval,
        walk_forward_rows=walk_rows,
    )


def print_summary(result: BacktestResult) -> None:
    trades = result.trades
    wins = [trade.net_pnl for trade in trades if trade.net_pnl > 0]
    losses = [trade.net_pnl for trade in trades if trade.net_pnl < 0]
    winrate = (len(wins) / len(trades) * 100) if trades else 0.0
    avg_win = (sum(wins, Decimal("0")) / len(wins)) if wins else None
    avg_loss = (abs(sum(losses, Decimal("0"))) / len(losses)) if losses else None
    profit_factor = None
    if losses:
        profit_factor = sum(wins, Decimal("0")) / abs(sum(losses, Decimal("0")))
    elif wins:
        profit_factor = Decimal("Infinity")
    expectancy = (result.realized_pnl / Decimal(len(trades))) if trades else None
    avg_hold = mean([trade.hold_minutes for trade in trades]) if trades else None

    print(f"Интервал: {result.interval}")
    print(f"Символы: {len(result.symbols)} ({', '.join(result.symbols[:10])}{' ...' if len(result.symbols) > 10 else ''})")
    print(f"Период: {result.start_time} -> {result.end_time}")
    print(f"Стартовый баланс: {result.start_balance.quantize(Decimal('0.01'))} USDT")
    print(f"Финальный баланс: {result.final_balance.quantize(Decimal('0.01'))} USDT")
    print(f"Закрытых сделок: {len(trades)}")
    print(f"Winrate: {winrate:.2f}%")
    print(f"Net PnL: {result.realized_pnl.quantize(Decimal('0.01'))} USDT")
    print(f"Gross PnL: {result.gross_pnl.quantize(Decimal('0.01'))} USDT")
    print(f"Комиссии: {result.total_fees.quantize(Decimal('0.01'))} USDT")
    print(f"Slippage: {result.total_slippage.quantize(Decimal('0.01'))} USDT")
    print(f"Avg win: {'-' if avg_win is None else avg_win.quantize(Decimal('0.01'))} USDT")
    print(f"Avg loss: {'-' if avg_loss is None else avg_loss.quantize(Decimal('0.01'))} USDT")
    print(
        "Profit factor: "
        + ("-" if profit_factor is None else ("inf" if profit_factor == Decimal("Infinity") else str(profit_factor.quantize(Decimal('0.001')))))
    )
    print(f"Expectancy: {'-' if expectancy is None else expectancy.quantize(Decimal('0.01'))} USDT")
    print(f"Max drawdown: {result.max_drawdown.quantize(Decimal('0.01'))} USDT")
    if avg_hold is not None:
        print(f"Среднее удержание: {avg_hold:.1f} мин")

    by_kind: dict[str, list[Decimal]] = defaultdict(list)
    by_symbol: dict[str, list[Decimal]] = defaultdict(list)
    for trade in trades:
        by_kind[trade.close_kind].append(trade.net_pnl)
        by_symbol[trade.symbol].append(trade.net_pnl)

    if by_kind:
        print("\nПо типу закрытия:")
        for kind, pnls in sorted(by_kind.items(), key=lambda item: sum(item[1], Decimal("0")), reverse=True):
            print(f"  {kind:<14} trades={len(pnls):<4} pnl={sum(pnls, Decimal('0')).quantize(Decimal('0.01'))} USDT")

    if by_symbol:
        print("\nТоп символов:")
        ranked = sorted(by_symbol.items(), key=lambda item: sum(item[1], Decimal("0")), reverse=True)
        for symbol, pnls in ranked[:5]:
            print(f"  {symbol:<14} trades={len(pnls):<4} pnl={sum(pnls, Decimal('0')).quantize(Decimal('0.01'))} USDT")

        print("\nХудшие символы:")
        for symbol, pnls in list(reversed(ranked[-5:])):
            print(f"  {symbol:<14} trades={len(pnls):<4} pnl={sum(pnls, Decimal('0')).quantize(Decimal('0.01'))} USDT")

    if result.walk_forward_rows:
        print("\nWalk-forward:")
        for start_text, end_text, trade_count, pnl, wf_winrate in result.walk_forward_rows:
            print(f"  {start_text} -> {end_text} | trades={trade_count:<4} pnl={pnl.quantize(Decimal('0.01'))} USDT winrate={wf_winrate:.1f}%")


def save_trades_csv(path: Path, trades: list[SimTrade]) -> None:
    fieldnames = [
        "symbol",
        "direction",
        "entry_time",
        "exit_time",
        "entry_reference_price",
        "entry_price",
        "exit_reference_price",
        "exit_price",
        "quantity",
        "notional_entry",
        "gross_pnl",
        "net_pnl",
        "entry_fee",
        "exit_fee",
        "total_fee",
        "entry_slippage_usdt",
        "exit_slippage_usdt",
        "total_slippage_usdt",
        "close_kind",
        "reason",
        "hold_minutes",
    ]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(
                {
                    "symbol": trade.symbol,
                    "direction": trade.direction,
                    "entry_time": trade.entry_time.isoformat(),
                    "exit_time": trade.exit_time.isoformat(),
                    "entry_reference_price": str(trade.entry_reference_price),
                    "entry_price": str(trade.entry_price),
                    "exit_reference_price": str(trade.exit_reference_price),
                    "exit_price": str(trade.exit_price),
                    "quantity": str(trade.quantity),
                    "notional_entry": str(trade.notional_entry),
                    "gross_pnl": str(trade.gross_pnl),
                    "net_pnl": str(trade.net_pnl),
                    "entry_fee": str(trade.entry_fee),
                    "exit_fee": str(trade.exit_fee),
                    "total_fee": str(trade.total_fee),
                    "entry_slippage_usdt": str(trade.entry_slippage_usdt),
                    "exit_slippage_usdt": str(trade.exit_slippage_usdt),
                    "total_slippage_usdt": str(trade.total_slippage_usdt),
                    "close_kind": trade.close_kind,
                    "reason": trade.reason,
                    "hold_minutes": f"{trade.hold_minutes:.2f}",
                }
            )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

    config = load_config()
    client = live_market_data_client()
    symbols_map = get_futures_symbols(client, config)

    explicit_symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    if explicit_symbols:
        symbols_map = {symbol: meta for symbol, meta in symbols_map.items() if symbol in explicit_symbols}
    if not symbols_map:
        raise RuntimeError("Не удалось определить символы для backtest.")

    extra_days = max(1, int(warmup_minutes(config) / 1440) + 2)
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=args.days + extra_days)
    requested_start = end_dt - timedelta(days=args.days)

    candles_by_symbol: dict[str, list[Candle]] = {}
    higher_candles_by_symbol: dict[str, list[Candle]] = {}
    funding_rates_by_symbol: dict[str, list[FundingRatePoint]] = {}
    for symbol in symbols_map:
        candles = fetch_symbol_candles(client, symbol, config.kline_interval, start_dt, end_dt)
        if len(candles) < minimum_required_bars(
            config.ema_slow,
            config.rsi_period,
            config.volume_avg_period,
            config.movement_lookback_candles,
        ) + 2:
            continue
        candles_by_symbol[symbol] = candles

        if config.higher_timeframe_enabled:
            higher_candles = fetch_symbol_candles(client, symbol, config.higher_timeframe_interval, start_dt, end_dt)
            if higher_candles:
                higher_candles_by_symbol[symbol] = higher_candles

        if config.funding_filter_enabled:
            funding_rates_by_symbol[symbol] = fetch_funding_history(client, symbol, start_dt, end_dt)

        if config.request_sleep_seconds > 0:
            time.sleep(min(config.request_sleep_seconds, 0.1))

    symbols_map = {symbol: meta for symbol, meta in symbols_map.items() if symbol in candles_by_symbol}
    if not candles_by_symbol:
        raise RuntimeError("Не удалось получить достаточно истории ни по одному символу.")

    print("Допущения backtest:")
    print("- сигнал считается на закрытии свечи, вход/выход по market моделируется на открытии следующей свечи;")
    print("- stop-loss/take-profit внутри свечи моделируются по high/low; если в одной свече задеты и стоп и тейк, выбирается стоп;")
    print(f"- taker fee: {args.taker_fee_pct}% на сторону;")
    print(f"- slippage: {args.slippage_bps} bps на сторону;")
    print("- список символов фиксирован на запуске backtest, историческая переоценка universe не делается.\n")

    result = simulate(
        config=config,
        symbol_meta_map=symbols_map,
        candles_by_symbol=candles_by_symbol,
        higher_candles_by_symbol=higher_candles_by_symbol,
        funding_rates_by_symbol=funding_rates_by_symbol,
        simulation_start_dt=requested_start,
        start_balance=args.start_balance,
        taker_fee_pct=args.taker_fee_pct,
        slippage_bps=args.slippage_bps,
        walk_forward_enabled=args.walk_forward,
        train_bars=args.train_bars,
        test_bars=args.test_bars,
    )
    print_summary(result)

    if args.save_trades:
        save_trades_csv(args.save_trades, result.trades)
        print(f"\nСделки сохранены в {args.save_trades}")


if __name__ == "__main__":
    main()
