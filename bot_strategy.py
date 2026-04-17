from __future__ import annotations

from decimal import Decimal

from bot_base import (
    CLOSE_LONG,
    CLOSE_SHORT,
    LONG,
    OPEN_LONG,
    OPEN_SHORT,
    SHORT,
    Config,
    MarketSnapshot,
    Position,
    ScanDecision,
    TradeSignal,
)

MOMENTUM_STRATEGY = "momentum"
TREND_PULLBACK_STRATEGY = "trend_pullback"


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


def trend_up(snapshot: MarketSnapshot) -> bool:
    price = float(snapshot.close)
    return price > snapshot.ema_fast > snapshot.ema_slow


def trend_down(snapshot: MarketSnapshot) -> bool:
    price = float(snapshot.close)
    return price < snapshot.ema_fast < snapshot.ema_slow


def higher_timeframe_trend_up(snapshot: MarketSnapshot) -> bool:
    if (
        snapshot.higher_timeframe_close is None
        or snapshot.higher_timeframe_ema_fast is None
        or snapshot.higher_timeframe_ema_slow is None
    ):
        return False
    higher_price = float(snapshot.higher_timeframe_close)
    return higher_price > snapshot.higher_timeframe_ema_fast > snapshot.higher_timeframe_ema_slow


def higher_timeframe_trend_down(snapshot: MarketSnapshot) -> bool:
    if (
        snapshot.higher_timeframe_close is None
        or snapshot.higher_timeframe_ema_fast is None
        or snapshot.higher_timeframe_ema_slow is None
    ):
        return False
    higher_price = float(snapshot.higher_timeframe_close)
    return higher_price < snapshot.higher_timeframe_ema_fast < snapshot.higher_timeframe_ema_slow


def long_funding_ok(snapshot: MarketSnapshot, config: Config) -> bool:
    if not config.funding_filter_enabled:
        return True
    return snapshot.funding_rate_pct is not None and snapshot.funding_rate_pct <= config.max_long_funding_rate_pct


def short_funding_ok(snapshot: MarketSnapshot, config: Config) -> bool:
    if not config.funding_filter_enabled:
        return True
    return snapshot.funding_rate_pct is not None and snapshot.funding_rate_pct >= config.min_short_funding_rate_pct


def higher_timeframe_ok(snapshot: MarketSnapshot, config: Config, direction: str) -> bool:
    if not config.higher_timeframe_enabled:
        return True
    if direction == LONG:
        return higher_timeframe_trend_up(snapshot)
    return higher_timeframe_trend_down(snapshot)


def funding_ok(snapshot: MarketSnapshot, config: Config, direction: str) -> bool:
    if direction == LONG:
        return long_funding_ok(snapshot, config)
    return short_funding_ok(snapshot, config)


def funding_text(snapshot: MarketSnapshot) -> str:
    if snapshot.funding_rate_pct is None:
        return "funding=-"
    return f"funding={snapshot.funding_rate_pct:.4f}%"


def higher_timeframe_state(snapshot: MarketSnapshot) -> str:
    if higher_timeframe_trend_up(snapshot):
        return "UP"
    if higher_timeframe_trend_down(snapshot):
        return "DOWN"
    if snapshot.higher_timeframe_close is None:
        return "-"
    return "MIXED"


def strategy_mode(config: Config) -> str:
    if config.strategy_mode == TREND_PULLBACK_STRATEGY:
        return TREND_PULLBACK_STRATEGY
    return MOMENTUM_STRATEGY


def pullback_trend_limit(config: Config) -> float:
    return max(config.movement_threshold_pct * 0.75, 0.15)


def pullback_counter_limit(config: Config) -> float:
    return max(config.movement_threshold_pct * 0.35, 0.10)


def pullback_volume_threshold(config: Config) -> float:
    if config.min_volume_ratio <= 0.8:
        return config.min_volume_ratio
    return config.min_volume_ratio * 0.8


def normalized_rsi_range(lower: float, upper: float) -> tuple[float, float]:
    lower = max(0.0, min(lower, 100.0))
    upper = max(0.0, min(upper, 100.0))
    if lower > upper:
        lower = upper
    return lower, upper


def long_pullback_rsi_range(config: Config) -> tuple[float, float]:
    center = min(max(config.buy_rsi_min + 4.0, 44.0), 58.0)
    lower = max(35.0, center - 8.0)
    upper = min(68.0, center + 8.0, config.buy_rsi_max)
    return normalized_rsi_range(lower, upper)


def short_pullback_rsi_range(config: Config) -> tuple[float, float]:
    center = min(max(config.sell_rsi_max + 6.0, 42.0), 56.0)
    lower = max(35.0, center - 8.0)
    upper = min(65.0, center + 8.0)
    return normalized_rsi_range(lower, upper)


def build_momentum_open_signal(snapshot: MarketSnapshot, config: Config) -> TradeSignal | None:
    long_trend_ok = trend_up(snapshot) or not config.require_ema_trend
    short_trend_ok = trend_down(snapshot) or not config.require_ema_trend
    volume_ok = snapshot.volume_ratio >= config.min_volume_ratio
    long_higher_ok = higher_timeframe_ok(snapshot, config, LONG)
    short_higher_ok = higher_timeframe_ok(snapshot, config, SHORT)
    long_funding_allowed = funding_ok(snapshot, config, LONG)
    short_funding_allowed = funding_ok(snapshot, config, SHORT)

    if (
        snapshot.pct_change >= config.movement_threshold_pct
        and long_trend_ok
        and volume_ok
        and long_higher_ok
        and long_funding_allowed
        and config.buy_rsi_min <= snapshot.rsi <= config.buy_rsi_max
    ):
        score = snapshot.pct_change + snapshot.volume_ratio + ((snapshot.rsi - 50) / 10)
        reason = (
            f"Импульс вверх {snapshot.pct_change:.2f}%, "
            f"RSI={snapshot.rsi:.1f}, объем/средний={snapshot.volume_ratio:.2f}, "
            f"EMA тренд вверх, HTF={higher_timeframe_state(snapshot)}, {funding_text(snapshot)}"
        )
        return TradeSignal(snapshot.symbol, OPEN_LONG, "BUY", LONG, score, snapshot.close, reason)

    if (
        snapshot.pct_change <= -config.movement_threshold_pct
        and short_trend_ok
        and volume_ok
        and short_higher_ok
        and short_funding_allowed
        and snapshot.rsi <= config.sell_rsi_max
    ):
        score = abs(snapshot.pct_change) + snapshot.volume_ratio + ((50 - snapshot.rsi) / 10)
        reason = (
            f"Импульс вниз {snapshot.pct_change:.2f}%, "
            f"RSI={snapshot.rsi:.1f}, объем/средний={snapshot.volume_ratio:.2f}, "
            f"EMA тренд вниз, HTF={higher_timeframe_state(snapshot)}, {funding_text(snapshot)}"
        )
        return TradeSignal(snapshot.symbol, OPEN_SHORT, "SELL", SHORT, score, snapshot.close, reason)

    return None


def build_trend_pullback_open_signal(snapshot: MarketSnapshot, config: Config) -> TradeSignal | None:
    long_trend_ok = trend_up(snapshot) or not config.require_ema_trend
    short_trend_ok = trend_down(snapshot) or not config.require_ema_trend
    long_higher_ok = higher_timeframe_ok(snapshot, config, LONG)
    short_higher_ok = higher_timeframe_ok(snapshot, config, SHORT)
    long_funding_allowed = funding_ok(snapshot, config, LONG)
    short_funding_allowed = funding_ok(snapshot, config, SHORT)
    volume_threshold = pullback_volume_threshold(config)
    volume_ok = snapshot.volume_ratio >= volume_threshold
    trend_limit = pullback_trend_limit(config)
    counter_limit = pullback_counter_limit(config)
    long_rsi_min, long_rsi_max = long_pullback_rsi_range(config)
    short_rsi_min, short_rsi_max = short_pullback_rsi_range(config)

    if (
        -counter_limit <= snapshot.pct_change <= trend_limit
        and long_trend_ok
        and volume_ok
        and long_higher_ok
        and long_funding_allowed
        and long_rsi_min <= snapshot.rsi <= long_rsi_max
    ):
        score = snapshot.volume_ratio + max(0.0, trend_limit - abs(snapshot.pct_change)) + ((64 - abs(snapshot.rsi - 52.0)) / 10)
        reason = (
            f"Пуллбек к восходящему тренду {snapshot.pct_change:.2f}%, "
            f"RSI={snapshot.rsi:.1f}, объем/средний={snapshot.volume_ratio:.2f}, "
            f"EMA тренд вверх, HTF={higher_timeframe_state(snapshot)}, {funding_text(snapshot)}"
        )
        return TradeSignal(snapshot.symbol, OPEN_LONG, "BUY", LONG, score, snapshot.close, reason)

    if (
        -trend_limit <= snapshot.pct_change <= counter_limit
        and short_trend_ok
        and volume_ok
        and short_higher_ok
        and short_funding_allowed
        and short_rsi_min <= snapshot.rsi <= short_rsi_max
    ):
        score = snapshot.volume_ratio + max(0.0, trend_limit - abs(snapshot.pct_change)) + ((64 - abs(snapshot.rsi - 48.0)) / 10)
        reason = (
            f"Пуллбек к нисходящему тренду {snapshot.pct_change:.2f}%, "
            f"RSI={snapshot.rsi:.1f}, объем/средний={snapshot.volume_ratio:.2f}, "
            f"EMA тренд вниз, HTF={higher_timeframe_state(snapshot)}, {funding_text(snapshot)}"
        )
        return TradeSignal(snapshot.symbol, OPEN_SHORT, "SELL", SHORT, score, snapshot.close, reason)

    return None


def build_open_signal(snapshot: MarketSnapshot, config: Config) -> TradeSignal | None:
    if strategy_mode(config) == TREND_PULLBACK_STRATEGY:
        return build_trend_pullback_open_signal(snapshot, config)
    return build_momentum_open_signal(snapshot, config)


def format_market_metrics(snapshot: MarketSnapshot) -> str:
    higher_close = "-" if snapshot.higher_timeframe_close is None else str(snapshot.higher_timeframe_close)
    higher_fast = "-" if snapshot.higher_timeframe_ema_fast is None else f"{snapshot.higher_timeframe_ema_fast:.8f}"
    higher_slow = "-" if snapshot.higher_timeframe_ema_slow is None else f"{snapshot.higher_timeframe_ema_slow:.8f}"
    return (
        f"цена={snapshot.close}, change={snapshot.pct_change:.2f}%, RSI={snapshot.rsi:.1f}, "
        f"EMA_FAST={snapshot.ema_fast:.8f}, EMA_SLOW={snapshot.ema_slow:.8f}, "
        f"volume/avg={snapshot.volume_ratio:.2f}, HTF={higher_timeframe_state(snapshot)} "
        f"(close={higher_close}, EMA_FAST={higher_fast}, EMA_SLOW={higher_slow}), {funding_text(snapshot)}"
    )


def momentum_explain_blockers(snapshot: MarketSnapshot, config: Config, direction: str) -> list[str]:
    volume_ok = snapshot.volume_ratio >= config.min_volume_ratio
    blockers: list[str] = []

    if direction == LONG:
        if snapshot.pct_change < config.movement_threshold_pct:
            blockers.append(f"рост {snapshot.pct_change:.2f}% < порога {config.movement_threshold_pct:.2f}%")
        if config.require_ema_trend and not trend_up(snapshot):
            blockers.append("нет EMA-тренда вверх")
        if config.higher_timeframe_enabled and not higher_timeframe_trend_up(snapshot):
            blockers.append(f"нет HTF-тренда вверх ({config.higher_timeframe_interval})")
        if not volume_ok:
            blockers.append(f"объем {snapshot.volume_ratio:.2f} < порога {config.min_volume_ratio:.2f}")
        if not (config.buy_rsi_min <= snapshot.rsi <= config.buy_rsi_max):
            blockers.append(f"RSI {snapshot.rsi:.1f} вне диапазона LONG {config.buy_rsi_min:.1f}-{config.buy_rsi_max:.1f}")
        if config.funding_filter_enabled and not long_funding_ok(snapshot, config):
            if snapshot.funding_rate_pct is None:
                blockers.append("funding недоступен")
            else:
                blockers.append(
                    f"funding {snapshot.funding_rate_pct:.4f}% выше LONG-порога {config.max_long_funding_rate_pct:.4f}%"
                )
        return blockers

    if snapshot.pct_change > -config.movement_threshold_pct:
        blockers.append(f"падение {snapshot.pct_change:.2f}% слабее порога -{config.movement_threshold_pct:.2f}%")
    if config.require_ema_trend and not trend_down(snapshot):
        blockers.append("нет EMA-тренда вниз")
    if config.higher_timeframe_enabled and not higher_timeframe_trend_down(snapshot):
        blockers.append(f"нет HTF-тренда вниз ({config.higher_timeframe_interval})")
    if not volume_ok:
        blockers.append(f"объем {snapshot.volume_ratio:.2f} < порога {config.min_volume_ratio:.2f}")
    if snapshot.rsi > config.sell_rsi_max:
        blockers.append(f"RSI {snapshot.rsi:.1f} выше SHORT-порога {config.sell_rsi_max:.1f}")
    if config.funding_filter_enabled and not short_funding_ok(snapshot, config):
        if snapshot.funding_rate_pct is None:
            blockers.append("funding недоступен")
        else:
            blockers.append(
                f"funding {snapshot.funding_rate_pct:.4f}% ниже SHORT-порога {config.min_short_funding_rate_pct:.4f}%"
            )
    return blockers


def trend_pullback_explain_blockers(snapshot: MarketSnapshot, config: Config, direction: str) -> list[str]:
    volume_threshold = pullback_volume_threshold(config)
    trend_limit = pullback_trend_limit(config)
    counter_limit = pullback_counter_limit(config)
    long_rsi_min, long_rsi_max = long_pullback_rsi_range(config)
    short_rsi_min, short_rsi_max = short_pullback_rsi_range(config)
    blockers: list[str] = []

    if direction == LONG:
        if snapshot.pct_change < -counter_limit or snapshot.pct_change > trend_limit:
            blockers.append(
                f"пуллбек LONG вне окна {snapshot.pct_change:.2f}% (нужно от -{counter_limit:.2f}% до {trend_limit:.2f}%)"
            )
        if config.require_ema_trend and not trend_up(snapshot):
            blockers.append("нет EMA-тренда вверх")
        if config.higher_timeframe_enabled and not higher_timeframe_trend_up(snapshot):
            blockers.append(f"нет HTF-тренда вверх ({config.higher_timeframe_interval})")
        if snapshot.volume_ratio < volume_threshold:
            blockers.append(f"объем {snapshot.volume_ratio:.2f} < pullback-порога {volume_threshold:.2f}")
        if not (long_rsi_min <= snapshot.rsi <= long_rsi_max):
            blockers.append(f"RSI {snapshot.rsi:.1f} вне окна LONG-pullback {long_rsi_min:.1f}-{long_rsi_max:.1f}")
        if config.funding_filter_enabled and not long_funding_ok(snapshot, config):
            if snapshot.funding_rate_pct is None:
                blockers.append("funding недоступен")
            else:
                blockers.append(
                    f"funding {snapshot.funding_rate_pct:.4f}% выше LONG-порога {config.max_long_funding_rate_pct:.4f}%"
                )
        return blockers

    if snapshot.pct_change < -trend_limit or snapshot.pct_change > counter_limit:
        blockers.append(
            f"пуллбек SHORT вне окна {snapshot.pct_change:.2f}% (нужно от -{trend_limit:.2f}% до {counter_limit:.2f}%)"
        )
    if config.require_ema_trend and not trend_down(snapshot):
        blockers.append("нет EMA-тренда вниз")
    if config.higher_timeframe_enabled and not higher_timeframe_trend_down(snapshot):
        blockers.append(f"нет HTF-тренда вниз ({config.higher_timeframe_interval})")
    if snapshot.volume_ratio < volume_threshold:
        blockers.append(f"объем {snapshot.volume_ratio:.2f} < pullback-порога {volume_threshold:.2f}")
    if not (short_rsi_min <= snapshot.rsi <= short_rsi_max):
        blockers.append(f"RSI {snapshot.rsi:.1f} вне окна SHORT-pullback {short_rsi_min:.1f}-{short_rsi_max:.1f}")
    if config.funding_filter_enabled and not short_funding_ok(snapshot, config):
        if snapshot.funding_rate_pct is None:
            blockers.append("funding недоступен")
        else:
            blockers.append(
                f"funding {snapshot.funding_rate_pct:.4f}% ниже SHORT-порога {config.min_short_funding_rate_pct:.4f}%"
            )
    return blockers


def explain_no_open_signal(snapshot: MarketSnapshot, config: Config) -> str:
    if strategy_mode(config) == TREND_PULLBACK_STRATEGY:
        long_blockers = trend_pullback_explain_blockers(snapshot, config, LONG)
        short_blockers = trend_pullback_explain_blockers(snapshot, config, SHORT)
    else:
        long_blockers = momentum_explain_blockers(snapshot, config, LONG)
        short_blockers = momentum_explain_blockers(snapshot, config, SHORT)

    long_text = "; ".join(long_blockers) if long_blockers else "условия LONG выполнены"
    short_text = "; ".join(short_blockers) if short_blockers else "условия SHORT выполнены"
    return f"LONG нет: {long_text}. SHORT нет: {short_text}"


def close_action_for_direction(direction: str) -> str:
    return CLOSE_LONG if direction == LONG else CLOSE_SHORT


def close_side_for_direction(direction: str) -> str:
    return "SELL" if direction == LONG else "BUY"


def ema_state(snapshot: MarketSnapshot) -> str:
    if trend_up(snapshot):
        return "UP"
    if trend_down(snapshot):
        return "DOWN"
    return "MIXED"


def momentum_signal_blockers(snapshot: MarketSnapshot, config: Config, direction: str) -> tuple[str, ...]:
    volume_ok = snapshot.volume_ratio >= config.min_volume_ratio
    blockers: list[str] = []

    if direction == LONG:
        if snapshot.pct_change < config.movement_threshold_pct:
            blockers.append("движение")
        if config.require_ema_trend and not trend_up(snapshot):
            blockers.append("EMA")
        if config.higher_timeframe_enabled and not higher_timeframe_trend_up(snapshot):
            blockers.append("HTF")
        if not volume_ok:
            blockers.append("объем")
        if not (config.buy_rsi_min <= snapshot.rsi <= config.buy_rsi_max):
            blockers.append("RSI")
        if config.funding_filter_enabled and not long_funding_ok(snapshot, config):
            blockers.append("funding")
        return tuple(blockers)

    if snapshot.pct_change > -config.movement_threshold_pct:
        blockers.append("движение")
    if config.require_ema_trend and not trend_down(snapshot):
        blockers.append("EMA")
    if config.higher_timeframe_enabled and not higher_timeframe_trend_down(snapshot):
        blockers.append("HTF")
    if not volume_ok:
        blockers.append("объем")
    if snapshot.rsi > config.sell_rsi_max:
        blockers.append("RSI")
    if config.funding_filter_enabled and not short_funding_ok(snapshot, config):
        blockers.append("funding")
    return tuple(blockers)


def trend_pullback_signal_blockers(snapshot: MarketSnapshot, config: Config, direction: str) -> tuple[str, ...]:
    volume_threshold = pullback_volume_threshold(config)
    trend_limit = pullback_trend_limit(config)
    counter_limit = pullback_counter_limit(config)
    long_rsi_min, long_rsi_max = long_pullback_rsi_range(config)
    short_rsi_min, short_rsi_max = short_pullback_rsi_range(config)
    blockers: list[str] = []

    if direction == LONG:
        if snapshot.pct_change < -counter_limit or snapshot.pct_change > trend_limit:
            blockers.append("движение")
        if config.require_ema_trend and not trend_up(snapshot):
            blockers.append("EMA")
        if config.higher_timeframe_enabled and not higher_timeframe_trend_up(snapshot):
            blockers.append("HTF")
        if snapshot.volume_ratio < volume_threshold:
            blockers.append("объем")
        if not (long_rsi_min <= snapshot.rsi <= long_rsi_max):
            blockers.append("RSI")
        if config.funding_filter_enabled and not long_funding_ok(snapshot, config):
            blockers.append("funding")
        return tuple(blockers)

    if snapshot.pct_change < -trend_limit or snapshot.pct_change > counter_limit:
        blockers.append("движение")
    if config.require_ema_trend and not trend_down(snapshot):
        blockers.append("EMA")
    if config.higher_timeframe_enabled and not higher_timeframe_trend_down(snapshot):
        blockers.append("HTF")
    if snapshot.volume_ratio < volume_threshold:
        blockers.append("объем")
    if not (short_rsi_min <= snapshot.rsi <= short_rsi_max):
        blockers.append("RSI")
    if config.funding_filter_enabled and not short_funding_ok(snapshot, config):
        blockers.append("funding")
    return tuple(blockers)


def signal_blockers(snapshot: MarketSnapshot, config: Config, direction: str) -> tuple[str, ...]:
    if strategy_mode(config) == TREND_PULLBACK_STRATEGY:
        return trend_pullback_signal_blockers(snapshot, config, direction)
    return momentum_signal_blockers(snapshot, config, direction)


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
    passed_checks = 6 - len(blockers)
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
            return TradeSignal(position.symbol, CLOSE_LONG, "SELL", LONG, 10_000.0, snapshot.close, reason)
        if config.take_profit_pct > 0 and snapshot.close >= take_price:
            reason = f"тейк-профит LONG: вход={position.entry_price}, текущая={snapshot.close}, цель={take_price}"
            return TradeSignal(position.symbol, CLOSE_LONG, "SELL", LONG, 9_000.0, snapshot.close, reason)

    if position.direction == SHORT:
        stop_price = position.entry_price * (Decimal("1") + stop_fraction)
        take_price = position.entry_price * (Decimal("1") - take_fraction)
        if snapshot.close >= stop_price:
            reason = f"стоп-лосс SHORT: вход={position.entry_price}, текущая={snapshot.close}, стоп={stop_price}"
            return TradeSignal(position.symbol, CLOSE_SHORT, "BUY", SHORT, 10_000.0, snapshot.close, reason)
        if config.take_profit_pct > 0 and snapshot.close <= take_price:
            reason = f"тейк-профит SHORT: вход={position.entry_price}, текущая={snapshot.close}, цель={take_price}"
            return TradeSignal(position.symbol, CLOSE_SHORT, "BUY", SHORT, 9_000.0, snapshot.close, reason)

    return None
