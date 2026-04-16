from __future__ import annotations

import logging
import time

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

from bot_base import CLOSE_LONG, CLOSE_SHORT, Position, Config, ScanDecision, SymbolMeta, TradeSignal
from bot_market import analyze_symbol, load_current_funding_rates
from bot_strategy import (
    build_open_signal,
    build_risk_exit_signal,
    close_action_for_direction,
    close_side_for_direction,
    explain_no_open_signal,
    format_market_metrics,
    no_signal_decision,
    signal_decision,
    skipped_decision,
)


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


def scan_market(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
    positions: dict[str, Position],
) -> tuple[list[TradeSignal], list[TradeSignal], list[ScanDecision]]:
    open_signals: list[TradeSignal] = []
    close_signals: list[TradeSignal] = []
    decisions: list[ScanDecision] = []
    funding_rates: dict[str, float] = {}

    if config.funding_filter_enabled:
        try:
            funding_rates = load_current_funding_rates(client, symbols)
        except (BinanceAPIException, BinanceRequestException) as exc:
            logging.warning("РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ funding-rate РїРѕ symbol universe: %s", exc)
        except Exception:
            logging.exception("РќРµРѕР¶РёРґР°РЅРЅР°СЏ РѕС€РёР±РєР° РїСЂРё Р·Р°РіСЂСѓР·РєРµ funding-rate.")

    for symbol in symbols:
        try:
            snapshot = analyze_symbol(client, symbol, config, funding_rates.get(symbol))
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
