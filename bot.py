from __future__ import annotations

import logging
import signal
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Any

from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL
from binance.exceptions import BinanceAPIException, BinanceRequestException
from bot_base import (
    CLOSE_LONG,
    CLOSE_SHORT,
    LONG,
    MarketSnapshot,
    OPEN_LONG,
    OPEN_SHORT,
    Position,
    RiskState,
    SHORT,
    Config,
    ScanDecision,
    SymbolMeta,
    TradeSignal,
    configure_logging,
    create_client,
    load_config,
)
from bot_exchange import get_futures_symbols, log_symbol_chunks
from bot_execution import (
    ensure_one_way_position_mode,
    place_close_order,
    place_open_order,
    sync_live_positions,
)
from bot_reporting import log_scan_summary
from bot_risk import (
    cooldown_remaining_text,
    default_risk_state,
    load_risk_state,
    normalize_risk_state,
    openings_blocked_reason,
    save_risk_state,
)
from bot_state import load_positions, save_positions
from bot_scan import scan_market





def execute_cycle(
    client: Client,
    config: Config,
    symbols: dict[str, SymbolMeta],
    positions: dict[str, Position],
    risk_state: RiskState,
) -> None:
    if config.live_trading and not config.use_test_order:
        positions.clear()
        positions.update(sync_live_positions(client, config, symbols, risk_state))

    if normalize_risk_state(risk_state):
        save_risk_state(config, risk_state)

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
        if symbol_meta and place_close_order(client, config, symbol_meta, signal, positions, risk_state):
            trades_count += 1

    block_reason = openings_blocked_reason(config, risk_state)
    if block_reason:
        logging.info("Открытие новых позиций остановлено: %s", block_reason)
        return

    for signal in open_signals:
        if trades_count >= config.max_trades_per_cycle:
            logging.info("Остановка открытия позиций: достигнут MAX_TRADES_PER_CYCLE=%s.", config.max_trades_per_cycle)
            break
        if signal.symbol in positions:
            logging.info("Пропуск %s: позиция уже открыта.", signal.symbol)
            continue
        cooldown_text = cooldown_remaining_text(risk_state, signal.symbol)
        if cooldown_text:
            logging.info("Пропуск %s: символ на cooldown, осталось %s.", signal.symbol, cooldown_text)
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
    logging.info("Запуск Binance %s-M Futures бота. Режим: %s. Endpoint: %s", config.futures_quote_asset, mode, endpoint)
    logging.info(
        "Профиль настроек: %s%s",
        config.env_profile_name,
        f" ({config.env_profile_file})" if config.env_profile_file else "",
    )
    if config.use_test_order:
        logging.info("USE_TEST_ORDER=true: ордера будут только проверяться, без исполнения.")

    client = create_client(config)
    ensure_one_way_position_mode(client, config)

    symbols = get_futures_symbols(client, config)
    risk_state = load_risk_state(config)
    positions = sync_live_positions(client, config, symbols, risk_state) if config.live_trading else load_positions(config)
    logging.info("Загружено активных %s-M perpetual символов: %s.", config.futures_quote_asset, len(symbols))
    block_reason = openings_blocked_reason(config, risk_state)
    logging.info(
        "Риск-контур: дневной PnL=%s USDT, серия убытков=%s, активных cooldown=%s.",
        risk_state.daily_realized_pnl.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
        risk_state.consecutive_losses,
        len(risk_state.cooldowns),
    )
    if block_reason:
        logging.info("Новые входы сейчас заблокированы: %s", block_reason)

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
            symbols = get_futures_symbols(client, config)
            logging.info("Обновлен список futures-символов %s: %s.", config.futures_quote_asset, len(symbols))

        try:
            execute_cycle(client, config, symbols, positions, risk_state)
        except (BinanceAPIException, BinanceRequestException) as exc:
            logging.error("Ошибка Binance в цикле: %s", exc)
        except Exception:
            logging.exception("Неожиданная ошибка в цикле.")

        sleep_until_next_cycle(config.scan_interval_minutes, stop_requested)

    save_positions(config, positions)
    save_risk_state(config, risk_state)
    logging.info("Бот остановлен.")


if __name__ == "__main__":
    main()







# cd /d d:\zhandos998\Desktop\binance_py
# .venv\Scripts\activate.bat
# set BOT_PROFILE_FILE=.env.work
# python bot.py
