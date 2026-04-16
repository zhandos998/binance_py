from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import mean

from bot_base import CLOSE_LONG, CLOSE_SHORT, OPEN_LONG, OPEN_SHORT, load_config, parse_iso_datetime
from bot_risk import close_reason_kind
from bot_storage import load_trade_rows_from_storage


OPEN_ACTIONS = {OPEN_LONG, OPEN_SHORT}
CLOSE_ACTIONS = {CLOSE_LONG, CLOSE_SHORT}


@dataclass(frozen=True)
class CompletedTrade:
    symbol: str
    direction: str
    entry_time: datetime | None
    exit_time: datetime | None
    quantity: Decimal | None
    entry_price: Decimal | None
    exit_price: Decimal | None
    gross_pnl: Decimal | None
    effective_pnl: Decimal | None
    commission_usdt: Decimal | None
    slippage_usdt: Decimal | None
    slippage_pct: Decimal | None
    close_kind: str
    status: str
    reason: str
    pnl_basis: str
    hold_minutes: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Статистика по журналу сделок Binance futures-бота.")
    parser.add_argument("--file", type=Path, help="Legacy CSV-файл журнала сделок.")
    parser.add_argument("--db", type=Path, help="SQLite-файл хранилища. По умолчанию используется DATABASE_FILE из активного профиля.")
    parser.add_argument("--top", type=int, default=5, help="Сколько символов показать в топе/антитопе.")
    parser.add_argument("--days", type=int, default=7, help="Сколько последних дней показать в дневной сводке.")
    return parser.parse_args()


def parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def format_decimal(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    pattern = "0." + ("0" * digits)
    return str(value.quantize(Decimal(pattern)))


def row_datetime(row: dict[str, str]) -> datetime | None:
    return parse_iso_datetime(row.get("timestamp"))


def row_fill_price(row: dict[str, str] | None) -> Decimal | None:
    if not row:
        return None
    value = parse_decimal(row.get("fill_price"))
    if value is not None and value > 0:
        return value

    raw_response = row.get("raw_response", "").strip()
    if raw_response:
        try:
            payload = json.loads(raw_response)
        except json.JSONDecodeError:
            payload = {}
        for field in ("avgPrice", "price"):
            value = parse_decimal(str(payload.get(field, "")))
            if value is not None and value > 0:
                return value

    value = parse_decimal(row.get("price"))
    if value is not None and value > 0:
        return value
    return None


def calculate_gross_pnl(direction: str, entry_price: Decimal, exit_price: Decimal, quantity: Decimal) -> Decimal:
    if direction == "LONG":
        return (exit_price - entry_price) * quantity
    return (entry_price - exit_price) * quantity


def build_completed_trade(open_row: dict[str, str] | None, close_row: dict[str, str]) -> CompletedTrade | None:
    direction = (close_row.get("direction") or (open_row or {}).get("direction") or "").strip().upper()
    quantity = parse_decimal(close_row.get("quantity")) or parse_decimal((open_row or {}).get("quantity"))
    entry_price = parse_decimal(close_row.get("entry_price")) or row_fill_price(open_row)
    exit_price = row_fill_price(close_row)

    gross_pnl = parse_decimal(close_row.get("gross_realized_pnl_usdt"))
    if gross_pnl is None and direction and entry_price is not None and exit_price is not None and quantity is not None:
        gross_pnl = calculate_gross_pnl(direction, entry_price, exit_price, quantity)

    commission_usdt = parse_decimal(close_row.get("commission_usdt"))
    effective_pnl = parse_decimal(close_row.get("realized_pnl_usdt"))
    pnl_basis = "NET"
    if effective_pnl is None:
        if gross_pnl is not None and commission_usdt is not None:
            effective_pnl = gross_pnl - commission_usdt
            pnl_basis = "GROSS-COMM"
        else:
            effective_pnl = gross_pnl
            pnl_basis = "GROSS"

    if effective_pnl is None:
        return None

    close_kind = (close_row.get("close_kind") or "").strip().upper()
    if not close_kind:
        close_kind = close_reason_kind(close_row.get("reason", ""))

    entry_time = row_datetime(open_row or {})
    exit_time = row_datetime(close_row)
    hold_minutes = None
    if entry_time is not None and exit_time is not None:
        hold_minutes = max((exit_time - entry_time).total_seconds() / 60, 0.0)

    return CompletedTrade(
        symbol=close_row.get("symbol", ""),
        direction=direction,
        entry_time=entry_time,
        exit_time=exit_time,
        quantity=quantity,
        entry_price=entry_price,
        exit_price=exit_price,
        gross_pnl=gross_pnl,
        effective_pnl=effective_pnl,
        commission_usdt=commission_usdt,
        slippage_usdt=parse_decimal(close_row.get("slippage_usdt")),
        slippage_pct=parse_decimal(close_row.get("slippage_pct")),
        close_kind=close_kind or "CLOSE",
        status=close_row.get("status", ""),
        reason=close_row.get("reason", ""),
        pnl_basis=pnl_basis,
        hold_minutes=hold_minutes,
    )


def load_completed_trades_from_rows(rows: list[dict[str, str]]) -> tuple[list[CompletedTrade], int, int]:
    rows.sort(key=lambda row: row.get("timestamp", ""))
    open_positions: dict[str, dict[str, str]] = {}
    completed: list[CompletedTrade] = []
    unmatched_closes = 0

    for row in rows:
        action = (row.get("action") or "").strip().upper()
        symbol = (row.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        if action in OPEN_ACTIONS:
            open_positions[symbol] = row
            continue

        if action not in CLOSE_ACTIONS:
            continue

        trade = build_completed_trade(open_positions.pop(symbol, None), row)
        if trade is None:
            unmatched_closes += 1
            continue
        completed.append(trade)

    return completed, unmatched_closes, len(open_positions)


def load_completed_trades_from_csv(csv_path: Path) -> tuple[list[CompletedTrade], int, int]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Файл не найден: {csv_path}")
    with csv_path.open("r", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    return load_completed_trades_from_rows(rows)


def load_completed_trades_from_db(config) -> tuple[list[CompletedTrade], int, int]:
    rows = load_trade_rows_from_storage(config)
    return load_completed_trades_from_rows(rows)


def max_drawdown(trades: list[CompletedTrade]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    for trade in trades:
        if trade.effective_pnl is None:
            continue
        equity += trade.effective_pnl
        if equity > peak:
            peak = equity
        current_drawdown = peak - equity
        if current_drawdown > drawdown:
            drawdown = current_drawdown
    return drawdown


def summarize_by_symbol(trades: list[CompletedTrade]) -> list[tuple[str, int, Decimal, float]]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for trade in trades:
        if trade.effective_pnl is not None:
            grouped[trade.symbol].append(trade.effective_pnl)

    result = []
    for symbol, pnls in grouped.items():
        count = len(pnls)
        total = sum(pnls, Decimal("0"))
        wins = sum(1 for pnl in pnls if pnl > 0)
        winrate = (wins / count) * 100 if count else 0.0
        result.append((symbol, count, total, winrate))
    result.sort(key=lambda item: item[2], reverse=True)
    return result


def summarize_by_close_kind(trades: list[CompletedTrade]) -> list[tuple[str, int, Decimal]]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for trade in trades:
        if trade.effective_pnl is not None:
            grouped[trade.close_kind].append(trade.effective_pnl)

    result = []
    for close_kind, pnls in grouped.items():
        result.append((close_kind, len(pnls), sum(pnls, Decimal("0"))))
    result.sort(key=lambda item: item[2], reverse=True)
    return result


def summarize_by_day(trades: list[CompletedTrade]) -> list[tuple[str, int, int, int, Decimal]]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for trade in trades:
        if trade.exit_time is None or trade.effective_pnl is None:
            continue
        day_key = trade.exit_time.astimezone().date().isoformat()
        grouped[day_key].append(trade.effective_pnl)

    result = []
    for day_key, pnls in grouped.items():
        wins = sum(1 for pnl in pnls if pnl > 0)
        losses = sum(1 for pnl in pnls if pnl < 0)
        result.append((day_key, len(pnls), wins, losses, sum(pnls, Decimal("0"))))
    result.sort(key=lambda item: item[0], reverse=True)
    return result


def print_summary(source_label: str, trades: list[CompletedTrade], unmatched_closes: int, open_positions_left: int, top_n: int, days_n: int) -> None:
    if not trades:
        print(f"Источник: {source_label}")
        print("Закрытых сделок не найдено.")
        return

    pnls = [trade.effective_pnl for trade in trades if trade.effective_pnl is not None]
    gross_pnls = [trade.gross_pnl for trade in trades if trade.gross_pnl is not None]
    commissions = [trade.commission_usdt for trade in trades if trade.commission_usdt is not None]
    slippages = [trade.slippage_usdt for trade in trades if trade.slippage_usdt is not None]
    hold_minutes = [trade.hold_minutes for trade in trades if trade.hold_minutes is not None]

    total = len(trades)
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    breakeven = total - len(wins) - len(losses)
    total_pnl = sum(pnls, Decimal("0"))
    total_gross = sum(gross_pnls, Decimal("0")) if gross_pnls else None
    total_commission = sum(commissions, Decimal("0")) if commissions else None
    total_slippage = sum(slippages, Decimal("0")) if slippages else None
    avg_win = (sum(wins, Decimal("0")) / len(wins)) if wins else None
    avg_loss = (abs(sum(losses, Decimal("0"))) / len(losses)) if losses else None
    profit_factor = None
    if losses:
        profit_factor = sum(wins, Decimal("0")) / abs(sum(losses, Decimal("0")))
    elif wins:
        profit_factor = Decimal("Infinity")
    expectancy = total_pnl / Decimal(total) if total else None
    mdd = max_drawdown(trades)
    basis_counts: dict[str, int] = defaultdict(int)
    for trade in trades:
        basis_counts[trade.pnl_basis] += 1

    print(f"Источник: {source_label}")
    print(f"Закрытых сделок: {total}")
    print("PnL basis: " + ", ".join(f"{basis}={count}" for basis, count in sorted(basis_counts.items())))
    print(f"Прибыльных: {len(wins)} | Убыточных: {len(losses)} | Безубыток: {breakeven}")
    print(f"Winrate: {(len(wins) / total * 100):.2f}%")
    print(f"Net PnL: {format_decimal(total_pnl)} USDT")
    if total_gross is not None:
        print(f"Gross PnL: {format_decimal(total_gross)} USDT")
    if total_commission is not None:
        print(f"Комиссии: {format_decimal(total_commission)} USDT")
    if total_slippage is not None:
        print(f"Slippage: {format_decimal(total_slippage)} USDT")
    print(f"Avg win: {format_decimal(avg_win)} USDT")
    print(f"Avg loss: {format_decimal(avg_loss)} USDT")
    print(f"Profit factor: {'inf' if profit_factor == Decimal('Infinity') else format_decimal(profit_factor, 3)}")
    print(f"Expectancy: {format_decimal(expectancy)} USDT")
    print(f"Max drawdown: {format_decimal(mdd)} USDT")
    if hold_minutes:
        print(f"Среднее удержание: {mean(hold_minutes):.1f} мин")
    print(f"Unmatched closes: {unmatched_closes} | Открытых позиций в конце журнала: {open_positions_left}")

    print("\nПо типу закрытия:")
    for close_kind, count, pnl in summarize_by_close_kind(trades):
        print(f"  {close_kind:<14} trades={count:<4} pnl={format_decimal(pnl)} USDT")

    symbols = summarize_by_symbol(trades)
    print("\nТоп символов:")
    for symbol, count, pnl, winrate in symbols[:top_n]:
        print(f"  {symbol:<14} trades={count:<4} pnl={format_decimal(pnl)} USDT winrate={winrate:.1f}%")

    print("\nХудшие символы:")
    for symbol, count, pnl, winrate in list(reversed(symbols[-top_n:])):
        print(f"  {symbol:<14} trades={count:<4} pnl={format_decimal(pnl)} USDT winrate={winrate:.1f}%")

    print("\nПоследние дни:")
    for day_key, count, wins_count, losses_count, pnl in summarize_by_day(trades)[:days_n]:
        print(f"  {day_key} trades={count:<3} wins={wins_count:<3} losses={losses_count:<3} pnl={format_decimal(pnl)} USDT")


def main() -> None:
    args = parse_args()
    config = load_config()

    if args.file:
        source_label = str(args.file)
        trades, unmatched_closes, open_positions_left = load_completed_trades_from_csv(args.file)
    else:
        db_config = config if args.db is None else replace(config, database_file=args.db)
        if db_config.database_file.exists():
            source_label = str(db_config.database_file)
            trades, unmatched_closes, open_positions_left = load_completed_trades_from_db(db_config)
        else:
            source_label = str(config.trade_log_file)
            trades, unmatched_closes, open_positions_left = load_completed_trades_from_csv(config.trade_log_file)

    print_summary(source_label, trades, unmatched_closes, open_positions_left, args.top, args.days)


if __name__ == "__main__":
    main()
