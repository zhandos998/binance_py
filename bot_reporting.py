from __future__ import annotations

import logging
from decimal import Decimal

from bot_base import Config, OPEN_LONG, OPEN_SHORT, ScanDecision, TradeSignal


def fmt_optional_float(value: float | None, width: int, precision: int, suffix: str = "") -> str:
    if value is None:
        return "-".rjust(width)
    return f"{value:{width}.{precision}f}{suffix}"


def fmt_optional_decimal(value: Decimal | None, width: int) -> str:
    if value is None:
        return "-".rjust(width)
    return f"{str(value):>{width}}"


def log_scan_summary(
    decisions: list[ScanDecision],
    open_signals: list[TradeSignal],
    close_signals: list[TradeSignal],
    config: Config,
) -> None:
    if not config.log_scan_summary:
        return

    total = len(decisions)
    skipped = sum(1 for item in decisions if item.status == "SKIPPED")
    no_signal = sum(1 for item in decisions if item.status in {"NO_SIGNAL", "HOLD"})
    open_long = sum(1 for item in open_signals if item.action == OPEN_LONG)
    open_short = sum(1 for item in open_signals if item.action == OPEN_SHORT)

    blocker_counts = {
        "движение": 0,
        "объем": 0,
        "EMA": 0,
        "HTF": 0,
        "RSI": 0,
        "funding": 0,
        "данные": 0,
    }
    for decision in decisions:
        if decision.status not in {"NO_SIGNAL", "HOLD", "SKIPPED"}:
            continue
        for blocker in decision.blockers:
            blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1

    logging.info(
        "Итог скана: символов=%s | LONG=%s | SHORT=%s | закрыть=%s | без входа=%s | пропуск=%s",
        total,
        open_long,
        open_short,
        len(close_signals),
        no_signal,
        skipped,
    )
    logging.info(
        "Главные причины без входа: движение=%s, объем=%s, EMA=%s, HTF=%s, RSI=%s, funding=%s, данные=%s",
        blocker_counts.get("движение", 0),
        blocker_counts.get("объем", 0),
        blocker_counts.get("EMA", 0),
        blocker_counts.get("HTF", 0),
        blocker_counts.get("RSI", 0),
        blocker_counts.get("funding", 0),
        blocker_counts.get("данные", 0),
    )

    candidates = [
        item
        for item in decisions
        if item.status in {"NO_SIGNAL", "HOLD", "OPEN_SIGNAL", "CLOSE_SIGNAL", "CLOSE_RISK"}
        and item.price is not None
    ]
    candidates.sort(key=lambda item: item.rank_score, reverse=True)
    if not candidates:
        return

    logging.info("Ближайшие кандидаты:")
    logging.info("  %-14s %-6s %9s %7s %7s %-6s %-18s %s", "SYMBOL", "DIR", "CHANGE", "RSI", "VOL", "EMA", "STATUS", "НЕ ХВАТИЛО / ПРИЧИНА")
    for item in candidates[: config.scan_summary_top_n]:
        details = item.details
        if len(details) > 72:
            details = f"{details[:69]}..."
        logging.info(
            "  %-14s %-6s %8s%% %7s %7s %-6s %-18s %s",
            item.symbol,
            item.best_direction,
            fmt_optional_float(item.pct_change, 7, 2),
            fmt_optional_float(item.rsi, 7, 1),
            fmt_optional_float(item.volume_ratio, 7, 2),
            item.ema_state,
            item.status,
            details,
        )
