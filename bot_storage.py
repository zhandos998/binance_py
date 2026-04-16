from __future__ import annotations

import csv
import json
import logging
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Iterator

from bot_base import Config, Position, RiskState, current_local_day, utc_now


TRADE_COLUMNS = (
    "timestamp",
    "mode",
    "market",
    "symbol",
    "action",
    "side",
    "direction",
    "leverage",
    "entry_price",
    "reference_price",
    "price",
    "fill_price",
    "quantity",
    "notional_usdt",
    "margin_usdt",
    "risk_at_stop_usdt",
    "commission_usdt",
    "slippage_usdt",
    "slippage_pct",
    "gross_realized_pnl_usdt",
    "realized_pnl_usdt",
    "close_kind",
    "reason",
    "status",
    "order_id",
    "raw_response",
)


@contextmanager
def db_connection(config: Config) -> Iterator[sqlite3.Connection]:
    config.database_file.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(config.database_file)
    connection.row_factory = sqlite3.Row
    try:
        initialize_database(connection)
        migrate_legacy_data_if_needed(config, connection)
        yield connection
        connection.commit()
    finally:
        connection.close()


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY,
            direction TEXT NOT NULL,
            entry_price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            margin_used TEXT NOT NULL,
            leverage INTEGER NOT NULL,
            opened_at TEXT NOT NULL,
            entry_reference_price TEXT NOT NULL,
            entry_commission_usdt TEXT NOT NULL,
            entry_slippage_usdt TEXT NOT NULL,
            stop_order_id TEXT,
            take_profit_order_id TEXT
        );

        CREATE TABLE IF NOT EXISTS risk_state (
            scope TEXT PRIMARY KEY,
            day TEXT NOT NULL,
            daily_realized_pnl TEXT NOT NULL,
            consecutive_losses INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS risk_cooldowns (
            symbol TEXT PRIMARY KEY,
            until_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            mode TEXT NOT NULL,
            market TEXT NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            side TEXT NOT NULL,
            direction TEXT NOT NULL,
            leverage TEXT NOT NULL,
            entry_price TEXT,
            reference_price TEXT,
            price TEXT NOT NULL,
            fill_price TEXT,
            quantity TEXT NOT NULL,
            notional_usdt TEXT NOT NULL,
            margin_usdt TEXT NOT NULL,
            risk_at_stop_usdt TEXT NOT NULL,
            commission_usdt TEXT,
            slippage_usdt TEXT,
            slippage_pct TEXT,
            gross_realized_pnl_usdt TEXT,
            realized_pnl_usdt TEXT,
            close_kind TEXT,
            reason TEXT NOT NULL,
            status TEXT NOT NULL,
            order_id TEXT,
            raw_response TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
        """
    )


def get_meta(connection: sqlite3.Connection, key: str) -> str | None:
    row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return None if row is None else str(row["value"])


def set_meta(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        """
        INSERT INTO meta(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def table_count(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return 0 if row is None else int(row["count"])


def migrate_legacy_data_if_needed(config: Config, connection: sqlite3.Connection) -> None:
    migrate_legacy_positions_if_needed(config, connection)
    migrate_legacy_risk_state_if_needed(config, connection)
    migrate_legacy_trade_log_if_needed(config, connection)


def migrate_legacy_positions_if_needed(config: Config, connection: sqlite3.Connection) -> None:
    if get_meta(connection, "positions_migrated") == "1":
        return
    if table_count(connection, "positions") > 0:
        set_meta(connection, "positions_migrated", "1")
        return
    if not config.positions_file.exists():
        set_meta(connection, "positions_migrated", "1")
        return

    try:
        raw_positions = json.loads(config.positions_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logging.warning("Не удалось мигрировать legacy positions %s: %s", config.positions_file, exc)
        set_meta(connection, "positions_migrated", "1")
        return

    for symbol, item in raw_positions.items():
        if "direction" not in item:
            continue
        connection.execute(
            """
            INSERT INTO positions(
                symbol, direction, entry_price, quantity, margin_used, leverage, opened_at,
                entry_reference_price, entry_commission_usdt, entry_slippage_usdt, stop_order_id, take_profit_order_id
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                item["direction"],
                str(item["entry_price"]),
                str(item["quantity"]),
                str(item.get("margin_used", "0")),
                int(item.get("leverage", config.leverage)),
                item.get("opened_at", utc_now()),
                str(item.get("entry_reference_price", item["entry_price"])),
                str(item.get("entry_commission_usdt", "0")),
                str(item.get("entry_slippage_usdt", "0")),
                item.get("stop_order_id"),
                item.get("take_profit_order_id"),
            ),
        )
    set_meta(connection, "positions_migrated", "1")


def migrate_legacy_risk_state_if_needed(config: Config, connection: sqlite3.Connection) -> None:
    if get_meta(connection, "risk_state_migrated") == "1":
        return
    if connection.execute("SELECT 1 FROM risk_state WHERE scope = 'main'").fetchone() is not None:
        set_meta(connection, "risk_state_migrated", "1")
        return
    if not config.risk_state_file.exists():
        set_meta(connection, "risk_state_migrated", "1")
        return

    try:
        raw_state = json.loads(config.risk_state_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logging.warning("Не удалось мигрировать legacy risk-state %s: %s", config.risk_state_file, exc)
        set_meta(connection, "risk_state_migrated", "1")
        return

    connection.execute(
        """
        INSERT INTO risk_state(scope, day, daily_realized_pnl, consecutive_losses)
        VALUES('main', ?, ?, ?)
        """,
        (
            str(raw_state.get("day", current_local_day())),
            str(raw_state.get("daily_realized_pnl", "0")),
            int(raw_state.get("consecutive_losses", 0)),
        ),
    )
    for symbol, until_value in dict(raw_state.get("cooldowns", {})).items():
        connection.execute(
            "INSERT OR REPLACE INTO risk_cooldowns(symbol, until_at) VALUES(?, ?)",
            (str(symbol), str(until_value)),
        )
    set_meta(connection, "risk_state_migrated", "1")


def migrate_legacy_trade_log_if_needed(config: Config, connection: sqlite3.Connection) -> None:
    if get_meta(connection, "trades_migrated") == "1":
        return
    if table_count(connection, "trades") > 0:
        set_meta(connection, "trades_migrated", "1")
        return
    if not config.trade_log_file.exists():
        set_meta(connection, "trades_migrated", "1")
        return

    try:
        with config.trade_log_file.open("r", newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
    except OSError as exc:
        logging.warning("Не удалось мигрировать legacy trade log %s: %s", config.trade_log_file, exc)
        set_meta(connection, "trades_migrated", "1")
        return

    for row in rows:
        payload = {column: row.get(column, "") for column in TRADE_COLUMNS}
        insert_trade_row(connection, payload)
    set_meta(connection, "trades_migrated", "1")


def load_positions_from_storage(config: Config) -> dict[str, Position]:
    with db_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT symbol, direction, entry_price, quantity, margin_used, leverage, opened_at,
                   entry_reference_price, entry_commission_usdt, entry_slippage_usdt,
                   stop_order_id, take_profit_order_id
            FROM positions
            ORDER BY symbol
            """
        ).fetchall()

    positions: dict[str, Position] = {}
    for row in rows:
        positions[str(row["symbol"])] = Position(
            symbol=str(row["symbol"]),
            direction=str(row["direction"]),
            entry_price=Decimal(str(row["entry_price"])),
            quantity=Decimal(str(row["quantity"])),
            margin_used=Decimal(str(row["margin_used"])),
            leverage=int(row["leverage"]),
            opened_at=str(row["opened_at"]),
            entry_reference_price=Decimal(str(row["entry_reference_price"])),
            entry_commission_usdt=Decimal(str(row["entry_commission_usdt"])),
            entry_slippage_usdt=Decimal(str(row["entry_slippage_usdt"])),
            stop_order_id=None if row["stop_order_id"] is None else str(row["stop_order_id"]),
            take_profit_order_id=None if row["take_profit_order_id"] is None else str(row["take_profit_order_id"]),
        )
    return positions


def save_positions_to_storage(config: Config, positions: dict[str, Position]) -> None:
    with db_connection(config) as connection:
        connection.execute("DELETE FROM positions")
        for position in positions.values():
            connection.execute(
                """
                INSERT INTO positions(
                    symbol, direction, entry_price, quantity, margin_used, leverage, opened_at,
                    entry_reference_price, entry_commission_usdt, entry_slippage_usdt, stop_order_id, take_profit_order_id
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.symbol,
                    position.direction,
                    str(position.entry_price),
                    str(position.quantity),
                    str(position.margin_used),
                    position.leverage,
                    position.opened_at,
                    str(position.entry_reference_price),
                    str(position.entry_commission_usdt),
                    str(position.entry_slippage_usdt),
                    position.stop_order_id,
                    position.take_profit_order_id,
                ),
            )


def load_risk_state_from_storage(config: Config) -> RiskState | None:
    with db_connection(config) as connection:
        row = connection.execute(
            "SELECT day, daily_realized_pnl, consecutive_losses FROM risk_state WHERE scope = 'main'"
        ).fetchone()
        cooldown_rows = connection.execute("SELECT symbol, until_at FROM risk_cooldowns ORDER BY symbol").fetchall()

    if row is None:
        return None

    return RiskState(
        day=str(row["day"]),
        daily_realized_pnl=Decimal(str(row["daily_realized_pnl"])),
        consecutive_losses=int(row["consecutive_losses"]),
        cooldowns={str(item["symbol"]): str(item["until_at"]) for item in cooldown_rows},
    )


def save_risk_state_to_storage(config: Config, state: RiskState) -> None:
    with db_connection(config) as connection:
        connection.execute(
            """
            INSERT INTO risk_state(scope, day, daily_realized_pnl, consecutive_losses)
            VALUES('main', ?, ?, ?)
            ON CONFLICT(scope) DO UPDATE SET
                day = excluded.day,
                daily_realized_pnl = excluded.daily_realized_pnl,
                consecutive_losses = excluded.consecutive_losses
            """,
            (state.day, str(state.daily_realized_pnl), state.consecutive_losses),
        )
        connection.execute("DELETE FROM risk_cooldowns")
        for symbol, until_at in state.cooldowns.items():
            connection.execute(
                "INSERT INTO risk_cooldowns(symbol, until_at) VALUES(?, ?)",
                (symbol, until_at),
            )


def insert_trade_row(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    connection.execute(
        f"""
        INSERT INTO trades({", ".join(TRADE_COLUMNS)})
        VALUES({", ".join("?" for _ in TRADE_COLUMNS)})
        """,
        tuple("" if payload.get(column) is None else str(payload.get(column)) for column in TRADE_COLUMNS),
    )


def append_trade_row_to_storage(config: Config, payload: dict[str, Any]) -> None:
    with db_connection(config) as connection:
        insert_trade_row(connection, payload)


def load_trade_rows_from_storage(config: Config) -> list[dict[str, str]]:
    with db_connection(config) as connection:
        rows = connection.execute(
            f"SELECT {', '.join(TRADE_COLUMNS)} FROM trades ORDER BY timestamp, id"
        ).fetchall()

    return [
        {column: "" if row[column] is None else str(row[column]) for column in TRADE_COLUMNS}
        for row in rows
    ]
