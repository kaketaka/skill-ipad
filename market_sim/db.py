from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

from .config import DATA_DIR, DB_PATH, DEFAULT_WEIGHTS, default_config, dumps_json, loads_json


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


@contextmanager
def session() -> Iterable[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with session() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prices (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                market TEXT NOT NULL,
                currency TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL NOT NULL,
                volume REAL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, date)
            );

            CREATE TABLE IF NOT EXISTS portfolio_cash (
                currency TEXT PRIMARY KEY,
                cash REAL NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                currency TEXT NOT NULL,
                quantity REAL NOT NULL,
                avg_cost REAL NOT NULL,
                last_price REAL NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                action TEXT NOT NULL,
                score REAL NOT NULL,
                recommendation_index INTEGER,
                recommendation_label TEXT,
                confidence REAL NOT NULL,
                close REAL NOT NULL,
                rationale TEXT NOT NULL,
                features TEXT NOT NULL,
                observed INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                currency TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                gross REAL NOT NULL,
                fee REAL NOT NULL,
                reason TEXT NOT NULL,
                signal_score REAL,
                source TEXT NOT NULL DEFAULT 'simulated',
                simulated INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                summary TEXT NOT NULL,
                metrics TEXT NOT NULL,
                lessons TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS strategy_weights (
                indicator TEXT PRIMARY KEY,
                weight REAL NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS equity_curve (
                date TEXT NOT NULL,
                currency TEXT NOT NULL,
                equity REAL NOT NULL,
                cash REAL NOT NULL,
                positions_value REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (date, currency)
            );
            """
        )
        _ensure_column(conn, "signals", "recommendation_index", "INTEGER")
        _ensure_column(conn, "signals", "recommendation_label", "TEXT")
        _ensure_column(conn, "signals", "observed", "INTEGER NOT NULL DEFAULT 0")

        now = utc_now()
        existing = conn.execute("SELECT value FROM config WHERE key = 'settings'").fetchone()
        if existing is None:
            cfg = default_config()
            conn.execute(
                "INSERT INTO config(key, value, updated_at) VALUES (?, ?, ?)",
                ("settings", dumps_json(cfg), now),
            )
            for currency, cash in cfg["portfolio"].items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO portfolio_cash(currency, cash, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (currency, float(cash), now),
                )

        for indicator, weight in DEFAULT_WEIGHTS.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO strategy_weights(indicator, weight, updated_at)
                VALUES (?, ?, ?)
                """,
                (indicator, weight, now),
            )


def get_settings(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute("SELECT value FROM config WHERE key = 'settings'").fetchone()
    cfg = default_config()
    if row:
        stored = loads_json(row["value"], {})
        if isinstance(stored, dict):
            cfg = _deep_merge(cfg, stored)
    return cfg


def save_settings(conn: sqlite3.Connection, settings: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO config(key, value, updated_at)
        VALUES ('settings', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (dumps_json(settings), utc_now()),
    )


def get_weights(conn: sqlite3.Connection) -> dict[str, float]:
    rows = conn.execute("SELECT indicator, weight FROM strategy_weights").fetchall()
    if not rows:
        return dict(DEFAULT_WEIGHTS)
    return {row["indicator"]: float(row["weight"]) for row in rows}


def save_weights(conn: sqlite3.Connection, weights: dict[str, float]) -> None:
    now = utc_now()
    for indicator, weight in weights.items():
        conn.execute(
            """
            INSERT INTO strategy_weights(indicator, weight, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(indicator) DO UPDATE
            SET weight = excluded.weight, updated_at = excluded.updated_at
            """,
            (indicator, float(weight), now),
        )


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
