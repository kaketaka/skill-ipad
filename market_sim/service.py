from __future__ import annotations

from typing import Any

from .broker import execute_signals, portfolio_snapshot, record_signal, update_equity, upsert_prices
from .data_sources import fetch_history, infer_market
from .db import get_settings, get_weights, init_db, rows_to_dicts, session
from .indicators import compute_indicators
from .review import create_daily_review
from .strategy import generate_signal


def run_market_cycle(markets: list[str] | None = None) -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
        weights = get_weights(conn)
        selected = [market.upper() for market in (markets or ["US", "JP"])]
        signals = []
        errors = []
        for market in selected:
            for symbol in settings["watchlists"].get(market, []):
                try:
                    info = infer_market(symbol)
                    frame, source = fetch_history(symbol, settings["data_sources"])
                    upsert_prices(conn, symbol, info.market, info.currency, source, frame.to_dict("records"))
                    signal = generate_signal(symbol, info.market, compute_indicators(frame), weights, settings)
                    record_signal(conn, signal)
                    signals.append(signal)
                except Exception as exc:
                    errors.append({"symbol": symbol, "error": str(exc)})
        trades = execute_signals(conn, signals, settings)
        equity = update_equity(conn)
        return {
            "signals": [signal.__dict__ for signal in signals],
            "trades": trades,
            "equity": equity,
            "errors": errors,
        }


def run_review() -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
        update_equity(conn)
        return create_daily_review(conn, settings)


def get_dashboard() -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
        weights = get_weights(conn)
        portfolio = portfolio_snapshot(conn)
        signals = rows_to_dicts(
            conn.execute(
                """
                SELECT * FROM signals
                ORDER BY ts DESC
                LIMIT 60
                """
            ).fetchall()
        )
        trades = rows_to_dicts(
            conn.execute(
                """
                SELECT * FROM trades
                ORDER BY ts DESC
                LIMIT 80
                """
            ).fetchall()
        )
        reviews = rows_to_dicts(
            conn.execute(
                """
                SELECT * FROM reviews
                ORDER BY created_at DESC
                LIMIT 20
                """
            ).fetchall()
        )
        prices = rows_to_dicts(
            conn.execute(
                """
                SELECT p.symbol, p.market, p.currency, p.date, p.close, p.source
                FROM prices p
                JOIN (
                    SELECT symbol, max(date) AS max_date
                    FROM prices
                    GROUP BY symbol
                ) latest ON latest.symbol = p.symbol AND latest.max_date = p.date
                ORDER BY p.market, p.symbol
                """
            ).fetchall()
        )
        return {
            "settings": settings,
            "weights": weights,
            "portfolio": portfolio,
            "signals": signals,
            "trades": trades,
            "reviews": reviews,
            "latest_prices": prices,
        }


def update_settings(patch: dict[str, Any]) -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
        if "watchlists" in patch and isinstance(patch["watchlists"], dict):
            for market in ["US", "JP"]:
                incoming = patch["watchlists"].get(market)
                if isinstance(incoming, list):
                    settings["watchlists"][market] = _clean_symbols(incoming)
        if "data_sources" in patch and isinstance(patch["data_sources"], list):
            allowed = {"yfinance", "stooq", "alpha_vantage"}
            sources = [source for source in patch["data_sources"] if source in allowed]
            if sources:
                settings["data_sources"] = sources
        if "risk" in patch and isinstance(patch["risk"], dict):
            for key in ["max_position_pct", "stop_loss_pct", "take_profit_pct"]:
                if key in patch["risk"]:
                    settings["risk"][key] = float(patch["risk"][key])
        if "strategy" in patch and isinstance(patch["strategy"], dict):
            for key in ["buy_threshold", "sell_threshold", "strong_sell_threshold", "learning_rate"]:
                if key in patch["strategy"]:
                    settings["strategy"][key] = float(patch["strategy"][key])
        from .db import save_settings

        save_settings(conn, settings)
        return settings


def _clean_symbols(symbols: list[Any]) -> list[str]:
    output: list[str] = []
    for symbol in symbols:
        clean = str(symbol).strip().upper()
        if clean and clean not in output:
            output.append(clean)
    return output[:60]
