from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .broker import execute_signals, portfolio_snapshot, record_signal, update_equity, upsert_prices
from .data_sources import fetch_history, infer_market
from .db import get_settings, get_weights, init_db, reset_paper_trading_state, rows_to_dicts, session
from .indicators import compute_indicators, latest_complete_row
from .review import create_daily_review
from .strategy import generate_signal, recommendation_guidance
from .universe import ensure_universe, observed_symbols, select_scan_symbols, sync_universe_market, universe_summary


def run_market_cycle(markets: list[str] | None = None) -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
        weights = get_weights(conn)
        selected = [market.upper() for market in (markets or ["US", "JP"])]
        universe_status = ensure_universe(settings, selected)
        signals = []
        errors = []
        scan_plan: dict[str, list[str]] = {}
        for market in selected:
            observed = observed_symbols(settings, market)
            symbols = select_scan_symbols(settings, market)
            scan_plan[market] = symbols
            for symbol in symbols:
                try:
                    info = infer_market(symbol)
                    frame, source = fetch_history(symbol, settings["data_sources"])
                    upsert_prices(conn, symbol, info.market, info.currency, source, frame.to_dict("records"))
                    signal = generate_signal(symbol, info.market, compute_indicators(frame), weights, settings)
                    record_signal(conn, signal, observed=symbol in observed)
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
            "scan_plan": {market: len(symbols) for market, symbols in scan_plan.items()},
            "universe": universe_status,
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
        observation_signals = rows_to_dicts(
            conn.execute(
                """
                SELECT s.*
                FROM signals s
                JOIN (
                    SELECT symbol, max(id) AS max_id
                    FROM signals
                    WHERE observed = 1
                    GROUP BY symbol
                ) latest ON latest.max_id = s.id
                ORDER BY s.market, s.recommendation_index DESC, s.symbol
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
        market_quotes = _market_quotes(conn, settings)
        positions_enriched = _enrich_positions(conn, portfolio.get("positions", []))
        traded_quotes = _traded_quotes(conn, portfolio.get("positions", []))
        return {
            "settings": settings,
            "weights": weights,
            "guidance": recommendation_guidance(settings),
            "universe": {
                "US": universe_summary("US").__dict__,
                "JP": universe_summary("JP").__dict__,
            },
            "portfolio": portfolio,
            "observation_signals": observation_signals,
            "signals": signals,
            "trades": trades,
            "reviews": reviews,
            "market_quotes": market_quotes,
            "positions_enriched": positions_enriched,
            "traded_quotes": traded_quotes,
        }


def _market_quotes(conn, settings: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    quotes: dict[str, list[dict[str, Any]]] = {"US": [], "JP": []}
    for market in ["US", "JP"]:
        symbols: list[str] = []
        benchmarks = settings.get("benchmarks", {}).get(market, [])
        for symbol in benchmarks:
            if isinstance(symbol, str) and symbol and symbol not in symbols:
                symbols.append(symbol)
        for symbol in settings.get("watchlists", {}).get(market, [])[:4]:
            if isinstance(symbol, str) and symbol and symbol not in symbols:
                symbols.append(symbol)
        for symbol in symbols[:8]:
            quote = _latest_quote(conn, symbol)
            if quote:
                quotes[market].append(quote)
    return quotes


def _traded_quotes(conn, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    symbols: list[str] = []
    for row in positions:
        symbol = str(row.get("symbol") or "").strip()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    since = (date.today() - timedelta(days=30)).isoformat()
    for row in conn.execute(
        """
        SELECT symbol, max(ts) AS last_ts
        FROM trades
        WHERE substr(ts, 1, 10) >= ?
        GROUP BY symbol
        ORDER BY last_ts DESC
        LIMIT 12
        """,
        (since,),
    ).fetchall():
        symbol = str(row["symbol"]).strip()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    output: list[dict[str, Any]] = []
    for symbol in symbols[:12]:
        quote = _latest_quote(conn, symbol)
        if not quote:
            continue
        candles = _candles(conn, symbol, limit=90)
        if candles:
            quote["candles"] = candles
            quote["indicators"] = _indicator_snapshot(candles)
        output.append(quote)
    return output


def _enrich_positions(conn, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in positions:
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            continue
        enriched = dict(row)
        qty = float(enriched.get("quantity") or 0.0)
        avg_cost = float(enriched.get("avg_cost") or 0.0)
        last_price = float(enriched.get("last_price") or 0.0)
        enriched["cost_value"] = round(qty * avg_cost, 2)
        enriched["market_value"] = round(qty * last_price, 2)
        quote = _latest_quote(conn, symbol)
        if quote:
            enriched["day_change"] = quote.get("change")
            enriched["day_change_pct"] = quote.get("change_pct")
            enriched["last_date"] = quote.get("date")
        candles = _candles(conn, symbol, limit=120)
        if candles:
            enriched["candles"] = candles
            enriched["indicators"] = _indicator_snapshot(candles)
        output.append(enriched)
    return output


def _latest_quote(conn, symbol: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT date, market, currency, close, source
        FROM prices
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT 2
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return None
    latest = rows[0]
    prev_close = float(rows[1]["close"]) if len(rows) > 1 else None
    close = float(latest["close"])
    change = round(close - prev_close, 4) if prev_close is not None else None
    change_pct = round(change / prev_close, 6) if prev_close not in (None, 0.0) else None
    return {
        "symbol": symbol,
        "market": latest["market"],
        "currency": latest["currency"],
        "date": latest["date"],
        "close": close,
        "prev_close": prev_close,
        "change": change,
        "change_pct": change_pct,
        "source": latest["source"],
    }


def _candles(conn, symbol: str, limit: int = 90) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT ?
        """,
        (symbol, int(limit)),
    ).fetchall()
    return [dict(row) for row in reversed(rows)]


def _indicator_snapshot(candles: list[dict[str, Any]]) -> dict[str, Any]:
    import pandas as pd

    frame = pd.DataFrame(candles)
    if frame.empty:
        return {}
    enriched = compute_indicators(frame)
    latest = latest_complete_row(enriched)
    keys = [
        "return_1d",
        "sma20",
        "sma50",
        "macd_hist",
        "rsi14",
        "atr14",
        "bb_upper",
        "bb_lower",
        "volume_sma20",
        "high20",
        "low20",
        "slope20",
        "obv",
        "cmf20",
        "mfi14",
    ]
    snapshot: dict[str, Any] = {}
    for key in keys:
        value = latest.get(key)
        if value is None:
            continue
        try:
            snapshot[key] = round(float(value), 6)
        except Exception:
            continue
    return snapshot


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
            for key in [
                "buy_threshold",
                "sell_threshold",
                "strong_sell_threshold",
                "learning_rate",
                "recommend_buy_score",
                "recommend_sell_score",
            ]:
                if key in patch["strategy"]:
                    settings["strategy"][key] = float(patch["strategy"][key])
        if "universe" in patch and isinstance(patch["universe"], dict):
            if "enabled" in patch["universe"]:
                settings["universe"]["enabled"] = bool(patch["universe"]["enabled"])
            if "daily_scan_limit" in patch["universe"] and isinstance(patch["universe"]["daily_scan_limit"], dict):
                for market in ["US", "JP"]:
                    if market in patch["universe"]["daily_scan_limit"]:
                        settings["universe"]["daily_scan_limit"][market] = int(patch["universe"]["daily_scan_limit"][market])
        from .db import save_settings

        save_settings(conn, settings)
        return settings


def sync_universe(markets: list[str] | None = None) -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
    selected = [market.upper() for market in (markets or ["US", "JP"])]
    return {market: sync_universe_market(market, settings) for market in selected}


def reset_demo_state(keep_signals: bool = True) -> dict[str, Any]:
    init_db()
    with session() as conn:
        reset_paper_trading_state(conn, keep_signals=keep_signals)
        equity = update_equity(conn)
        return {"ok": True, "keep_signals": keep_signals, "equity": equity}


def _clean_symbols(symbols: list[Any]) -> list[str]:
    output: list[str] = []
    for symbol in symbols:
        clean = str(symbol).strip().upper()
        if clean and clean not in output:
            output.append(clean)
    return output[:200]
