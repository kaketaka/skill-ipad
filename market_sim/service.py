from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from .broker import enforce_position_risk, execute_signals, portfolio_snapshot, record_signal, update_equity, upsert_prices
from .data_sources import fetch_history, fetch_latest_quote as fetch_market_quote, fetch_latest_quotes as fetch_market_quotes, infer_market
from .db import get_settings, get_weights, init_db, reset_paper_trading_state, rows_to_dicts, session, utc_now
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
        refresh_open_position_prices(conn, settings)
        risk_trades = enforce_position_risk(conn, settings)
        blocked_symbols = {trade["symbol"] for trade in risk_trades if trade.get("reason") == "stop_loss_exit"}
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
        trades = risk_trades + execute_signals(conn, [signal for signal in signals if signal.symbol not in blocked_symbols], settings)
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
        refresh_open_position_prices(conn, settings)
        update_equity(conn)
        return create_daily_review(conn, settings)


def get_dashboard() -> dict[str, Any]:
    init_db()
    with session() as conn:
        settings = get_settings(conn)
        weights = get_weights(conn)
        quote_refresh = refresh_open_position_prices(conn, settings)
        observation_refresh = refresh_watchlist_signals(conn, settings, weights)
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
        risk_alerts = _position_risk_alerts(portfolio.get("positions", []), settings)
        traded_quotes = _traded_quotes(conn, portfolio.get("positions", []))
        portfolio_summary = _portfolio_summary(conn, settings, portfolio)
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
            "risk_alerts": risk_alerts,
            "traded_quotes": traded_quotes,
            "portfolio_summary": portfolio_summary,
            "quote_refresh": quote_refresh,
            "observation_refresh": observation_refresh,
            "auto_refresh_seconds": int(settings.get("refresh", {}).get("dashboard_seconds", 30)),
        }


def refresh_open_position_prices(conn, settings: dict[str, Any]) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT symbol
        FROM positions
        ORDER BY market, symbol
        """
    ).fetchall()
    refreshed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    symbols = [row["symbol"] for row in rows]
    try:
        quotes = fetch_market_quotes(symbols, settings.get("data_sources", ["yfinance"])) if len(symbols) > 1 else {}
    except Exception as exc:
        quotes = {}
        errors.append({"symbol": "*", "error": str(exc)})
    for row in rows:
        symbol = row["symbol"]
        try:
            quote = quotes.get(symbol) or fetch_market_quote(symbol, settings.get("data_sources", ["yfinance"]))
            now = utc_now()
            upsert_prices(
                conn,
                symbol,
                quote.market,
                quote.currency,
                quote.source,
                _quote_price_rows(quote),
            )
            conn.execute(
                """
                UPDATE positions
                SET last_price = ?, updated_at = ?
                WHERE symbol = ?
                """,
                (quote.price, now, symbol),
            )
            refreshed.append(
                {
                    "symbol": symbol,
                    "price": round(quote.price, 4),
                    "previous_close": round(quote.previous_close, 4) if quote.previous_close is not None else None,
                    "as_of": quote.as_of,
                    "source": quote.source,
                }
            )
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})
    return {"refreshed": refreshed, "errors": errors, "updated_at": utc_now()}


def refresh_watchlist_signals(conn, settings: dict[str, Any], weights: dict[str, float]) -> dict[str, Any]:
    interval = int(settings.get("refresh", {}).get("watchlist_signal_seconds", 30))
    refreshed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    due_symbols: list[str] = []
    for symbol in _watchlist_symbols(settings):
        age = _latest_observed_signal_age(conn, symbol)
        if age is not None and age < interval:
            skipped.append({"symbol": symbol, "age_seconds": round(age, 1)})
            continue
        due_symbols.append(symbol)
    try:
        quotes = fetch_market_quotes(due_symbols, settings.get("data_sources", ["yfinance"])) if due_symbols else {}
    except Exception as exc:
        quotes = {}
        errors.append({"symbol": "*", "error": str(exc)})
    for symbol in due_symbols:
        try:
            info = infer_market(symbol)
            if _stored_price_count(conn, symbol) < 60:
                frame, source = fetch_history(symbol, settings["data_sources"])
                upsert_prices(conn, symbol, info.market, info.currency, source, frame.to_dict("records"))
            quote = quotes.get(symbol) or fetch_market_quote(symbol, settings.get("data_sources", ["yfinance"]))
            upsert_prices(conn, symbol, quote.market, quote.currency, quote.source, _quote_price_rows(quote))
            candles = _candles(conn, symbol, limit=260)
            signal = generate_signal(symbol, info.market, compute_indicators(_frame_from_candles(candles)), weights, settings)
            signal.close = round(float(quote.price), 4)
            record_signal(conn, signal, observed=True)
            refreshed.append(
                {
                    "symbol": symbol,
                    "action": signal.action,
                    "recommendation_index": signal.recommendation_index,
                    "price": signal.close,
                    "source": quote.source,
                }
            )
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})
    return {
        "refreshed": refreshed,
        "skipped": skipped,
        "errors": errors,
        "interval_seconds": interval,
        "updated_at": utc_now(),
    }


def _quote_price_rows(quote) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if quote.previous_close is not None and quote.previous_as_of is not None:
        rows.append(
            {
                "date": quote.previous_as_of,
                "open": quote.previous_close,
                "high": quote.previous_close,
                "low": quote.previous_close,
                "close": quote.previous_close,
                "volume": None,
            }
        )
    rows.append(
        {
            "date": quote.as_of,
            "open": quote.open if quote.open is not None else quote.price,
            "high": quote.high if quote.high is not None else quote.price,
            "low": quote.low if quote.low is not None else quote.price,
            "close": quote.price,
            "volume": quote.volume,
        }
    )
    return rows


def _watchlist_symbols(settings: dict[str, Any]) -> list[str]:
    output: list[str] = []
    for market in ["US", "JP"]:
        for symbol in settings.get("watchlists", {}).get(market, []):
            clean = str(symbol).strip().upper()
            if clean and clean not in output:
                output.append(clean)
    return output


def _latest_observed_signal_age(conn, symbol: str) -> float | None:
    row = conn.execute(
        """
        SELECT ts
        FROM signals
        WHERE symbol = ? AND observed = 1
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if not row:
        return None
    try:
        timestamp = datetime.fromisoformat(str(row["ts"]).replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds())


def _stored_price_count(conn, symbol: str) -> int:
    row = conn.execute("SELECT count(*) AS count FROM prices WHERE symbol = ?", (symbol,)).fetchone()
    return int(row["count"] or 0) if row else 0


def _frame_from_candles(candles: list[dict[str, Any]]):
    import pandas as pd

    return pd.DataFrame(candles)


def _portfolio_summary(conn, settings: dict[str, Any], portfolio: dict[str, Any]) -> list[dict[str, Any]]:
    fees = {
        row["currency"]: float(row["fees"] or 0.0)
        for row in conn.execute(
            """
            SELECT currency, sum(fee) AS fees
            FROM trades
            GROUP BY currency
            """
        ).fetchall()
    }
    initial = {currency: float(value) for currency, value in settings.get("portfolio", {}).items()}
    output: list[dict[str, Any]] = []
    for row in portfolio.get("equity", []):
        currency = row.get("currency")
        equity = float(row.get("equity") or 0.0)
        starting_equity = initial.get(currency, 0.0)
        output.append(
            {
                "currency": currency,
                "initial_equity": round(starting_equity, 2),
                "equity_delta": round(equity - starting_equity, 2),
                "equity_delta_pct": round((equity / starting_equity - 1.0), 6) if starting_equity else None,
                "total_fees": round(fees.get(currency, 0.0), 2),
            }
        )
    return output


def _position_risk_alerts(positions: list[dict[str, Any]], settings: dict[str, Any]) -> list[dict[str, Any]]:
    stop_loss_pct = float(settings.get("risk", {}).get("stop_loss_pct", 0.05))
    take_profit_pct = float(settings.get("risk", {}).get("take_profit_pct", 0.12))
    output: list[dict[str, Any]] = []
    for row in positions:
        avg_cost = float(row.get("avg_cost") or 0.0)
        last_price = float(row.get("last_price") or 0.0)
        if avg_cost <= 0 or last_price <= 0:
            continue
        change = last_price / avg_cost - 1.0
        if change <= -stop_loss_pct:
            status = "已触发止损"
        elif change <= -stop_loss_pct * 0.8:
            status = "接近止损"
        elif change >= take_profit_pct:
            status = "已触发止盈减仓"
        elif change >= take_profit_pct * 0.8:
            status = "接近止盈"
        else:
            status = "正常"
        output.append(
            {
                "symbol": row.get("symbol"),
                "market": row.get("market"),
                "change_pct": round(change, 6),
                "stop_price": round(avg_cost * (1.0 - stop_loss_pct), 4),
                "take_profit_price": round(avg_cost * (1.0 + take_profit_pct), 4),
                "status": status,
            }
        )
    return output


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
            enriched["price_source"] = quote.get("source")
            enriched["prev_close"] = quote.get("prev_close")
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
        LIMIT 10
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return None
    latest = rows[0]
    latest_day = str(latest["date"])[:10]
    previous = next((row for row in rows[1:] if str(row["date"])[:10] < latest_day), None)
    if previous is None and len(rows) > 1:
        previous = rows[1]
    prev_close = float(previous["close"]) if previous is not None else None
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
    import math

    import pandas as pd

    frame = pd.DataFrame(candles)
    if frame.empty:
        return {}
    enriched = compute_indicators(frame)
    latest = latest_complete_row(enriched)
    keys = [
        "return_1d",
        "return_5d",
        "return_20d",
        "return_60d",
        "sma20",
        "sma50",
        "sma200",
        "macd_hist",
        "rsi14",
        "atr14",
        "atr_pct",
        "bb_upper",
        "bb_lower",
        "volume_sma20",
        "dollar_volume_sma20",
        "high20",
        "low20",
        "slope20",
        "obv",
        "obv_slope20",
        "cmf20",
        "mfi14",
        "adx14",
        "plus_di14",
        "minus_di14",
        "volatility20",
    ]
    snapshot: dict[str, Any] = {}
    for key in keys:
        value = latest.get(key)
        if value is None:
            continue
        try:
            numeric = float(value)
        except Exception:
            continue
        if math.isfinite(numeric):
            snapshot[key] = round(numeric, 6)
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
