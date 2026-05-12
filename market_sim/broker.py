from __future__ import annotations

import sqlite3
from dataclasses import asdict
from typing import Any

from .data_sources import infer_market
from .db import utc_now
from .strategy import Signal


def execute_signals(conn: sqlite3.Connection, signals: list[Signal], settings: dict[str, Any]) -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    orders_by_market: dict[str, int] = {}
    for signal in signals:
        info = infer_market(signal.symbol)
        if not info.tradeable:
            continue
        _mark_position(conn, signal.symbol, signal.close)
        risk_trade = _risk_exit(conn, signal, settings)
        if risk_trade:
            executed.append(risk_trade)
            continue
        if signal.action == "BUY":
            if not _can_open_buy(conn, signal, settings):
                continue
            current_count = orders_by_market.get(signal.market, 0)
            max_orders = int(settings["risk"]["max_new_orders_per_market"])
            if current_count >= max_orders:
                continue
            trade = _buy(conn, signal, settings)
            if trade:
                orders_by_market[signal.market] = current_count + 1
                executed.append(trade)
        elif signal.action == "SELL":
            trade = _sell(conn, signal, settings, full=signal.score <= settings["strategy"]["strong_sell_threshold"])
            if trade:
                executed.append(trade)
    return executed


def enforce_position_risk(conn: sqlite3.Connection, settings: dict[str, Any]) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT symbol, market, quantity, avg_cost, last_price
        FROM positions
        ORDER BY market, symbol
        """
    ).fetchall()
    executed: list[dict[str, Any]] = []
    for row in rows:
        avg_cost = float(row["avg_cost"])
        last_price = float(row["last_price"])
        if avg_cost <= 0 or last_price <= 0:
            continue
        change = last_price / avg_cost - 1.0
        signal = Signal(
            symbol=row["symbol"],
            market=row["market"],
            action="SELL",
            score=-1.0 if change < 0 else 0.4,
            recommendation_index=0 if change < 0 else 70,
            recommendation_label="持仓风控",
            confidence=1.0,
            close=last_price,
            rationale=[f"持仓浮动盈亏 {change:+.2%}"],
            features={},
        )
        if change <= -float(settings["risk"]["stop_loss_pct"]):
            trade = _sell(conn, signal, settings, full=True, reason="stop_loss_exit")
        elif change >= float(settings["risk"]["take_profit_pct"]):
            trade = _sell(conn, signal, settings, full=False, reason="take_profit_trim")
        else:
            trade = None
        if trade:
            executed.append(trade)
    return executed


def record_signal(conn: sqlite3.Connection, signal: Signal, observed: bool = False) -> int:
    cursor = conn.execute(
        """
        INSERT INTO signals(
            ts, symbol, market, action, score, recommendation_index, recommendation_label,
            confidence, close, rationale, features, observed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, json(?), json(?), ?)
        """,
        (
            utc_now(),
            signal.symbol,
            signal.market,
            signal.action,
            signal.score,
            signal.recommendation_index,
            signal.recommendation_label,
            signal.confidence,
            signal.close,
            _json_text(signal.rationale),
            _json_text(signal.features),
            1 if observed else 0,
        ),
    )
    return int(cursor.lastrowid)


def upsert_prices(
    conn: sqlite3.Connection,
    symbol: str,
    market: str,
    currency: str,
    source: str,
    rows: list[dict[str, Any]],
) -> None:
    now = utc_now()
    for row in rows:
        conn.execute(
            """
            INSERT INTO prices(symbol, date, market, currency, open, high, low, close, volume, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                market = excluded.market,
                currency = excluded.currency,
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                row["date"],
                market,
                currency,
                _float_or_none(row.get("open")),
                _float_or_none(row.get("high")),
                _float_or_none(row.get("low")),
                float(row["close"]),
                _float_or_none(row.get("volume")),
                source,
                now,
            ),
        )


def update_equity(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    currencies = conn.execute("SELECT currency, cash FROM portfolio_cash").fetchall()
    output = []
    today = utc_now()[:10]
    for row in currencies:
        currency = row["currency"]
        cash = float(row["cash"])
        positions = conn.execute(
            "SELECT quantity, last_price FROM positions WHERE currency = ?",
            (currency,),
        ).fetchall()
        positions_value = sum(float(pos["quantity"]) * float(pos["last_price"]) for pos in positions)
        equity = cash + positions_value
        conn.execute(
            """
            INSERT INTO equity_curve(date, currency, equity, cash, positions_value, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, currency) DO UPDATE SET
                equity = excluded.equity,
                cash = excluded.cash,
                positions_value = excluded.positions_value,
                updated_at = excluded.updated_at
            """,
            (today, currency, equity, cash, positions_value, utc_now()),
        )
        output.append(
            {
                "currency": currency,
                "equity": round(equity, 2),
                "cash": round(cash, 2),
                "positions_value": round(positions_value, 2),
            }
        )
    return output


def portfolio_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    cash = [dict(row) for row in conn.execute("SELECT currency, cash FROM portfolio_cash ORDER BY currency")]
    positions = [
        dict(row)
        for row in conn.execute(
            """
            SELECT symbol, market, currency, quantity, avg_cost, last_price, updated_at,
                   quantity * avg_cost AS cost_value,
                   quantity * last_price AS market_value,
                   (last_price - avg_cost) * quantity AS unrealized_pnl,
                   CASE WHEN avg_cost > 0 THEN (last_price / avg_cost - 1.0) ELSE 0 END AS unrealized_pct
            FROM positions
            ORDER BY market, symbol
            """
        )
    ]
    equity = update_equity(conn)
    return {"cash": cash, "positions": positions, "equity": equity}


def _can_open_buy(conn: sqlite3.Connection, signal: Signal, settings: dict[str, Any]) -> bool:
    info = infer_market(signal.symbol)
    current = conn.execute("SELECT quantity FROM positions WHERE symbol = ?", (signal.symbol,)).fetchone()
    is_new_position = current is None or float(current["quantity"]) <= 0
    filters = settings.get("risk", {}).get("entry_filters", {})
    if signal.recommendation_index < int(settings.get("strategy", {}).get("recommend_buy_score", 75)):
        return False
    if is_new_position and filters.get("watchlist_only_new_positions", True) and not _is_watchlist_symbol(signal.symbol, settings):
        return False
    min_price = float(filters.get("min_price", {}).get(info.currency, 0.0))
    if min_price and signal.close < min_price:
        return False
    features = signal.features or {}
    checks = {
        "liquidity": float(filters.get("min_liquidity", 0.0)),
        "trend_strength": float(filters.get("min_trend_strength", 0.05)),
        "momentum": float(filters.get("min_momentum", 0.0)),
        "money_flow": float(filters.get("min_money_flow", -0.05)),
    }
    for name, minimum in checks.items():
        if float(features.get(name, 0.0)) < minimum:
            return False
    if float(features.get("risk", 0.0)) < -0.15:
        return False
    return True


def _buy(conn: sqlite3.Connection, signal: Signal, settings: dict[str, Any]) -> dict[str, Any] | None:
    info = infer_market(signal.symbol)
    currency = info.currency
    cash_row = conn.execute("SELECT cash FROM portfolio_cash WHERE currency = ?", (currency,)).fetchone()
    if not cash_row:
        return None
    cash = float(cash_row["cash"])
    equity = _currency_equity(conn, currency)
    target_value = equity * float(settings["risk"]["max_position_pct"]) * max(0.35, min(1.0, signal.confidence))
    current = conn.execute("SELECT quantity, avg_cost FROM positions WHERE symbol = ?", (signal.symbol,)).fetchone()
    current_value = float(current["quantity"]) * signal.close if current else 0.0
    order_value = min(target_value - current_value, cash * 0.95)
    min_order = float(settings["risk"]["min_order_value"][currency])
    if order_value < min_order:
        return None
    lot = _lot_size(signal.symbol)
    quantity = int(order_value / signal.close / lot) * lot
    if quantity <= 0:
        return None
    gross = quantity * signal.close
    fee = _fee(currency, gross, settings)
    if gross + fee > cash:
        quantity = int((cash - fee) / signal.close / lot) * lot
        gross = quantity * signal.close
        fee = _fee(currency, gross, settings)
    if quantity <= 0 or gross + fee > cash:
        return None
    if current:
        old_qty = float(current["quantity"])
        old_cost = float(current["avg_cost"])
        new_qty = old_qty + quantity
        avg_cost = ((old_qty * old_cost) + gross) / new_qty
    else:
        new_qty = quantity
        avg_cost = signal.close
    conn.execute("UPDATE portfolio_cash SET cash = cash - ?, updated_at = ? WHERE currency = ?", (gross + fee, utc_now(), currency))
    conn.execute(
        """
        INSERT INTO positions(symbol, market, currency, quantity, avg_cost, last_price, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            quantity = excluded.quantity,
            avg_cost = excluded.avg_cost,
            last_price = excluded.last_price,
            updated_at = excluded.updated_at
        """,
        (signal.symbol, signal.market, currency, new_qty, avg_cost, signal.close, utc_now()),
    )
    return _record_trade(conn, signal, "BUY", quantity, gross, fee, "signal_buy")


def _sell(
    conn: sqlite3.Connection,
    signal: Signal,
    settings: dict[str, Any],
    full: bool,
    reason: str | None = None,
) -> dict[str, Any] | None:
    row = conn.execute("SELECT quantity, currency FROM positions WHERE symbol = ?", (signal.symbol,)).fetchone()
    if not row:
        return None
    quantity_held = float(row["quantity"])
    if quantity_held <= 0:
        return None
    lot = _lot_size(signal.symbol)
    quantity = quantity_held if full else max(lot, int((quantity_held * 0.5) / lot) * lot)
    quantity = min(quantity, quantity_held)
    gross = quantity * signal.close
    currency = row["currency"]
    fee = _fee(currency, gross, settings)
    conn.execute("UPDATE portfolio_cash SET cash = cash + ?, updated_at = ? WHERE currency = ?", (gross - fee, utc_now(), currency))
    remaining = quantity_held - quantity
    if remaining <= 0:
        conn.execute("DELETE FROM positions WHERE symbol = ?", (signal.symbol,))
    else:
        conn.execute(
            "UPDATE positions SET quantity = ?, last_price = ?, updated_at = ? WHERE symbol = ?",
            (remaining, signal.close, utc_now(), signal.symbol),
        )
    trade_reason = reason or ("signal_sell_full" if full else "signal_sell_half")
    return _record_trade(conn, signal, "SELL", quantity, gross, fee, trade_reason)


def _risk_exit(conn: sqlite3.Connection, signal: Signal, settings: dict[str, Any]) -> dict[str, Any] | None:
    row = conn.execute("SELECT quantity, avg_cost FROM positions WHERE symbol = ?", (signal.symbol,)).fetchone()
    if not row:
        return None
    avg_cost = float(row["avg_cost"])
    if avg_cost <= 0:
        return None
    change = signal.close / avg_cost - 1.0
    if change <= -float(settings["risk"]["stop_loss_pct"]):
        forced = Signal(**{**asdict(signal), "action": "SELL"})
        return _sell(conn, forced, settings, full=True, reason="stop_loss_exit")
    if change >= float(settings["risk"]["take_profit_pct"]):
        forced = Signal(**{**asdict(signal), "action": "SELL"})
        return _sell(conn, forced, settings, full=False, reason="take_profit_trim")
    return None


def _record_trade(
    conn: sqlite3.Connection,
    signal: Signal,
    side: str,
    quantity: float,
    gross: float,
    fee: float,
    reason: str,
) -> dict[str, Any]:
    info = infer_market(signal.symbol)
    cursor = conn.execute(
        """
        INSERT INTO trades(ts, symbol, market, currency, side, quantity, price, gross, fee, reason, signal_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now(),
            signal.symbol,
            signal.market,
            info.currency,
            side,
            quantity,
            signal.close,
            gross,
            fee,
            reason,
            signal.score,
        ),
    )
    trade = {
        "id": int(cursor.lastrowid),
        "symbol": signal.symbol,
        "market": signal.market,
        "currency": info.currency,
        "side": side,
        "quantity": quantity,
        "price": signal.close,
        "gross": round(gross, 2),
        "fee": round(fee, 2),
        "reason": reason,
        "signal_score": signal.score,
    }
    return trade


def _mark_position(conn: sqlite3.Connection, symbol: str, price: float) -> None:
    conn.execute("UPDATE positions SET last_price = ?, updated_at = ? WHERE symbol = ?", (price, utc_now(), symbol))


def _currency_equity(conn: sqlite3.Connection, currency: str) -> float:
    cash_row = conn.execute("SELECT cash FROM portfolio_cash WHERE currency = ?", (currency,)).fetchone()
    cash = float(cash_row["cash"]) if cash_row else 0.0
    positions = conn.execute("SELECT quantity, last_price FROM positions WHERE currency = ?", (currency,)).fetchall()
    return cash + sum(float(row["quantity"]) * float(row["last_price"]) for row in positions)


def _fee(currency: str, gross: float, settings: dict[str, Any]) -> float:
    fee_cfg = settings["risk"]["fees"][currency]
    return max(float(fee_cfg["min"]), gross * float(fee_cfg["rate"]))


def _lot_size(symbol: str) -> int:
    return 100 if symbol.upper().endswith(".T") else 1


def _is_watchlist_symbol(symbol: str, settings: dict[str, Any]) -> bool:
    wanted = symbol.strip().upper()
    for rows in settings.get("watchlists", {}).values():
        for item in rows:
            if str(item).strip().upper() == wanted:
                return True
    return False


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_text(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False)
