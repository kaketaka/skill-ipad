from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

from .db import get_weights, save_weights, utc_now


def create_daily_review(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, Any]:
    review_date = date.today().isoformat()
    trades = conn.execute(
        """
        SELECT * FROM trades
        WHERE substr(ts, 1, 10) = ?
        ORDER BY ts
        """,
        (review_date,),
    ).fetchall()
    signals = conn.execute(
        """
        SELECT * FROM signals
        WHERE substr(ts, 1, 10) = ?
        ORDER BY abs(score) DESC
        LIMIT 20
        """,
        (review_date,),
    ).fetchall()
    equity = conn.execute(
        """
        SELECT currency, equity, cash, positions_value
        FROM equity_curve
        WHERE date = ?
        ORDER BY currency
        """,
        (review_date,),
    ).fetchall()
    positions = conn.execute(
        """
        SELECT symbol, market, currency, quantity, avg_cost, last_price,
               (last_price / avg_cost - 1.0) AS pnl_pct
        FROM positions
        ORDER BY market, symbol
        """,
    ).fetchall()
    metrics = _metrics(trades, equity, positions)
    lessons = _learn_from_open_positions(conn, settings)
    summary = _summary(metrics, lessons, signals)
    conn.execute(
        """
        INSERT INTO reviews(review_date, created_at, summary, metrics, lessons)
        VALUES (?, ?, ?, json(?), json(?))
        """,
        (review_date, utc_now(), summary, _json(metrics), _json(lessons)),
    )
    return {"review_date": review_date, "summary": summary, "metrics": metrics, "lessons": lessons}


def _metrics(trades: list[sqlite3.Row], equity: list[sqlite3.Row], positions: list[sqlite3.Row]) -> dict[str, Any]:
    buy_count = sum(1 for row in trades if row["side"] == "BUY")
    sell_count = sum(1 for row in trades if row["side"] == "SELL")
    fees_by_currency: dict[str, float] = {}
    turnover_by_currency: dict[str, float] = {}
    for row in trades:
        currency = row["currency"]
        fees_by_currency[currency] = fees_by_currency.get(currency, 0.0) + float(row["fee"])
        turnover_by_currency[currency] = turnover_by_currency.get(currency, 0.0) + float(row["gross"])
    unrealized = {}
    for row in positions:
        currency = row["currency"]
        pnl = (float(row["last_price"]) - float(row["avg_cost"])) * float(row["quantity"])
        unrealized[currency] = unrealized.get(currency, 0.0) + pnl
    return {
        "trade_count": len(trades),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "fees_by_currency": {key: round(value, 2) for key, value in fees_by_currency.items()},
        "turnover_by_currency": {key: round(value, 2) for key, value in turnover_by_currency.items()},
        "unrealized_pnl_by_currency": {key: round(value, 2) for key, value in unrealized.items()},
        "equity": [dict(row) for row in equity],
        "open_positions": len(positions),
    }


def _learn_from_open_positions(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT s.features, s.action, s.symbol, p.avg_cost, p.last_price
        FROM signals s
        JOIN positions p ON p.symbol = s.symbol
        WHERE s.action = 'BUY'
        ORDER BY s.ts DESC
        LIMIT 80
        """
    ).fetchall()
    weights = get_weights(conn)
    if not rows:
        return {"message": "暂无足够持仓样本，策略权重保持不变。", "weights": weights}
    learning_rate = float(settings["strategy"]["learning_rate"])
    min_weight = float(settings["strategy"]["min_weight"])
    max_weight = float(settings["strategy"]["max_weight"])
    adjustments = {key: 0.0 for key in weights}
    sample_count = 0
    for row in rows:
        try:
            features = _loads(row["features"])
        except Exception:
            continue
        pnl_pct = float(row["last_price"]) / float(row["avg_cost"]) - 1.0
        if abs(pnl_pct) < 0.002:
            continue
        direction = 1.0 if pnl_pct > 0 else -1.0
        for name, value in features.items():
            if name in adjustments and abs(float(value)) > 0.15:
                adjustments[name] += direction * (1 if float(value) > 0 else -1)
        sample_count += 1
    if not sample_count:
        return {"message": "持仓尚未产生足够盈亏样本，策略权重保持不变。", "weights": weights}
    for name, adjustment in adjustments.items():
        delta = learning_rate * adjustment / sample_count
        weights[name] = min(max_weight, max(min_weight, float(weights[name]) + delta))
    total = sum(weights.values()) or 1.0
    weights = {key: round(value / total, 4) for key, value in weights.items()}
    save_weights(conn, weights)
    top_changes = sorted(adjustments.items(), key=lambda item: abs(item[1]), reverse=True)[:3]
    return {
        "message": f"用 {sample_count} 个仍在持仓的买入样本做了有边界的权重微调。",
        "adjustments": {key: round(value, 4) for key, value in top_changes},
        "weights": weights,
    }


def _summary(metrics: dict[str, Any], lessons: dict[str, Any], signals: list[sqlite3.Row]) -> str:
    strongest = [f"{row['symbol']} {row['action']} {float(row['score']):+.2f}" for row in signals[:3]]
    equity_text = ", ".join(f"{row['currency']} {float(row['equity']):,.0f}" for row in metrics["equity"]) or "暂无权益记录"
    trade_text = f"今天模拟成交 {metrics['trade_count']} 笔，买入 {metrics['buy_count']}，卖出 {metrics['sell_count']}。"
    signal_text = "强信号：" + "；".join(strongest) if strongest else "今天没有可复盘信号。"
    return f"{trade_text} 当前权益 {equity_text}。{signal_text} {lessons['message']}"


def _json(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False)


def _loads(value: str) -> Any:
    import json

    return json.loads(value)
