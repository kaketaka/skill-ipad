from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

from .db import get_weights, save_weights, utc_now
from .indicators import compute_indicators, latest_complete_row


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
    market_wrap = _market_wrap(conn, settings)
    watchlist_notes = _watchlist_notes(conn, settings)
    summary = _summary(metrics, lessons, signals, market_wrap, watchlist_notes, settings)

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

    unrealized: dict[str, float] = {}
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
        "message": f"基于 {sample_count} 个仍在持仓的买入样本，做了有边界的权重微调。",
        "adjustments": {key: round(value, 4) for key, value in top_changes},
        "weights": weights,
    }


def _summary(
    metrics: dict[str, Any],
    lessons: dict[str, Any],
    signals: list[sqlite3.Row],
    market_wrap: dict[str, list[str]],
    watchlist_notes: list[str],
    settings: dict[str, Any],
) -> str:
    strongest = [f"- {row['symbol']}：{row['action']}（score {float(row['score']):+.2f}）" for row in signals[:5]]
    equity_text = " / ".join(f"{row['currency']} {float(row['equity']):,.0f}" for row in metrics["equity"]) or "暂无权益记录"
    fees = metrics.get("fees_by_currency") or {}
    fee_text = " / ".join(f"{key} {value:,.2f}" for key, value in fees.items()) if fees else "0"
    turnover = metrics.get("turnover_by_currency") or {}
    turnover_text = " / ".join(f"{key} {value:,.2f}" for key, value in turnover.items()) if turnover else "0"
    unreal = metrics.get("unrealized_pnl_by_currency") or {}
    unreal_text = " / ".join(f"{key} {value:,.2f}" for key, value in unreal.items()) if unreal else "0"
    buy_cut = int(settings.get("strategy", {}).get("recommend_buy_score", 70))

    lines: list[str] = []
    lines.append(f"【复盘 {date.today().isoformat()}】")
    lines.append(f"成交：{metrics['trade_count']}（买 {metrics['buy_count']} / 卖 {metrics['sell_count']}）")
    lines.append(f"换手：{turnover_text}；手续费：{fee_text}")
    lines.append(f"权益：{equity_text}；未实现盈亏：{unreal_text}；持仓数：{metrics['open_positions']}")
    lines.append("")
    lines.append("【大盘与基准】")
    for market in ["US", "JP"]:
        block = market_wrap.get(market) or []
        if not block:
            lines.append(f"- {market}：暂无行情数据")
        else:
            lines.append(f"- {market}：")
            lines.extend([f"  {item}" for item in block])
    lines.append("")
    lines.append("【关注/观察要点】")
    lines.extend(watchlist_notes or ["- 暂无观察池/关注列表的最新信号"])
    if strongest:
        lines.append("")
        lines.append("【今日信号（强度 Top 5）】")
        lines.extend(strongest)
    lines.append("")
    lines.append("【策略学习】")
    lines.append(f"- {lessons.get('message','')}")
    if isinstance(lessons.get("adjustments"), dict) and lessons["adjustments"]:
        adj = " / ".join(f"{k} {v:+.2f}" for k, v in lessons["adjustments"].items())
        lines.append(f"- 权重方向：{adj}")
    lines.append("")
    lines.append("【明日计划】")
    lines.append(f"- 优先复盘：推荐指数 ≥ {buy_cut} 的标的，必须同时看 ADX 趋势强度、20/60日动量和 CMF/MFI 资金流。")
    lines.append("- 对持仓：先看趋势是否延续（SMA20/SMA50、ADX、MACD_hist），再看风险（ATR、20日波动、布林带、止损线）。")
    return "\n".join([line for line in lines if line is not None])


def _market_wrap(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {"US": [], "JP": []}
    for market in ["US", "JP"]:
        symbols = settings.get("benchmarks", {}).get(market, [])
        if not isinstance(symbols, list):
            continue
        for symbol in symbols[:6]:
            quote = _latest_quote(conn, symbol)
            if not quote:
                continue
            snapshot = _indicator_snapshot(conn, symbol, limit=120)
            change_pct = quote.get("change_pct")
            change_text = f"{(change_pct * 100):+.2f}%" if isinstance(change_pct, float) else "—"
            adx = snapshot.get("adx14")
            momentum20 = snapshot.get("return_20d")
            cmf = snapshot.get("cmf20")
            output[market].append(
                f"{symbol} 收盘 {quote['close']:.2f}（{change_text}） · ADX14 {adx if adx is not None else '—'} · 20日动量 {momentum20 if momentum20 is not None else '—'} · CMF20 {cmf if cmf is not None else '—'}"
            )
    return output


def _watchlist_notes(conn: sqlite3.Connection, settings: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    for market in ["US", "JP"]:
        symbols = settings.get("watchlists", {}).get(market, [])
        if not isinstance(symbols, list):
            continue
        for symbol in symbols[:10]:
            row = conn.execute(
                """
                SELECT ts, action, recommendation_label, recommendation_index, close, confidence
                FROM signals
                WHERE symbol = ?
                ORDER BY ts DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if not row:
                continue
            index = row["recommendation_index"]
            label = row["recommendation_label"] or row["action"]
            notes.append(
                f"- {market} {symbol}：{label} {index if index is not None else ''} · 收盘 {float(row['close']):.2f} · 置信度 {float(row['confidence']) * 100:.0f}% · {str(row['ts'])[:16]}"
            )
    return notes[:16]


def _latest_quote(conn: sqlite3.Connection, symbol: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT date, close
        FROM prices
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT 2
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return None
    close = float(rows[0]["close"])
    prev = float(rows[1]["close"]) if len(rows) > 1 else None
    change = (close - prev) if prev is not None else None
    change_pct = (change / prev) if prev not in (None, 0.0) else None
    return {"symbol": symbol, "close": close, "prev_close": prev, "change": change, "change_pct": change_pct}


def _indicator_snapshot(conn: sqlite3.Connection, symbol: str, limit: int = 120) -> dict[str, float]:
    import math

    import pandas as pd

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
    if not rows:
        return {}
    frame = pd.DataFrame([dict(row) for row in reversed(rows)])
    enriched = compute_indicators(frame)
    latest = latest_complete_row(enriched)
    snapshot: dict[str, float] = {}
    for key in [
        "return_20d",
        "return_60d",
        "rsi14",
        "macd_hist",
        "cmf20",
        "mfi14",
        "atr14",
        "sma20",
        "sma50",
        "slope20",
        "adx14",
        "plus_di14",
        "minus_di14",
        "volatility20",
    ]:
        try:
            value = float(latest.get(key))
        except Exception:
            continue
        if math.isfinite(value):
            snapshot[key] = round(value, 4)
    return snapshot


def _json(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False)


def _loads(value: str) -> Any:
    import json

    return json.loads(value)

