from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .indicators import latest_complete_row


@dataclass
class Signal:
    symbol: str
    market: str
    action: str
    score: float
    recommendation_index: int
    recommendation_label: str
    confidence: float
    close: float
    rationale: list[str]
    features: dict[str, float]


def generate_signal(
    symbol: str,
    market: str,
    indicators: pd.DataFrame,
    weights: dict[str, float],
    settings: dict[str, Any],
) -> Signal:
    row = latest_complete_row(indicators)
    previous = indicators.iloc[-2] if len(indicators) > 1 else row
    close = float(row.get("close") or 0)
    features = {
        "trend": _trend_score(row),
        "macd": _macd_score(row),
        "rsi": _rsi_score(row),
        "breakout": _breakout_score(row, previous),
        "mean_reversion": _mean_reversion_score(row),
        "volume": _volume_score(row),
        "risk": _risk_score(row),
    }
    total_weight = sum(abs(float(weights.get(name, 0))) for name in features) or 1.0
    raw_score = sum(features[name] * float(weights.get(name, 0)) for name in features) / total_weight
    score = max(-1.0, min(1.0, raw_score))
    recommendation_index = int(round((score + 1.0) * 50))
    confidence = min(1.0, 0.35 + abs(score) * 0.55 + _data_quality_bonus(indicators))
    buy_threshold = float(settings["strategy"]["buy_threshold"])
    sell_threshold = float(settings["strategy"]["sell_threshold"])
    if score >= buy_threshold:
        action = "BUY"
    elif score <= sell_threshold:
        action = "SELL"
    else:
        action = "HOLD"
    return Signal(
        symbol=symbol,
        market=market,
        action=action,
        score=round(score, 4),
        recommendation_index=recommendation_index,
        recommendation_label=_recommendation_label(recommendation_index, settings),
        confidence=round(confidence, 4),
        close=round(close, 4),
        rationale=_rationale(features),
        features={name: round(value, 4) for name, value in features.items()},
    )


def recommendation_guidance(settings: dict[str, Any]) -> dict[str, Any]:
    buy_score = int(settings["strategy"].get("recommend_buy_score", 70))
    sell_score = int(settings["strategy"].get("recommend_sell_score", 30))
    return {
        "score_range": "0-100",
        "strong_buy": max(80, buy_score + 10),
        "buy": buy_score,
        "hold_low": sell_score + 1,
        "hold_high": buy_score - 1,
        "sell": sell_score,
        "strong_sell": min(20, sell_score - 10),
        "meaning": "分数越高越偏买入，分数越低越偏卖出。模拟交易仍会受仓位、止损、止盈和手续费约束。",
    }


def _recommendation_label(index: int, settings: dict[str, Any]) -> str:
    guidance = recommendation_guidance(settings)
    if index >= guidance["strong_buy"]:
        return "强买入观察"
    if index >= guidance["buy"]:
        return "适合买入"
    if index <= guidance["strong_sell"]:
        return "强卖出/回避"
    if index <= guidance["sell"]:
        return "适合卖出"
    return "继续观察"


def _trend_score(row: pd.Series) -> float:
    close = float(row.get("close") or 0)
    sma20 = float(row.get("sma20") or 0)
    sma50 = float(row.get("sma50") or 0)
    slope20 = float(row.get("slope20") or 0)
    score = 0.0
    if close > sma20 > sma50:
        score += 0.75
    elif close < sma20 < sma50:
        score -= 0.75
    score += max(-0.25, min(0.25, slope20 * 10))
    return _clip(score)


def _macd_score(row: pd.Series) -> float:
    hist = float(row.get("macd_hist") or 0)
    macd = float(row.get("macd") or 0)
    if hist > 0 and macd > 0:
        return 0.75
    if hist < 0 and macd < 0:
        return -0.75
    return 0.25 if hist > 0 else -0.25


def _rsi_score(row: pd.Series) -> float:
    rsi = float(row.get("rsi14") or 50)
    if rsi < 30:
        return 0.75
    if rsi < 42:
        return 0.35
    if rsi > 75:
        return -0.8
    if rsi > 65:
        return -0.35
    return 0.05 if 48 <= rsi <= 62 else 0.0


def _breakout_score(row: pd.Series, previous: pd.Series) -> float:
    close = float(row.get("close") or 0)
    previous_high20 = float(previous.get("high20") or 0)
    previous_low20 = float(previous.get("low20") or 0)
    if previous_high20 and close > previous_high20:
        return 0.8
    if previous_low20 and close < previous_low20:
        return -0.8
    return 0.0


def _mean_reversion_score(row: pd.Series) -> float:
    close = float(row.get("close") or 0)
    lower = float(row.get("bb_lower") or 0)
    upper = float(row.get("bb_upper") or 0)
    mid = float(row.get("bb_mid") or 0)
    if lower and close < lower:
        return 0.65
    if upper and close > upper:
        return -0.45
    if mid and close > mid:
        return 0.1
    if mid and close < mid:
        return -0.1
    return 0.0


def _volume_score(row: pd.Series) -> float:
    volume = float(row.get("volume") or 0)
    volume_sma = float(row.get("volume_sma20") or 0)
    ret = float(row.get("return_1d") or 0)
    if not volume_sma:
        return 0.0
    if volume > volume_sma * 1.35 and ret > 0:
        return 0.45
    if volume > volume_sma * 1.35 and ret < 0:
        return -0.45
    return 0.0


def _risk_score(row: pd.Series) -> float:
    atr = float(row.get("atr14") or 0)
    close = float(row.get("close") or 0)
    if not close:
        return 0.0
    atr_pct = atr / close
    if atr_pct < 0.018:
        return 0.25
    if atr_pct > 0.055:
        return -0.35
    return 0.0


def _data_quality_bonus(indicators: pd.DataFrame) -> float:
    rows = len(indicators.dropna(subset=["close"]))
    if rows >= 180:
        return 0.1
    if rows >= 80:
        return 0.05
    return 0.0


def _rationale(features: dict[str, float]) -> list[str]:
    labels = {
        "trend": "趋势",
        "macd": "MACD",
        "rsi": "RSI",
        "breakout": "突破",
        "mean_reversion": "布林/均值回归",
        "volume": "量能",
        "risk": "波动风险",
    }
    ordered = sorted(features.items(), key=lambda item: abs(item[1]), reverse=True)
    lines = []
    for name, value in ordered[:4]:
        direction = "偏多" if value > 0.15 else "偏空" if value < -0.15 else "中性"
        lines.append(f"{labels[name]}{direction}({value:+.2f})")
    return lines


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, value))
