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
        "trend_strength": _trend_strength_score(row),
        "momentum": _momentum_score(row),
        "macd": _macd_score(row),
        "rsi": _rsi_score(row),
        "breakout": _breakout_score(row, previous),
        "mean_reversion": _mean_reversion_score(row),
        "volume": _volume_score(row),
        "money_flow": _money_flow_score(row),
        "risk": _risk_score(row, settings),
        "liquidity": _liquidity_score(row, market, settings),
    }
    total_weight = sum(abs(float(weights.get(name, 0))) for name in features) or 1.0
    raw_score = sum(features[name] * float(weights.get(name, 0)) for name in features) / total_weight
    score = max(-1.0, min(1.0, raw_score))
    recommendation_index = int(round((score + 1.0) * 50))
    confidence = min(1.0, 0.35 + abs(score) * 0.55 + _data_quality_bonus(indicators))
    buy_threshold = float(settings["strategy"]["buy_threshold"])
    sell_threshold = float(settings["strategy"]["sell_threshold"])
    action = _action(score, features, buy_threshold, sell_threshold, settings)
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
        "meaning": "分数越高越偏买入，分数越低越偏卖出。模拟交易仍会受仓位、趋势强度、资金流、止损、止盈和手续费约束。",
    }


def _action(
    score: float,
    features: dict[str, float],
    buy_threshold: float,
    sell_threshold: float,
    settings: dict[str, Any],
) -> str:
    weak_trend = features["trend_strength"] < -0.1 and features["breakout"] <= 0
    weak_flow = features["money_flow"] < -0.25 and features["volume"] <= 0
    if score >= buy_threshold and not (weak_trend or weak_flow) and _passes_entry_quality(features, settings):
        return "BUY"
    if score <= sell_threshold or (features["risk"] < -0.6 and features["trend"] < 0):
        return "SELL"
    return "HOLD"


def _passes_entry_quality(features: dict[str, float], settings: dict[str, Any]) -> bool:
    filters = settings.get("risk", {}).get("entry_filters", {})
    return (
        features.get("liquidity", 0.0) >= float(filters.get("min_liquidity", 0.0))
        and features.get("trend_strength", 0.0) >= float(filters.get("min_trend_strength", 0.05))
        and features.get("momentum", 0.0) >= float(filters.get("min_momentum", 0.0))
        and features.get("money_flow", 0.0) >= float(filters.get("min_money_flow", -0.05))
        and features.get("risk", 0.0) >= -0.15
    )


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
    sma200 = float(row.get("sma200") or 0)
    slope20 = float(row.get("slope20") or 0)
    score = 0.0
    if close > sma20 > sma50:
        score += 0.65
    elif close < sma20 < sma50:
        score -= 0.65
    if sma200:
        score += 0.15 if close > sma200 else -0.15
    score += max(-0.25, min(0.25, slope20 * 10))
    return _clip(score)


def _trend_strength_score(row: pd.Series) -> float:
    adx = float(row.get("adx14") or 0)
    plus_di = float(row.get("plus_di14") or 0)
    minus_di = float(row.get("minus_di14") or 0)
    if not adx:
        return 0.0
    if adx < 16:
        return -0.2
    strength = 0.25 + min(0.55, max(0.0, (adx - 18) / 35))
    if plus_di > minus_di:
        return _clip(strength)
    if minus_di > plus_di:
        return _clip(-strength)
    return 0.0


def _momentum_score(row: pd.Series) -> float:
    ret5 = float(row.get("return_5d") or 0)
    ret20 = float(row.get("return_20d") or 0)
    ret60 = float(row.get("return_60d") or 0)
    score = ret5 * 1.5 + ret20 * 3.0 + ret60 * 1.2
    if ret20 > 0 and ret60 > 0:
        score += 0.18
    elif ret20 < 0 and ret60 < 0:
        score -= 0.18
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
    adx = float(row.get("adx14") or 0)
    if rsi < 28:
        return 0.7
    if rsi < 42:
        return 0.3
    if rsi > 78:
        return -0.35 if adx > 24 else -0.75
    if rsi > 68:
        return -0.15 if adx > 24 else -0.35
    return 0.05 if 48 <= rsi <= 62 else 0.0


def _breakout_score(row: pd.Series, previous: pd.Series) -> float:
    close = float(row.get("close") or 0)
    previous_high20 = float(previous.get("high20") or 0)
    previous_low20 = float(previous.get("low20") or 0)
    volume = float(row.get("volume") or 0)
    volume_sma = float(row.get("volume_sma20") or 0)
    boost = 0.15 if volume_sma and volume > volume_sma * 1.2 else 0.0
    if previous_high20 and close > previous_high20:
        return _clip(0.65 + boost)
    if previous_low20 and close < previous_low20:
        return _clip(-0.65 - boost)
    return 0.0


def _mean_reversion_score(row: pd.Series) -> float:
    close = float(row.get("close") or 0)
    lower = float(row.get("bb_lower") or 0)
    upper = float(row.get("bb_upper") or 0)
    mid = float(row.get("bb_mid") or 0)
    adx = float(row.get("adx14") or 0)
    if lower and close < lower:
        return 0.55 if adx < 25 else 0.25
    if upper and close > upper:
        return -0.45 if adx < 25 else -0.15
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


def _liquidity_score(row: pd.Series, market: str, settings: dict[str, Any]) -> float:
    close = float(row.get("close") or 0)
    volume_sma = float(row.get("volume_sma20") or 0)
    dollar_volume_sma = float(row.get("dollar_volume_sma20") or 0)
    currency = "JPY" if market.upper() == "JP" else "USD"
    filters = settings.get("risk", {}).get("entry_filters", {})
    min_price = float(filters.get("min_price", {}).get(currency, 0.0))
    min_dollar_volume = float(filters.get("min_dollar_volume", {}).get(currency, 0.0))
    if close <= 0:
        return -1.0
    score = 0.15 if close >= min_price else -0.7
    if dollar_volume_sma > 0 and min_dollar_volume > 0:
        ratio = dollar_volume_sma / min_dollar_volume
        if ratio >= 3:
            score += 0.45
        elif ratio >= 1:
            score += 0.25
        elif ratio >= 0.5:
            score -= 0.25
        else:
            score -= 0.65
    elif volume_sma >= 500000:
        score += 0.2
    elif volume_sma > 0:
        score -= 0.25
    return _clip(score)


def _money_flow_score(row: pd.Series) -> float:
    cmf = float(row.get("cmf20") or 0)
    mfi = float(row.get("mfi14") or 50)
    obv_slope = float(row.get("obv_slope20") or 0)
    score = max(-0.4, min(0.4, cmf * 1.8))
    score += max(-0.25, min(0.25, obv_slope))
    if mfi > 80:
        score -= 0.15
    elif mfi > 55:
        score += 0.15
    elif mfi < 20:
        score += 0.15
    elif mfi < 45:
        score -= 0.15
    return _clip(score)


def _risk_score(row: pd.Series, settings: dict[str, Any]) -> float:
    atr = float(row.get("atr14") or 0)
    close = float(row.get("close") or 0)
    volatility = float(row.get("volatility20") or 0)
    sma20 = float(row.get("sma20") or 0)
    if not close:
        return 0.0
    atr_pct = atr / close
    filters = settings.get("risk", {}).get("entry_filters", {})
    max_entry_atr_pct = float(filters.get("max_entry_atr_pct", 0.045))
    max_entry_volatility = float(filters.get("max_entry_volatility20", 0.05))
    score = 0.0
    if atr_pct < 0.018:
        score += 0.2
    elif atr_pct > max_entry_atr_pct:
        score -= 0.4
    if volatility > max_entry_volatility:
        score -= 0.25
    if sma20 and close > sma20 * 1.18:
        score -= 0.2
    if sma20 and close < sma20 * 0.82:
        score -= 0.2
    return _clip(score)


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
        "trend_strength": "ADX趋势强度",
        "momentum": "中期动量",
        "macd": "MACD",
        "rsi": "RSI",
        "breakout": "突破",
        "mean_reversion": "布林/均值回归",
        "volume": "量能",
        "money_flow": "资金流",
        "risk": "波动风险",
        "liquidity": "流动性",
    }
    ordered = sorted(features.items(), key=lambda item: abs(item[1]), reverse=True)
    lines = []
    for name, value in ordered[:5]:
        direction = "偏多" if value > 0.15 else "偏空" if value < -0.15 else "中性"
        lines.append(f"{labels[name]}{direction}({value:+.2f})")
    return lines


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, value))
