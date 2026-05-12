from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "trader.sqlite"


DEFAULT_CONFIG: dict[str, Any] = {
    "watchlists": {
        "US": [
            "SPY",
            "QQQ",
            "AAPL",
            "MSFT",
            "NVDA",
            "AMZN",
            "GOOGL",
            "META",
            "TSLA",
        ],
        "JP": [
            "^N225",
            "7203.T",
            "6758.T",
            "9984.T",
            "8306.T",
            "7974.T",
            "6861.T",
            "9432.T",
        ],
    },
    "benchmarks": {
        "US": ["SPY", "QQQ"],
        "JP": ["^N225", "TOPIX100.T"],
    },
    "data_sources": ["yfinance", "stooq", "alpha_vantage"],
    "universe": {
        "enabled": True,
        "refresh_days": 7,
        "daily_scan_limit": {
            "US": 40,
            "JP": 40,
        },
        "include_etfs": False,
    },
    "portfolio": {
        "USD": 100000.0,
        "JPY": 10000000.0,
    },
    "risk": {
        "max_position_pct": 0.08,
        "max_new_orders_per_market": 2,
        "stop_loss_pct": 0.05,
        "take_profit_pct": 0.12,
        "min_order_value": {
            "USD": 1000.0,
            "JPY": 100000.0,
        },
        "entry_filters": {
            "watchlist_only_new_positions": True,
            "min_price": {
                "USD": 20.0,
                "JPY": 1000.0,
            },
            "min_dollar_volume": {
                "USD": 20000000.0,
                "JPY": 500000000.0,
            },
            "max_entry_atr_pct": 0.045,
            "max_entry_volatility20": 0.05,
            "min_trend_strength": 0.05,
            "min_momentum": 0.0,
            "min_money_flow": -0.05,
            "min_liquidity": 0.0,
        },
        "fees": {
            "USD": {"rate": 0.0005, "min": 1.0},
            "JPY": {"rate": 0.00055, "min": 100.0},
        },
    },
    "strategy": {
        "buy_threshold": 0.5,
        "sell_threshold": -0.35,
        "recommend_buy_score": 75,
        "recommend_sell_score": 30,
        "strong_sell_threshold": -0.7,
        "learning_rate": 0.025,
        "min_weight": 0.05,
        "max_weight": 0.35,
    },
}


DEFAULT_WEIGHTS: dict[str, float] = {
    "trend": 0.18,
    "trend_strength": 0.13,
    "momentum": 0.15,
    "macd": 0.1,
    "rsi": 0.07,
    "breakout": 0.1,
    "mean_reversion": 0.06,
    "volume": 0.05,
    "money_flow": 0.08,
    "risk": 0.05,
    "liquidity": 0.03,
}


def env_alpha_vantage_key() -> str | None:
    value = os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHA_VANTAGE_API_KEY")
    return value.strip() if value and value.strip() else None


def default_config() -> dict[str, Any]:
    return deepcopy(DEFAULT_CONFIG)


def dumps_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def loads_json(value: str | None, fallback: Any = None) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback
