from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from typing import Iterable

import pandas as pd
import requests
import yfinance as yf

from .config import env_alpha_vantage_key


@dataclass(frozen=True)
class MarketInfo:
    market: str
    currency: str
    tradeable: bool = True


def infer_market(symbol: str) -> MarketInfo:
    clean = symbol.upper()
    if clean.startswith("^") or clean in {"TOPIX100.T"}:
        return MarketInfo("JP" if clean in {"^N225", "TOPIX100.T"} else "US", "JPY" if clean in {"^N225", "TOPIX100.T"} else "USD", False)
    if clean.endswith(".T"):
        return MarketInfo("JP", "JPY", True)
    return MarketInfo("US", "USD", True)


def fetch_history(symbol: str, sources: Iterable[str], period: str = "1y") -> tuple[pd.DataFrame, str]:
    errors: list[str] = []
    for source in sources:
        try:
            if source == "yfinance":
                frame = fetch_yfinance(symbol, period=period)
            elif source == "stooq":
                frame = fetch_stooq(symbol)
            elif source == "alpha_vantage":
                frame = fetch_alpha_vantage(symbol)
            else:
                continue
            if frame is not None and not frame.empty:
                return _normalize(frame), source
        except Exception as exc:  # Keep fallback sources usable.
            errors.append(f"{source}: {exc}")
    raise RuntimeError(f"No data for {symbol}. " + "; ".join(errors))


def fetch_yfinance(symbol: str, period: str = "1y") -> pd.DataFrame:
    ticker = yf.Ticker(symbol)
    frame = ticker.history(period=period, interval="1d", auto_adjust=False)
    return frame.reset_index()


def fetch_stooq(symbol: str) -> pd.DataFrame:
    candidates = _stooq_candidates(symbol)
    last_error = ""
    for candidate in candidates:
        url = f"https://stooq.com/q/d/l/?s={candidate}&i=d"
        response = requests.get(url, timeout=20, headers={"User-Agent": "market-sim-trader/0.1"})
        response.raise_for_status()
        text = response.text.strip()
        if "No data" in text or len(text.splitlines()) < 2:
            last_error = text[:120]
            continue
        frame = pd.read_csv(StringIO(text))
        if not frame.empty and {"Date", "Open", "High", "Low", "Close"}.issubset(frame.columns):
            return frame
    raise RuntimeError(last_error or f"Stooq returned no rows for {symbol}")


def fetch_alpha_vantage(symbol: str) -> pd.DataFrame:
    api_key = env_alpha_vantage_key()
    if not api_key:
        raise RuntimeError("ALPHAVANTAGE_API_KEY is not set")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()
    series = payload.get("Time Series (Daily)")
    if not series:
        message = payload.get("Note") or payload.get("Information") or payload.get("Error Message") or "No time series"
        raise RuntimeError(str(message))
    rows = []
    for date, values in series.items():
        rows.append(
            {
                "Date": date,
                "Open": values.get("1. open"),
                "High": values.get("2. high"),
                "Low": values.get("3. low"),
                "Close": values.get("4. close"),
                "Volume": values.get("5. volume"),
            }
        )
    return pd.DataFrame(rows)


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.rename(
        columns={
            "Date": "date",
            "Datetime": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    ).copy()
    if "date" not in normalized.columns:
        normalized["date"] = normalized.index
    keep = ["date", "open", "high", "low", "close", "volume"]
    for column in keep:
        if column not in normalized.columns:
            normalized[column] = 0.0
    normalized = normalized[keep]
    normalized["date"] = pd.to_datetime(normalized["date"]).dt.date.astype(str)
    for column in ["open", "high", "low", "close", "volume"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["close"]).sort_values("date")
    normalized = normalized.drop_duplicates(subset=["date"], keep="last")
    return normalized.tail(260).reset_index(drop=True)


def _stooq_candidates(symbol: str) -> list[str]:
    raw = symbol.lower()
    if raw.startswith("^"):
        mapping = {
            "^spx": "^spx",
            "^dji": "^dji",
            "^ixic": "^ixic",
            "^n225": "^nkx",
        }
        return [mapping.get(raw, raw)]
    if raw.endswith(".t"):
        return [raw.replace(".t", ".jp"), raw[:-2]]
    if "." not in raw:
        return [f"{raw}.us", raw]
    return [raw]
