from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .config import DATA_DIR


NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
JPX_LISTED_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"

UNIVERSE_FILES = {
    "US": DATA_DIR / "universe_us.csv",
    "JP": DATA_DIR / "universe_jp.csv",
}

US_EXCLUDE_KEYWORDS = (
    "WARRANT",
    "RIGHT",
    "UNIT",
    "FUND",
    "ETF",
    "ETN",
    "TRUST",
    "ACQUISITION",
    "SPAC",
    "PREFERRED",
    "PREFERENCE",
    "DEPOSITARY",
    "LIMITED PARTNERSHIP",
    "NOTE",
    "BOND",
)

JP_EXCLUDE_PRODUCTS = (
    "ETFS",
    "ETNS",
    "REIT",
    "INFRASTRUCTURE",
)


@dataclass(frozen=True)
class UniverseSummary:
    market: str
    count: int
    source: str
    updated_at: str | None
    file: str


def ensure_universe(settings: dict[str, Any], markets: list[str]) -> dict[str, Any]:
    if not settings.get("universe", {}).get("enabled", True):
        return {"enabled": False, "markets": {}}
    refresh_days = int(settings.get("universe", {}).get("refresh_days", 7))
    refreshed: dict[str, Any] = {}
    for market in markets:
        market = market.upper()
        path = UNIVERSE_FILES[market]
        if _needs_refresh(path, refresh_days):
            refreshed[market] = sync_universe_market(market, settings)
    return {"enabled": True, "markets": {market: universe_summary(market).__dict__ for market in markets}, "refreshed": refreshed}


def sync_universe_market(market: str, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    market = market.upper()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    include_etfs = bool((settings or {}).get("universe", {}).get("include_etfs", False))
    if market == "US":
        frame = _fetch_us_universe(include_etfs=include_etfs)
        source = "Nasdaq Trader Symbol Directory"
    elif market == "JP":
        frame = _fetch_jp_universe(include_etfs=include_etfs)
        source = "JPX List of TSE-listed Issues"
    else:
        raise ValueError(f"Unsupported market: {market}")
    frame = frame.drop_duplicates(subset=["symbol"]).sort_values("symbol")
    frame["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    frame["source"] = source
    frame.to_csv(UNIVERSE_FILES[market], index=False, encoding="utf-8")
    return {"market": market, "count": int(len(frame)), "source": source, "file": str(UNIVERSE_FILES[market])}


def load_universe(market: str) -> pd.DataFrame:
    path = UNIVERSE_FILES[market.upper()]
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "name", "market", "exchange", "asset_type"])
    return pd.read_csv(path)


def select_scan_symbols(settings: dict[str, Any], market: str) -> list[str]:
    market = market.upper()
    observed = _clean_symbols(settings.get("watchlists", {}).get(market, []))
    if not settings.get("universe", {}).get("enabled", True):
        return observed
    universe = load_universe(market)
    if universe.empty:
        return observed
    scan_limit = int(settings.get("universe", {}).get("daily_scan_limit", {}).get(market, 40))
    all_symbols = [str(symbol).upper() for symbol in universe["symbol"].dropna()]
    candidates = [symbol for symbol in all_symbols if symbol not in observed]
    rotating = _daily_slice(candidates, scan_limit)
    return _dedupe(observed + rotating)


def observed_symbols(settings: dict[str, Any], market: str | None = None) -> set[str]:
    markets = [market.upper()] if market else ["US", "JP"]
    output: set[str] = set()
    for current in markets:
        output.update(_clean_symbols(settings.get("watchlists", {}).get(current, [])))
    return output


def universe_summary(market: str) -> UniverseSummary:
    market = market.upper()
    path = UNIVERSE_FILES[market]
    if not path.exists():
        return UniverseSummary(market=market, count=0, source="", updated_at=None, file=str(path))
    try:
        frame = pd.read_csv(path, usecols=["symbol", "source", "updated_at"])
        source = str(frame["source"].dropna().iloc[0]) if not frame.empty and "source" in frame else ""
        updated = str(frame["updated_at"].dropna().iloc[0]) if not frame.empty and "updated_at" in frame else None
        return UniverseSummary(market=market, count=int(len(frame)), source=source, updated_at=updated, file=str(path))
    except Exception:
        return UniverseSummary(market=market, count=0, source="", updated_at=None, file=str(path))


def _fetch_us_universe(include_etfs: bool) -> pd.DataFrame:
    nasdaq = _read_symbol_directory(NASDAQ_LISTED_URL)
    other = _read_symbol_directory(OTHER_LISTED_URL)

    nasdaq_rows = pd.DataFrame(
        {
            "symbol": nasdaq["Symbol"].map(_normalize_us_symbol),
            "name": nasdaq["Security Name"],
            "exchange": "NASDAQ",
            "test_issue": nasdaq["Test Issue"],
            "etf": nasdaq["ETF"],
        }
    )
    other_rows = pd.DataFrame(
        {
            "symbol": other["ACT Symbol"].map(_normalize_us_symbol),
            "name": other["Security Name"],
            "exchange": other["Exchange"],
            "test_issue": other["Test Issue"],
            "etf": other["ETF"],
        }
    )
    frame = pd.concat([nasdaq_rows, other_rows], ignore_index=True)
    frame["name_upper"] = frame["name"].astype(str).str.upper()
    frame = frame[frame["test_issue"].astype(str).str.upper().eq("N")]
    if not include_etfs:
        frame = frame[frame["etf"].astype(str).str.upper().eq("N")]
    for keyword in US_EXCLUDE_KEYWORDS:
        frame = frame[~frame["name_upper"].str.contains(keyword, na=False)]
    frame = frame[frame["symbol"].str.match(r"^[A-Z][A-Z0-9.-]{0,12}$", na=False)]
    frame["market"] = "US"
    frame["asset_type"] = "ETF" if include_etfs else "Stock"
    return frame[["symbol", "name", "market", "exchange", "asset_type"]]


def _fetch_jp_universe(include_etfs: bool) -> pd.DataFrame:
    response = requests.get(JPX_LISTED_URL, timeout=30, headers={"User-Agent": "market-sim-trader/0.1"})
    response.raise_for_status()
    frame = pd.read_excel(BytesIO(response.content))
    frame = frame.rename(
        columns={
            "Local Code": "local_code",
            "Name (English)": "name",
            "Section/Products": "section",
        }
    )
    frame["local_code"] = frame["local_code"].astype(str).str.strip().str.upper()
    frame["section_upper"] = frame["section"].astype(str).str.upper()
    if not include_etfs:
        for product in JP_EXCLUDE_PRODUCTS:
            frame = frame[~frame["section_upper"].str.contains(product, na=False)]
    frame = frame[frame["local_code"].str.match(r"^[0-9A-Z]{4}$", na=False)]
    frame["symbol"] = frame["local_code"] + ".T"
    frame["market"] = "JP"
    frame["exchange"] = "TSE"
    frame["asset_type"] = "Stock"
    return frame[["symbol", "name", "market", "exchange", "asset_type"]]


def _read_symbol_directory(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=30, headers={"User-Agent": "market-sim-trader/0.1"})
    response.raise_for_status()
    text = "\n".join(line for line in response.text.splitlines() if "|" in line and not line.startswith("File Creation Time"))
    return pd.read_csv(StringIO(text), sep="|")


def _normalize_us_symbol(symbol: Any) -> str:
    clean = str(symbol).strip().upper()
    return clean.replace("/", "-")


def _needs_refresh(path: Path, refresh_days: int) -> bool:
    if not path.exists():
        return True
    age_seconds = datetime.now().timestamp() - path.stat().st_mtime
    return age_seconds > refresh_days * 24 * 60 * 60


def _daily_slice(symbols: list[str], limit: int) -> list[str]:
    if limit <= 0 or not symbols:
        return []
    if len(symbols) <= limit:
        return symbols
    start = (date.today().toordinal() * limit) % len(symbols)
    doubled = symbols + symbols
    return doubled[start : start + limit]


def _clean_symbols(symbols: list[Any]) -> list[str]:
    return _dedupe(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip())


def _dedupe(symbols: Any) -> list[str]:
    output: list[str] = []
    for symbol in symbols:
        if symbol and symbol not in output:
            output.append(symbol)
    return output
