from __future__ import annotations

import numpy as np
import pandas as pd


def compute_indicators(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    volume = frame["volume"].astype(float)

    frame["return_1d"] = close.pct_change()
    frame["sma10"] = close.rolling(10).mean()
    frame["sma20"] = close.rolling(20).mean()
    frame["sma50"] = close.rolling(50).mean()
    frame["ema12"] = close.ewm(span=12, adjust=False).mean()
    frame["ema26"] = close.ewm(span=26, adjust=False).mean()
    frame["macd"] = frame["ema12"] - frame["ema26"]
    frame["macd_signal"] = frame["macd"].ewm(span=9, adjust=False).mean()
    frame["macd_hist"] = frame["macd"] - frame["macd_signal"]
    frame["rsi14"] = _rsi(close, 14)
    frame["atr14"] = _atr(high, low, close, 14)
    frame["bb_mid"] = frame["sma20"]
    frame["bb_std"] = close.rolling(20).std()
    frame["bb_upper"] = frame["bb_mid"] + 2 * frame["bb_std"]
    frame["bb_lower"] = frame["bb_mid"] - 2 * frame["bb_std"]
    frame["volume_sma20"] = volume.rolling(20).mean()
    frame["high20"] = high.rolling(20).max()
    frame["low20"] = low.rolling(20).min()
    frame["slope20"] = frame["sma20"].diff(5) / frame["sma20"].shift(5)
    frame["obv"] = _obv(close, volume)
    frame["cmf20"] = _cmf(high, low, close, volume, 20)
    frame["mfi14"] = _mfi(high, low, close, volume, 14)
    frame = frame.replace([np.inf, -np.inf], np.nan)
    return frame


def latest_complete_row(frame: pd.DataFrame) -> pd.Series:
    complete = frame.dropna(subset=["sma20", "sma50", "macd_hist", "rsi14", "atr14", "cmf20", "mfi14"])
    if complete.empty:
        return frame.iloc[-1]
    return complete.iloc[-1]


def _rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0.0))
    signed_volume = volume.fillna(0.0) * direction
    return signed_volume.cumsum()


def _cmf(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, window: int) -> pd.Series:
    denom = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / denom
    mfv = mfm.fillna(0.0) * volume.fillna(0.0)
    mfv_sum = mfv.rolling(window).sum()
    vol_sum = volume.fillna(0.0).rolling(window).sum().replace(0, np.nan)
    return mfv_sum / vol_sum


def _mfi(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, window: int) -> pd.Series:
    typical = (high + low + close) / 3.0
    raw_flow = typical * volume.fillna(0.0)
    direction = np.sign(typical.diff().fillna(0.0))
    positive = raw_flow.where(direction > 0, 0.0)
    negative = raw_flow.where(direction < 0, 0.0).abs()
    pos_sum = positive.rolling(window).sum()
    neg_sum = negative.rolling(window).sum().replace(0, np.nan)
    ratio = pos_sum / neg_sum
    return 100 - (100 / (1 + ratio))
