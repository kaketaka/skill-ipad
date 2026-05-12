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
    frame["return_5d"] = close.pct_change(5)
    frame["return_20d"] = close.pct_change(20)
    frame["return_60d"] = close.pct_change(60)
    frame["sma10"] = close.rolling(10).mean()
    frame["sma20"] = close.rolling(20).mean()
    frame["sma50"] = close.rolling(50).mean()
    frame["sma200"] = close.rolling(200).mean()
    frame["ema12"] = close.ewm(span=12, adjust=False).mean()
    frame["ema26"] = close.ewm(span=26, adjust=False).mean()
    frame["macd"] = frame["ema12"] - frame["ema26"]
    frame["macd_signal"] = frame["macd"].ewm(span=9, adjust=False).mean()
    frame["macd_hist"] = frame["macd"] - frame["macd_signal"]
    frame["rsi14"] = _rsi(close, 14)
    frame["atr14"] = _atr(high, low, close, 14)
    frame["atr_pct"] = frame["atr14"] / close.replace(0, np.nan)
    frame["bb_mid"] = frame["sma20"]
    frame["bb_std"] = close.rolling(20).std()
    frame["bb_upper"] = frame["bb_mid"] + 2 * frame["bb_std"]
    frame["bb_lower"] = frame["bb_mid"] - 2 * frame["bb_std"]
    frame["volume_sma20"] = volume.rolling(20).mean()
    frame["dollar_volume"] = close * volume
    frame["dollar_volume_sma20"] = frame["dollar_volume"].rolling(20).mean()
    frame["high20"] = high.rolling(20).max()
    frame["low20"] = low.rolling(20).min()
    frame["slope20"] = frame["sma20"].diff(5) / frame["sma20"].shift(5)
    frame["obv"] = _obv(close, volume)
    frame["obv_slope20"] = frame["obv"].diff(20) / volume.fillna(0.0).rolling(20).sum().replace(0, np.nan)
    frame["cmf20"] = _cmf(high, low, close, volume, 20)
    frame["mfi14"] = _mfi(high, low, close, volume, 14)
    adx = _adx(high, low, close, 14)
    frame["plus_di14"] = adx["plus_di"]
    frame["minus_di14"] = adx["minus_di"]
    frame["adx14"] = adx["adx"]
    frame["volatility20"] = frame["return_1d"].rolling(20).std()
    frame = frame.replace([np.inf, -np.inf], np.nan)
    return frame


def latest_complete_row(frame: pd.DataFrame) -> pd.Series:
    complete = frame.dropna(subset=["sma20", "sma50", "macd_hist", "rsi14", "atr14", "cmf20", "mfi14", "adx14"])
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


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.DataFrame:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=high.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=high.index)
    atr = _atr(high, low, close, window).replace(0, np.nan)
    plus_di = 100 * plus_dm.ewm(alpha=1 / window, min_periods=window, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / window, min_periods=window, adjust=False).mean() / atr
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    return pd.DataFrame({"plus_di": plus_di, "minus_di": minus_di, "adx": adx})
