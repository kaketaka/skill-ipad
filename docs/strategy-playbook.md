# Strategy Playbook

This paper-trading system uses a transparent multi-factor score. It is not a profit guarantee and should stay in simulation unless separately reviewed.

## Added Factors

- Trend alignment: price versus SMA20/SMA50/SMA200 plus SMA20 slope.
- Trend strength: ADX14 with +DI/-DI direction to avoid weak chop.
- Time-series momentum: 5/20/60 day returns, with extra confirmation when 20 and 60 days agree.
- Breakout confirmation: 20-day breakout with volume confirmation.
- Mean reversion: Bollinger band pressure, reduced when ADX says a strong trend is already in force.
- Money flow: CMF20, MFI14, and OBV slope.
- Risk pressure: ATR percent, 20-day volatility, and distance from SMA20.

## Operating Rules

- Prefer long signals only when score clears the buy threshold and neither trend strength nor money flow is weak.
- Treat high RSI differently in strong trends; overbought is a caution, not an automatic sell.
- Keep position sizing and exits bounded by the existing risk module: max position percent, stop loss, take profit, and fees.
- Use reviews to learn from actual open-position outcomes, not from unfilled signals.

## Research Notes

- Moving-average and breakout rules are classic technical rules, but transaction costs and robustness matter.
- ADX is used as a regime filter for trend strength, not as a direction signal by itself.
- Momentum is treated as time-series confirmation, not a stand-alone reason to buy.
- Money-flow indicators are confirmation inputs; they should not overpower price trend and risk.
