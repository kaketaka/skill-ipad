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
- Liquidity quality: minimum price and 20-day traded value so thin/small names do not dominate the simulated book.

## Operating Rules

- Prefer long signals only when score clears the buy threshold and neither trend strength nor money flow is weak.
- New simulated positions are limited to the watchlists by default; full-market scans are treated as observation signals unless a symbol is deliberately added.
- Buy execution now requires a higher recommendation score, positive momentum, acceptable ADX trend strength, acceptable money flow, and non-negative liquidity.
- Treat high RSI differently in strong trends; overbought is a caution, not an automatic sell.
- Keep position sizing and exits bounded by the risk module: lower max position percent, fewer new orders, stop loss, take profit, and fees.
- At the start of each simulation run, open positions are refreshed with the latest quote and checked for stop-loss exits or take-profit trims before new buys are considered.
- Use reviews to learn from actual open-position outcomes, not from unfilled signals.

## Research Notes

- Moving-average and breakout rules are classic technical rules, but transaction costs and robustness matter.
- ADX is used as a regime filter for trend strength, not as a direction signal by itself.
- Momentum is treated as time-series confirmation, not a stand-alone reason to buy.
- Money-flow indicators are confirmation inputs; they should not overpower price trend and risk.
- Stop losses are treated as a drawdown-control mechanism, not a standalone alpha source; they work best when paired with momentum/regime filters and realistic trading costs.
