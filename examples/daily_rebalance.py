"""PRODUCTION shape: what should I hold today?

    pull a warmup window -> strategy.decide(current book) -> a trade list

This is the whole production path. Note what it does *not* do: no backtest, no metrics, no
parameter search. It pulls ``strategy.min_history`` rows rather than a decade, because that
is all a decision needs -- and because doing so exercises the claim that a short window
reproduces the full-history answer.

The decision comes from ``Strategy.decide``, the same method the backtest calls at every
rebalance. Swap ``current`` for a real broker position and this is a live job.

Run:  uv run python -m examples.daily_rebalance
First run hits Yahoo Finance; results are cached to .cache/ thereafter.
"""

from __future__ import annotations

import pandas as pd

from examples.strategy_def import build
from quantzy.construction import Portfolio
from quantzy.data import CachedReader, DataBundle, YFinanceReader

#: Where the real book comes from. A positions file or a broker API replaces this literal --
#: it is the one input a production run must not invent.
CURRENT_HOLDINGS = pd.Series({"QQQ": 0.40, "VOO": 0.40, "URTH": 0.10, "SGOV": 0.10})
NAV = 100_000.0


def main() -> None:
    strategy = build()
    universe = strategy.universe

    # Pull enough history for the signals to have converged, and no more.
    needed = strategy.min_history
    reader = CachedReader(YFinanceReader())
    prices = reader.history(universe.tickers, start="2015-01-01").dropna(how="any")
    window = DataBundle.build(prices).tail(needed)

    print(f"Strategy: {strategy.name}")
    print(f"Universe: {universe.tickers}")
    print(f"Warmup needed: {needed} rows; pulled {len(window.index)} "
          f"({window.index[0].date()} .. {window.index[-1].date()})")

    # A production job must never trade on data it has not checked the age of. CachedReader
    # keys on (tickers, start, end, field) with no TTL, so an `end=None` request caches
    # forever and a daily run would replay the same stale prices indefinitely.
    stale_days = (pd.Timestamp.today().normalize() - window.index[-1]).days
    if stale_days > 4:
        print(f"  !! last bar is {stale_days} days old -- delete .cache/ to refresh")
    print()

    current = Portfolio(weights=CURRENT_HOLDINGS, nav=NAV)
    decision = strategy.decide(window, current=current)

    print(f"Decision as of {decision.asof.date()}  (solver: {decision.status})")
    print(decision.to_frame().round(4).to_string())
    print(f"\nTurnover: {decision.turnover:.4f}   "
          f"Est. cost: {decision.trades.cost:.6f} of NAV "
          f"(${decision.trades.cost * NAV:,.2f})")

    to_trade = decision.trades.nonzero()
    print(f"\n{len(to_trade.trades)} order(s) to place:")
    for t in to_trade.trades:
        print(f"  {t.side.upper():4s} {t.ticker:5s} {abs(t.delta_notional):>12,.2f}")


if __name__ == "__main__":
    main()
