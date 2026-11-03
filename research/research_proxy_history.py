"""Proxy history: the saved book from 1995 on index-fund, index and futures proxies.

The book's own data starts on 2008-09-10, when UUP (2007-03) has warmed up. The question
(2026-11-03): how does the saved book do over a longer history, and in particular before
2008-09, which the priors and weights were not chosen on? ``quantzy.strategies.
core_proxies`` extends each ETF back with what tracked the same exposure before it listed
(Ken French industries for the sectors, ^NDX, Vanguard index funds, GC=F, ^SPGSCI, DXY) and
joins the levels; the strategy, its settings and costs are unchanged.

**Fixed before the run.** Each proxy measured against its ETF where both trade: daily
correlation, beta, vol ratio, tracking error, drift. The saved book plus its five overlays,
and the priors-only baseline, on the extended data from the first warmed-up day (1995-10-27,
VEIEX the binding proxy) and reported for 1995-99, 2000-02, 2003-07, 2008-09 on, and the
actual-data run beside the last. No adoption rule: a check that nothing breaks and how far
the backtest's Sharpe depends on the post-2008 years.

**Result (2026-11-03): the book holds up before 2008, but not through the dot-com bust.**
Proxies track well (correlation 0.84-0.99, drift within +/-2.4% a year) except gold before
2000-08 (miners hedged and demeaned, 0.57). Core plus overlays, 1995-10-27 .. 2026-11-02:
Sharpe 1.32, 13.6% return, 10.3% vol, max drawdown -21.2% (Dec 2000 - Sep 2001), against
1.44 / 14.1% / -15.1% on actual data from 2008-09. Before 2008-09 (out of sample, proxy
data): Sharpe 1.20, baseline 0.85, equal weight 0.83. By period, strategy / baseline:
1995-99 1.89 / 1.55, 2000-02 0.03 / -0.63, 2003-08 1.32 / 1.34, 2008-09 on 1.42 / 1.09 (the
proxy and actual runs correlate 0.999 there). The tilts carried 2000-02 (+0.4% a year
against -7.2%) and added nothing in 2003-08. The flat 2000-02 is the case the backtest from
2008 never saw: a slow, three-year equity bear market. Tear-sheet: the "Full, with
proxies", "Since 2000" and "Before ETF data" views.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.data.splice import tracking
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

PERIODS = [("1995-10-27", "1999-12-31"), ("2000-01-01", "2002-12-31"),
           ("2003-01-01", "2008-09-09"), ("2008-09-10", None)]


def book(b, alphas=None) -> pd.Series:
    warm = core.warmup(b)
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    if alphas is None:
        for ov in overlays(core):
            r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return r


def stats(r: pd.Series) -> str:
    nav = (1 + r).cumprod()
    ann = np.sqrt(TRADING_DAYS)
    return (f"Sharpe {r.mean() / r.std() * ann:5.2f}  return {r.mean() * TRADING_DAYS:6.1%}  "
            f"vol {r.std() * ann:5.1%}  max DD {(nav / nav.cummax() - 1).min():6.1%}")


def main() -> None:
    own = CachedReader(YFinanceReader()).history(
        list(core_proxies.PROXIES), start=core_proxies.START).pct_change(fill_method=None)
    rows = [{"etf": t, "proxy": lab, **tracking(own[t], src(core_proxies.START, None))}
            for t, chain in core_proxies.PROXIES.items()
            for lab, src in zip(core_proxies.LABELS[t], chain)]
    print(pd.DataFrame(rows).drop(columns="from").round(3).to_string(index=False))

    long, actual = core_proxies.load(), core.load()
    runs = {"strategy": book(long), "baseline": book(long, {"prior": core.ALPHAS["prior"]}),
            "actual": book(actual)}
    print(f"\nfull {runs['strategy'].index[0].date()} ..  {stats(runs['strategy'])}")
    for a, z in PERIODS:
        print(f"{a} .. {z or 'end':10s}")
        for k, r in runs.items():
            if len(r.loc[a:z]):
                print(f"  {k:9s} {stats(r.loc[a:z])}")


if __name__ == "__main__":
    main()
