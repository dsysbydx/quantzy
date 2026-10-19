"""The VIX-regime hedge on VIXY alone: real prices, no option model.

``research_tail_hedge.py`` found that holding protection while the VIX is low and selling it
into spikes makes a hedge nearly free, but its SPX puts were priced off the VIX and the gain
was within that model's error. VIXY (short-term VIX futures, January 2011 on) is tradeable
and its history carries the real roll cost (contango). **Fixed before any test.**

* Position: long VIXY at a target of ``SIZE`` x NAV while the regime is on, flat when off.
  The holding drifts with VIXY's price and is reset to target when it is more than 25% away
  from it; 5 bp of the notional traded on every trade. Return excess over cash.
* The regime is the VIX through the prior close: on when VIX <= low, off when VIX >= high.
  Primary low 15 / high 25, size 0.05; grid low 13 / 15 / 18 by high 25 / 30 / 40, sizes
  0.025 / 0.05 / 0.10; and always on.
* Added to the saved book (core plus overlays) over VIXY's history; the test half is
  2017-07-26 on.

Adopted only if the primary lifts both the full-period and the test-half Sharpe by at least
0.02, and most of the grid at the primary size lifts the test half.

**Result (2026-10-19): not adopted (misses the full-period bar).** The primary (15/25, 0.05x)
moves the saved book from 1.245 to 1.256 since 2011 (+0.011, short of +0.02); the test half
gains +0.066 (1.184 -> 1.250) but the train half loses -0.075 (1.394 -> 1.319). The sleeve
cost 2.0% of NAV a year in 2011-17 and earned +0.1% since (2018, 2020 and more frequent
spikes). The grid agrees: at 0.05x every cell lifts the test half (median +0.074) but only
6 of 9 the full period (median +0.009); at 0.10x the full period falls in 8 of 9. Timing
against always-on is a trade: the rule cuts the cost from -2.2% to -0.8% a year but keeps
only +1.2% of 2020's +8.0%, since it sells at VIX 25. It works only when spikes are frequent.

Calmar falls in every cell (book 0.847; 0.73-0.84 with the sleeve): the book's worst
drawdown is the slow 2022 sell-off (Sep 2022, -17.2%), which VIX futures did not hedge, so
the sleeve's cost lowers CAGR without reducing it.

**Adopted anyway (2026-10-19), as insurance by choice:** 13/30 at 0.05x, the best grid cell,
as the ``vix_hedge`` overlay (``quantzy/strategies/vix_hedge.py``), which trades a day later
than here (the job's orders execute at the next close): saved book since 2011 1.245 ->
1.290, test half 1.184 -> 1.293, Calmar 0.847 -> 0.826. Review if it costs over 1.5% of NAV
a year for about three years without paying off in a spike.

Usage:
    uv run python research/research_vixy_hedge.py
"""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
from research_bounce_filtered import saved_book, stats
from research_tail_hedge import EVENTS, regime

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core

COST = 5e-4
BAND = 0.25
ANN = np.sqrt(TRADING_DAYS)


def vixy_sleeve(ret: pd.Series, cash: pd.Series, on: pd.Series, size: float) -> pd.DataFrame:
    """Daily excess P&L (fraction of NAV) and the weight held into each day."""
    target = np.where(on.shift(1, fill_value=False).to_numpy(), size, 0.0)
    r, c = ret.fillna(0).to_numpy(), cash.fillna(0).to_numpy()
    w, pnl, held = 0.0, np.zeros(len(r)), np.zeros(len(r))
    for i in range(len(r)):
        t = target[i]  # set at the prior close
        if (t == 0 and w != 0) or (t > 0 and abs(w - t) > BAND * t):
            pnl[i] -= abs(t - w) * COST
            w = t
        held[i] = w
        pnl[i] += w * (r[i] - c[i])
        w *= (1 + r[i]) / (1 + w * r[i] + (1 - w) * c[i])  # drift as a fraction of NAV
    return pd.DataFrame({"pnl": pnl, "weight": held}, index=ret.index)


def main() -> None:
    pd.set_option("display.width", 320)
    raw = CachedReader(YFinanceReader()).history(["VIXY", "^VIX", "^IRX", "^VIX3M"],
                                                 start="2010-01-01")
    raw = raw[raw["VIXY"].notna()]
    vix = raw["^VIX"].ffill()
    ret = raw["VIXY"].pct_change(fill_method=None)
    cash = ((1 + raw["^IRX"].ffill() / 100) ** (1 / TRADING_DAYS) - 1).shift(1)
    b = latest_complete(core.load(), core.RISKY)
    book = saved_book(b).loc[ret.index[1]:]

    slope = (raw["^VIX"] / raw["^VIX3M"]).ffill()
    rows = {}
    for lab, g in pd.concat([ret.shift(-1).rename("r"), pd.cut(vix, [0, 13, 15, 18, 25, 30, 40,
                                                                      100]).rename("b"),
                             slope.rename("s")], axis=1).dropna().groupby("b", observed=True):
        rows[str(lab)] = {"days": len(g), "VIXY next-day %/yr": g["r"].mean() * TRADING_DAYS * 100,
                          "VIX/VIX3M": g["s"].mean()}
    print("1. VIXY's NEXT-DAY RETURN BY VIX LEVEL (the cost of holding it there), 2011 on")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.2f}"))

    runs = {"always on, 0.05": (pd.Series(True, index=ret.index), 0.05)}
    for (low, high), size in itertools.product(itertools.product((13, 15, 18), (25, 30, 40)),
                                               (0.025, 0.05, 0.10)):
        runs[f"{low}/{high}, {size}"] = (regime(vix, low, high), size)
    alone, comb = {}, {"saved book": stats(book)}
    for name, (on, size) in runs.items():
        s = vixy_sleeve(ret, cash, on, size).reindex(book.index).fillna(0)
        x = s["pnl"]
        alone[name] = {"on %": (s["weight"] > 0).mean(), "%/yr": x.mean() * TRADING_DAYS * 100,
                       "2011-17 %/yr": x.loc[:"2017-07-25"].mean() * TRADING_DAYS * 100,
                       "2017-26 %/yr": x.loc["2017-07-26":].mean() * TRADING_DAYS * 100,
                       "corr book": x.corr(book),
                       **{k: (1 + x.loc[a:z]).prod() - 1 for k, (a, z) in EVENTS.items()
                          if a >= "2011"}}
        comb[f"+ {name}"] = stats(book + x)
    print(f"\n2. VIXY SLEEVES ALONE (fraction of NAV, net), {book.index[0].date()} on")
    print(pd.DataFrame(alone).T.to_string(float_format=lambda v: f"{v:+.3f}"))
    t = pd.DataFrame(comb).T.drop(columns=[c for c in stats(book) if c.startswith("2008")])
    print("\n3. SAVED BOOK (core + overlays) PLUS EACH, same window")
    print(t.to_string(float_format=lambda v: f"{v:+.3f}"))
    base = t.loc["saved book"]
    for size in (0.025, 0.05, 0.10):
        g = t[t.index.str.endswith(f", {size}") & ~t.index.str.contains("always")]
        print(f"size {size}: test half up in {(g['test'] > base['test']).sum()} of {len(g)}, "
              f"full up in {(g['Sharpe'] > base['Sharpe']).sum()}; median change "
              f"{(g['Sharpe'] - base['Sharpe']).median():+.3f} full, "
              f"{(g['test'] - base['test']).median():+.3f} test")


if __name__ == "__main__":
    main()
