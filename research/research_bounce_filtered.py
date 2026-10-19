"""The equity bounce-back overlay, switched off in crash conditions.

``research_reversal_sleeves.py``: buying SPY after a bad week (5-day time-series reversal,
IC t 2.3-2.8) made Sharpe 0.42 alone but lowered the saved book's (1.21 -> 1.17) and deepened
the 2020 crash (-13.8% against -8.9%): in a crash one bad week follows another. This keeps
the ordinary dips and skips the crash weeks. **Fixed before any test.**

The overlay is unchanged: SPY weight ``0.125 * clip(-z5, 0, 2)`` (at most 0.25x NAV), ``z5`` the
5-day return over daily vol times sqrt 5 through the prior close, 1 bp a trade, skipping
changes under 1% of NAV. It is on only when its filter allows, flat otherwise:

* none (the reference);
* VRP: the saved book's VRP z-score > 0 (implied variance above realized: fear, not panic);
* VIX: VIX below 1.2x its own 60-day mean (no spike in progress);
* trend: SPY above its 200-day EMA;
* VRP and trend.

All filters use data through the prior close. Reported: each overlay's Sharpe by period,
correlation with the saved book (core with its overlays), returns in the 2008, 2020 and 2022
sell-offs, and the saved book plus each overlay.

**Result (2026-10-19): not added.** The trend filter (SPY above its EMA-200) removes the
crash damage (2008 0.0%, 2020 -0.8%, against -2.5% / -5.2% unfiltered) and lifts the
overlay's own Sharpe from 0.42 to 0.60 (0.53 since 2017); the VRP filter still bought into
March 2020. But the overlay stays 0.35-0.44 correlated with the book, and the best version
moves the saved book from 1.231 to 1.241 with a flat test half (1.184 -> 1.183) and slightly
worse 2020 and 2022.

Usage:
    uv run python research/research_bounce_filtered.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from research_reversal_sleeves import ANN, SELLOFFS

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme


def overlay(spy: pd.Series, rf: pd.Series, allow: pd.Series) -> tuple[pd.Series, pd.Series]:
    r = spy.pct_change(fill_method=None)
    vol = r.rolling(60, min_periods=60).std()
    z = ((spy / spy.shift(5) - 1) / (vol * np.sqrt(5))).shift(1)
    target = (0.125 * (-z).clip(0, 2)).where(allow.reindex(spy.index).fillna(False), 0.0)
    held, cost, w = [], [], 0.0
    for t in target.fillna(0).to_numpy():
        c = 0.0
        if abs(t - w) >= 0.01 or (t == 0 and w != 0):
            c, w = abs(t - w) * 1e-4, t
        held.append(w)
        cost.append(c)
    held = pd.Series(held, index=spy.index)
    ex = held.shift(1).fillna(0) * (r - rf.reindex(r.index).fillna(0)) - pd.Series(cost, r.index)
    return ex.dropna(), held


def saved_book(b) -> pd.Series:
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, core.build())
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    return r + tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None),
                                  cash).reindex(r.index).fillna(0)


def stats(r: pd.Series) -> dict:
    nav = (1 + r).cumprod()
    s0, s1 = r.loc[:"2017-07-25"], r.loc["2017-07-26":]
    return {"Sharpe": r.mean() / r.std() * ANN, "train": s0.mean() / s0.std() * ANN,
            "test": s1.mean() / s1.std() * ANN, "max DD": (nav / nav.cummax() - 1).min(),
            **{lab: (1 + r.loc[a:z]).prod() - 1 for lab, (a, z) in SELLOFFS.items()}}


def main() -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    book = saved_book(b)
    rf = b.prices[core.CASH].pct_change(fill_method=None)
    spy = b["market"].iloc[:, 0]
    vix = b["vix"].iloc[:, 0].ffill()
    vrp = core.ALPHAS["vrp"][0].premium(b)
    ema = spy.ewm(span=200, adjust=False, min_periods=200).mean()
    filters = {
        "none": pd.Series(True, index=b.index),
        "VRP > 0": (vrp > 0).shift(1),
        "VIX < 1.2x its 60d mean": (vix < 1.2 * vix.rolling(60, min_periods=60).mean()).shift(1),
        "SPY above EMA-200": (spy > ema).shift(1),
        "VRP > 0 and above EMA-200": ((vrp > 0) & (spy > ema)).shift(1),
    }
    rows, adds = {}, {}
    for name, allow in filters.items():
        ex, held = overlay(spy, rf, allow.astype("boolean").fillna(False).astype(bool))
        ex = ex.loc[book.index[0]:]
        row = {"on (% of days)": (held.loc[book.index[0]:] > 0).mean(),
               "avg weight": held.loc[book.index[0]:].mean(),
               "excess %/yr": ex.mean() * TRADING_DAYS * 100,
               "Sharpe": ex.mean() / ex.std() * ANN,
               "corr with book": ex.corr(book)}
        for a, z in (("2008", "2016"), ("2017", "2026")):
            y = ex.loc[a:z]
            row[f"Sharpe {a}-{z[2:]}"] = y.mean() / y.std() * ANN
        for lab, (a, z) in SELLOFFS.items():
            row[lab] = (1 + ex.loc[a:z]).prod() - 1
        rows[name] = row
        adds[name] = ex
    print("1. OVERLAYS ALONE (excess over cash, net), from "
          f"{book.index[0].date()}")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))
    comb = {"saved book": stats(book)}
    for name, ex in adds.items():
        comb[f"+ overlay, filter: {name}"] = stats(book + ex.reindex(book.index).fillna(0))
    print("\n2. SAVED BOOK (core + overlays) PLUS EACH BOUNCE-BACK OVERLAY")
    print(pd.DataFrame(comb).T.to_string(float_format=lambda v: f"{v:+.3f}"))


if __name__ == "__main__":
    main()
