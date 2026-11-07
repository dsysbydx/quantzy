"""Minimum holdings per asset class: does a floor keep the book diversified at little cost?

The user's idea (2026-11-07), after 2026-11-05: the book held no Treasuries from March 2026
and 5% gold and commodities, so nothing cushioned a 6% equity fall. A floor is a constraint
in the optimizer (``PortfolioConfig.group_floors``, ``constraints.min_group``), not an
overlay: the optimizer funds it from what it likes least and still sizes the rest for the
best trade-off, whereas an overlay on top would add leverage the optimizer does not see.

How often the saved book is below the floor (2008-09 .. 2026-11): equity index 53% of days
under 5% (82% under 10%), US dollar 49% (65%), gold and commodities 16% (30%), Treasuries
10% (13%; 77% in 2026), US sectors never. So a floor on *every* class mostly forces more
equity (EFA, held 0.2% on average) and the dollar (prior Sharpe 0); the diversifiers that
cushion an equity fall are Treasuries and gold. **Fixed before running.** Floors are shares
of capital; everything else saved, including the VIX hedge with its exposure part.

  F1  every class >= 5%                      (the idea as stated)
  F2  every class >= 10%
  F3  Treasuries >= 5%, gold & commodities >= 5%
  F4  Treasuries >= 10%, gold & commodities >= 10%
  F5  Treasuries + gold & commodities + dollar >= 20% together (one floor; the optimizer
      picks which)

Metrics by period (history on proxy data 1995-10 .. 2008-09, train, test, full, 2026 to date,
2026-11-05): annualized return, vol, Sharpe, max drawdown, Calmar, worst 5 days; mean class
weights; correlation of daily returns with SPY on its worst 5% of days.

Adoption rule (a risk control, as ``research_exposure_controls``): Sharpe full, train and
test each no more than 0.03 below the saved book, the proxy run's too; and at least two of
max drawdown (>= 1.5 points better), worst 5 days (>= 1.5 points) and Calmar (>= 0.05), on
actual prices *or* on the proxy history. Among passers, the best Calmar. The user's call.

**Result (2026-11-07, prices to 2026-11-05): not adopted; floors mostly shrink the book
rather than diversify it.** All runs with the VIX hedge's exposure part.

* F2 (every class >= 10%) is the only one to pass the rule, on the proxy history: max
  drawdown -19.5% against -21.2%, worst 5 days -10.9% against -12.9%, Sharpe 1.32 against
  1.30. But it does so by holding less equity (sectors 52% of capital against 65%), at
  0.4-0.5 points of return a year (actual 13.7% against 14.2%). Against the saved book scaled
  to the same vol (0.93-0.97x) it is identical on actual prices (13.7%, -14.8% / -14.7%,
  -8.5% / -8.7%), and only a little better before 2008 (worst 5 days -10.9% against -12.3%,
  max drawdown -19.0% against -19.8%). On actual prices its test-half drawdown is worse
  (-14.8% against -13.9%).
* F1 (every class >= 5%): Sharpe 1.40 against 1.41, return -0.4 points a year; no tail gain
  that passes. F3 / F4 (Treasuries and gold at 5% / 10%): almost no change (Sharpe 1.40 /
  1.39; F4's train half 1.38 fails the Sharpe rule); F5 (defensive >= 20% together) binds
  only in 2026 and deepened that year's drawdown (-6.1% against -5.3%).
* None cushions the bad days: on SPY's worst 5% of days the book loses -0.88% to -0.90%
  under every variant. 2026-11-05: -2.9% to -3.1% against -3.2%. In 2026 the bonds a floor
  forces in fell with equities early in November, as the 2022 lesson warned.
* Reading: a floor holds assets the model expects to do worse, so it is a less-levered book
  with extra steps. If less risk is wanted, ``risk_aversion`` or the leverage cap is the
  cleaner dial; the diversifiers the book drops are dropped because their trend and carry
  are negative, and forcing them back has not paid since 2008.

Usage:
    uv run python research/research_class_floors.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.portfolio import GroupFloor
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

SPLIT = pd.Timestamp("2017-07-26")
K = core.CLASSES
DIVERS = ("Treasuries", "gold & commodities")


def _all(f: float) -> tuple[GroupFloor, ...]:
    return tuple(GroupFloor(tuple(ts), f) for ts in K.values())


def _div(f: float) -> tuple[GroupFloor, ...]:
    return tuple(GroupFloor(tuple(K[c]), f) for c in DIVERS)


FLOORS = {
    "saved": (),
    "F1 all>=5%": _all(0.05),
    "F2 all>=10%": _all(0.10),
    "F3 bonds,gold>=5%": _div(0.05),
    "F4 bonds,gold>=10%": _div(0.10),
    "F5 defensive>=20%": (GroupFloor(tuple(K["Treasuries"] + K["gold & commodities"]
                                           + K["US dollar"]), 0.20),),
}


def run(args):
    data, name = args
    b = (core.load if data == "actual" else core_proxies.load)()
    s = core.build(portfolio=replace(core.PORTFOLIO, group_floors=FLOORS[name]))
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, core.build())).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    eq = res.weights[core.EQUITY].sum(axis=1)
    for ov in overlays(core):
        kw = {"equity": eq} if getattr(ov, "USES_EQUITY", False) else {}
        r = r + ov.backtest_returns(b, **kw).reindex(r.index).fillna(0.0)
    cls = pd.DataFrame({c: res.weights[ts].sum(axis=1) for c, ts in K.items()})
    return data, name, r, cls


def stats(r: pd.Series) -> dict:
    nav = (1 + r).cumprod()
    yrs = len(r) / 252
    dd = float((nav / nav.cummax() - 1).min())
    cagr = nav.iloc[-1] ** (1 / yrs) - 1 if yrs >= 1 else np.nan
    vol = r.std() * np.sqrt(252)
    return {"ann ret": cagr, "total": nav.iloc[-1] - 1, "vol": vol,
            "sharpe": r.mean() * 252 / vol, "max dd": dd,
            "calmar": cagr / -dd if yrs >= 1 else np.nan, "worst 5d": r.rolling(5).sum().min()}


PERIODS = [("History 1995-10..2008-09 (proxy)", "proxy", None, "2008-09-09"),
           ("Train 2008-09..2017-07", "actual", "2008-09-10", "2017-07-25"),
           ("Test 2017-07..2026-10", "actual", "2017-07-26", "2026-10-30"),
           ("Full actual 2008-09..2026-11-05", "actual", None, None),
           ("Full with proxies 1995-10..2026-11-05", "proxy", None, None),
           ("2026 to date", "actual", "2026-01-01", None)]


def main() -> None:
    pd.set_option("display.width", 240)
    pd.set_option("display.max_rows", 200)
    jobs = [(d, n) for d in ("actual", "proxy") for n in FLOORS]
    with ProcessPoolExecutor(min(len(jobs), 8)) as pool:
        out = {(d, n): (r, c) for d, n, r, c in pool.map(run, jobs)}

    rows = []
    for lab, d, a, z in PERIODS:
        for n in FLOORS:
            rows.append({"period": lab, "book": n, **stats(out[(d, n)][0].loc[a:z])})
    t = pd.DataFrame(rows).set_index(["period", "book"])
    f = {c: ("{:.2f}".format if c in ("sharpe", "calmar") else "{:+.1%}".format) for c in t}
    print(t.to_string(formatters=f, na_rep="-"))
    t.to_csv(".cache/research/class_floors_periods.csv")

    print("\n2026-11-05: " + ", ".join(f"{n} {out[('actual', n)][0].loc['2026-11-05']:+.2%}"
                                      for n in FLOORS))
    print("\nmean class weight, actual 2008-09 on / 2026")
    w = pd.DataFrame({n: out[("actual", n)][1].mean() for n in FLOORS}).T
    w26 = pd.DataFrame({n: out[("actual", n)][1].loc["2026"].mean() for n in FLOORS}).T
    print(pd.concat([w, w26], axis=1, keys=["all", "2026"]).to_string(float_format="{:.0%}".format))

    spy = core.load()["market"].iloc[:, 0].pct_change()
    bad = spy[spy <= spy.quantile(0.05)].index
    print("\nmean return on SPY's worst 5% of days (actual): " + ", ".join(
        f"{n} {out[('actual', n)][0].reindex(bad).mean():+.2%}" for n in FLOORS))

    print("\n=== adoption rule")
    full = t.xs("Full actual 2008-09..2026-11-05", level="period")
    tr = t.xs("Train 2008-09..2017-07", level="period")
    te = t.xs("Test 2017-07..2026-10", level="period")
    px = t.xs("Full with proxies 1995-10..2026-11-05", level="period")
    for n in list(FLOORS)[1:]:
        sh = (full.loc[n, "sharpe"] >= full.loc["saved", "sharpe"] - 0.03
              and tr.loc[n, "sharpe"] >= tr.loc["saved", "sharpe"] - 0.03
              and te.loc[n, "sharpe"] >= te.loc["saved", "sharpe"] - 0.03
              and px.loc[n, "sharpe"] >= px.loc["saved", "sharpe"] - 0.03)

        def gains(x):
            return sum([x.loc[n, "max dd"] - x.loc["saved", "max dd"] >= 0.015,
                        x.loc[n, "worst 5d"] - x.loc["saved", "worst 5d"] >= 0.015,
                        x.loc[n, "calmar"] - x.loc["saved", "calmar"] >= 0.05])
        g = max(gains(full), gains(px))
        print(f"   {n:<20} Sharpe within 0.03: {sh}; tail gains actual {gains(full)}, "
              f"proxy {gains(px)} -> {'PASS' if sh and g >= 2 else 'fail'}")


if __name__ == "__main__":
    main()
