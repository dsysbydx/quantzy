"""Book size: risk aversion x leverage cap, with the overlays.

``research_prior_weight.py`` found that a smaller book has a higher Sharpe (1.15 -> 1.30 at
half the prior, test 1.31) and that the gain is the size, not the prior's weight: at the
saved settings the 1.3x leverage cap binds and the optimizer cuts Treasuries, the asset that
needs the most notional for its risk, leaving an equity-heavy book. So the choice is between
a smaller book and a higher cap. Both are portfolio settings; this maps them for a decision,
it does not tune them to an alpha. **Fixed before any test.**

* Risk aversion 1.5 (saved), 2, 3, 4 against a leverage cap of 1.3x (saved), 1.6x, 2.0x;
  everything else in ``PORTFOLIO`` unchanged (the 20% cap is of the invested book, so it
  scales with it; borrowing at T-bill + 50 bp).
* The saved book: core plus the Treasury month-end and VIX hedge overlays, as traded.
* Gross is the core's risky weight plus the overlays' (0.5x TLT on the last two days of each
  month, 0.05x VIXY while the regime is on). Reg T allows 2x overnight: days above it are
  counted, and a cell that needs them is not tradeable as is.

Reported: Sharpe in both halves (split 2017-07-26), return, vol, drawdown, Calmar, average and
peak gross, days above 2x, and the core's weight in Treasuries and sectors.

**Result (2026-10-27).** 2008-09-10 .. 2026-10-16, core plus overlays.
* A higher cap buys return, not Sharpe: at every risk aversion Sharpe is flat to lower from
  1.3x to 2x (1.27 / 1.26 / 1.24 at lambda 1.5), drawdowns deepen (-17.2% -> -22.2%), the
  extra room goes mostly to sectors and Treasuries, and with the month-end overlay gross
  passes Reg T's 2x on 5-24% of days. Not tradeable as is above 1.3x.
* Sharpe rises with risk aversion at every cap, in both halves: lambda 1.5 / 2 / 3 / 4 at
  1.3x gives 1.27 (1.25 / 1.29), 1.33 (1.32 / 1.35), 1.40 (1.36 / 1.44), 1.46 (1.37 / 1.55);
  return 14.3% / 13.4% / 11.7% / 10.3%, vol 11.0% / 9.8% / 8.1% / 6.8%, drawdown -17.2% /
  -15.5% / -13.8% / -10.9%. Sectors fall from 71% to 45% of NAV.
* The leverage cap explains only part of it: at lambda 4 the book is at 1.05x and the cap
  does not bind, yet Sharpe is still higher than at 3. Less borrowing (0.28x at T-bill +
  50 bp at lambda 1.5) is another part; the rest is not explained here.
* At lambda 3 or 4 vol is at or below the 8-12% band. Lambda 2 keeps it mid-band (9.8%)
  for +0.06 Sharpe, -1.7 points of drawdown and -0.9 points of return.

Usage:
    uv run python research/research_size_grid.py [--workers 6]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme
from quantzy.strategies import vix_hedge

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
RISK_AVERSION = (1.5, 2.0, 3.0, 4.0)
LEVERAGE = (1.3, 1.6, 2.0)
REG_T = 2.0


def name(ra: float, lev: float) -> str:
    return f"lambda {ra:g}, cap {lev:g}x"


def run_one(args):
    ra, lev = args
    b = latest_complete(core.load(), core.RISKY)
    p = replace(core.PORTFOLIO, risk_aversion=ra, max_leverage=lev)
    s = core.build(portfolio=p)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    r = p.net_of_borrowing(res.returns, res.weights, CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name(ra, lev), r, res.weights, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    overlays = tme.backtest_returns(b) + vix_hedge.backtest_returns(b)
    # overlay weight held into each day
    days = b.prices["TLT"].dropna().index
    tme_w = (tme.in_window(days).astype(float) * tme.SIZE).reindex(b.index).fillna(0.0)
    vix = b["vix"].iloc[:, 0].ffill()
    vix_w = vix_hedge.regime(vix).shift(1, fill_value=False).astype(float) * vix_hedge.SIZE
    vix_w = vix_w.where(b["vixy"].iloc[:, 0].notna(), 0.0)
    over_w = tme_w + vix_w

    grid = list(itertools.product(RISK_AVERSION, LEVERAGE))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, grid)}
    names = [name(ra, lev) for ra, lev in grid]
    runs = {n: out[n][0] + overlays.reindex(out[n][0].index).fillna(0) for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "sharpe": t["full"]["sharpe"], "tr_sharpe": t["train"]["sharpe"],
        "te_sharpe": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["calmar"] = m["ret"] / -m["max_dd"]
    core_gross = {n: out[n][1].drop(columns=CASH).sum(axis=1).shift(1) for n in names}
    gross = {n: (g + over_w.reindex(g.index).fillna(0)).dropna() for n, g in core_gross.items()}
    m["core gross"] = [core_gross[n].mean() for n in names]
    m["peak gross"] = [gross[n].max() for n in names]
    m["days > 2x"] = [(gross[n] > REG_T + 1e-9).mean() for n in names]
    w = {n: out[n][1] for n in names}
    m["Treasuries"] = [w[n][core.CLASSES["Treasuries"]].sum(axis=1).mean() for n in names]
    m["sectors"] = [w[n][core.CLASSES["US sectors"]].sum(axis=1).mean() for n in names]
    m["turnover"] = [out[n][2] for n in names]
    idx = runs[names[0]].index
    pct = "{:.1%}".format
    print(f"\nSAVED BOOK (CORE + OVERLAYS) BY RISK AVERSION AND LEVERAGE CAP, {idx[0].date()} .. "
          f"{idx[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "days > 2x",
                                                   "Treasuries", "sectors")}
                      | {c: "{:.2f}".format for c in ("sharpe", "tr_sharpe", "te_sharpe",
                                                      "calmar", "core gross", "peak gross")}
                      | {"turnover": "{:.1f}".format}))
    for col in ("sharpe", "ret", "max_dd"):
        piv = m[col].copy()
        piv.index = pd.MultiIndex.from_tuples(grid, names=["lambda", "cap"])
        f = "{:.2f}".format if col == "sharpe" else "{:.1%}".format
        print(f"\n   {col}")
        print(piv.unstack().to_string(float_format=f))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
