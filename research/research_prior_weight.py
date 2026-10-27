"""The prior's weight in ``mu``: how much the book should lean on the Sharpe priors.

``research_prior_sensitivity.py`` found, after the fact, that halving every prior lifted the
core book from 1.15 to 1.31 (both halves) and 1.5x dropped it to 1.00. A smaller prior gives
trend, EWMAC, VRP and dollar value more say, but it also shrinks the book (vol 11.3% ->
8.7%, gross 1.28x -> 1.22x). This separates the two and picks the weight out of sample.
**Fixed before any test.**

* Grid: the prior's weight in ``ALPHAS`` at 1.0 (saved), 0.75, 0.5, 0.35, 0.25, 0.1; the
  other alphas and ``PORTFOLIO`` unchanged.
* Same size: at 0.5 and 0.25 also with risk aversion times the weight. The prior's own
  portfolio is then the size it is now and only its weight against the other alphas changes
  (the same as scaling the other alphas up by 1 / weight; the cost penalty is not scaled).
  A diagnostic: risk aversion is a portfolio setting and is not being tuned here.
* Selection: the weight with the best train-half Sharpe (2008-09 .. 2017-07-25), judged on
  the test half. Adopt only if it beats 1.0 by at least 0.02 there and over the whole window.

Core book only (net of borrowing; the overlays do not depend on ``mu``), from the saved warmup.

**Result (2026-10-27): the gain is the book's size, not the prior's weight; not adopted as a
prior change.** 2008-09-10 .. 2026-10-16. The grid: x1 1.15 (1.17 / 1.14), x0.75 1.22,
x0.5 1.30 (1.30 / 1.31), x0.35-x0.1 1.26-1.28; the train half picks x0.5, test 1.31 against
1.14. But return and vol fall with it (13.1% / 11.3% -> 11.3% / 8.5% at x0.5, 6.5% / 5.1% at
x0.1) and gross from 1.28x to 1.18x, below the 1.3x cap that binds at x1. At the same size
the prior's weight does nothing: x0.5 with half the risk aversion 1.15 (1.21 / 1.12), x0.25
1.08 (1.21 / 1.00). So trend, VRP and value given more say do not help; what helps is a book
small enough that the leverage cap stops binding: it then holds less in sectors (71% ->
53%) and more in Treasuries (30% -> 35%), the unconstrained mean-variance shape. That is a
question for risk aversion and the leverage cap (portfolio settings), not for the prior.
(``diff t`` is on the daily return difference, so it measures the lower return, not Sharpe.)

Usage:
    uv run python research/research_prior_weight.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
GRID = (1.0, 0.75, 0.5, 0.35, 0.25, 0.1)
SAME_SIZE = (0.5, 0.25)
CLASS_OF = {t: c for c, ts in core.CLASSES.items() for t in ts}


def variants() -> dict[str, tuple[float, float]]:
    """``{name: (prior weight, risk-aversion multiplier)}``."""
    out = {f"prior x{w:g}": (w, 1.0) for w in GRID}
    out |= {f"prior x{w:g}, same size": (w, w) for w in SAME_SIZE}
    return out


def run_one(args):
    name, (w, ra) = args
    b = latest_complete(core.load(), core.RISKY)
    sig = core.ALPHAS["prior"][0]
    p = replace(core.PORTFOLIO, risk_aversion=core.PORTFOLIO.risk_aversion * ra)
    s = core.build(alphas={**core.ALPHAS, "prior": (sig, w)}, portfolio=p)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, res.weights, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    vs = variants()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, vs.items())}
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "sharpe": t["full"]["sharpe"], "tr_sharpe": t["train"]["sharpe"],
        "te_sharpe": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[list(vs)]
    base = runs["prior x1"]
    ex = pd.DataFrame({n: runs[n] - base for n in vs})
    m["diff t"] = ex.mean() / ex.std() * np.sqrt(len(ex))
    m.loc["prior x1", "diff t"] = np.nan
    w = {n: v[1].drop(columns=CASH) for n, v in out.items()}
    m["gross"] = [w[n].sum(axis=1).mean() for n in vs]
    for c in ("US sectors", "Treasuries", "gold & commodities"):
        m[c] = [w[n][core.CLASSES[c]].sum(axis=1).mean() for n in vs]
    m["turnover"] = [out[n][2] for n in vs]
    idx = base.index
    pct = "{:.1%}".format
    print(f"\nCORE BOOK BY PRIOR WEIGHT, {idx[0].date()} .. {idx[-1].date()}, split "
          f"{SPLIT.date()}")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "US sectors",
                                                   "Treasuries")}
                      | {"gold & commodities": pct}
                      | {c: "{:.2f}".format for c in ("sharpe", "tr_sharpe", "te_sharpe",
                                                      "gross")}
                      | {"diff t": "{:+.1f}".format, "turnover": "{:.1f}".format}))

    grid = m.loc[[f"prior x{g:g}" for g in GRID]]
    pick = grid["tr_sharpe"].idxmax()
    print(f"\n   picked on the train half: {pick} (train {grid.loc[pick, 'tr_sharpe']:.2f}); "
          f"test {grid.loc[pick, 'te_sharpe']:.2f} against {m.loc['prior x1', 'te_sharpe']:.2f}"
          f" saved; full {grid.loc[pick, 'sharpe']:.2f} against "
          f"{m.loc['prior x1', 'sharpe']:.2f}")

    yr = pd.DataFrame({n: (1 + runs[n]).groupby(runs[n].index.year).prod() - 1
                       for n in ("prior x1", "prior x0.5", "prior x0.25",
                                 "prior x0.5, same size")})
    print("\n   calendar years")
    print(yr.to_string(float_format="{:+.1%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
