"""What if the book could short? Long-only against long/short, with and without relative momentum.

Relative sector momentum has the strongest IC found so far (1-day cross-sectional +0.025,
t 4.7, +0.017 after removing what EWMAC already knows) but lowered the long-only book's
Sharpe: with weights at or near zero, a ranking signal can mostly only add risk to winners.
This lifts the long-only rule. **Fixed before any test.**

Long/short rules: gross exposure (the sum of |weights| outside cash) at most 1.3x, the same
footprint as the saved book; net at most 1.3x; each ETF between -26% and +26% of capital
(the saved 20%-of-book cap at full leverage). Risk aversion and the cost band unchanged.
Shorts pay 1% a year of their notional (about 0.5% borrow fee and 0.5% less interest on the
short proceeds than T-bills earn); the dividends a short owes are already in total-return
prices. Borrowing cash still costs T-bill + 0.5%.

Variants: the saved book; long/short with the saved alphas; long/short plus relative
momentum (12-1 month, ranked within the 9 sector SPDRs and QQQ, weight 0.072 as in
``research_sector_priors.py``); long-only plus relative momentum; and long/short plus
relative momentum with gross up to 2x.

**Result (2026-10-17): negative; keep the book long-only.** Shorting lowered Sharpe in every
variant: long/short with the saved alphas 1.06 against 1.13 (difference t -2.9), mostly from
hedging shorts (DBC short on 76% of days, EEM, EFA, UUP, bonds) that cost 8.0 points in 2020
and 4.4 in 2021; with relative momentum 1.01; at 2x gross 1.01 (train 1.15, test 0.91,
drawdown -22.8%). Long-only plus relative momentum 1.04. The long-only rule acts as shrinkage
against estimation error, and these assets all carry a positive premium. Relative momentum
fails in every structure because its edge is mostly next-day (1-day IC t 4.7, 21-day t 0.9)
while the book trades about once a year by design.

Usage:
    uv run python examples/research_long_short.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from research_sector_priors import US_GROUP, RelativeMomentum

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare
from quantzy.signals import Identity
from quantzy.strategies import core_multiasset as core
from quantzy.strategy import Strategy

SPLIT = pd.Timestamp("2017-07-26")
REL_MOM_K = 0.072
SHORT_COST = 0.01  # a year, on short notional
CAP = 0.26


def gross_risky(limit: float):
    def _build(w, ctx):
        idx = [i for i, t in enumerate(ctx.tickers) if t != core.CASH]
        return [ctx.cp.norm1(w[idx]) <= limit]
    return _build


def abs_cap(limit: float):
    def _build(w, ctx):
        idx = [i for i, t in enumerate(ctx.tickers) if t != core.CASH]
        return [w[idx] <= limit, w[idx] >= -limit]
    return _build


def strategy(long_short: bool, rel_mom: bool, gross: float = 1.3) -> Strategy:
    p = core.PORTFOLIO
    alphas = dict(core.ALPHAS)
    if rel_mom:
        alphas["rel_mom"] = (RelativeMomentum(normalizer=Identity()), REL_MOM_K)
    if not long_short:
        return core.build(alphas=alphas)
    opt = MeanVariance(
        risk_aversion=p.risk_aversion,
        constraints=[C.budget(), C.max_leverage(p.max_leverage, core.CASH),
                     gross_risky(gross), abs_cap(CAP)],
        cost_model=LinearCost(p.cost_bps), cost_aversion=p.cost_aversion,
        execution_cost=LinearCost(0.0, spread_bps=dict(p.execution_cost_bps)))
    return Strategy(universe=core.UNIVERSE, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=opt, name="long_short")


def run_one(args: tuple[str, bool, bool, float]):
    name, ls, rm, gross = args
    bundle = core.load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)).run(
        bundle, strategy(ls, rm, gross))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    short = (-res.weights[core.RISKY]).clip(lower=0).sum(axis=1)
    r = r - short.reindex(r.index).fillna(0) * SHORT_COST / TRADING_DAYS
    return name, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


JOBS = [("saved book (long-only)", False, False, 1.3),
        ("long/short, saved alphas", True, False, 1.3),
        ("long/short + relative momentum", True, True, 1.3),
        ("long-only + relative momentum", False, True, 1.3),
        ("long/short + relative momentum, gross 2x", True, True, 2.0)]


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, JOBS)}
    names = [j[0] for j in JOBS]
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][2] for n in names]
    for n in names:
        w = out[n][1][core.RISKY]
        m.loc[n, "gross"] = w.abs().sum(axis=1).mean()
        m.loc[n, "net"] = w.sum(axis=1).mean()
        m.loc[n, "short"] = (-w).clip(lower=0).sum(axis=1).mean()
        m.loc[n, "short cost/yr"] = m.loc[n, "short"] * SHORT_COST
        us = w[US_GROUP]
        m.loc[n, "US eq net"] = us.sum(axis=1).mean()
        m.loc[n, "US eq gross"] = us.abs().sum(axis=1).mean()
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    pct = "{:.1%}".format
    r0 = runs[names[0]]
    print(f"BOOK, {r0.index[0].date()} .. {r0.index[-1].date()}, split {SPLIT.date()}; "
          f"exposures are averages, share of capital")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "short", "short cost/yr", "net",
                            "US eq net", "US eq gross")},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t", "gross")},
        "turnover": "{:.1f}".format}))
    print("\n   average weight by ETF, % of capital")
    wt = pd.DataFrame({n: out[n][1][core.RISKY].mean() for n in names}).T
    print(wt.to_string(float_format="{:+.0%}".format))
    print("\n   share of days each ETF is short")
    sh = pd.DataFrame({n: (out[n][1][core.RISKY] < -1e-4).mean() for n in names[1:]}).T
    print(sh.loc[[n for n in names[1:] if "long/short" in n]].to_string(
        float_format="{:.0%}".format))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved book (percentage points)")
    print(yr[names[1:]].sub(yr[names[0]], axis=0).to_string(
        float_format=lambda v: f"{v * 100:+.1f}"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
