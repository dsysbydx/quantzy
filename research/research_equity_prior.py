"""Equity Sharpe prior: 0.30 (saved) against 0.35 and 0.40.

``research_prior_check.py`` found every US sector earned an excess Sharpe of 0.38-0.51 over
100 years, QQQ 0.57 since 1985 and EEM 0.40 since 1989, against a prior of 0.30. The 0.30
is a deliberate haircut for today's valuations. This shows what raising it does to the book.

A higher equity prior changes two things at once: the mix (more equities, fewer bonds) and
the size (with risk aversion fixed, a larger ``mu`` means a larger, more volatile book). So
each prior is run twice, all else in ``core_multiasset`` unchanged:

* at the saved risk aversion, 1.5;
* with risk aversion re-calibrated on the train window (to 2017-07-26) to the saved book's
  train-window vol, which isolates the mix effect.

**Result (2026-10-16): keep 0.30.** At risk aversion 1.5, 0.35 and 0.40 grow the book
(equity weight 76% -> 84% -> 91%, test-window vol 12.3% -> 13.9%) and lower Sharpe (1.09 ->
1.06 -> 1.00). Vol-matched (risk aversion 1.85 and 2.24) the mix barely moves (equity 78-79%)
and Sharpe is 1.09 and 1.06: the optimizer funds the extra equity view mostly from gold and
commodities, which earned well here, and keeps Treasuries as the hedge.

Usage:
    uv run python research/research_equity_prior.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.research import calibrate_risk_aversion, compare
from quantzy.signals import Identity, SharpePrior
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
PRIORS = (0.30, 0.35, 0.40)
EQUITY_CLASSES = ("US sectors", "equity index")
SELLOFFS = {"2008-09 .. 2009-03": ("2008-09-10", "2009-03-09"),
            "2020-02 .. 2020-03": ("2020-02-19", "2020-03-23"),
            "2022": ("2022-01-01", "2022-12-31")}


def alphas(equity_sr: float) -> dict:
    sr = {t: (equity_sr if c in EQUITY_CLASSES else core.CLASS_SR[c])
          for c, ts in core.CLASSES.items() for t in ts}
    sr["DBC"] = core.PRIOR_SR["DBC"]
    return {**core.ALPHAS, "prior": (SharpePrior(sr, normalizer=Identity()), 1.0)}


def strategy(equity_sr: float, risk_aversion: float):
    return core.build(alphas=alphas(equity_sr),
                      portfolio=replace(core.PORTFOLIO, risk_aversion=risk_aversion))


def backtest(args: tuple[float, float]) -> tuple[float, float, pd.Series, pd.DataFrame, float]:
    equity_sr, ra = args
    bundle = core.load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)).run(
        bundle, strategy(equity_sr, ra))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return equity_sr, ra, r, res.weights, turnover


def calibrate(args: tuple[float, float]) -> tuple[float, float, list]:
    equity_sr, target = args
    bundle = core.load()
    fit = calibrate_risk_aversion(
        bundle, lambda ra: strategy(equity_sr, ra), target, end=SPLIT,
        backtester=WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)),
        bounds=(0.5, 10.0), tol=0.001)
    return equity_sr, fit.risk_aversion, fit.trials


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    ra0 = core.PORTFOLIO.risk_aversion
    with ProcessPoolExecutor(max_workers=workers) as pool:
        base = {sr: (r, w, t) for sr, _, r, w, t in pool.map(backtest, [(p, ra0) for p in PRIORS])}
        r0 = base[0.30][0]
        target = float(r0.loc[:SPLIT].std() * np.sqrt(TRADING_DAYS))
        print(f"saved book's train-window vol: {target:.2%}; calibrating the others to it")
        fits = {sr: ra for sr, ra, _ in pool.map(calibrate, [(p, target) for p in PRIORS[1:]])}
        cal = {sr: (r, w, t) for sr, _, r, w, t in
               pool.map(backtest, [(p, fits[p]) for p in PRIORS[1:]])}

    runs, weights, turn = {}, {}, {}
    for sr in PRIORS:
        n = f"equity {sr:.2f}, risk aversion {ra0:g}"
        runs[n], weights[n], turn[n] = base[sr]
    for sr in PRIORS[1:]:
        n = f"equity {sr:.2f}, risk aversion {fits[sr]:.2f} (vol-matched)"
        runs[n], weights[n], turn[n] = cal[sr]
    names = list(runs)
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["tr_vol"] = [runs[n].loc[:SPLIT].std() * np.sqrt(TRADING_DAYS) for n in names]
    m["te_vol"] = [runs[n].loc[SPLIT:].std() * np.sqrt(TRADING_DAYS) for n in names]
    m["turnover"] = [turn[n] for n in names]
    eq = [t for c in EQUITY_CLASSES for t in core.CLASSES[c]]
    m["equity wt"] = [weights[n][eq].sum(axis=1).mean() for n in names]
    m["Treasury wt"] = [weights[n][core.CLASSES["Treasuries"]].sum(axis=1).mean() for n in names]
    m["gold+cmdty wt"] = [weights[n][core.CLASSES["gold & commodities"]].sum(axis=1).mean()
                          for n in names]
    for lab, (a, z) in SELLOFFS.items():
        m[lab] = [(1 + runs[n].loc[a:z]).prod() - 1 for n in names]
    pct = "{:.1%}".format
    r = runs[names[0]]
    print(f"\nBOOK, {r.index[0].date()} .. {r.index[-1].date()}, split {SPLIT.date()} | "
          f"all other settings as core_multiasset")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "tr_vol", "te_vol", "equity wt",
                            "Treasury wt", "gold+cmdty wt", *SELLOFFS)},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe")},
        "turnover": "{:.1f}".format}))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar years")
    print(yr[names].to_string(float_format="{:+.1%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
