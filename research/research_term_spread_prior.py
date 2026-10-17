"""A Treasury prior that moves with the yield curve, in place of a fixed 0.25.

``research_fast_valuation.py`` found the term spread (long yield minus the 3-month bill)
forecasts Treasury excess returns: 1-month rank IC +0.15 (t 3.9) on the 10-year since 1962,
+0.13 (t 3.2) on the 30-year since 1977, in both halves and out of sample. The saved book
gives IEF and TLT a fixed Sharpe prior of 0.25, so it held its usual bond weight through the
inverted curve of 2022-24.

**Fixed before any test.** The term spread is the expected excess return of a long bond
over cash (carry); over its long-run vol it is a Sharpe ratio:

    IEF: SR = w * (10y yield - 3m bill) / 0.07 + (1 - w) * 0.25
    TLT: SR = w * (30y yield - 3m bill) / 0.14 + (1 - w) * 0.25

with ``w = 0.5`` (half the way from the judgment prior to the market's carry). Every other
asset keeps its prior; trend, EWMAC and VRP are unchanged. Variants: the saved book; ``w =
0.5`` at risk aversion 1.5; ``w = 0.5`` with risk aversion re-calibrated to the saved
book's train-window vol; and ``w = 1`` (carry alone) as a sensitivity check.

**Result (2026-10-17): adopted at w = 0.5** (``CarrySharpePrior`` in ``core_multiasset``).
Train Sharpe unchanged (1.13; the curve was steep, so the prior sat near 0.25), test 1.07 ->
1.13, full 1.09 -> 1.12 (difference t 1.9; 1.6 vol-matched at risk aversion 1.54). Treasury
weight 2022-24 fell from 18% to 8% of capital; better in 2022 (+1.7 points), 2023, 2024
(+3.9) and 2025, worse in 2020 (-0.9). Carry alone (w = 1) scored 1.14 but was not the
pre-set choice. In the book's sample this is mostly one episode; the 1962+ evidence carries
the weight.

Usage:
    uv run python research/research_term_spread_prior.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import calibrate_risk_aversion, compare
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
CARRY = {"IEF": ("^TNX", 0.07), "TLT": ("^TYX", 0.14)}
TREASURY_PRIOR = core.CLASS_SR["Treasuries"]


def load() -> DataBundle:
    """The saved book's bundle plus ``spread`` (long yield minus bill) for IEF and TLT."""
    b = core.load()
    px = b.prices
    y = CachedReader(YFinanceReader()).history(["^TNX", "^TYX", "^IRX"], start=core.START)
    y = (y / 100).reindex(px.index).ffill()
    spread = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
    for t, (ticker, _) in CARRY.items():
        spread[t] = y[ticker] - y["^IRX"]
    return DataBundle.build(px, vix=b["vix"], market=b["market"], spread=spread)


@dataclass
class CarryPrior(Signal):
    """The judgment Sharpe prior, with IEF and TLT moved ``weight`` of the way to carry/vol."""

    name: ClassVar[str] = "prior"
    weight: float = 0.5
    vol_window: int = 60

    def sharpe(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        sr = pd.DataFrame({t: float(core.PRIOR_SR.get(t, np.nan)) for t in px.columns},
                          index=px.index)
        for t, (_, vol) in CARRY.items():
            carry = data["spread"][t] / vol
            sr[t] = (self.weight * carry + (1 - self.weight) * TREASURY_PRIOR).fillna(
                TREASURY_PRIOR)
        return sr

    def raw(self, data: DataBundle) -> pd.DataFrame:
        return self.sharpe(data) * realized_vol(data.prices, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return self.vol_window + 1


def alphas(weight: float | None) -> dict:
    if weight is None:
        return core.ALPHAS
    return {**core.ALPHAS, "prior": (CarryPrior(weight, normalizer=Identity()), 1.0)}


def strategy(weight: float | None, ra: float):
    return core.build(alphas=alphas(weight),
                      portfolio=replace(core.PORTFOLIO, risk_aversion=ra))


def backtest(args: tuple[str, float | None, float]):
    name, weight, ra = args
    bundle = load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)).run(
        bundle, strategy(weight, ra))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def calibrate(target: float) -> float:
    bundle = load()
    fit = calibrate_risk_aversion(
        bundle, lambda ra: strategy(0.5, ra), target, end=SPLIT,
        backtester=WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)),
        bounds=(0.5, 10.0), tol=0.001)
    return fit.risk_aversion


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    bundle = load()
    sr = CarryPrior(0.5).sharpe(bundle)[list(CARRY)]
    print("Treasury prior with w = 0.5 (fixed prior: 0.25), year-end values")
    print(sr.groupby(sr.index.year).last().loc[2007:].T.to_string(
        float_format="{:+.2f}".format))

    ra0 = core.PORTFOLIO.risk_aversion
    jobs = [("saved book (0.25 fixed)", None, ra0), ("carry prior w=0.5", 0.5, ra0),
            ("carry prior w=1 (carry alone)", 1.0, ra0)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(backtest, jobs)}
        target = float(out[jobs[0][0]][0].loc[:SPLIT].std() * np.sqrt(TRADING_DAYS))
        ra = pool.submit(calibrate, target).result()
        n, r, w, t = pool.submit(backtest, (f"carry prior w=0.5, risk aversion {ra:.2f} "
                                            "(vol-matched)", 0.5, ra)).result()
        out[n] = (r, w, t)
    names = list(out)
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["tr_vol"] = [runs[n].loc[:SPLIT].std() * np.sqrt(TRADING_DAYS) for n in names]
    m["te_vol"] = [runs[n].loc[SPLIT:].std() * np.sqrt(TRADING_DAYS) for n in names]
    m["turnover"] = [out[n][2] for n in names]
    m["2022"] = [(1 + runs[n].loc["2022"]).prod() - 1 for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    pct = "{:.1%}".format
    r0 = runs[names[0]]
    print(f"\nBOOK, {r0.index[0].date()} .. {r0.index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={**{c: pct for c in ("ret", "vol", "max_dd", "tr_vol",
                                                      "te_vol", "2022")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe", "diff t")},
                                  "turnover": "{:.1f}".format}))
    print("\n   Treasury weight (IEF + TLT, % of capital), average by period")
    tw = pd.DataFrame({n: out[n][1][list(CARRY)].sum(axis=1) for n in names})
    print(tw.groupby(tw.index.year // 3 * 3).mean().rename(lambda y: f"{y}-{y + 2}").T
          .to_string(float_format="{:.0%}".format))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    d = yr[names[1:]].sub(yr[names[0]], axis=0)
    print("\n   calendar-year return minus the saved book (percentage points)")
    print(d.to_string(float_format=lambda v: f"{v * 100:+.1f}"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
