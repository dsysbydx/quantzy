"""A fair breadth test: more ETFs inside each class, with the class mix held fixed.

``research_broad_universe.py`` added 48 ETFs and lost (Sharpe 0.88 against 1.17), but mostly
because equal priors moved a fifth of the book into emerging markets: it tested new prior
bets, not breadth. Here each class's total weight is pinned, day by day, to what the saved
17-ETF book holds in that class, so the only question is whether more instruments let the
same alphas pick better *within* each class. **Fixed before any test.**

Classes and their extra ETFs (from the 65 in ``research_broad_universe.py``, keeping only
classes the saved book already holds):

* US equity (9 sectors, QQQ): + IWM, MDY
* developed equity (EFA): + 13 country ETFs
* emerging equity (EEM): + 10 country ETFs
* Treasuries (IEF, TLT): + SHY, IEI, TIP
* gold and commodities (GLD, DBC): + SLV, DBB, DBA, USO, UNG, UGA
* currencies (UUP): + FXE, FXY, FXB, FXC, FXF

56 ETFs. Each class's weight equals the saved book's target weight in that class on the same
day (so cash and leverage match too). Priors, alphas, caps and costs as in
``research_broad_universe.py`` (per-ETF cap 20% of the invested book, as saved).

Runs: the saved book (the source of the class mix); the 17 with the mix pinned (a check: it
should reproduce the saved book); the 56 with the mix pinned; and both with the prior alone
(no trend, EWMAC or VRP), which splits what breadth does through diversification from what
it does through the alphas.

**Result (2026-10-17): breadth adds about nothing.** With the class mix pinned (2009-10 ..
2026-10), 56 ETFs made Sharpe 1.21 against 1.19 for the 17 (+0.30% a year, t 0.8); with the
prior alone 1.14 against 1.12 (+0.34%, t 1.0). The alphas add the same ~0.07 to both, so they
do not use the extra instruments; the new ETFs get 1-2% at most (IWM, IEI, Turkey,
Thailand), the classes where breadth could matter (ex-US equity) hold 4-5% of capital, and
the extras mostly duplicate existing exposures.

Usage:
    uv run python examples/research_fixed_mix_breadth.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace

import numpy as np
import pandas as pd
import research_broad_universe as broad

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.research import compare
from quantzy.signals import CarrySharpePrior, Identity, VarianceRiskPremium
from quantzy.strategies import core_multiasset as core
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
CLASSES = {
    "US equity": [*core.CLASSES["US sectors"], "QQQ", "IWM", "MDY"],
    "developed equity": broad.CLASSES["developed equity"],
    "emerging equity": broad.CLASSES["emerging equity"],
    "Treasuries": ["IEF", "TLT", "SHY", "IEI", "TIP"],
    "gold & commodities": ["GLD", "DBC", "SLV", "DBB", "DBA", "USO", "UNG", "UGA"],
    "currencies": ["UUP", "FXE", "FXY", "FXB", "FXC", "FXF"],
}
BROAD = [t for ts in CLASSES.values() for t in ts]
assert len(BROAD) == 56 and set(core.RISKY) <= set(BROAD)
CLASS_OF = {t: c for c, ts in CLASSES.items() for t in ts}


@dataclass
class _Params:
    """One cvxpy parameter per class target, shared by every problem the optimizer builds."""

    values: dict[str, object] = field(default_factory=dict)


def fixed_mix(holder: _Params, tickers: list[str]):
    import cvxpy as cp

    classes = [c for c in CLASSES if any(t in tickers for t in CLASSES[c])]
    holder.values = {c: cp.Parameter(nonneg=True, value=0.0) for c in classes}

    def _build(w, ctx):
        cons = []
        for c in classes:
            idx = [i for i, t in enumerate(ctx.tickers) if CLASS_OF.get(t) == c]
            cons.append(ctx.cp.sum(w[idx]) == holder.values[c])
        return cons
    return _build


@dataclass
class FixedMixStrategy(Strategy):
    """A strategy whose optimizer pins each class's weight to a daily schedule."""

    schedule: pd.DataFrame | None = None
    holder: _Params | None = None

    def decide(self, data, asof=None, current=None):
        row = self.schedule.loc[:asof].iloc[-1] if asof is not None else self.schedule.iloc[-1]
        for c, p in self.holder.values.items():
            p.value = max(float(row.get(c, 0.0)), 0.0)
        return super().decide(data, asof=asof, current=current)


def strategy(tickers: list[str], alphas_on: bool, costs: dict[str, float],
             schedule: pd.DataFrame | None) -> Strategy:
    prior = {t: broad.PRIOR[t] for t in tickers}
    alphas = {"prior": (CarrySharpePrior(prior, carry_vol=core.CARRY_VOL, carry_weight=0.5,
                                         normalizer=Identity()), 1.0)}
    if alphas_on:
        alphas |= {"trend": core.ALPHAS["trend"], "ewmac": core.ALPHAS["ewmac"],
                   "vrp": (VarianceRiskPremium(tuple(t for t in broad.EQUITY if t in tickers),
                                               normalizer=Identity()), core.ALPHAS["vrp"][1])}
    p = replace(core.PORTFOLIO, execution_cost_bps={t: costs[t] for t in tickers})
    opt = p.optimizer(CASH)
    holder = _Params()
    if schedule is not None:
        opt = type(opt)(risk_aversion=opt.risk_aversion,
                        constraints=[*opt._constraints, fixed_mix(holder, tickers)],
                        cost_model=opt.cost_model, cost_aversion=opt.cost_aversion,
                        execution_cost=opt.execution_cost)
    uni = Universe(name="fixed_mix", assets=[Asset(t, t, "etf") for t in tickers]
                   + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    kw = dict(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
              weights={n: w for n, (_, w) in alphas.items()},
              risk_model=p.risk_model(), optimizer=opt, name="fixed_mix")
    if schedule is None:
        return Strategy(**kw)
    return FixedMixStrategy(**kw, schedule=schedule, holder=holder)


def bundle_for(tickers: list[str]) -> DataBundle:
    full = broad.load()
    px = full.prices[[*tickers, CASH]]
    return DataBundle.build(px, **{k: full[k][px.columns] for k in ("vix", "market", "carry")})


def run(args):
    name, tickers, alphas_on, costs, schedule, start = args
    bundle = bundle_for(tickers)
    if name == "saved book":
        s = core.build()
        start = core.warmup(bundle, s)
    else:
        s = strategy(tickers, alphas_on, costs, schedule)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    targets = pd.DataFrame({d.asof: d.target.weights for d in res.decisions}).T
    years = len(r) / TRADING_DAYS
    turnover = sum(d.turnover for d in res.decisions) / years
    return name, r, res.weights, targets, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    costs = {**broad.cost_tiers(), **core.EXECUTION_COST_BPS}
    _, r0, w0, t0, to0 = run(("saved book", list(core.RISKY), True, costs, None, 0))
    sched = t0.drop(columns=CASH).T.groupby(
        lambda t: CLASS_OF[t] if t in CLASS_OF else "other").sum().T
    b = bundle_for(BROAD)
    probe = strategy(BROAD, True, costs, sched)
    start = broad.warmup(b, probe, BROAD)
    start = max(start, b.index.get_loc(sched.index[0]))
    print(f"class mix from the saved book; common start {b.index[start].date()}")
    jobs = [("17, mix pinned (check)", list(core.RISKY), True, costs, sched, start),
            ("56, mix pinned", BROAD, True, costs, sched, start),
            ("17, mix pinned, prior only", list(core.RISKY), False, costs, sched, start),
            ("56, mix pinned, prior only", BROAD, False, costs, sched, start)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, tg, to) for n, r, w, tg, to in pool.map(run, jobs)}
    out = {"saved book": (r0, w0, t0, to0), **out}
    names = list(out)
    s0 = b.index[start]
    runs = {n: out[n][0].loc[s0:] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][3] for n in names]
    m["held >1%"] = [(out[n][1].drop(columns=CASH).loc[s0:] > 0.01).sum(axis=1).mean()
                     for n in names]
    m["mix gap"] = [0.5 * (out[n][1].drop(columns=CASH).loc[s0:].T.groupby(
        lambda x: CLASS_OF.get(x, "other")).sum().T.reindex(columns=sched.columns, fill_value=0)
        - sched.reindex(out[n][1].loc[s0:].index).ffill()).abs().sum(axis=1).mean()
        for n in names]
    ex = pd.DataFrame({n: runs[n] - runs["saved book"] for n in names[1:]})
    m["diff t vs saved"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    d = runs["56, mix pinned"] - runs["17, mix pinned (check)"]
    d0 = runs["56, mix pinned, prior only"] - runs["17, mix pinned, prior only"]
    pct = "{:.1%}".format
    print(f"\nBOOK, {s0.date()} .. {runs['saved book'].index[-1].date()}, split {SPLIT.date()}; "
          f"mix gap = half the summed |class weight - saved class weight| (held weights)")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "mix gap")},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t vs saved")},
        "turnover": "{:.1f}".format, "held >1%": "{:.0f}".format}))
    for lab, x in (("56 minus 17, all alphas", d), ("56 minus 17, prior only", d0)):
        tstat = x.mean() / x.std() * np.sqrt(len(x))
        print(f"   {lab}: {x.mean() * TRADING_DAYS:+.2%} a year, tracking vol "
              f"{x.std() * np.sqrt(TRADING_DAYS):.2%}, t {tstat:+.2f}")
    print("\n   average weight within each class, 56 with mix pinned (% of capital)")
    w = out["56, mix pinned"][1].drop(columns=CASH).loc[s0:].mean()
    for c, ts in CLASSES.items():
        print(f"   {c:20s} " + ", ".join(f"{x} {w[x]:.1%}" for x in sorted(ts, key=lambda x: -w[x])
                                         if w[x] >= 0.001))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the 17 with mix pinned (percentage points)")
    print(yr[["56, mix pinned", "56, mix pinned, prior only"]].sub(
        yr["17, mix pinned (check)"], axis=0).to_string(float_format=lambda v: f"{v * 100:+.1f}"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
