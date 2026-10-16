"""Execution cost: is the book over-penalizing small trims?

``PortfolioConfig`` charges a flat 5 bp one way on every weight change and multiplies it by
48 in the optimizer (a trade must pay back in about a week), so the optimizer behaves as if a
unit of turnover cost 2.4%. Two things may make that too strict:

* the cost is also charged on the T-bill sleeve, so a trade funded from cash pays twice,
  although cash is not traded;
* 5 bp is above the likely cost of these ETFs at a broker such as IBKR: about 1 bp for QQQ,
  GLD, IEF and TLT, 2 bp for the sector SPDRs, EFA and EEM, and 5 bp for DBC and UUP
  (commission and half-spread; an assumption to check against real fills).

**Fixed before any test.** Cost models, each used both in the optimizer and as the cost
charged to returns:

* ``flat 5 bp, cash charged``: the saved book's model.
* ``flat 5 bp, cash free``: no cost on the T-bill leg.
* ``realistic, cash free``: the per-ETF rates above.

Cost aversion 6, 12, 24, 48 and 96 for each, plus 0 (the frictionless target the book would
hold if trading were free, charged the realistic cost) to measure how far each book lags it.
All other settings as ``core_multiasset``.

For a $3M account, order sizes are checked against IBKR's tiered minimum commission ($0.35 an
order, about 1 bp of a $3,500 order).

**Result (2026-10-17): keep the optimizer's penalty; charge realistic costs.** The
frictionless target (no penalty) is the worst book: Sharpe 0.96 at 24x turnover, 0.91% a
year of cost, and a lower return before cost than the banded book; the daily target chases
noise. Every looser setting is worse in every cost model (cost aversion 6-24: 0.96-1.07).
The best are a strong band: the saved flat 5 bp with cash charged at 48 (1.13) and realistic
costs at 96 (1.11). Freeing cash lowers Sharpe (1.08 at 48): the double charge damps swings
in net exposure, mostly reactions to vol spikes. 48 is a peak (neighbours 1.07), so the exact
value is partly luck. At $3M the median order is about $7,000; ~30% are under $3,500, where
IBKR's $0.35 minimum exceeds 1 bp, which is under $100 a year in total.

Usage:
    uv run python examples/research_execution_cost.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
NAV = 3_000_000
MIN_COMMISSION = 0.35
REALISTIC_BPS = {**{t: 2.0 for t in [*core.CLASSES["US sectors"], "EFA", "EEM"]},
                 **{t: 1.0 for t in ("QQQ", "GLD", "IEF", "TLT")},
                 "DBC": 5.0, "UUP": 5.0, core.CASH: 0.0}
MODELS = {
    "flat 5 bp, cash charged": {t: 5.0 for t in core.UNIVERSE.tickers},
    "flat 5 bp, cash free": {**{t: 5.0 for t in core.RISKY}, core.CASH: 0.0},
    "realistic, cash free": REALISTIC_BPS,
}
AVERSIONS = (6, 12, 24, 48, 96)
TOL = 1e-5  # smallest weight change counted as an order


@dataclass
class PerAssetCost:
    """Linear cost with one rate per ticker, in the universe's order."""

    rates: np.ndarray

    def cvxpy_cost(self, w, w_current, tickers=None):
        import cvxpy as cp

        return cp.sum(cp.multiply(self.rates, cp.abs(w - w_current)))

    def estimate(self, delta_weights: np.ndarray, tickers=None) -> float:
        return float(np.sum(self.rates * np.abs(delta_weights)))


def strategy(model: str, aversion: float):
    s = core.build(portfolio=replace(core.PORTFOLIO, cost_aversion=aversion))
    bps = MODELS[model]
    s.optimizer.cost_model = PerAssetCost(
        np.array([bps[t] for t in core.UNIVERSE.tickers]) / 1e4)
    return s


def run_one(args: tuple[str, str, float]):
    label, model, aversion = args
    bundle = core.load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)).run(
        bundle, strategy(model, aversion))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    rows = [(d.asof, t.ticker, t.delta_weight) for d in res.decisions for t in d.trades.trades
            if t.ticker != core.CASH and abs(t.delta_weight) > TOL]
    orders = pd.DataFrame(rows, columns=["date", "ticker", "dw"])
    cost = pd.Series({d.asof: d.trades.cost or 0.0 for d in res.decisions})
    return label, r, res.weights, orders, cost


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    jobs = [(f"{m} | ca {a}", m, a) for m in MODELS for a in AVERSIONS]
    jobs.append(("frictionless target | ca 0", "realistic, cash free", 0.0))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {lab: rest for lab, *rest in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    runs = {n: out[n][0] for n in names}
    years = len(runs[names[0]]) / TRADING_DAYS
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "sharpe": t["full"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    target = out["frictionless target | ca 0"][1][core.RISKY]
    for n in names:
        _, w, orders, cost = out[n]
        dollars = orders["dw"].abs() * NAV
        m.loc[n, "turnover"] = orders["dw"].abs().sum() / 2 / years
        m.loc[n, "cost/yr"] = cost.sum() / years
        m.loc[n, "trade days/yr"] = orders["date"].nunique() / years
        m.loc[n, "orders/yr"] = len(orders) / years
        m.loc[n, "median order $"] = dollars.median()
        m.loc[n, "orders < $3.5k"] = (dollars < MIN_COMMISSION / 1e-4).mean()
        m.loc[n, "lag to target"] = 0.5 * (w[core.RISKY] - target).abs().sum(axis=1).mean()
        m.loc[n, "gross"] = w[core.RISKY].sum(axis=1).mean()
    pct = "{:.1%}".format
    r0 = runs[names[0]]
    print(f"BOOK, {r0.index[0].date()} .. {r0.index[-1].date()}, split {SPLIT.date()}. "
          f"turnover = one-way risky turnover a year; cost/yr = cost charged; lag to target = "
          f"half the summed |w - frictionless w|; orders at ${NAV / 1e6:.0f}M NAV")
    print(m.to_string(formatters={
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "gross")},
        **{c: pct for c in ("ret", "vol", "max_dd", "orders < $3.5k", "lag to target")},
        "turnover": "{:.1f}x".format, "cost/yr": "{:.2%}".format,
        "trade days/yr": "{:.0f}".format, "orders/yr": "{:.0f}".format,
        "median order $": "{:,.0f}".format}))

    best = m.drop(index="frictionless target | ca 0")
    for model in MODELS:
        sub = best[best.index.str.startswith(model)]
        print(f"\n   {model}: Sharpe by cost aversion (train / test / full)")
        print("   " + "  ".join(f"ca {n.split('ca ')[1]}: {r.tr_sharpe:.2f}/{r.te_sharpe:.2f}/"
                                 f"{r.sharpe:.2f}" for n, r in sub.iterrows()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
