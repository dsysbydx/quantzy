"""QQQ timing as a daily optimization rather than a monthly switch.

``research_qqq_timing.py`` asks a rule for a weight once a month. This runs the framework's
full path every day instead: signals -> ``mu``, risk model -> ``Sigma``, and a mean-variance
optimizer that starts from the book it holds and pays for every trade. It trades only when
the improvement in ``mu'w - lambda * w'Sigma w`` beats the cost of getting there.

The forecast for QQQ's excess return over T-bills is

    mu = premium + k * tanh(log(P / SMA_n) / monthly vol)

- **premium**: the equity premium you expect from holding QQQ at all. With ``k = 0`` the
  strategy is a pure vol target, since the optimum ``mu / (2 lambda sigma^2)`` shrinks as
  vol rises.
- **k * trend**: moves the forecast up or down with QQQ's distance from its ``n``-day average,
  measured in months of volatility and squashed to (-1, 1). It is continuous, so a close that
  grazes the average moves the forecast a little, not all the way to cash.
- **T-bills**: ``mu = 0``, since the forecast is already in excess of cash.

``cost_aversion`` scales the cost in the objective (see
:class:`~quantzy.optimize.base.BaseOptimizer`). It is the dial for how much better the new
book has to be before the optimizer moves. 1 means a trade must pay for itself within a year,
and 12 means within a month.

Usage:
    uv run python research/research_qqq_optimizer.py [--workers 8]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd
from research_qqq_timing import (
    CASH,
    FMT,
    RISKY,
    UNIVERSE,
    Overlay,
    above_sma,
    load,
    table,
    warmup_rows,
)

from quantzy.backtest import BacktestResult, FixedWeights, WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import yearly
from quantzy.risk import SampleCovariance
from quantzy.signals import TrendStrength
from quantzy.signals.base import Signal
from quantzy.signals.normalize import Identity
from quantzy.strategy import Strategy

COST_BPS = 5.0
GRID = {
    "k": [0.0, 0.05, 0.10],             # trend weight, annual return per unit of trend
    "cost_aversion": [1.0, 4.0, 12.0, 52.0],
}
BASE = {"premium": 0.05, "risk_aversion": 0.5, "n": 200, "halflife": 20}


# --- signals ---------------------------------------------------------------------------------

@dataclass
class Premium(Signal):
    """A constant expected excess return for one asset; zero elsewhere."""

    name: ClassVar[str] = "premium"
    premium: float = 0.05
    asset: str = RISKY

    def raw(self, data: DataBundle) -> pd.DataFrame:
        out = pd.DataFrame(np.nan, index=data.index, columns=data.tickers)
        out[self.asset] = self.premium
        return out


def build(k: float, cost_aversion: float, premium: float, risk_aversion: float, n: int,
          halflife: float) -> Strategy:
    return Strategy(
        universe=UNIVERSE,
        signals={"premium": Premium(premium, normalizer=Identity()),
                 "trend": TrendStrength(n, exclude=(CASH,), normalizer=Identity())},
        weights={"trend": k},
        risk_model=SampleCovariance(halflife=halflife, lookback=250),
        optimizer=MeanVariance(
            risk_aversion=risk_aversion,
            constraints=[C.budget(), C.long_only()],
            cost_model=LinearCost(COST_BPS),
            cost_aversion=cost_aversion,
        ),
        name=f"k={k:g} ca={cost_aversion:g}",
    )


# --- runs ------------------------------------------------------------------------------------

def run_one(params: dict) -> tuple[str, BacktestResult]:
    """One daily backtest; module-level so a process pool can pickle it."""
    bundle = load()
    strategy = build(**params)
    engine = WalkForwardBacktester(rebalance_every=1, warmup=warmup_rows(bundle))
    return strategy.name, engine.run(bundle, strategy)


def trade_days(res: BacktestResult, threshold: float = 1e-3) -> float:
    """Days per year on which the book actually traded more than ``threshold`` of NAV."""
    days = sum(d.turnover > threshold for d in res.decisions)
    return days / (len(res.returns) / TRADING_DAYS)


def main(workers: int) -> None:
    bundle = load()
    cost = LinearCost(COST_BPS)

    combos = [{**BASE, "k": k, "cost_aversion": ca}
              for k, ca in itertools.product(GRID["k"], GRID["cost_aversion"])]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        sweep = dict(pool.map(run_one, combos))

    monthly = WalkForwardBacktester(rebalance_every=21, warmup=warmup_rows(bundle))
    daily = WalkForwardBacktester(rebalance_every=1, warmup=warmup_rows(bundle))
    bench = {
        "buy & hold QQQ": daily.run(bundle, FixedWeights(UNIVERSE, weights={RISKY: 1.0},
                                                         cost_model=cost)),
        "trend 200d switch, monthly": monthly.run(
            bundle, Overlay(above_sma(200), 200, cost_model=cost, name="sma200_m")),
        "trend 200d switch, daily": daily.run(
            bundle, Overlay(above_sma(200), 200, cost_model=cost, name="sma200_d")),
    }
    runs = {**bench, **sweep}

    idx = bench["buy & hold QQQ"].returns.index
    split = idx[len(idx) // 2]
    exposure = {k: float(v.weights[RISKY].mean()) for k, v in runs.items()}

    print(f"\nSample {idx[0].date()} .. {idx[-1].date()} ({len(idx) / TRADING_DAYS:.1f} yrs) | "
          f"daily decisions | cost {COST_BPS:g}bps per leg | halves split at {split.date()}")
    print(f"Fixed: {BASE}\n")
    out = table(runs, split, exposure)
    out["trade_days"] = [trade_days(runs[k]) for k in out.index]
    fmt = {**FMT, "trade_days": "{:.0f}".format}
    cols = ["ann_return", "ann_vol", "sharpe", "max_dd", "calmar", "avg_exp", "turnover",
            "trade_days", "sh_1st", "sh_2nd"]
    print(out[cols].to_string(formatters=fmt))

    print("\nSharpe, trend weight k (rows) x cost_aversion (cols):")
    grid = pd.DataFrame({ca: {k: out.loc[f"k={k:g} ca={ca:g}", "sharpe"]
                              for k in GRID["k"]} for ca in GRID["cost_aversion"]})
    print(grid.rename_axis(index="k", columns="ca").to_string(float_format="{:.2f}".format))
    print("\nTrade days per year:")
    td = pd.DataFrame({ca: {k: out.loc[f"k={k:g} ca={ca:g}", "trade_days"]
                            for k in GRID["k"]} for ca in GRID["cost_aversion"]})
    print(td.rename_axis(index="k", columns="ca").to_string(float_format="{:.0f}".format))

    # A run that never buys in (a cost penalty too high to ever pay from a flat start) sits in
    # T-bills, whose near-zero vol gives it a high Sharpe. It is not a QQQ strategy.
    invested = [k for k in sweep if exposure[k] >= 0.5]
    best = out.loc[invested, "sharpe"].idxmax()
    pick = ["buy & hold QQQ", "trend 200d switch, monthly", best]
    yr = yearly({k: runs[k] for k in pick})
    print(f"\nCalendar-year return and max drawdown (best by full-sample Sharpe among runs "
          f"averaging >= 50% QQQ: {best}):")
    print(pd.concat({"return": yr["return"], "max_dd": yr["max_dd"]}, axis=1)
          .to_string(float_format="{:+.1%}".format))

    last = sweep[best].decisions[-1]
    print(f"\nToday ({last.asof.date()}), {best}: mu[QQQ] = {last.mu[RISKY]:+.1%}, "
          f"vol = {np.sqrt(last.sigma.loc[RISKY, RISKY]):.1%}, "
          f"target QQQ = {last.weights[RISKY]:.0%}, cash = {last.weights[CASH]:.0%}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    main(ap.parse_args().workers)
