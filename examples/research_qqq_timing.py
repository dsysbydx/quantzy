"""QQQ timing: does a trend or vol-target overlay improve on buy-and-hold?

Holds QQQ and moves part of the book to T-bills when a rule says so. Two questions:

1. Does the overlay improve risk-adjusted return and drawdown over buy-and-hold QQQ?
2. Does it beat a *static* QQQ/cash mix holding the same average exposure? An overlay that is
   in cash 30% of the time will show a lower vol and drawdown just for holding less QQQ.
   Only the margin over a 70/30 static book is timing skill.

Every rule runs as a strategy through :class:`WalkForwardBacktester`, so it gets the same
rebalance dates, weight drift between rebalances and costs as its static benchmark. A rule
decides at ``asof`` from closes strictly before ``asof``, which is the same one-day lag the
signals package uses.

Cash is a synthetic T-bill series compounded from the 13-week yield (``^IRX``), so the
sample starts at QQQ's 1999 launch and includes the 2000-02 crash. SHY and BIL start in 2002
and 2007 and would miss it. Sharpe is the framework's: return over vol, not in excess of cash.

Usage:
    uv run python examples/research_qqq_timing.py [--rebalance-every 21] [--cost-bps 5]
        [--vol-target 0.15] [--vol-window 60]
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from quantzy.backtest import BacktestResult, FixedWeights, WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.construction.portfolio import Portfolio
from quantzy.construction.trades import diff
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.data.bundle import as_bundle
from quantzy.metrics import sharpe_ratio
from quantzy.optimize import LinearCost
from quantzy.optimize.cost import CostModel
from quantzy.research import compare, yearly
from quantzy.strategy import Decision
from quantzy.universe import Asset, Universe

RISKY, CASH = "QQQ", "TBILL"
UNIVERSE = Universe(
    name="qqq_timing",
    assets=[Asset(RISKY, "Invesco QQQ (Nasdaq-100)", "etf"),
            Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")],
)
QQQ_START = "1999-03-10"  # QQQ inception
HISTORY_START = "1990-01-02"  # Nasdaq-100 index backfill, for signal warmup only
TRADE_START = "2000-05-18"  # first trading day of every backtest, so all results line up


def warmup_rows(bundle: DataBundle) -> int:
    """Rows of history before :data:`TRADE_START`, the warmup every engine is given."""
    return int(bundle.index.get_loc(pd.Timestamp(TRADE_START)))

# A rule maps the risky asset's closes up to yesterday to an exposure in [0, 1].
Rule = Callable[[pd.Series], float]


# --- rules -----------------------------------------------------------------------------------

def above_sma(n: int) -> Rule:
    """Fully in when the last close is above its ``n``-day simple moving average."""
    return lambda px: float(px.iloc[-1] > px.iloc[-n:].mean())


def sma_blend(ns: tuple[int, ...]) -> Rule:
    """Average of several :func:`above_sma` votes: a stepped exposure instead of all-or-none."""
    rules = [above_sma(n) for n in ns]
    return lambda px: float(np.mean([r(px) for r in rules]))


def momentum(n: int = 252) -> Rule:
    """Fully in when the trailing ``n``-day return is positive."""
    return lambda px: float(px.iloc[-1] > px.iloc[-n - 1])


def vol_target(target: float, window: int) -> Rule:
    """Exposure ``target / realized vol``, capped at 1 (no leverage)."""
    def rule(px: pd.Series) -> float:
        vol = px.iloc[-window - 1:].pct_change().std() * np.sqrt(TRADING_DAYS)
        return float(min(1.0, target / vol)) if vol > 0 else 1.0
    return rule


def average(*rules: Rule) -> Rule:
    return lambda px: float(np.mean([r(px) for r in rules]))


# --- strategy --------------------------------------------------------------------------------

@dataclass
class Overlay:
    """Hold ``exposure`` in the risky asset and the rest in cash, as one rule dictates.

    Args:
        rule: Maps closes strictly before ``asof`` to a risky-asset weight in [0, 1].
        lookback: Closes the rule reads; sets :attr:`min_history`.
        cost_model: Charged on every rebalance, as for any strategy.
    """

    rule: Rule
    lookback: int
    universe: Universe = UNIVERSE
    cost_model: CostModel | None = None
    name: str = "overlay"

    @property
    def min_history(self) -> int:
        return self.lookback + 2  # lookback closes, all strictly before asof

    def decide(
        self,
        data: DataBundle | pd.DataFrame,
        asof: pd.Timestamp | None = None,
        current: Portfolio | None = None,
    ) -> Decision:
        bundle = as_bundle(data)
        asof = bundle.index[-1] if asof is None else pd.Timestamp(asof)
        px = bundle.prices[RISKY]
        exposure = float(np.clip(self.rule(px.loc[px.index < asof].dropna()), 0.0, 1.0))

        tickers = list(self.universe.tickers)
        if current is None:
            current = Portfolio.empty(tickers)
        target = Portfolio(
            weights=pd.Series({RISKY: exposure, CASH: 1.0 - exposure}).reindex(tickers),
            nav=current.nav,
        )
        delta = (target.weights - current.weights.reindex(tickers).fillna(0.0)).to_numpy()
        cost = self.cost_model.estimate(delta) if self.cost_model is not None else None
        return Decision(
            asof=asof,
            target=target,
            trades=diff(current, target, cost=cost).nonzero(),
            mu=pd.Series(0.0, index=tickers),
            sigma=pd.DataFrame(index=tickers, columns=tickers, dtype=float),
            status="rule",
            diagnostics={"strategy": self.name, "exposure": exposure},
        )


# --- data ------------------------------------------------------------------------------------

def load() -> DataBundle:
    """QQQ closes plus a T-bill price compounded from ``^IRX``.

    Before QQQ's launch the price is the Nasdaq-100 index (``^NDX``), rescaled to join QQQ on
    its first day. That history exists only so long-memory signals (an EMA, say) are warmed up
    by :data:`TRADE_START`. No backtest trades on it.

    ``^IRX`` is an annualized percent yield. The yield quoted at yesterday's close is what
    today's cash earns.
    """
    raw = CachedReader(YFinanceReader()).history([RISKY, "^NDX", "^IRX"], start=HISTORY_START)
    first = pd.Timestamp(QQQ_START)
    ndx = raw["^NDX"].loc[:first].dropna()
    qqq = pd.concat([(ndx / ndx.iloc[-1] * raw.loc[first, RISKY]).iloc[:-1],
                     raw[RISKY].loc[first:].dropna()])
    yld = raw["^IRX"].reindex(qqq.index).ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    tbill = (1.0 + cash_ret).cumprod()
    prices = pd.DataFrame({RISKY: qqq, CASH: tbill})
    return DataBundle.build(prices)


# --- reporting -------------------------------------------------------------------------------

def table(runs: dict[str, BacktestResult], split: pd.Timestamp,
          exposure: dict[str, float]) -> pd.DataFrame:
    """Full-period metrics, plus Sharpe for each half of the sample."""
    out = compare(runs)
    for name, period in (("sh_1st", slice(None, split - pd.Timedelta(days=1))),
                         ("sh_2nd", slice(split, None))):
        out[name] = [sharpe_ratio(runs[k].returns.loc[period]) for k in out.index]
    out["avg_exp"] = [exposure[k] for k in out.index]
    cols = ["ann_return", "ann_vol", "sharpe", "max_dd", "calmar", "avg_exp", "turnover",
            "sh_1st", "sh_2nd"]
    return out[cols]


FMT = {
    "ann_return": "{:.1%}".format, "ann_vol": "{:.1%}".format, "sharpe": "{:.2f}".format,
    "max_dd": "{:.1%}".format, "calmar": "{:.2f}".format, "avg_exp": "{:.0%}".format,
    "turnover": "{:.1f}".format, "sh_1st": "{:.2f}".format, "sh_2nd": "{:.2f}".format,
}


def main(rebalance_every: int, cost_bps: float, target: float, window: int) -> None:
    bundle = load()
    cost = LinearCost(cost_bps)

    blend = (100, 150, 200)
    rules: dict[str, tuple[Rule, int]] = {
        "trend 200d": (above_sma(200), 200),
        "trend blend 100/150/200": (sma_blend(blend), 200),
        "12m momentum": (momentum(252), 253),
        f"vol target {target:.0%}": (vol_target(target, window), window + 1),
        "blend + vol target (avg)": (average(sma_blend(blend), vol_target(target, window)), 200),
        "trend 200d x vol target": (lambda px: above_sma(200)(px) * vol_target(target, window)(px),
                                    200),
    }
    grid = {f"SMA {n}": (above_sma(n), n) for n in range(50, 301, 25)}

    # One start date for everything, so every row covers the same dates.
    warmup = warmup_rows(bundle)
    engine = WalkForwardBacktester(rebalance_every=rebalance_every, warmup=warmup)

    def static(w: float, name: str) -> BacktestResult:
        return engine.run(bundle, FixedWeights(UNIVERSE, weights={RISKY: w, CASH: 1.0 - w},
                                               cost_model=cost, name=name))

    def overlay(rule: Rule, lookback: int, name: str) -> BacktestResult:
        return engine.run(bundle, Overlay(rule, lookback, cost_model=cost, name=name))

    def avg_exposure(res: BacktestResult) -> float:
        return float(res.weights[RISKY].mean())

    runs: dict[str, BacktestResult] = {
        "buy & hold QQQ": static(1.0, "buy_hold"),
        "static 80/20 QQQ/cash": static(0.8, "static_80"),
    }
    for name, (rule, lb) in rules.items():
        runs[name] = res = overlay(rule, lb, name)
        avg = avg_exposure(res)
        runs[f"   vs static {avg:.0%} QQQ"] = static(avg, f"static_{avg:.2f}")
    sweep = {name: overlay(rule, lb, name) for name, (rule, lb) in grid.items()}

    idx = runs["buy & hold QQQ"].returns.index
    split = idx[len(idx) // 2]
    exposure = {k: avg_exposure(v) for k, v in {**runs, **sweep}.items()}

    print(f"\nSample {idx[0].date()} .. {idx[-1].date()} ({len(idx) / TRADING_DAYS:.1f} yrs) | "
          f"rebalance every {rebalance_every}d | cost {cost_bps:g}bps | "
          f"halves split at {split.date()}\n")
    print(table(runs, split, exposure).to_string(formatters=FMT))

    print("\nSMA lookback plateau (want a smooth ridge, not one spike):")
    g = table(sweep, split, exposure)
    print(g[["ann_return", "ann_vol", "sharpe", "max_dd", "avg_exp", "turnover",
             "sh_1st", "sh_2nd"]].to_string(formatters=FMT))

    pick = ["buy & hold QQQ", "trend 200d", "trend blend 100/150/200",
            f"vol target {target:.0%}", "blend + vol target (avg)"]
    yr = yearly({k: runs[k] for k in pick})
    print("\nCalendar-year return:")
    print(yr["return"].to_string(float_format="{:+.1%}".format))
    print("\nCalendar-year max drawdown:")
    print(yr["max_dd"].to_string(float_format="{:.1%}".format))

    n = len(rules) + len(grid)
    print(f"\n{n} variants examined. Trust a rule only if it beats its same-exposure static row,"
          " holds up in both halves, and sits on a plateau.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebalance-every", type=int, default=21, help="rows between rebalances")
    ap.add_argument("--cost-bps", type=float, default=5.0)
    ap.add_argument("--vol-target", type=float, default=0.15)
    ap.add_argument("--vol-window", type=int, default=60)
    a = ap.parse_args()
    main(a.rebalance_every, a.cost_bps, a.vol_target, a.vol_window)
