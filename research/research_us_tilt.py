"""How much of the saved book's Sharpe is a hindsight US-equity tilt?

Every equity ETF in ``core_multiasset`` has the same 0.30 prior, but 10 of its 12 equity ETFs
are US (nine sector SPDRs and QQQ), against one EFA and one EEM. With equal priors the
optimizer treats ten partly independent US bets as a diversified sleeve: about 77% of capital
and 82% of book variance sit in US equity. That universe was chosen in 2026, with US
outperformance known. **Fixed before any test.** All use the saved alphas and portfolio
settings unless stated.

1. *US-neutral universe*, 2008-09 on: SPY, EFA, EEM, IEF, TLT, GLD, DBC, UUP; equities all at
   0.30, VRP on the three equity ETFs. At the saved 20%-of-book cap and at 40% (with eight
   assets the cap binds much harder).
2. *World-cap anchor*: the 17 ETFs with equity split 60% US (sectors and QQQ), 30% EFA, 10%
   EEM, as equality constraints; the optimizer still sets total equity and the US mix.
3. *Longer window*, from late 2004: without GLD, DBC and UUP (launched 2004-07), so the book
   starts once EEM (2003) has enough history. US-heavy (14 ETFs), US-neutral (SPY, EFA, EEM,
   IEF, TLT at a 40% cap: five assets at 20% would force equal weights), and the 14 with the
   60/30/10 anchor. Reported by period: 2004-07 (when ex-US equities led), 2008-09, 2010+.

**Result (2026-10-17).** The saved book has no region constraint; its 73% US-equity weight
comes from 10 of 12 equity ETFs being US. Anchoring equity at 60/30/10 (the cleanest
measure) cut Sharpe from 1.13 to 1.04, so the US tilt was worth about 0.09 since 2008. A
US-neutral 8-ETF book made 0.94-1.02, but also loses the sector diversification. From 2004,
without GLD, DBC and UUP: US-heavy 0.96, US-neutral 0.89, anchored 0.81; in 2004-07, when
ex-US led, 1.45 against 1.58. Without those three diversifiers the drawdown is -30.6%. No
change to the saved book.

Usage:
    uv run python research/research_us_tilt.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import compare
from quantzy.signals import CarrySharpePrior, Identity, VarianceRiskPremium
from quantzy.strategies import core_multiasset as core
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
US = [*core.CLASSES["US sectors"], "QQQ"]
EQUITY_ALL = [*US, "SPY", "EFA", "EEM"]
PRIOR = {**core.PRIOR_SR, "SPY": 0.30}
COSTS = {**core.EXECUTION_COST_BPS, "SPY": 1.0}
ANCHOR = {"US": 0.60, "EFA": 0.30, "EEM": 0.10}
PERIODS = [("2004", "2007"), ("2008", "2009"), ("2010", "2026")]


@dataclass(frozen=True)
class Spec:
    name: str
    tickers: tuple[str, ...]
    cap: float = 0.20
    anchor: bool = False


SAVED = tuple(core.RISKY)
NO_LATE = tuple(t for t in core.RISKY if t not in ("GLD", "DBC", "UUP"))
SPECS = [
    Spec("1. saved 17", SAVED),
    Spec("1. US-neutral 8, cap 20%", ("SPY", "EFA", "EEM", "IEF", "TLT", "GLD", "DBC", "UUP")),
    Spec("1. US-neutral 8, cap 40%", ("SPY", "EFA", "EEM", "IEF", "TLT", "GLD", "DBC", "UUP"),
         cap=0.40),
    Spec("2. saved 17, 60/30/10 anchor", SAVED, anchor=True),
    Spec("3. long: US-heavy 14", NO_LATE),
    Spec("3. long: US-neutral 5, cap 40%", ("SPY", "EFA", "EEM", "IEF", "TLT"), cap=0.40),
    Spec("3. long: US-heavy 14, 60/30/10 anchor", NO_LATE, anchor=True),
]


def load() -> DataBundle:
    tickers = sorted({t for s in SPECS for t in s.tickers})
    yields = list(core.CARRY_YIELDS.values())
    raw = CachedReader(YFinanceReader()).history([*tickers, "^IRX", "^VIX", *yields],
                                                 start=core.START)
    yld = raw["^IRX"].ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    prices = raw[tickers].copy()
    prices[CASH] = (1.0 + cash_ret).cumprod()

    def wide(s: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({t: s for t in prices.columns}, index=prices.index)

    carry = pd.DataFrame(index=prices.index, columns=prices.columns, dtype=float)
    for t, y in core.CARRY_YIELDS.items():
        carry[t] = (raw[y].ffill() - raw["^IRX"].ffill()) / 100.0
    return DataBundle.build(prices, vix=wide(raw["^VIX"]), market=wide(raw["SPY"]), carry=carry)


def anchor_constraint(tickers: tuple[str, ...]):
    def _build(w, ctx):
        pos = {t: i for i, t in enumerate(ctx.tickers)}
        us = [pos[t] for t in US if t in pos]
        eq = us + [pos["EFA"], pos["EEM"]]
        total = ctx.cp.sum(w[eq])
        return [ctx.cp.sum(w[us]) == ANCHOR["US"] * total,
                w[pos["EFA"]] == ANCHOR["EFA"] * total,
                w[pos["EEM"]] == ANCHOR["EEM"] * total]
    return _build


def strategy(spec: Spec) -> Strategy:
    t = spec.tickers
    alphas = {
        "prior": (CarrySharpePrior({k: PRIOR[k] for k in t}, carry_vol=core.CARRY_VOL,
                                   carry_weight=0.5, normalizer=Identity()), 1.0),
        "trend": core.ALPHAS["trend"],
        "ewmac": core.ALPHAS["ewmac"],
        "vrp": (VarianceRiskPremium(tuple(x for x in EQUITY_ALL if x in t),
                                    normalizer=Identity()), core.ALPHAS["vrp"][1]),
    }
    p = replace(core.PORTFOLIO, max_position=spec.cap,
                execution_cost_bps={k: COSTS[k] for k in t})
    opt = p.optimizer(CASH)
    if spec.anchor:
        opt = type(opt)(risk_aversion=opt.risk_aversion,
                        constraints=[*opt._constraints, anchor_constraint(t)],
                        cost_model=opt.cost_model, cost_aversion=opt.cost_aversion,
                        execution_cost=opt.execution_cost)
    uni = Universe(name=spec.name, assets=[Asset(x, x, "etf") for x in t]
                   + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    return Strategy(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=opt, name=spec.name)


def warmup(bundle: DataBundle, s: Strategy, tickers) -> int:
    first = bundle.prices[list(tickers)].apply(
        lambda x: bundle.index.get_loc(x.first_valid_index()))
    per_asset = max([s.risk_model.min_history]
                    + [getattr(g, "asset_history", g.min_history) + g.lag + 1
                       for g in s.signals.values()])
    return int(max(first.max() + per_asset, s.min_history))


def run_one(spec: Spec):
    full = load()
    px = full.prices[[*spec.tickers, CASH]]
    bundle = DataBundle.build(px, **{k: full[k][px.columns] for k in ("vix", "market", "carry")})
    s = strategy(spec)
    res = WalkForwardBacktester(rebalance_every=1, warmup=warmup(bundle, s, spec.tickers)).run(
        bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    return spec.name, r, res.weights


def table(runs: dict[str, pd.Series], weights: dict[str, pd.DataFrame], start) -> pd.DataFrame:
    runs = {n: r.loc[start:] for n, r in runs.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[list(runs)]
    for n in runs:
        w = weights[n].loc[start:]
        us = [c for c in w.columns if c in [*US, "SPY"]]
        m.loc[n, "US eq wt"] = w[us].sum(axis=1).mean()
        m.loc[n, "ex-US eq wt"] = w[[c for c in ("EFA", "EEM") if c in w]].sum(axis=1).mean()
        m.loc[n, "bonds wt"] = w[["IEF", "TLT"]].sum(axis=1).mean()
    return m


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, SPECS)}
    runs = {n: v[0] for n, v in out.items()}
    weights = {n: v[1] for n, v in out.items()}
    fmt = {**{c: "{:.1%}".format for c in ("ret", "vol", "max_dd", "US eq wt", "ex-US eq wt",
                                           "bonds wt")},
           **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe")}}
    short = [s.name for s in SPECS if not s.name.startswith("3.")]
    long = [s.name for s in SPECS if s.name.startswith("3.")]
    s1 = max(runs[n].index[0] for n in short)
    print(f"TESTS 1-2, {s1.date()} .. {runs[short[0]].index[-1].date()}, split {SPLIT.date()}")
    print(table({n: runs[n] for n in short}, weights, s1).to_string(formatters=fmt))
    s3 = max(runs[n].index[0] for n in long)
    end = runs[long[0]].index[-1].date()
    print(f"\nTEST 3, {s3.date()} .. {end} (train/test split {SPLIT.date()})")
    print(table({n: runs[n] for n in long}, weights, s3).to_string(formatters=fmt))
    rows = {}
    for n in long + short:
        r = runs[n]
        row = {}
        for a, b in PERIODS:
            x = r.loc[max(pd.Timestamp(a), s3 if n in long else s1):b]
            row[f"{a}-{b[2:]} Sharpe"] = (x.mean() / x.std() * np.sqrt(TRADING_DAYS)
                                          if len(x) > 120 else np.nan)
            row[f"{a}-{b[2:]} return"] = ((1 + x).prod() ** (TRADING_DAYS / len(x)) - 1
                                          if len(x) > 120 else np.nan)
        rows[n] = row
    print("\n   by period (annualized)")
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.2f}".format))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar years")
    print(yr[long + short].loc[2004:].to_string(float_format="{:+.1%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
