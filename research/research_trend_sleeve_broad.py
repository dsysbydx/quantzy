"""More ETFs as trend-only exposure: inside the core book at a zero prior, or as its own sleeve.

The fixed-mix test showed that more ETFs add little to the core book: its alphas are small
next to its priors and its no-trade band. A zero-prior asset can only enter through trend,
which is worth at most about 1-3% a year of expected return for a 20%-vol ETF, below the
band's ~2.4% hurdle. A separate trend strategy avoids competing with the core for its
leverage, risk and covariance. **Fixed before any test.**

1. *In the core book*: the 65 ETFs of ``research_broad_universe.py``; the 17 keep their saved
   priors, carry and VRP, the 48 extras get a zero prior; trend and EWMAC on all 65.
   Saved caps, costs and settings.
2. *A separate long-only trend sleeve*, built by rule, not by the optimizer:

   * signal ``s = (EMA-200 trend score + EWMAC 64/256 forecast / 2) / 2``, in (-1, 1); held
     only when ``s > 0``;
   * weight ``s / vol``, with an equal risk budget per asset class (nine classes), split
     equally across the class's ETFs;
   * scaled to 10% vol on the sleeve's trailing 60-day vol, gross capped at 1.5x, the rest
     in T-bills (borrowing at T-bill + 0.5%);
   * rebalanced weekly to target, per-ETF costs as in ``research_broad_universe.py``.

   On the 65, on the 17 only, and on the 48 extras only. Then combined with the core book at
   10%, 20% and 30% of risk, each side scaled by its trailing one-year vol (causal).

**Result (2026-10-17): negative.** Inside the core, extras at a zero prior barely enter
(2.7% of capital, 0.3 held above 0.5% on average, mostly FXF, SHY, FXY as hedges): Sharpe
1.19 against 1.17. The separate sleeve made 0.44 on the 65 (test 0.26), 0.69 on the 17 and
0.33 on the 48 extras, at 0.60-0.73 monthly correlation with the core (both are long risk
assets). Combined, the 65-ETF sleeve lowers Sharpe at every share (1.17 -> 1.01 at 30% of
risk); the 17-ETF sleeve leaves it flat to lower (1.17 -> 1.12). Equal class risk put the
sleeve 46% in rates and 33% in credit, where trend was weak. Breadth in long-only ETFs does
not pay with these alphas.

Usage:
    uv run python research/research_trend_sleeve_broad.py [--workers 2]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd
import research_broad_universe as broad

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.research import compare
from quantzy.signals import CarrySharpePrior, Identity, VarianceRiskPremium
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
EXTRAS = [t for t in broad.ALL if t not in core.RISKY]
CLASS_OF = {t: c for c, ts in broad.CLASSES.items() for t in ts}
TARGET_VOL, MAX_GROSS, REBAL = 0.10, 1.5, 5


def bundle_for(tickers: list[str]) -> DataBundle:
    full = broad.load()
    px = full.prices[[*tickers, CASH]]
    return DataBundle.build(px, **{k: full[k][px.columns] for k in ("vix", "market", "carry")})


# --- 1. zero priors inside the core book --------------------------------------------------------

def core_run(args):
    name, costs, start = args
    bundle = bundle_for(broad.ALL if name.startswith("65") else list(core.RISKY))
    if name.startswith("65"):
        s = broad.strategy(broad.ALL, False, costs)
        prior = {t: (core.PRIOR_SR[t] if t in core.PRIOR_SR else 0.0) for t in broad.ALL}
        s.signals["prior"] = CarrySharpePrior(prior, carry_vol=core.CARRY_VOL, carry_weight=0.5,
                                              normalizer=Identity())
        s.signals["vrp"] = VarianceRiskPremium(tuple(core.EQUITY), normalizer=Identity())
        s.optimizer = replace(core.PORTFOLIO, execution_cost_bps={t: costs[t] for t in broad.ALL}
                              ).optimizer(CASH)  # the saved 20% cap, not the broad test's 10%
    else:
        s = core.build()
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    return name, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


# --- 2. the sleeve -----------------------------------------------------------------------------

def sleeve(tickers: list[str], costs: dict[str, float], start: pd.Timestamp):
    b = bundle_for(tickers)
    px = b.prices[tickers]
    vol = realized_vol(px, 60, annualize=True)
    trend = core.ALPHAS["trend"][0].raw(b)[tickers] / vol
    ewmac = core.ALPHAS["ewmac"][0].raw(b)[tickers] / vol
    s = ((trend + ewmac / 2) / 2).clip(lower=0).shift(1)  # known at the prior close
    n_class = pd.Series({t: sum(CLASS_OF[x] == CLASS_OF[t] for x in tickers) for t in tickers})
    n_classes = len({CLASS_OF[t] for t in tickers})
    raw = s / vol.shift(1) / n_class / n_classes
    rets = px.pct_change(fill_method=None).fillna(0.0)
    unscaled = (raw.shift(1) * rets).sum(axis=1)  # pseudo-returns of the raw weights
    svol = unscaled.rolling(60, min_periods=60).std().shift(1) * np.sqrt(TRADING_DAYS)
    scale = (TARGET_VOL / svol).replace([np.inf, -np.inf], np.nan)
    target = raw.mul(scale, axis=0)
    gross = target.sum(axis=1)
    target = target.div(np.maximum(gross / MAX_GROSS, 1.0), axis=0).fillna(0.0)
    target = target.loc[start:]
    held = target.iloc[::REBAL].reindex(target.index).ffill()  # weekly rebalance
    rr = rets.loc[start:]
    rf = b.prices[CASH].pct_change().loc[start:].fillna(0)
    w_prev = held.shift(1).fillna(0.0)
    cash = 1 - w_prev.sum(axis=1)
    gross_ret = (w_prev * rr).sum(axis=1) + cash * rf
    borrow = (-cash).clip(lower=0) * core.PORTFOLIO.borrow_spread_bps / 1e4 / TRADING_DAYS
    trades = held.diff().abs().fillna(held.abs())
    cost = (trades * pd.Series(costs)[tickers] / 1e4).sum(axis=1)
    r = (gross_ret - borrow - cost).iloc[1:]
    return r, held, trades.sum(axis=1).sum() / 2 / (len(r) / TRADING_DAYS), cost


def stats(runs: dict[str, pd.Series]) -> pd.DataFrame:
    t = compare(runs, split_date=SPLIT)
    return pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[list(runs)]


FMT = {**{c: "{:.1%}".format for c in ("ret", "vol", "max_dd", "extras wt", "cost/yr")},
       **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "gross",
                                       "corr with core (daily)", "corr with core (monthly)")},
       "turnover": "{:.1f}".format, "extras held >0.5%": "{:.1f}".format}


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    costs = {**broad.cost_tiers(), **core.EXECUTION_COST_BPS}
    b = bundle_for(broad.ALL)
    start = broad.warmup(b, broad.strategy(broad.ALL, False, costs), broad.ALL)
    s0 = b.index[start]
    print(f"common start {s0.date()}")
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(core_run, [("17 (saved)", costs, start),
                                                           ("65, extras at zero prior", costs,
                                                            start)])}
    runs = {n: v[0].loc[s0:] for n, v in out.items()}
    m = stats(runs)
    m["turnover"] = [out[n][2] for n in runs]
    w65 = out["65, extras at zero prior"][1].loc[s0:]
    m["extras wt"] = [0.0, w65[EXTRAS].sum(axis=1).mean()]
    m["extras held >0.5%"] = [0.0, (w65[EXTRAS] > 0.005).sum(axis=1).mean()]
    print(f"\n1. CORE BOOK, {s0.date()} .. {runs['17 (saved)'].index[-1].date()}")
    print(m.to_string(formatters=FMT))
    top = w65[EXTRAS].mean().sort_values(ascending=False).head(8)
    print("   most-held extras (average % of capital): "
          + ", ".join(f"{t} {v:.1%}" for t, v in top.items()))

    core_r = runs["17 (saved)"]
    sl = {}
    for name, tick in (("sleeve, 65 ETFs", broad.ALL), ("sleeve, 17 ETFs", list(core.RISKY)),
                       ("sleeve, 48 extras", EXTRAS)):
        r, held, to, cost = sleeve(tick, costs, s0)
        sl[name] = (r.loc[s0:], held, to, cost)
    sm = stats({n: v[0] for n, v in sl.items()})
    sm["turnover"] = [v[2] for v in sl.values()]
    sm["cost/yr"] = [v[3].sum() / (len(v[0]) / TRADING_DAYS) for v in sl.values()]
    sm["gross"] = [v[1].loc[s0:].sum(axis=1).mean() for v in sl.values()]
    sm["corr with core (daily)"] = [v[0].corr(core_r) for v in sl.values()]
    mon = lambda x: (1 + x).resample("ME").prod() - 1  # noqa: E731
    sm["corr with core (monthly)"] = [mon(v[0]).corr(mon(core_r)) for v in sl.values()]
    print("\n2. LONG-ONLY TREND SLEEVE (rule-based, 10% vol target, weekly)")
    print(sm.to_string(formatters=FMT))

    held65 = sl["sleeve, 65 ETFs"][1]
    rets = b.prices[broad.ALL].pct_change(fill_method=None).fillna(0).loc[s0:]
    contrib = (held65.shift(1).fillna(0) * rets).T.groupby(CLASS_OF).sum().T
    print("   65-ETF sleeve by class: average weight / P&L a year")
    print("   " + ", ".join(f"{c} {held65.T.groupby(CLASS_OF).sum().T[c].mean():.0%} / "
                            f"{contrib[c].mean() * TRADING_DAYS:+.1%}" for c in broad.CLASSES))

    combos = {}
    for name in ("sleeve, 65 ETFs", "sleeve, 17 ETFs"):
        rs = sl[name][0]
        z = pd.concat([core_r.rename("c"), rs.rename("s")], axis=1).dropna()
        vc = z["c"].rolling(252, min_periods=126).std().shift(1)
        vs = z["s"].rolling(252, min_periods=126).std().shift(1)
        for x in (0.0, 0.1, 0.2, 0.3):
            comb = ((1 - x) * z["c"] * (0.10 / np.sqrt(TRADING_DAYS) / vc)
                    + x * z["s"] * (0.10 / np.sqrt(TRADING_DAYS) / vs)).dropna()
            combos[f"core + {name}, {x:.0%} of risk"] = comb
    cm = stats(combos)
    print("\n   COMBINED with the core book (both scaled by trailing 1-year vol; Sharpe is "
          "scale-free)")
    print(cm.to_string(formatters=FMT))
    yr = pd.DataFrame({n: (1 + v[0]).groupby(v[0].index.year).prod() - 1 for n, v in sl.items()})
    yr["core (saved 17)"] = (1 + core_r).groupby(core_r.index.year).prod() - 1
    print("\n   calendar years")
    print(yr.to_string(float_format="{:+.1%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2)
    main(ap.parse_args().workers)
