"""QQQ timing with an honest risk model: any trend view goes in ``mu``, not in the volatility.

``research_qqq_volmodel.py`` showed that scaling QQQ's volatility up in a downtrend makes the
volatility *forecast* worse: a plain EWMA is already unbiased on downtrend days, and the
scaled one over-forecasts. The drawdown it saved came from holding less QQQ, which is a view,
not a risk estimate. A risk model shared across strategies should only be judged on how well
it forecasts, so here it is fixed to the best forecaster, ``ewma_blend((5, 20, 60))``, and
the trend gets its own place in the expected return:

- ``sym``:  ``mu = premium + k * trend``. Trend tilts the return view both ways.
- ``down``: ``mu = premium + k * min(trend, 0)``. Only a downtrend counts, the shape of the
  old volatility overlay, where an uptrend changed nothing.

with ``trend = tanh(log(P / MA) / monthly vol)`` from :class:`~quantzy.signals.TrendStrength`.

First a time-series IC check on whether trend predicts anything at all, then two tuning
stages chosen on the train window (before 2013-07-31) and reported on the test window:

- **Stage 1, the view** (risk aversion and cost aversion fixed): shape, MA type, window, k.
- **Stage 2, sizing and trading** around the stage-1 winner: risk aversion, cost aversion.

Usage:
    uv run python examples/research_qqq_mu.py [--workers 8]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd
from research_qqq_optimizer import Premium, trade_days
from research_qqq_timing import CASH, RISKY, UNIVERSE, load, warmup_rows

from quantzy.backtest import FixedWeights, WalkForwardBacktester
from quantzy.data import DataBundle
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare, realized_returns
from quantzy.risk import ewma_blend
from quantzy.signals import Identity, TrendStrength
from quantzy.strategy import Strategy

SPLIT = pd.Timestamp("2013-07-31")
PREMIUM, COST_BPS, MIN_EXPOSURE = 0.05, 5.0, 0.5
HALFLIVES = (5, 20, 60)
BASE = {"shape": "none", "ma": "ema", "window": 200, "k": 0.0, "risk_aversion": 0.5,
        "cost_aversion": 12.0}
STAGE1 = {"shape": ["sym", "down"], "ma": ["sma", "ema"], "window": [100, 150, 200, 250],
          "k": [0.025, 0.05, 0.1, 0.15]}
STAGE2 = {"risk_aversion": [0.35, 0.5, 0.7], "cost_aversion": [4.0, 12.0, 24.0]}
IC_HORIZONS = (5, 21, 63)


@dataclass
class DownTrend(TrendStrength):
    """:class:`TrendStrength` with the positive side cut off: ``min(trend, 0)``."""

    name: ClassVar[str] = "down_trend"

    def raw(self, data: DataBundle) -> pd.DataFrame:
        return super().raw(data).clip(upper=0.0)


def key(p: dict) -> str:
    v = "no view"
    if p["shape"] != "none":
        v = f"{p['shape']} {p['ma']}{p['window']} k={p['k']:g}"
    return f"{v} | ra={p['risk_aversion']:g} ca={p['cost_aversion']:g}"


def view(p: dict) -> TrendStrength:
    cls = DownTrend if p["shape"] == "down" else TrendStrength
    return cls(p["window"], exclude=(CASH,), ma=p["ma"], normalizer=Identity())


def build(p: dict) -> Strategy:
    signals = {"premium": Premium(PREMIUM, normalizer=Identity())}
    weights = {}
    if p["shape"] != "none":
        signals["trend"] = view(p)
        weights["trend"] = p["k"]
    return Strategy(
        universe=UNIVERSE,
        signals=signals,
        weights=weights,
        risk_model=ewma_blend(HALFLIVES),
        optimizer=MeanVariance(
            risk_aversion=p["risk_aversion"],
            constraints=[C.budget(), C.long_only()],
            cost_model=LinearCost(COST_BPS),
            cost_aversion=p["cost_aversion"],
        ),
    )


def run_one(p: dict) -> tuple[str, dict, pd.Series, float, float]:
    bundle = load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=warmup_rows(bundle)).run(bundle,
                                                                                   build(p))
    return key(p), p, res.returns, float(res.weights[RISKY].mean()), trade_days(res)


def run_all(params: list[dict], workers: int) -> dict[str, tuple]:
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return {k: (p, r, e, t) for k, p, r, e, t in pool.map(run_one, params)}


# --- IC ----------------------------------------------------------------------------------------

def ic_table(bundle: DataBundle) -> pd.DataFrame:
    """Time-series IC of the trend view against QQQ's forward return, train and test.

    One asset, so the IC is a correlation over time, not across names. Overlapping forward
    windows inflate the t-stat, so it is scaled by sqrt(horizon) as a rough correction.
    """
    rets = realized_returns(bundle, RISKY)
    start = bundle.index[warmup_rows(bundle)]
    rows = {}
    for shape, ma, window in itertools.product(("sym", "down"), ("sma", "ema"), (100, 200)):
        s = view({"shape": shape, "ma": ma, "window": window}).scores(bundle)[RISKY]
        for h in IC_HORIZONS:
            fwd = np.log1p(rets).rolling(h).sum().shift(-h)  # return over the next h days
            for period, sl in (("train", slice(start, SPLIT)), ("test", slice(SPLIT, None))):
                x = pd.concat([s, fwd], axis=1).loc[sl].dropna()
                n = len(x)
                for method in ("spearman", "pearson"):
                    ic = x.iloc[:, 0].corr(x.iloc[:, 1], method=method)
                    t = ic * np.sqrt(n / h)
                    rows[(f"{shape} {ma}{window}", h, period, method)] = (ic, t)
    out = pd.Series({k: f"{ic:+.3f} ({t:+.1f})" for k, (ic, t) in rows.items()})
    out.index.names = ["view", "h", "period", "method"]
    return out.unstack(["period", "method"])


# --- report ------------------------------------------------------------------------------------

def metrics(results: dict[str, tuple], bench: dict[str, pd.Series]) -> pd.DataFrame:
    runs = {k: v[1] for k, v in results.items()} | bench
    t = compare(runs, split_date=SPLIT)
    out = pd.DataFrame({
        "tr_ret": t["train"]["ann_return"], "tr_vol": t["train"]["ann_vol"],
        "tr_sharpe": t["train"]["sharpe"], "tr_dd": t["train"]["max_dd"],
        "te_ret": t["test"]["ann_return"], "te_vol": t["test"]["ann_vol"],
        "te_sharpe": t["test"]["sharpe"], "te_dd": t["test"]["max_dd"],
        "full_ret": t["full"]["ann_return"], "full_sharpe": t["full"]["sharpe"],
        "full_dd": t["full"]["max_dd"],
    })
    out["avg_qqq"] = [results[k][2] if k in results else np.nan for k in out.index]
    out["trade_days"] = [results[k][3] if k in results else np.nan for k in out.index]
    return out


FMT = {c: "{:.1%}".format for c in ("tr_ret", "tr_vol", "tr_dd", "te_ret", "te_vol", "te_dd",
                                    "full_ret", "full_dd", "avg_qqq")}
FMT |= {c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "full_sharpe")}
FMT["trade_days"] = "{:.0f}".format


def pivot(m: pd.DataFrame, results: dict, col: str, rows: str, cols: str,
          fixed: dict) -> pd.DataFrame:
    data = {(p[rows], p[cols]): m.loc[k, col] for k, (p, *_) in results.items()
            if p["shape"] != "none" and all(p[f] == v for f, v in fixed.items())}
    s = pd.Series(data)
    s.index.names = [rows, cols]
    return s.unstack(cols)


def pick(m: pd.DataFrame, keys: list[str]) -> str:
    """Best train Sharpe among runs that actually hold QQQ most of the time."""
    ok = m.loc[keys]
    ok = ok[ok["avg_qqq"] >= MIN_EXPOSURE]
    return ok["tr_sharpe"].idxmax()


def main(workers: int) -> None:
    bundle = load()
    warm = warmup_rows(bundle)
    engine = WalkForwardBacktester(rebalance_every=1, warmup=warm)
    cost = LinearCost(COST_BPS)
    bench = {
        "buy & hold QQQ": engine.run(bundle, FixedWeights(UNIVERSE, weights={RISKY: 1.0},
                                                          cost_model=cost)).returns,
        "80/20 6m": WalkForwardBacktester(rebalance_every=126, warmup=warm).run(
            bundle, FixedWeights(UNIVERSE, weights={RISKY: 0.8, CASH: 0.2},
                                 cost_model=cost)).returns,
    }
    pd.set_option("display.width", 250)
    print(f"\nTrading {bundle.index[warm].date()} .. {bundle.index[-1].date()} | risk model "
          f"ewma_blend{HALFLIVES} | train before {SPLIT.date()} (chooses) | premium "
          f"{PREMIUM:.0%}, cost {COST_BPS:g}bp per leg")

    print("\n0. TIME-SERIES IC of the view vs QQQ's forward log return: ic (t, overlap-adjusted)")
    print(ic_table(bundle).to_string())

    # --- stage 1: the view ---
    fixed = {k: BASE[k] for k in ("risk_aversion", "cost_aversion")}
    s1 = [BASE] + [{**fixed, "shape": sh, "ma": ma, "window": w, "k": k}
                   for sh, ma, w, k in itertools.product(*STAGE1.values())]
    results = run_all(s1, workers)
    m1 = metrics(results, bench)
    print(f"\n1. STAGE 1: the view, with ra={fixed['risk_aversion']} "
          f"ca={fixed['cost_aversion']:g}")
    for sh, ma in itertools.product(STAGE1["shape"], STAGE1["ma"]):
        tabs = {col: pivot(m1, results, col, "window", "k", {"shape": sh, "ma": ma})
                for col in ("tr_sharpe", "te_sharpe", "tr_dd", "avg_qqq")}
        print(f"\n  {sh} {ma.upper()}  (rows window, cols k)")
        print(pd.concat({"train Sharpe": tabs["tr_sharpe"], "test Sharpe": tabs["te_sharpe"]},
                        axis=1).to_string(float_format="{:.2f}".format))
        print(pd.concat({"train DD": tabs["tr_dd"], "avg QQQ": tabs["avg_qqq"]},
                        axis=1).to_string(float_format="{:.1%}".format))
    nv = key(BASE)
    print(f"\n  no view: tr_sharpe {m1.loc[nv, 'tr_sharpe']:.2f}  te_sharpe "
          f"{m1.loc[nv, 'te_sharpe']:.2f}  tr_dd {m1.loc[nv, 'tr_dd']:.1%}  "
          f"full_dd {m1.loc[nv, 'full_dd']:.1%}")
    win1 = pick(m1, list(results))
    best1 = results[win1][0]
    print(f"\n  Stage-1 winner on train Sharpe (avg QQQ >= {MIN_EXPOSURE:.0%}): {win1}")

    # --- stage 2: sizing and trading ---
    v = {k: best1[k] for k in ("shape", "ma", "window", "k")}
    s2 = [{**v, "risk_aversion": ra, "cost_aversion": ca}
          for ra, ca in itertools.product(*STAGE2.values())]
    s2 += [{**BASE, "risk_aversion": ra, "cost_aversion": ca}
           for ra, ca in itertools.product(*STAGE2.values())]
    results |= run_all([p for p in s2 if key(p) not in results], workers)
    m = metrics(results, bench)
    print(f"\n2. STAGE 2: sizing and trading, with view {v} (and no view, for reference)")
    for label, fixed_v in (("view", v), ("no view", {"shape": "none"})):
        sel = {k: r for k, r in results.items()
               if all(r[0][f] == x for f, x in fixed_v.items())}
        data = {(r[0]["risk_aversion"], r[0]["cost_aversion"]): m.loc[k, ["tr_sharpe",
                "te_sharpe", "full_dd"]] for k, r in sel.items()}
        tab = pd.DataFrame(data).T.sort_index()
        tab.index.names = ["ra", "ca"]
        print(f"\n  {label}")
        print(tab.to_string(formatters={"tr_sharpe": "{:.2f}".format,
                                        "te_sharpe": "{:.2f}".format,
                                        "full_dd": "{:.1%}".format}))
    win = pick(m, [key(p) for p in s2 if p["shape"] != "none"])
    print(f"\n  Final pick on train Sharpe: {win}")

    print("\n3. TOP 10 BY TRAIN SHARPE (avg QQQ >= 50%), with baselines:")
    cand = m.drop(index=list(bench))
    top = cand[cand["avg_qqq"] >= MIN_EXPOSURE].sort_values("tr_sharpe", ascending=False)
    base_keys = [key({**BASE, "risk_aversion": ra}) for ra in STAGE2["risk_aversion"]]
    show = pd.concat([top.head(10), m.loc[[win, *base_keys, *bench]]])
    show = show[~show.index.duplicated()]
    show.index = [("* " if k == win else "  ") + k for k in show.index]
    print(show.to_string(formatters=FMT))
    print(f"\n{len(results)} configurations run. Trust a pick on a plateau that also holds up "
          "in the test window.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    main(ap.parse_args().workers)
