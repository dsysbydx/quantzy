"""Tune the QQQ risk overlay in two stages, choosing on 2000-13 and judging on 2013-26.

The model, from ``research_qqq_volmodel.py``: ``mu`` is a flat premium, and the risk model is
an EWMA volatility whose QQQ vol is multiplied by ``1 + downside * max(-trend, 0)``. The
optimizer then holds ``premium / (2 * risk_aversion * vol^2)`` of QQQ, capped at 100%, and
trades only when the improvement beats ``cost_aversion`` times the cost.

Tuning everything at once would mean hundreds of daily runs and a high chance of fitting
noise. Instead:

- **Stage 1, the trend** (vol halflife, risk aversion and cost aversion fixed): moving-average
  type, window and downside scale.
- **Stage 2, sizing and trading**, around the stage-1 winner: vol halflife, risk aversion and
  cost aversion. Premium and risk aversion only matter as a ratio. It sets the vol at which
  the book stops holding 100% QQQ, ``sqrt(premium / (2 * risk_aversion))``, so only risk
  aversion is varied.

Every choice is made on the train window (Sharpe before 2013-07-31). The test window is
reported, never used to choose.

Usage:
    uv run python examples/research_qqq_tune.py [--workers 8]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from research_qqq_optimizer import Premium, trade_days
from research_qqq_timing import CASH, RISKY, UNIVERSE, load, warmup_rows

from quantzy.backtest import FixedWeights, WalkForwardBacktester
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare
from quantzy.risk import RiskModel, SampleCovariance, StateScaledRisk
from quantzy.signals import Identity, TrendStrength
from quantzy.strategy import Strategy

SPLIT = pd.Timestamp("2013-07-31")
PREMIUM, COST_BPS = 0.05, 5.0
CURRENT = {"ma": "sma", "window": 200, "downside": 1.0, "hl": 10, "risk_aversion": 0.5,
           "cost_aversion": 12.0}
STAGE1 = {"ma": ["sma", "ema"], "window": [100, 150, 200, 250],
          "downside": [0.5, 1.0, 1.5, 2.0, 3.0]}
STAGE2 = {"hl": [5, 10, 20], "risk_aversion": [0.35, 0.5, 0.7], "cost_aversion": [4.0, 12.0, 24.0]}


def key(p: dict) -> str:
    if p["downside"] == 0:
        trend = "no trend"
    else:
        trend = f"{p['ma']}{p['window']} d={p['downside']:g}"
    return (f"{trend} | hl={p['hl']} ra={p['risk_aversion']:g} ca={p['cost_aversion']:g}")


def full_size_vol(risk_aversion: float) -> float:
    return float(np.sqrt(PREMIUM / (2 * risk_aversion)))


def build(p: dict) -> Strategy:
    risk: RiskModel = SampleCovariance(halflife=p["hl"], lookback=250)
    if p["downside"] > 0:
        state = TrendStrength(p["window"], exclude=(CASH,), ma=p["ma"], normalizer=Identity())
        risk = StateScaledRisk(risk, state, downside=p["downside"], assets=(RISKY,))
    return Strategy(
        universe=UNIVERSE,
        signals={"premium": Premium(PREMIUM, normalizer=Identity())},
        risk_model=risk,
        optimizer=MeanVariance(
            risk_aversion=p["risk_aversion"],
            constraints=[C.budget(), C.long_only()],
            cost_model=LinearCost(COST_BPS),
            cost_aversion=p["cost_aversion"],
        ),
    )


def run_one(p: dict) -> tuple[str, dict, pd.Series, float, float]:
    """One daily backtest. Returns only what the report needs, to keep pickling light."""
    bundle = load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=warmup_rows(bundle)).run(bundle,
                                                                                   build(p))
    return key(p), p, res.returns, float(res.weights[RISKY].mean()), trade_days(res)


def run_all(params: list[dict], workers: int) -> dict[str, tuple]:
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return {k: (p, r, e, t) for k, p, r, e, t in pool.map(run_one, params)}


def metrics(results: dict[str, tuple], bench: pd.Series) -> pd.DataFrame:
    runs = {k: v[1] for k, v in results.items()} | {"buy & hold QQQ": bench}
    t = compare(runs, split_date=SPLIT)
    out = pd.DataFrame({
        "tr_ret": t["train"]["ann_return"], "tr_vol": t["train"]["ann_vol"],
        "tr_sharpe": t["train"]["sharpe"], "tr_dd": t["train"]["max_dd"],
        "te_ret": t["test"]["ann_return"], "te_vol": t["test"]["ann_vol"],
        "te_sharpe": t["test"]["sharpe"], "te_dd": t["test"]["max_dd"],
        "full_sharpe": t["full"]["sharpe"], "full_dd": t["full"]["max_dd"],
    })
    out["avg_qqq"] = [results[k][2] if k in results else 1.0 for k in out.index]
    out["trade_days"] = [results[k][3] if k in results else 0.0 for k in out.index]
    return out


FMT = {c: "{:.1%}".format for c in ("tr_ret", "tr_vol", "tr_dd", "te_ret", "te_vol", "te_dd",
                                    "full_dd", "avg_qqq")}
FMT |= {c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "full_sharpe")}
FMT["trade_days"] = "{:.0f}".format


def pivot(m: pd.DataFrame, results: dict, col: str, rows: str, cols: str,
          fixed: dict) -> pd.DataFrame:
    sel = {k: results[k][0] for k in m.index if k in results}
    data = {}
    for k, p in sel.items():
        if p["downside"] > 0 and all(p[f] == v for f, v in fixed.items()):
            data[(p[rows], p[cols])] = m.loc[k, col]
    s = pd.Series(data)
    s.index.names = [rows, cols]
    return s.unstack(cols)


def main(workers: int) -> None:
    bundle = load()
    warm = warmup_rows(bundle)
    bench = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(
        bundle, FixedWeights(UNIVERSE, weights={RISKY: 1.0}, cost_model=LinearCost(COST_BPS))
    ).returns
    pd.set_option("display.width", 250)
    print(f"\nTrading {bundle.index[warm].date()} .. {bundle.index[-1].date()} | train before "
          f"{SPLIT.date()} (chooses) | test after (reports) | premium {PREMIUM:.0%}, "
          f"cost {COST_BPS:g}bp per leg")

    # --- stage 1: trend ---
    base = {k: CURRENT[k] for k in ("hl", "risk_aversion", "cost_aversion")}
    s1 = [{**base, "ma": "sma", "window": 200, "downside": 0.0}]
    s1 += [{**base, "ma": ma, "window": w, "downside": d}
           for ma, w, d in itertools.product(*STAGE1.values())]
    results = run_all(s1, workers)
    m1 = metrics(results, bench)
    print(f"\nSTAGE 1: trend, with hl={base['hl']} ra={base['risk_aversion']} "
          f"ca={base['cost_aversion']:g}")
    for ma in STAGE1["ma"]:
        for col in ("tr_sharpe", "te_sharpe", "tr_dd"):
            fmt = "{:.2f}" if "sharpe" in col else "{:.1%}"
            tab = pivot(m1, results, col, "window", "downside", {"ma": ma, **base})
            print(f"\n  {ma.upper()}  {col}  (rows window, cols downside)")
            print(tab.to_string(float_format=fmt.format))
    nt = key(s1[0])
    print(f"\n  no trend: tr_sharpe {m1.loc[nt, 'tr_sharpe']:.2f}  te_sharpe "
          f"{m1.loc[nt, 'te_sharpe']:.2f}  tr_dd {m1.loc[nt, 'tr_dd']:.1%}")
    win1 = m1.loc[list(results), "tr_sharpe"].idxmax()
    best1 = results[win1][0]
    print(f"\n  Stage-1 winner on train Sharpe: {win1}")

    # --- stage 2: sizing and trading around the winner ---
    trend = {k: best1[k] for k in ("ma", "window", "downside")}
    s2 = [{**trend, "hl": hl, "risk_aversion": ra, "cost_aversion": ca}
          for hl, ra, ca in itertools.product(*STAGE2.values())]
    todo = [p for p in s2 if key(p) not in results]
    if key(CURRENT) not in results:
        todo.append(CURRENT)
    results |= run_all(todo, workers)
    m = metrics(results, bench)
    print(f"\nSTAGE 2: sizing and trading, with trend {trend}")
    print("  (full-size vol: ra 0.35 -> {:.0%}, 0.5 -> {:.0%}, 0.7 -> {:.0%})".format(
        *(full_size_vol(r) for r in STAGE2["risk_aversion"])))
    for ca in STAGE2["cost_aversion"]:
        tab = pivot(m, results, "tr_sharpe", "hl", "risk_aversion", {**trend, "cost_aversion": ca})
        te = pivot(m, results, "te_sharpe", "hl", "risk_aversion", {**trend, "cost_aversion": ca})
        print(f"\n  ca={ca:g}: train Sharpe (rows hl, cols ra) | test Sharpe")
        print(pd.concat({"train": tab, "test": te}, axis=1).to_string(float_format="{:.2f}".format))

    stage2 = [key(p) for p in s2]
    win = m.loc[stage2, "tr_sharpe"].idxmax()
    print(f"\n  Final pick on train Sharpe: {win}")

    print("\nTOP 10 OF ALL RUNS BY TRAIN SHARPE, with the current model and buy & hold:")
    top = m.drop(index="buy & hold QQQ").sort_values("tr_sharpe", ascending=False).head(10)
    show = pd.concat([top, m.loc[[key(CURRENT), win, "buy & hold QQQ"]]])
    show = show[~show.index.duplicated()]
    show.index = [("* " if k == win else "  ") + ("(current) " if k == key(CURRENT) else "") + k
                  for k in show.index]
    print(show.to_string(formatters=FMT))
    n = len(results)
    print(f"\n{n} configurations run. The more tried, the more the top train Sharpe is inflated "
          "by luck. Trust a pick on a plateau that also holds up in the test window.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    main(ap.parse_args().workers)
