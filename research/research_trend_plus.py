"""The 17-ETF book plus a few diversifiers with a long-run premium.

The 45-ETF universe made the trend IC easier to detect but a worse book: equal priors across
equity groups loaded emerging-market countries. This instead adds only assets that (a) earn a
premium for a reason, so they deserve a positive prior, and (b) are not already in the book.
Correlations were checked on the train window (2008-05 .. 2017-07) before any backtest:

    EMB  EM sovereign USD bonds   corr with book +0.31, US equity +0.26, IEF +0.03
    LQD  US investment-grade      corr with book +0.32, US equity +0.12, IEF +0.42

Rejected on the same screen: HYG and VNQ (equity-like, correlation with US equity 0.65 and
0.79), TIP and SHY (mostly duration, already held through IEF/TLT), DBB and SLV (no long-run
premium after roll costs), FXY and FXE (no premium; FXY is tested once as a pure hedge with
a zero prior).

Priors were fixed before the backtest: credit 0.25, the same as Treasuries. Everything else
is the selected book, with the shared portfolio settings (``PORTFOLIO``).

Usage:
    uv run python research/research_trend_plus.py [--workers 8]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import research_trend_breadth as rb
import research_trend_global as g

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare
from quantzy.risk import ewma_blend
from quantzy.signals import Identity, SharpePrior, VolScaledTrend
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

CASH = rb.CASH
ADDED_PRIOR = {"EMB": 0.25, "LQD": 0.25, "FXY": 0.0}
VARIANTS = {
    "17 ETF": [],
    "+ EMB": ["EMB"],
    "+ LQD": ["LQD"],
    "+ EMB, LQD": ["EMB", "LQD"],
    "+ EMB, LQD, FXY (hedge)": ["EMB", "LQD", "FXY"],
}
START = "2009-03-04"  # EMB's history plus warmup; every variant is scored from here


def bundle_for(extra: list[str]) -> DataBundle:
    px = rb.load().prices  # the 17 ETFs and T-bills, on the 17-ETF book's dates
    if extra:
        px = px.join(g.load().prices[extra].reindex(px.index))
    return DataBundle.build(px[[*rb.RISKY, *extra, CASH]])


def build(extra: list[str], k: float) -> Strategy:
    tickers = [*rb.RISKY, *extra]
    prior = {**rb.PRIORS["by class"], **{t: ADDED_PRIOR[t] for t in extra}}
    p = rb.PORTFOLIO
    ma, w = rb.ALPHA["spec"]
    signals = {"prior": SharpePrior(prior, normalizer=Identity())}
    if k:
        signals["trend"] = VolScaledTrend(w, exclude=(CASH,), ma=ma, ewm_tol=rb.EWM_TOL,
                                       normalizer=Identity())
    return Strategy(
        universe=Universe(name="trend_plus", assets=[Asset(t, t, "etf") for t in tickers]
                          + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")]),
        signals=signals,
        weights={"trend": k} if k else {},
        risk_model=ewma_blend(p["hls"]),
        optimizer=MeanVariance(
            risk_aversion=p["ra"],
            constraints=[C.budget(), C.long_only(exclude=(CASH,)),
                         C.max_leverage(p["lev"], CASH),
                         C.max_book_share(p["cap"], exclude=(CASH,))],
            cost_model=LinearCost(rb.COST_BPS),
            cost_aversion=p["ca"],
        ),
    )


def run_one(args: tuple[str, float]) -> tuple[str, pd.Series, pd.DataFrame, float]:
    name, k = args
    extra = VARIANTS[name]
    bundle = bundle_for(extra)
    warm = bundle.index.get_loc(pd.Timestamp(START))
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(bundle, build(extra, k))
    borrowed = (-res.weights[CASH]).clip(lower=0)
    r = res.returns - borrowed * rb.BORROW_SPREAD_BPS / 1e4 / TRADING_DAYS
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return f"{name} | k={k:g}", r, res.weights, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    jobs = [(n, k) for n in VARIANTS for k in (0.0, rb.ALPHA["k"])]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, jobs)}
    runs = {n: v[0] for n, v in out.items()}
    split = pd.Timestamp("2017-12-19")  # the midpoint, as in research_trend_global.py
    t = compare(runs, split_date=split)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    })
    m["turnover"] = [out[n][2] for n in m.index]
    m["added wt"] = [out[n][1][VARIANTS[n.split(" | ")[0]]].sum(axis=1).mean()
                     if VARIANTS[n.split(" | ")[0]] else 0.0 for n in m.index]
    m["Treasuries"] = [out[n][1][["IEF", "TLT"]].sum(axis=1).mean() for n in m.index]
    pct = "{:.1%}".format
    print(f"\nBOOK + DIVERSIFIERS, {START} .. {runs[next(iter(runs))].index[-1].date()}, split "
          f"{split.date()} | settings {rb.PORTFOLIO} | added priors {ADDED_PRIOR}")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "added wt",
                                                   "Treasuries")}
                      | {c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe")}
                      | {"turnover": "{:.1f}".format}))
    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()
                       if n.endswith(f"k={rb.ALPHA['k']:g}")})
    print("\n   calendar years, with trend")
    print(yr.to_string(float_format="{:+.1%}".format))
    for n in ("+ EMB, LQD | k=0.1",):
        w = out[n][1]
        print(f"\n   {n}: weight on EMB and LQD over time (% of capital)")
        print(w[["EMB", "LQD"]].describe().loc[["mean", "min", "50%", "max"]].to_string(
            float_format="{:.0%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    main(ap.parse_args().workers)
