"""Is the slow EWMAC(64, 256) a real edge over EMA-200, or one lucky setting?

``research_ewmac_accel.py`` found the best 1-day IC at the slowest crossover, 64/256 (t 3.2),
but mostly in fold A (t 3.6) against fold B (t 1.1). That was seen after the fact. This checks
whether it is a plateau or a spike, and whether it matters in the book:

1. IC of neighbouring speeds (32/128 .. 128/512) and of a plain slower trend (price against
   EMA-400), on one common set of cells so every rule is scored on the same dates and assets.
   Both fold directions, several chunk sizes, five-year periods, longer horizons, per class.
2. Backtests replacing the saved EMA-200 alpha with each, at the same trend budget (``k``
   matched to its train-window dispersion, as before), with ``PORTFOLIO`` unchanged.

A real edge should show up as a smooth plateau across neighbouring speeds, in both folds and
in most periods, not as one speed standing out.

**Result (2026-10-16): adopted at half the trend budget.** A plateau: 48/192 to 128/512 all
have IC about +0.017 (t 2.4-2.7) against +0.006 (t 0.9) for EMA-200 on the same cells, are
positive in every five-year period, and are stable across chunk sizes. The fold A/B gap is
date luck shared by every rule, not overfitting. In the book (from 2010) 64/256 gave Sharpe
1.14 against 1.13 and the half-and-half blend 1.15: noise, because trend is a small tilt and
the extra edge is mostly in capped, correlated US sectors. A 60/200 check gave the same (score
correlation 0.99 with 64/256). The cost of slower trend: Sep-Oct 2008 lost 2-3 points more.

Usage:
    uv run python examples/research_ewmac_slow.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from research_ewmac_accel import EWMAC, SPLIT

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity, VolScaledTrend
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

CHUNK, NW_LAGS, VOL_IC = 47, 21, 60
SPEEDS = (32, 48, 64, 96, 128)
SAVED = "EMA-200 (saved)"
FOCUS = "EWMAC 64/256"


def rules(**kw) -> dict[str, Signal]:
    trend = core.ALPHAS["trend"][0]
    out: dict[str, Signal] = {
        SAVED: trend if not kw else VolScaledTrend(200, ma="ema", ewm_tol=core.EWM_TOL, **kw),
        "EMA-400": VolScaledTrend(400, ma="ema", ewm_tol=core.EWM_TOL, **kw),
    }
    out.update({f"EWMAC {n}/{4 * n}": EWMAC(speeds=(n,), **kw) for n in SPEEDS})
    return out


def unit_scores(px: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Each rule's score before the times-vol, on the cells where every rule is live."""
    bundle = DataBundle.build(px)
    rows_live = px.notna().cumsum()
    raw = {}
    for name, sig in rules().items():
        vol = realized_vol(px, sig.vol_window, annualize=True)
        raw[name] = (sig.raw(bundle) / vol).where(rows_live > sig.min_history)
    common = np.logical_and.reduce([v.notna() for v in raw.values()])
    return {k: v.where(common) for k, v in raw.items()}


# --- 1. IC -------------------------------------------------------------------------------------

def products(score: pd.DataFrame, px: pd.DataFrame, h: int, fit: pd.Series | None) -> pd.DataFrame:
    zx, zy = _ts_pairs(score.shift(1), px, h, "spearman", VOL_IC, fit=fit)
    return zx * zy


def fmt(s: pd.Series, lags: int = NW_LAGS) -> str:
    s = s.dropna()
    return f"{s.mean():+.4f} ({newey_west_t(s, lags):+.1f})"


def ic_report(bundle: DataBundle) -> dict[str, float]:
    px = bundle.prices[core.RISKY]
    u = unit_scores(px)
    live = next(iter(u.values())).notna().any(axis=1)
    print(f"common IC sample: {live.idxmax().date()} .. {px.index[-1].date()}, "
          f"{int(live.sum())} days")

    folds = chunk_folds(bundle.index, CHUNK)
    a, b = folds == 0, folds == 1
    rows = {}
    for name, s in u.items():
        pa = products(s, px, 1, a).mean(axis=1)
        pb = products(s, px, 1, b).mean(axis=1)
        full = products(s, px, 1, None).mean(axis=1)
        rows[name] = {"A (fit A)": fmt(pa[a]), "B (fit A)": fmt(pa[b]),
                      "B (fit B)": fmt(pb[b]), "A (fit B)": fmt(pb[a]), "all": fmt(full)}
    print(f"\n1a. POOLED TS RANK IC vs next-day return / {VOL_IC}d vol, {CHUNK}-day folds, "
          f"both directions. Mean (Newey-West t, {NW_LAGS} lags)")
    print(pd.DataFrame(rows).T.to_string())

    print("\n1b. held-out fold, by chunk size (fit on the other fold): mean of the two directions")
    rows = {}
    for name, s in u.items():
        row = {}
        for p in (13, 23, 47, 97, 251):
            f = chunk_folds(bundle.index, p)
            x0 = products(s, px, 1, f == 0).mean(axis=1)[f == 1]
            x1 = products(s, px, 1, f == 1).mean(axis=1)[f == 0]
            x = pd.concat([x0, x1]).sort_index()
            row[f"P={p}"] = fmt(x)
        rows[name] = row
    print(pd.DataFrame(rows).T.to_string())

    print("\n1c. by period, all dates (Newey-West t)")
    periods = [("1999", "2004"), ("2005", "2009"), ("2010", "2014"), ("2015", "2019"),
               ("2020", "2026")]
    rows = {}
    for name, s in u.items():
        full = products(s, px, 1, None).mean(axis=1)
        rows[name] = {f"{p0}-{p1[2:]}": fmt(full.loc[p0:p1]) for p0, p1 in periods}
    print(pd.DataFrame(rows).T.to_string())

    print("\n1d. longer horizons: forward return over h days / vol * sqrt(h), all dates "
          "(Newey-West lags = max(21, 2h))")
    rows = {}
    for name, s in u.items():
        rows[name] = {f"h={h}": fmt(products(s, px, h, None).mean(axis=1), max(NW_LAGS, 2 * h))
                      for h in (1, 5, 21, 63)}
    print(pd.DataFrame(rows).T.to_string())

    print("\n1e. by asset class, 1-day, all dates")
    rows = {}
    for name in (SAVED, "EMA-400", "EWMAC 32/128", FOCUS, "EWMAC 128/512"):
        prod = products(u[name], px, 1, None)
        rows[name] = {c: fmt(prod[ts].mean(axis=1)) for c, ts in core.CLASSES.items()}
    print(pd.DataFrame(rows).to_string())

    print(f"\n1f. {FOCUS} per asset, 1-day, all dates")
    prod = products(u[FOCUS], px, 1, None)
    cls = {t: c for c, ts in core.CLASSES.items() for t in ts}
    print(pd.DataFrame({t: {"class": cls[t], "IC (t)": fmt(prod[t])} for t in px.columns})
          .T.to_string())

    print("\n1g. correlation of scores, pooled")
    print(pd.DataFrame({k: v.stack() for k, v in u.items()}).corr()
          .to_string(float_format="{:+.2f}".format))
    return {k: float(v.loc[:SPLIT].stack().std()) for k, v in u.items()}


# --- 2. portfolio ------------------------------------------------------------------------------

def run_one(args: tuple[str, dict]) -> tuple[str, pd.Series, float]:
    name, alphas = args
    bundle = core.load()
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle, s)).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def portfolio_report(sd: dict[str, float], workers: int) -> None:
    prior, k0 = core.ALPHAS["prior"], core.ALPHAS["trend"][1]
    sig = rules(exclude=(core.CASH,), normalizer=Identity())
    k = {n: k0 * sd[SAVED] / sd[n] for n in sd}
    jobs = {"prior only": {"prior": prior}}
    jobs |= {n: {"prior": prior, "trend": (sig[n], k[n])} for n in sig}
    jobs[f"{SAVED} + {FOCUS}, half each"] = {
        "prior": prior, "trend": (sig[SAVED], k[SAVED] / 2), "slow": (sig[FOCUS], k[FOCUS] / 2)}
    print("\n   k matched to the saved alpha's train-window dispersion: "
          + ", ".join(f"{n} {v:.3f}" for n, v in k.items()))

    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, t) for n, r, t in pool.map(run_one, jobs.items())}
    start = max(r.index[0] for r, _ in out.values())
    runs = {n: r.loc[start:] for n, (r, _) in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[list(jobs)]
    m["turnover"] = [out[n][1] for n in m.index]
    pct = "{:.1%}".format
    print(f"\n2. PORTFOLIO, {start.date()} .. {runs[SAVED].index[-1].date()}, split "
          f"{SPLIT.date()}, trend alpha replaced | {core.PORTFOLIO}")
    print(m.to_string(formatters={**{c: pct for c in ("ret", "vol", "max_dd")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe")},
                                  "turnover": "{:.1f}".format}))
    ex = pd.DataFrame({n: runs[n] - runs["prior only"] for n in jobs if n != "prior only"})
    print("\n   what the trend alpha adds over prior only: annualized return / tracking vol, "
          "by period")
    g = ex.groupby(ex.index.year // 5 * 5)
    ir = (g.mean() / g.std() * np.sqrt(TRADING_DAYS)).rename(lambda y: f"{y}-{y + 4}").T
    ir["all"] = ex.mean() / ex.std() * np.sqrt(TRADING_DAYS)
    print(ir.to_string(float_format="{:+.2f}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    sd = ic_report(core.load())
    portfolio_report(sd, workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
