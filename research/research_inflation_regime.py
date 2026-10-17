"""An inflation-regime tilt: real assets up and Treasuries down when inflation expectations rise.

In 2022 stocks and bonds fell together while commodities rose, and the book held little
inflation protection (DBC about 1%). This asks whether a market reading of inflation
expectations would have helped, and whether it forecasts in general. **Fixed before any
test.**

1. *Breakeven momentum* (daily, from 2003). ``log(TIP / IEF)`` rises when TIPS beat nominal
   Treasuries of similar duration, that is when expected inflation rises. The score is its
   126-day change, z-scored over the trailing 5 years (from 2 years of history) and capped
   at +/-2; a 63-day change is the sensitivity check. The view: ``+score`` on GLD and DBC,
   ``-score`` on IEF and TLT, none on equities.
2. *Realized inflation, long history* (monthly). CPI acceleration (the 6-month change in
   year-on-year CPI inflation, CPI lagged a month) against the 10-year Treasury par bond
   from 1962 and the S&P GSCI from 1984, from Shiller's CPI (to 2024-09).

Tests: time-series IC of the signed score (1, 21 and 63 days; held-out 47-day folds) per
asset and pooled, correlation with the trend alphas, and the book with the tilt added at a
weight matched to the trend budget's train-window dispersion; variants: 126-day, 63-day,
and real assets only (no bond leg). ``PORTFOLIO`` unchanged.

**Result (2026-10-17): negative, not adopted.** Pooled IC is zero (126-day: +0.000 held
out; 63-day +0.008, t 0.8) and the legs disagree: rising breakevens preceded *weaker* gold
(21-day IC -0.13, t -2.4) and, only since 2016, weaker Treasuries (IEF +0.19, t 2.2; the
opposite before). CPI acceleration forecast Treasuries only in the 1960s-80s (12-month IC
+0.31 first half, +0.04 second) and commodities not at all. In the book: Sharpe 1.15, 1.13
and 1.11 against 1.13 (difference t 0.5 at best), and the 126- and 63-day versions moved 2022
in opposite directions (+1.8 and -1.6 points). Correlation with trend on these assets 0.35.

Usage:
    uv run --with xlrd python research/research_inflation_regime.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from research_fast_valuation import load as monthly_load
from research_reentry import MarketStateView, daily_ic, fmt

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.signals import Identity
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
SIGN = {"GLD": 1.0, "DBC": 1.0, "IEF": -1.0, "TLT": -1.0}
REAL_ONLY = {"GLD": 1.0, "DBC": 1.0}
CHUNK = 47


def breakeven_score(window: int) -> pd.Series:
    px = CachedReader(YFinanceReader()).history(["TIP", "IEF"], start="2003-01-01").dropna()
    x = np.log(px["TIP"] / px["IEF"]).diff(window)
    roll = x.rolling(1260, min_periods=504)
    return ((x - roll.mean()) / roll.std()).clip(-2, 2).rename(f"breakeven {window}d")


def bundle_with(views: dict[str, tuple[int, dict[str, float]]]) -> DataBundle:
    b = core.load()
    px = b.prices
    panels = {}
    for name, (window, signs) in views.items():
        s = breakeven_score(window).reindex(px.index).ffill()
        p = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
        for t, sg in signs.items():
            p[t] = sg * s
        panels[name] = p.where(px.notna())
    return DataBundle.build(px, vix=b["vix"], market=b["market"], carry=b["carry"], **panels)


# --- 1. daily IC -------------------------------------------------------------------------------

def ic_report() -> None:
    b = bundle_with({"b126": (126, SIGN), "b63": (63, SIGN)})
    px = b.prices[list(SIGN)]
    folds = chunk_folds(b.index, CHUNK)
    a, bb = folds == 0, folds == 1
    rows = {}
    for name in ("b126", "b63"):
        sc = b[name][list(SIGN)].shift(1)
        for label, cols in [("pooled", list(SIGN)), *[(t, [t]) for t in SIGN]]:
            s, p = sc[cols], px[cols]
            held = pd.concat([daily_ic(s, p, 1, a)[bb], daily_ic(s, p, 1, bb)[a]])
            row = {"1d held out": fmt(held, 21)}
            for h in (1, 21, 63):
                ic = daily_ic(s, p, h)
                row[f"h={h}"] = fmt(ic, max(21, 2 * h))
                if h == 21:
                    row["h=21 2004-15"] = fmt(ic.loc[:"2015"], 42)
                    row["h=21 2016+"] = fmt(ic.loc["2016":], 42)
            rows[(name, label)] = row
    print("1. TIME-SERIES RANK IC of the signed breakeven score (+ for GLD/DBC, - for IEF/TLT) "
          "vs forward return / 60d vol; Newey-West t")
    print(pd.DataFrame(rows).T.to_string())

    vol = realized_vol(b.prices, 60, annualize=True)
    trend = core.ALPHAS["trend"][0].raw(b) / vol
    ewmac = core.ALPHAS["ewmac"][0].raw(b) / vol
    st = pd.DataFrame({"breakeven 126d": b["b126"][list(SIGN)].stack(),
                       "EMA-200": trend[list(SIGN)].stack(),
                       "EWMAC 64/256": ewmac[list(SIGN)].stack()}).dropna()
    print("\n   correlation with the trend scores on GLD, DBC, IEF, TLT (pooled)")
    print(st.corr().to_string(float_format="{:+.2f}".format))
    s = breakeven_score(126)
    print("\n   breakeven 126d score, year-end")
    print(s.groupby(s.index.year).last().loc[2006:].to_frame().T.to_string(
        float_format="{:+.1f}".format))


# --- 2. long history, realized inflation -------------------------------------------------------

def long_report() -> None:
    from research_shiller_valuation import load as shiller

    targets, _, _ = monthly_load()
    cpi = shiller()["CPI"]
    yoy = cpi / cpi.shift(12) - 1
    accel = (yoy - yoy.shift(6)).shift(1)  # known a month later
    rows = {}
    for asset, sign in (("10y Treasury", -1.0), ("commodities (GSCI)", 1.0)):
        r = targets[asset]
        for h in (1, 3, 12):
            y = r.rolling(h).sum().shift(-h)
            z = pd.concat([sign * accel, y], axis=1).dropna()
            ra, rb = z.iloc[:, 0].rank(), z.iloc[:, 1].rank()
            prod = (ra - ra.mean()) / ra.std() * (rb - rb.mean()) / rb.std()
            half = z.index[len(z) // 2]
            rows[(asset, f"h={h}")] = {
                "from": f"{z.index[0]:%Y-%m}", "IC": f"{prod.mean():+.3f}",
                "t": f"{newey_west_t(prod, max(h, 3)):+.1f}",
                "1st half": f"{prod.loc[:half].mean():+.3f}",
                "2nd half": f"{prod.loc[half:].mean():+.3f}"}
    print("\n2. LONG HISTORY: CPI acceleration (sign: - for Treasuries, + for commodities) vs the "
          "forward excess return, monthly rank IC")
    print(pd.DataFrame(rows).T.to_string())


# --- 3. book -----------------------------------------------------------------------------------

VARIANTS = {"saved book": None,
            "+ breakeven 126d": (126, SIGN),
            "+ breakeven 63d": (63, SIGN),
            "+ breakeven 126d, real assets only": (126, REAL_ONLY)}


def matched_k(view: tuple[int, dict[str, float]]) -> float:
    b = bundle_with({"view": view})
    vol = realized_vol(b.prices, 60, annualize=True)
    cols = list(view[1])
    trend_sd = (core.ALPHAS["trend"][0].raw(b) / vol)[cols].loc[:SPLIT].stack().std()
    return 0.1 * trend_sd / b["view"][cols].loc[:SPLIT].stack().std()


def run_one(args: tuple[str, tuple | None, float]):
    name, view, k = args
    bundle = bundle_with({"view": view or (126, SIGN)})
    alphas = dict(core.ALPHAS)
    if view is not None:
        alphas["inflation"] = (MarketStateView("view", normalizer=Identity()), k)
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def book_report(workers: int) -> None:
    jobs = [(n, v, matched_k(v) if v else 0.0) for n, v in VARIANTS.items()]
    print("\n   matched k: " + ", ".join(f"{n} {k:.3f}" for n, v, k in jobs if v))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, jobs)}
    names = list(VARIANTS)
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][2] for n in names]
    for y in ("2021", "2022"):
        m[y] = [(1 + runs[n].loc[y]).prod() - 1 for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    w = {n: out[n][1] for n in names}
    m["real assets wt"] = [w[n][["GLD", "DBC"]].sum(axis=1).mean() for n in names]
    m["real wt 2022"] = [w[n][["GLD", "DBC"]].loc["2022"].sum(axis=1).mean() for n in names]
    m["Treasury wt 2022"] = [w[n][["IEF", "TLT"]].loc["2022"].sum(axis=1).mean() for n in names]
    pct = "{:.1%}".format
    r0 = runs[names[0]]
    print(f"\n3. BOOK, {r0.index[0].date()} .. {r0.index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "2021", "2022", "real assets wt",
                            "real wt 2022", "Treasury wt 2022")},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t")},
        "turnover": "{:.1f}".format}))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved book (percentage points)")
    print(yr[names[1:]].sub(yr[names[0]], axis=0).to_string(
        float_format=lambda v: f"{v * 100:+.1f}"))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    ic_report()
    long_report()
    book_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
