"""The variance risk premium as an equity alpha: robustness before promoting it.

``research_reentry.py`` found ``VRP = (VIX/100)^2 - SPY realized variance (21 days)``, as a
5-year z-score capped at +/-2, has a 1-day IC of +0.023 (t 2.4) on SPY since 1993 and lifted
the book from Sharpe 1.06 to 1.09. Before adding it to ``ALPHAS`` this checks, all fixed in
advance:

1. *What drives it.* VRP is implied minus realized variance. If only the realized part
   predicts (low realized vol, then higher return), it duplicates what vol-scaled sizing
   already does. So the IC of each part alone: ``-z(realized variance)`` and ``z(VIX^2)``.
2. *Out of sample.* SPY from 1995 to the book's start (2008-09) is data the book never used.
3. *Overlap* with the trend alphas: correlation of scores on the equity ETFs.
4. *Robustness in the book:* weight at 0.5x, 1x, 1.5x and 2x the matched ``k``; a 10-year
   z-score window; 5-day smoothing; US equity only (no EFA, EEM); and the realized-variance
   part alone at the matched ``k``.
5. *Where it helps and hurts:* the difference from the saved book by year.

**Result (2026-10-16): adopted at the matched weight, 0.091.** Neither part alone predicts
(-RV +0.007, VIX^2 +0.005; -RV adds nothing in the book). Before the book's sample (SPY
1995-2008) the 1-day IC is +0.020 but t 1.1. Uncorrelated with trend. Every variant except
5-day smoothing lifts the book (Sharpe 1.09-1.12, difference t 1.6-2.7); the 10-year window
scored best but the pre-set 5-year one is kept. Better in 10 of 19 years; 2008 and 2021 give
about three quarters of the gain, and average equity weight rises from 74% to 77%.

Usage:
    uv run python examples/research_vrp.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace

import numpy as np
import pandas as pd
from research_reentry import EQUITY, MarketStateView, daily_ic, fmt

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import chunk_folds, compare
from quantzy.signals import Identity
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
CHUNK = 47
US_EQUITY = [*core.CLASSES["US sectors"], "QQQ"]


@dataclass(frozen=True)
class Spec:
    """One way to turn VIX and SPY into a daily score."""

    part: str = "vrp"  # "vrp", "rv" (-realized variance) or "vix2"
    z_window: int = 1260
    smooth: int = 0  # EMA span applied to the capped z-score; 0 = none
    assets: tuple[str, ...] = tuple(EQUITY)


BASE = Spec()


def score(spec: Spec, start: str = "1990-01-01") -> pd.Series:
    raw = CachedReader(YFinanceReader()).history(["^VIX", "SPY"], start=start)
    vix, spy = raw["^VIX"].ffill(), raw["SPY"]
    rv = (spy.pct_change() ** 2).rolling(21, min_periods=21).mean() * TRADING_DAYS
    x = {"vrp": (vix / 100) ** 2 - rv, "rv": -rv, "vix2": (vix / 100) ** 2}[spec.part]
    roll = x.rolling(spec.z_window, min_periods=504)
    z = ((x - roll.mean()) / roll.std()).clip(-2, 2)
    if spec.smooth:
        z = z.ewm(span=spec.smooth, adjust=False).mean()
    return z[spy.notna()].rename(spec.part)


def spy_prices(start: str = "1990-01-01") -> pd.DataFrame:
    return CachedReader(YFinanceReader()).history(["SPY"], start=start)


def bundle_with(specs: dict[str, Spec]) -> DataBundle:
    px = core.load().prices
    panels = {}
    for name, spec in specs.items():
        s = score(spec).reindex(px.index).ffill()
        p = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
        for t in spec.assets:
            p[t] = s
        panels[name] = p.where(px.notna())
    return DataBundle.build(px, **panels)


# --- 1-3. IC and overlap -----------------------------------------------------------------------

def ic_report() -> None:
    spy = spy_prices()
    rows = {}
    for part in ("vrp", "rv", "vix2"):
        s = score(replace(BASE, part=part)).to_frame("SPY").shift(1).reindex(spy.index)
        folds = chunk_folds(spy.index, CHUNK)
        a, b = folds == 0, folds == 1
        held = pd.concat([daily_ic(s, spy, 1, a)[b], daily_ic(s, spy, 1, b)[a]]).sort_index()
        d1, d21 = daily_ic(s, spy, 1), daily_ic(s, spy, 21)
        rows[part] = {
            "1d held out, both": fmt(held, 21),
            "1d 1995-2008-08 (pre-book)": fmt(d1.loc[:"2008-08"], 21),
            "1d 2008-09+": fmt(d1.loc["2008-09":], 21),
            "21d 1995-2008-08": fmt(d21.loc[:"2008-08"], 42),
            "21d 2008-09+": fmt(d21.loc["2008-09":], 42),
        }
    print("\n1-2. TIME-SERIES RANK IC on SPY vs forward return / trailing 60d vol * sqrt(h) "
          "(Newey-West t). vrp = VIX^2 - RV; rv = -RV; vix2 = VIX^2")
    print(pd.DataFrame(rows).T.to_string())

    s = pd.DataFrame({p: score(replace(BASE, part=p)) for p in ("vrp", "rv", "vix2")})
    print("\n   correlation of the three scores (daily, SPY dates)")
    print(s.corr().to_string(float_format="{:+.2f}".format))

    b = bundle_with({"vrp": BASE})
    vol = realized_vol(b.prices, 60, annualize=True)
    trend = core.ALPHAS["trend"][0].raw(b) / vol
    ewmac = core.ALPHAS["ewmac"][0].raw(b) / vol
    stacked = pd.DataFrame({"vrp": b["vrp"][EQUITY].stack(), "EMA-200": trend[EQUITY].stack(),
                            "EWMAC 64/256": ewmac[EQUITY].stack()}).dropna()
    print("\n3. correlation of scores on the 12 equity ETFs, pooled")
    print(stacked.corr().to_string(float_format="{:+.2f}".format))


# --- 4-5. book ---------------------------------------------------------------------------------

def run_one(args: tuple[str, Spec | None, float]) -> tuple[str, pd.Series, pd.DataFrame, float]:
    name, spec, k = args
    bundle = bundle_with({"view": spec or BASE})
    alphas = dict(core.ALPHAS)
    if spec is not None:
        alphas["vrp"] = (MarketStateView("view", normalizer=Identity()), k)
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle, core.build())
                                ).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def matched_k(spec: Spec) -> float:
    b = bundle_with({"view": spec})
    vol = realized_vol(b.prices, 60, annualize=True)
    trend_sd = (core.ALPHAS["trend"][0].raw(b) / vol)[EQUITY].loc[:SPLIT].stack().std()
    return 0.1 * trend_sd / b["view"][EQUITY].loc[:SPLIT].stack().std()


def book_report(workers: int) -> None:
    k = {n: matched_k(s) for n, s in {"base": BASE, "rv": replace(BASE, part="rv"),
                                       "z10": replace(BASE, z_window=2520),
                                       "sm5": replace(BASE, smooth=5)}.items()}
    print("\n   matched k: " + ", ".join(f"{n} {v:.3f}" for n, v in k.items()))
    jobs = [("saved book", None, 0.0)]
    jobs += [(f"VRP k x{m:g}", BASE, k["base"] * m) for m in (0.5, 1, 1.5, 2)]
    jobs += [("VRP z 10y", replace(BASE, z_window=2520), k["z10"]),
             ("VRP smoothed 5d", replace(BASE, smooth=5), k["sm5"]),
             ("VRP US equity only", replace(BASE, assets=tuple(US_EQUITY)), k["base"]),
             ("-RV only", replace(BASE, part="rv"), k["rv"])]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    start = max(v[0].index[0] for v in out.values())
    runs = {n: v[0].loc[start:] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][2] for n in names]
    ex = pd.DataFrame({n: runs[n] - runs["saved book"] for n in names[1:]})
    m["diff IR"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(TRADING_DAYS))
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    pct = "{:.1%}".format
    print(f"\n4. BOOK, {start.date()} .. {runs['saved book'].index[-1].date()}, split "
          f"{SPLIT.date()} | {core.PORTFOLIO}")
    print(m.to_string(formatters={**{c: pct for c in ("ret", "vol", "max_dd")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe", "diff IR", "diff t")},
                                  "turnover": "{:.1f}".format}))

    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    d = yr[names[1:]].sub(yr["saved book"], axis=0)
    print("\n5. calendar-year return minus the saved book (percentage points)")
    print(d.to_string(float_format=lambda v: f"{v * 100:+.1f}"))
    base = d["VRP k x1"]
    print(f"\n   VRP k x1: better in {(base > 0).sum()} of {len(base)} years; worst "
          + ", ".join(f"{y} {v * 100:+.1f}" for y, v in base.nsmallest(3).items())
          + "; best " + ", ".join(f"{y} {v * 100:+.1f}" for y, v in base.nlargest(3).items()))
    w = out["VRP k x1"][1].loc[start:, EQUITY].sum(axis=1)
    w0 = out["saved book"][1].loc[start:, EQUITY].sum(axis=1)
    print(f"   average equity weight: {w0.mean():.0%} -> {w.mean():.0%}; "
          f"std of the change {(w - w0).std():.0%}")


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    ic_report()
    book_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
