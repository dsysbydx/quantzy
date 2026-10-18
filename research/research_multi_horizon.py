"""Multi-horizon time-series momentum, and whether some classes trend faster than EMA-200.

The saved trend alphas are slow (EMA-200, EWMAC 64/256). Pooled over the 17 ETFs, faster
crossovers had no IC, but by class the fast blend was +0.027 (t 2.4) on gold and commodities
and +0.019 on the dollar, against -0.009 on US sectors; commodities and currencies are often
found to trend faster than equities. **Fixed before any test.**

Signal (Moskowitz, Ooi and Pedersen style): for each horizon h in 1, 3, 6, 12 months (21, 63,
126, 252 days), the past return over h divided by daily vol times sqrt(h), a t-stat of the
past return, capped at +/-2; the multi-horizon score is the average of the four.

1. Time-series IC (next-day return over trailing vol) per class and horizon, on fold A, fold
   B (held out) and all days; 47-day interleaved folds; EMA-200 and EWMAC 64/256 alongside.
2. Per class, keep the horizons whose fold-A IC has t > 1, and judge the blend on fold B only.
3. Book, ``PORTFOLIO`` unchanged, core only (the overlay is unaffected): the saved book; the
   multi-horizon score replacing EMA-200 and EWMAC on every asset; the per-class horizons of
   step 2 replacing them; and the saved trend alphas plus a 1-3 month component on the
   non-equity classes at half the trend budget. Weights matched to the trend budget's
   train-window dispersion (0.1 on EMA-200 for the replacements).

**Result (2026-10-18): no change.** By class, 6- and 12-month TSMOM match the saved trend
where trend works (equity index, gold and commodities, the dollar) and EWMAC 64/256 is the
best single signal; 1-month TSMOM is reversal on US sectors (IC -0.016, t -2.3); Treasuries
show no trend at any horizon. Horizons kept per class on fold A did no better than EMA-200
on fold B. In the book: multi-horizon 1.08, per-class 1.05, saved + fast non-equity 1.14,
against 1.13 (difference t -1.1 to +1.2).

Usage:
    uv run python research/research_multi_horizon.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
HORIZONS = (21, 63, 126, 252)
LABEL = {21: "1m", 63: "3m", 126: "6m", 252: "12m"}
CAP = 2.0
CLASSES = core.CLASSES
NON_EQUITY = [*CLASSES["Treasuries"], *CLASSES["gold & commodities"], *CLASSES["US dollar"]]


@dataclass
class TSMomentum(Signal):
    """Average over ``horizons`` of capped past-return t-stats, per asset, times annual vol.

    ``by_asset`` restricts each asset to its own horizons (and ``assets`` to a subset); an
    asset with no horizons gets no view."""

    name: ClassVar[str] = "tsmom"
    horizons: tuple[int, ...] = HORIZONS
    by_asset: dict[str, tuple[int, ...]] | None = None
    vol_window: int = 60

    def score(self, px: pd.DataFrame) -> pd.DataFrame:
        r = px.pct_change(fill_method=None)
        vol = r.rolling(self.vol_window, min_periods=self.vol_window).std()
        z = {h: ((px / px.shift(h) - 1) / (vol * np.sqrt(h))).clip(-CAP, CAP)
             for h in sorted({h for hs in (self.by_asset or {}).values() for h in hs}
                             | set(self.horizons))}
        out = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
        for t in px.columns:
            if t == core.CASH:
                continue
            hs = self.horizons if self.by_asset is None else self.by_asset.get(t, ())
            if hs:
                out[t] = sum(z[h][t] for h in hs) / len(hs)
        return out

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        return self.score(px) * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        hs = set(self.horizons) | {h for v in (self.by_asset or {}).values() for h in v}
        return max(hs) + self.vol_window + 1


# --- 1-2. IC ----------------------------------------------------------------------------------

def ic_report(b: DataBundle) -> dict[str, tuple[int, ...]]:
    px = b.prices[core.RISKY]
    vol = realized_vol(px, 60, annualize=True)
    folds = chunk_folds(b.index, 47)
    a, bb = folds == 0, folds == 1
    scores = {f"TSMOM {LABEL[h]}": TSMomentum((h,)).score(px) for h in HORIZONS}
    scores["TSMOM 1-12m avg"] = TSMomentum().score(px)
    scores["EMA-200 (saved)"] = core.ALPHAS["trend"][0].raw(b)[core.RISKY] / vol
    scores["EWMAC 64/256 (saved)"] = core.ALPHAS["ewmac"][0].raw(b)[core.RISKY] / vol

    def prods(s, fit):
        zx, zy = _ts_pairs(s.shift(1), px, 1, "spearman", 60, fit=fit)
        return zx * zy

    rows, fold_a = {}, {}
    for name, s in scores.items():
        pa = prods(s, a)
        pf = prods(s, None)
        for c, ts in CLASSES.items():
            xa = pa[ts].mean(axis=1)[a].dropna()
            xbo = pa[ts].mean(axis=1)[bb].dropna()  # fold B, fit on fold A (held out)
            xf = pf[ts].mean(axis=1).dropna()
            rows[(c, name)] = {
                "fold A": f"{xa.mean():+.4f} ({newey_west_t(xa, 21):+.1f})",
                "fold B (held out)": f"{xbo.mean():+.4f} ({newey_west_t(xbo, 21):+.1f})",
                "all": f"{xf.mean():+.4f} ({newey_west_t(xf, 21):+.1f})",
                "2010+": f"{xf.loc['2010':].mean():+.4f}"}
            fold_a[(c, name)] = newey_west_t(xa, 21)
    print("1. TIME-SERIES RANK IC BY CLASS, next-day return / 60d vol (Newey-West t)")
    print(pd.DataFrame(rows).T.to_string())

    chosen = {c: tuple(h for h in HORIZONS if fold_a[(c, f"TSMOM {LABEL[h]}")] > 1.0)
              for c in CLASSES}
    print("\n2. HORIZONS KEPT PER CLASS (fold-A t > 1): "
          + "; ".join(f"{c}: {', '.join(LABEL[h] for h in hs) or 'none'}"
                      for c, hs in chosen.items()))
    by_asset = {t: chosen[c] for c, ts in CLASSES.items() for t in ts}
    s = TSMomentum(by_asset=by_asset).score(px)
    pa = prods(s, a)
    for c, ts in CLASSES.items():
        if not chosen[c]:
            continue
        x = pa[ts].mean(axis=1)[bb].dropna()
        ref = prods(scores["EMA-200 (saved)"], a)[ts].mean(axis=1)[bb].dropna()
        print(f"   {c:20s} chosen blend, fold B: {x.mean():+.4f} ({newey_west_t(x, 21):+.1f})"
              f"   EMA-200 fold B: {ref.mean():+.4f} ({newey_west_t(ref, 21):+.1f})")
    return by_asset


# --- 3. book -----------------------------------------------------------------------------------

def matched_k(b: DataBundle, sig: TSMomentum, cols: list[str], budget: float) -> float:
    vol = realized_vol(b.prices, 60, annualize=True)
    trend_sd = (core.ALPHAS["trend"][0].raw(b) / vol)[cols].loc[:SPLIT].stack().std()
    return budget * trend_sd / sig.score(b.prices)[cols].loc[:SPLIT].stack().std()


def run_one(args):
    name, alphas = args
    b = latest_complete(core.load(), core.RISKY)
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def book_report(b: DataBundle, by_asset: dict[str, tuple[int, ...]], workers: int) -> None:
    base = {"prior": core.ALPHAS["prior"], "vrp": core.ALPHAS["vrp"]}
    multi = TSMomentum(normalizer=Identity())
    perclass = TSMomentum(by_asset=by_asset, normalizer=Identity())
    fast = TSMomentum(by_asset={t: (21, 63) for t in NON_EQUITY}, normalizer=Identity())
    k_multi = matched_k(b, multi, core.RISKY, 0.1)
    live = [t for t, hs in by_asset.items() if hs]
    k_per = matched_k(b, perclass, live, 0.1) if live else 0.0
    k_fast = matched_k(b, fast, NON_EQUITY, 0.05)
    print(f"\n   weights: multi-horizon {k_multi:.3f}, per-class {k_per:.3f}, "
          f"fast non-equity {k_fast:.3f}")
    jobs = [("saved book", dict(core.ALPHAS)),
            ("multi-horizon replaces trend", {**base, "tsmom": (multi, k_multi)}),
            ("per-class horizons replace trend", {**base, "tsmom": (perclass, k_per)}),
            ("saved + fast (1-3m) on non-equity", {**core.ALPHAS, "fast": (fast, k_fast)})]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, t) for n, r, t in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][1] for n in names]
    for lab, (a, z) in {"2008-09..2009-03": ("2008-09-10", "2009-03-09"),
                        "2020-02..03": ("2020-02-19", "2020-03-23"),
                        "2022": ("2022-01-01", "2022-12-31")}.items():
        m[lab] = [(1 + runs[n].loc[a:z]).prod() - 1 for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    pct = "{:.1%}".format
    print(f"\n3. BOOK (core only), {runs[names[0]].index[0].date()} .. "
          f"{runs[names[0]].index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "2008-09..2009-03", "2020-02..03", "2022")},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t")},
        "turnover": "{:.1f}".format}))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    by_asset = ic_report(b)
    book_report(b, by_asset, workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
