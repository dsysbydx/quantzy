"""The variance risk premium beyond equities: Treasuries (MOVE), gold (GVZ), oil (OVX).

The book's ``vrp`` alpha -- VIX^2 minus SPY's realized variance, a 5-year z-score capped at
+/-2, weight 0.091 -- is its strongest uncorrelated alpha (``research_vrp.py``), and
``research_implied_vol_mu.py`` found equity forward Sharpe rising steeply with implied over
realized vol. The same premium is documented in other options markets: investors pay for
protection wherever they hold risk. The equity VRP applies to the 12 equity ETFs only; the
other 5 have no such view. **Fixed before any test; nothing is fitted.**

Per class, the saved ``VarianceRiskPremium`` signal unchanged (21-day realized variance,
5-year z-score capped at +/-2, times the asset's 60-day vol), at the equity VRP's weight:

* ``vrp_bonds``: MOVE (1-month Treasury yield vol, bp) against TLT's realized variance,
  MOVE turned into TLT price vol at a fixed duration of 17 years (``MOVE * 0.17`` in VIX
  points); applied to IEF and TLT. From 2008 (MOVE starts 2002-11).
* ``vrp_gold``: GVZ (GLD options) against GLD; applied to GLD. From mid-2013.
* ``vrp_oil``: OVX (USO options) against USO; applied to DBC (energy-heavy). From 2012.

Emerging markets (VXEEM) are left out: Yahoo has one day of it.

Tests: (1) pooled time-series rank IC of each class's capped z against its ETFs' next-day
and 21-day return over 60-day vol, 47-day interleaved folds both directions, Newey-West t;
(2) the saved core book (no overlays) with ``PORTFOLIO`` fixed, split 2017-07-26. **One
candidate: all three together**; each alone is a diagnostic. Rule: +0.02 Sharpe over the
window and in both halves. The book with the overlays is reported as well.

**Result (2026-10-31): none adopted; the premium does not travel.** Core book 2008-09-10 ..
2026-10-30. IC, 1-day held-out: bonds +0.010 (t 0.8), gold -0.013 (t -0.7; 21-day -0.067,
t -1.3, the wrong sign), oil on DBC +0.029 (t 1.5). Book: saved 1.21 (train 1.24 / test
1.20); all three 1.16 (1.23 / 1.11), bonds 1.19, gold 1.18, oil 1.20 (difference t -0.6 to
-1.7); with the overlays 1.32 against 1.37. Weights barely move (Treasuries 33-34%, gold
14%): the tilts add noise, not a view. A side finding: the saved equity VRP's 1-day IC is
+0.003 (t 0.2) since 2017, against +0.016 (t 1.8) over the whole sample (21-day t 2.0);
it has faded too.

Usage:
    uv run python research/research_vrp_multi.py [--workers 4]
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.backtest import WalkForwardBacktester  # noqa: E402
from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, DataBundle, YFinanceReader  # noqa: E402
from quantzy.jobs.daily import latest_complete  # noqa: E402
from quantzy.research import chunk_folds, compare, newey_west_t  # noqa: E402
from quantzy.research.ic import _ts_pairs  # noqa: E402
from quantzy.signals import Identity, VarianceRiskPremium  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
CHUNK, NW_LAGS, VOL_IC = 47, 21, 60
TLT_DURATION = 17.0
K = core.ALPHAS["vrp"][1]
# alpha -> (assets, implied-vol panel, market panel)
CLASSES = {
    "vrp_bonds": (("IEF", "TLT"), "move_iv", "tlt_px"),
    "vrp_gold": (("GLD",), "gvz", "gld_px"),
    "vrp_oil": (("DBC",), "ovx", "uso_px"),
}
MODELS = {"saved": (), "all three": tuple(CLASSES), **{n: (n,) for n in CLASSES}}
CANDIDATE = "all three"


def load() -> DataBundle:
    b = latest_complete(core.load(), core.RISKY)
    raw = CachedReader(YFinanceReader()).history(["^MOVE", "^GVZ", "^OVX", "USO"],
                                                 start="2002-01-01").reindex(b.index).ffill()
    px = b.prices

    def wide(s: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({t: s for t in px.columns}, index=b.index)

    return DataBundle({**b, "move_iv": wide(raw["^MOVE"] * TLT_DURATION / 100),
                       "tlt_px": wide(px["TLT"]), "gvz": wide(raw["^GVZ"]),
                       "gld_px": wide(px["GLD"]), "ovx": wide(raw["^OVX"]),
                       "uso_px": wide(raw["USO"])})


def signal(name: str) -> VarianceRiskPremium:
    assets, iv, mkt = CLASSES[name]
    return VarianceRiskPremium(assets, vix_panel=iv, market_panel=mkt, normalizer=Identity())


def ic_report(b: DataBundle) -> None:
    folds = chunk_folds(b.index, CHUNK)

    def pooled(score, px, h, fit):
        zx, zy = _ts_pairs(score, px, h, "spearman", VOL_IC, fit=fit)
        return (zx * zy).mean(axis=1)

    def fmt(s: pd.Series, lags: int = NW_LAGS) -> str:
        s = s.dropna()
        return f"{s.mean():+.4f} ({newey_west_t(s, lags):+.1f}, {len(s)}d)"

    rows = {}
    for name in ("vrp (saved, equity)", *CLASSES):
        if name in CLASSES:
            assets = list(CLASSES[name][0])
            z = signal(name).premium(b)
        else:
            assets = list(core.EQUITY)
            z = core.ALPHAS["vrp"][0].premium(b)
        score = pd.DataFrame({t: z for t in assets}, index=b.index).shift(1)  # lag 1
        px = b.prices[assets]
        held = pd.concat([pooled(score, px, 1, folds == 1)[folds == 0],
                          pooled(score, px, 1, folds == 0)[folds == 1]]).sort_index()
        full = pooled(score, px, 1, None)
        rows[name] = {"1d held-out": fmt(held), "1d all": fmt(full),
                      "1d since 2017": fmt(full.loc["2017":]),
                      "21d all": fmt(pooled(score, px, 21, None), 42)}
    print("\n1. POOLED TS RANK IC of the capped VRP z vs return / 60d vol, "
          f"{CHUNK}-day folds; mean (Newey-West t, days)")
    print(pd.DataFrame(rows).T.to_string())


def run_one(args):
    name, start = args
    b = load()
    alphas = {**core.ALPHAS, **{n: (signal(n), K) for n in MODELS[name]}}
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    w = res.weights.reindex(r.index).fillna(0.0)
    return name, r, w


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    b = load()
    start = core.warmup(b, core.build())
    print(f"core book from {b.index[start].date()} .. {b.index[-1].date()}")
    ic_report(b)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, [(n, start) for n in MODELS])}
    runs = {n: v[0] for n, v in out.items()}
    with_ov = {f"{n} + overlays": export.with_overlays(r, b) for n, r in runs.items()
               if n in ("saved", CANDIDATE)}
    t = compare({**runs, **with_ov}, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    for cls, ts in (("Treasuries", ["IEF", "TLT"]), ("gold", ["GLD"]), ("DBC", ["DBC"])):
        m[f"{cls} w"] = [float(out[n][1][ts].sum(axis=1).mean()) if n in out else np.nan
                         for n in m.index]
    print(f"\n2. CORE BOOK, split {SPLIT.date()}, PORTFOLIO fixed; candidate: {CANDIDATE}")
    print(m.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS if n != "saved"})
    print("\n   difference from saved, t-stat: " + ", ".join(
        f"{n} {v:+.1f}" for n, v in (ex.mean() / ex.std() * np.sqrt(len(ex))).items()))
    years = len(ex) / TRADING_DAYS
    print("   added return, bp/yr: " + ", ".join(f"{n} {v * 1e4 / years:+.0f}"
                                                for n, v in ex.sum().items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
