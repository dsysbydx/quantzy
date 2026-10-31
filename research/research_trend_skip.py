"""Skip the most recent month in the trend alphas?

Cross-sectional momentum is built as 12-1: the last month is skipped because single names
reverse over a month (Jegadeesh and Titman, 1993). Time-series trend on futures usually
does not skip (Moskowitz, Ooi and Pedersen, 2012). The book's two trend alphas -- price
against its 200-day EMA, and the EWMAC 64/256 crossover -- use prices through yesterday.
Earlier studies found the reversal here too, but only in equities: 1-month TSMOM is
reversal on US sectors (IC -0.016, t -2.3, ``research_multi_horizon``) and 5-day
time-series reversal survives in equity ETFs (t 2.6-2.8, ``research_short_reversal``), while
commodities show short-term momentum. Sectors are ~65% of the book. **Fixed before any
test.**

A skip of ``k`` rows delays each alpha's unit score (its Sharpe view) by ``k`` rows and keeps
the vol that turns it into a return current, so only the trend reading is older.

* ``saved``: no skip.
* ``skip 21`` (candidate A): both trend alphas, every asset.
* ``skip 21 equity`` (candidate B): the 12 equity ETFs only, where the reversal was found.
* ``skip 5``, ``skip 5 equity``: sensitivity only, not candidates.

**Added after the first run (2026-10-31), at the user's request: a sweep** of 5, 7, 10, 15
and 20 days, all assets and equity only. Chosen after seeing the 21-day result, so it is
exploratory and gets a stricter rule: a setting counts only if it clears +0.02 Sharpe over
the window and in both halves, *and* its return difference against saved has t >= 2, *and*
its neighbours on the curve agree (a plateau, not one spike).

Weights unchanged (a delayed score has the same dispersion). Tests:

1. Pooled time-series rank IC of each alpha's unit score against the next day's return over
   60-day vol, 47-day interleaved folds both directions, Newey-West t; equity ETFs and the
   other five separately; and 21 days ahead.
2. The saved core book (no overlays) with ``PORTFOLIO`` fixed, from a common start, split
   2017-07-26. Adoption rule: +0.02 Sharpe over the window and in both halves. Two
   candidates are tested, so a pass at t below 2 on the return difference is reported as
   weak.

**Result (2026-10-31): no skip; the saved trend alphas stay as they are.** 2008-10-09 ..
2026-10-30. The earlier studies' reversal shows up in the IC: on the equity ETFs, EMA-200's
held-out 1-day IC rises steadily with the skip, +0.005 (t 0.6) unskipped to +0.020 (t 2.8)
at 21 days, and EWMAC's +0.020 (t 2.6) to +0.024 (t 3.1); on the other five it barely moves.
The book goes the other way, as steadily: saved 1.24 (train 1.30 / test 1.20); equity-only
skip 5 / 7 / 10 / 15 / 20 / 21 gives 1.23 / 1.23 / 1.22 / 1.19 / 1.21 / 1.21 (test 1.19 down
to 1.11-1.13), all assets 1.18-1.23; max drawdown -14.8% unskipped to -16.9% at 21 days. No
setting passes either rule (best difference t +0.2). The loss is in fast crashes: Feb-Mar
2020 -12.1% against -9.5% for the 21-day equity skip (-2.9 points that year), the 2025
tariff selloff -12.8% against -12.0%; the slower rebound gave back about one point. An older
trend reading forecasts ordinary days better and exits crashes later, and the book's trend
tilt earns its keep in the exits. A 1-day IC cannot see that.

Usage:
    uv run python research/research_trend_skip.py [--workers 5]
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
from quantzy.data.bundle import DataBundle
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
CHUNK, NW_LAGS, VOL_IC = 47, 21, 60
NON_EQUITY = [t for t in core.RISKY if t not in core.EQUITY]
TREND = ("trend", "ewmac")
# name -> (skip, equity only)
SWEEP = (5, 7, 10, 15, 20)
MODELS = {"saved": (0, False), "skip 21": (21, False), "skip 21 equity": (21, True)}
MODELS |= {f"skip {k}{' equity' if eq else ''}": (k, eq) for k in SWEEP for eq in (False, True)}
CANDIDATES = ("skip 21", "skip 21 equity")


@dataclass
class Skipped(Signal):
    """``inner``'s unit score delayed ``skip`` rows, times the current annualized vol."""

    name: ClassVar[str] = "skipped"
    inner: Signal | None = None
    skip: int = 21

    def raw(self, data: DataBundle) -> pd.DataFrame:
        vol = realized_vol(data.prices, self.inner.vol_window, annualize=True)
        unit = self.inner.raw(data) / vol.where(vol > 0)
        return unit.shift(self.skip) * vol

    @property
    def min_history(self) -> int:
        return self.inner.min_history + self.skip


def with_exclude(sig: Signal, exclude: tuple[str, ...]) -> Signal:
    params = sig.params() | {"exclude": exclude}
    return type(sig)(**params, normalizer=Identity(), lag=sig.lag)


def alphas(name: str) -> dict:
    skip, equity_only = MODELS[name]
    out = dict(core.ALPHAS)
    if not skip:
        return out
    for a in TREND:
        sig, k = core.ALPHAS[a]
        if equity_only:
            eq = with_exclude(sig, (CASH, *NON_EQUITY))
            rest = with_exclude(sig, (CASH, *core.EQUITY))
            out[a] = (Skipped(inner=eq, skip=skip, normalizer=Identity()), k)
            out[f"{a} rest"] = (rest, k)
        else:
            out[a] = (Skipped(inner=sig, skip=skip, normalizer=Identity()), k)
    return out


def pooled(score: pd.DataFrame, px: pd.DataFrame, h: int, fit: pd.Series | None) -> pd.Series:
    """Per-date pooled time-series rank IC contribution (fit: rows whose stats standardize)."""
    zx, zy = _ts_pairs(score, px, h, "spearman", VOL_IC, fit=fit)
    return (zx * zy).mean(axis=1)


def ic_report(b: DataBundle) -> None:
    px = b.prices[core.RISKY]
    folds = chunk_folds(b.index, CHUNK)

    def fmt(s: pd.Series, lags: int = NW_LAGS) -> str:
        s = s.dropna()
        return f"{s.mean():+.4f} ({newey_west_t(s, lags):+.1f})"

    for a in TREND:
        sig = core.ALPHAS[a][0]
        vol = realized_vol(px, sig.vol_window, annualize=True)
        unit0 = (sig.raw(b)[core.RISKY] / vol.where(vol > 0))
        units = {f"skip {k}": unit0.shift(k) for k in (0, *SWEEP, 21)}
        common = np.logical_and.reduce([u.notna() for u in units.values()])
        rows = {}
        for n, u in units.items():
            u = u.where(common).shift(1)  # tradeable a day later, as the alpha's lag
            row = {}
            for g, cols in (("equity", core.EQUITY), ("other 5", NON_EQUITY)):
                held = pd.concat([pooled(u[cols], px[cols], 1, folds == 1)[folds == 0],
                                  pooled(u[cols], px[cols], 1, folds == 0)[folds == 1]])
                row[f"{g} 1d held-out"] = fmt(held.sort_index())
                row[f"{g} 21d"] = fmt(pooled(u[cols], px[cols], 21, None), 42)
            rows[n] = row
        print(f"\n1. {a}: POOLED TS RANK IC vs return / {VOL_IC}d vol, {CHUNK}-day folds, "
              "mean (Newey-West t)")
        print(pd.DataFrame(rows).T.to_string())


def run_one(args):
    name, start = args
    b = latest_complete(core.load(), core.RISKY)
    s = core.build(alphas=alphas(name))
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    b = latest_complete(core.load(), core.RISKY)
    start = max(core.warmup(b, core.build(alphas=alphas(n))) for n in MODELS)
    print(f"common start {b.index[start].date()} .. {b.index[-1].date()}")
    ic_report(b)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, [(n, start) for n in MODELS])}
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "sharpe": t["full"]["sharpe"], "tr_sharpe": t["train"]["sharpe"],
        "te_sharpe": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[list(MODELS)]
    m["turnover"] = [out[n][1] for n in m.index]
    print(f"\n2. CORE BOOK (no overlays), split {SPLIT.date()}, PORTFOLIO fixed; "
          f"candidates: {', '.join(CANDIDATES)}")
    print(m.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS if n != "saved"})
    print("\n   difference from saved, t-stat: " + ", ".join(
        f"{n} {v:+.1f}" for n, v in (ex.mean() / ex.std() * np.sqrt(len(ex))).items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
