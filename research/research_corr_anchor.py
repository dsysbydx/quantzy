"""Anchor the risk model's correlations to the long run.

On 2026-10-16 the saved book held about 103% of capital in four US sectors (XLE, XLK, XLV,
XLP, each near its 26% cap) and no bonds or gold. The alphas barely matter (the prior-only
book is as concentrated); the cause is the risk model: over the past year those sectors moved
against each other (XLK/XLP -0.39, XLE/XLK -0.23 over one year; +0.47 and +0.45 over
2008-2025), so equal priors look like a well-hedged set of bets. The book's ex-ante vol is
8.8% under the risk model and 21.1% under 2008-2025 correlations. **Fixed before any test.**

Vols stay those of the saved blend; correlations become ``(1 - a) * rho_model + a *
rho_long``, where ``rho_long`` is the pairwise correlation of daily returns over a trailing
window (returns through t-1, at least 252 overlapping days per pair; a pair without them
keeps the model's). Variants: 3-year window with ``a = 0.5`` (the candidate), 5-year with
0.5, 3-year with 0.25. The saved 17, everything else unchanged, from 2008-09.

Reported: Sharpe in both halves, return, vol, drawdown, turnover; concentration (top-four
weight, effective number of positions ``1 / sum(w^2)`` on the risky book); ex-ante vol under
the risk model used against realized vol over the next 63 days; and today's book under each.

**Result (2026-10-18): no change to the book.** Anchoring correlations made no measurable
difference (Sharpe 1.13 saved, 1.13 / 1.14 / 1.12 for 3y 0.5 / 5y 0.5 / 3y 0.25; difference
t -0.5 to -1.0) and slightly deeper drawdowns. The saved risk model has been well calibrated
(ex-ante 10.4% against 10.7% realized over the next 63 days) and the book's usual
concentration is unchanged (top-four weight ~85%, ~7 effective positions). Today's book
shrinks only partly (stress vol 21.1% -> 16.4% at 5y): the same four sectors stay on top.
The daily job now prints the stress vol instead.

Usage:
    uv run python research/research_corr_anchor.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data.bundle import as_bundle
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.risk.base import RiskModel
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
R = core.RISKY
SPLIT = pd.Timestamp("2017-07-26")
VARIANTS = {"saved (no anchor)": None, "3y, a = 0.5": (756, 0.5), "5y, a = 0.5": (1260, 0.5),
            "3y, a = 0.25": (756, 0.25)}


@dataclass
class AnchoredCorrelation:
    """``base``'s vols; correlations blended with a long trailing window's."""

    base: RiskModel
    window: int = 756
    weight: float = 0.5
    min_overlap: int = 252
    _rets: tuple | None = field(default=None, init=False, repr=False, compare=False)

    def covariance(self, data, asof=None) -> pd.DataFrame:
        bundle = as_bundle(data)
        cov = self.base.covariance(bundle, asof)
        key = bundle.fingerprint()
        if self._rets is None or self._rets[0] != key:
            self._rets = (key, bundle.prices.pct_change(fill_method=None))
        rets = self._rets[1]
        hist = (rets.loc[:asof] if asof is not None else rets).iloc[:-1].iloc[-self.window:]
        cols = [c for c in cov.index if c != CASH]
        long = hist[cols].corr(min_periods=self.min_overlap).to_numpy()
        s = cov.loc[cols, cols].to_numpy()
        vol = np.sqrt(np.clip(np.diag(s), 1e-18, None))
        rho = s / np.outer(vol, vol)
        rho = np.where(np.isfinite(long), (1 - self.weight) * rho + self.weight * long, rho)
        np.fill_diagonal(rho, 1.0)
        out = cov.copy()
        out.loc[cols, cols] = rho * np.outer(vol, vol)
        return out

    @property
    def min_history(self) -> int:
        return self.base.min_history


def strategy(variant: str):
    s = core.build()
    spec = VARIANTS[variant]
    if spec is not None:
        s.risk_model = AnchoredCorrelation(s.risk_model, *spec)
    return s


def run_one(variant: str):
    bundle = latest_complete(core.load(), R)
    s = strategy(variant)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle)).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    # ex-ante vol of the held book at month-ends, against realized vol over the next 63 days
    w = res.weights[R]
    rows = []
    for t in r.resample("ME").last().index[:-3]:
        a = bundle.index[bundle.index.searchsorted(t, "right") - 1]
        c = s.risk_model.covariance(bundle, a).loc[R, R].to_numpy()
        wt = w.loc[:a].iloc[-1].to_numpy()
        fwd = r.loc[a:].iloc[1:64]
        rows.append((np.sqrt(wt @ c @ wt), fwd.std() * np.sqrt(TRADING_DAYS)))
    cal = pd.DataFrame(rows, columns=["ex_ante", "realized"])
    today = s.decide(bundle).target.weights.reindex(R)
    years = len(r) / TRADING_DAYS
    return variant, r, w, sum(d.turnover for d in res.decisions) / years, cal, today


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {v: rest for v, *rest in pool.map(run_one, VARIANTS)}
    names = list(VARIANTS)
    runs = {n: out[n][0] for n in names}
    s0 = max(r.index[0] for r in runs.values())
    runs = {n: r.loc[s0:] for n, r in runs.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    for n in names:
        w = out[n][1].loc[s0:]
        risky = w.clip(lower=0)
        share = risky.div(risky.sum(axis=1), axis=0)
        m.loc[n, "turnover"] = out[n][2]
        m.loc[n, "top-4 wt"] = risky.apply(lambda x: x.nlargest(4).sum(), axis=1).mean()
        m.loc[n, "eff. positions"] = (1 / (share ** 2).sum(axis=1)).mean()
        cal = out[n][3]
        m.loc[n, "ex-ante vol"] = cal["ex_ante"].mean()
        m.loc[n, "realized next 63d"] = cal["realized"].mean()
        m.loc[n, "realized / ex-ante"] = (cal["realized"] / cal["ex_ante"]).median()
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    pct = "{:.1%}".format
    print(f"BOOK, {s0.date()} .. {runs[names[0]].index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "top-4 wt", "ex-ante vol",
                            "realized next 63d")},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t",
                                        "realized / ex-ante")},
        "turnover": "{:.1f}".format, "eff. positions": "{:.1f}".format}))

    bundle = latest_complete(core.load(), R)
    r_hist = bundle.prices[R].pct_change(fill_method=None).loc["2008":"2025"]
    rm = core.build().risk_model
    vol = np.sqrt(np.diag(rm.covariance(bundle, bundle.index[-1]).loc[R, R]))
    c_hist = r_hist.corr().to_numpy() * np.outer(vol, vol)
    today = pd.DataFrame({n: out[n][4] for n in names})
    print(f"\n   today's book ({bundle.index[-1].date()}), % of capital, and its ex-ante vol under "
          "2008-2025 correlations (current vols)")
    print(today.to_string(float_format="{:.1%}".format))
    print("   ex-ante vol, 2008-2025 correlations: " + ", ".join(
        f"{n} {np.sqrt(today[n].to_numpy() @ c_hist @ today[n].to_numpy()):.1%}" for n in names))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved book (percentage points)")
    print(yr[names[1:]].sub(yr[names[0]], axis=0).to_string(
        float_format=lambda v: f"{v * 100:+.1f}"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
