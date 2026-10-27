"""Correlation shrinkage toward class blocks, and slower correlations, on today's book.

``research_shrinkage.py`` (2026-10-17) shrank correlations toward identity and found no gain:
it fixes conditioning, but it also weakens the stock-bond hedge. ``research_prior_sensitivity``
then showed what does hurt the book: noise between near-substitutes (random prior errors of
0.10 per asset cost a median -0.21 Sharpe), the nine sector SPDRs above all. Shrinking toward
a *class-block* target removes noise between substitutes and keeps the hedge: each pair's
correlation moves toward the average for its pair of classes (sector-sector toward the mean
sector-sector correlation, sector-Treasury toward the mean of those, and so on), measured on
the same day's matrix. **Fixed before any test.**

Vols always from the saved 5/20/60 blend; only correlations change, ``(1 - d) * rho + d * T``:

* ``saved``.
* ``identity 0.1``, ``identity 0.25``: ``T = I``, the old variants on today's book.
* ``block 0.25``, ``block 0.5``, ``block 1``: ``T`` the class-block average (``block 1`` is a
  constant correlation within each pair of classes). Classes as in ``core.CLASSES``.
* ``corr hl 120``, ``corr hl 250``, ``corr hl 500``: ``rho`` from an EWMA with that halflife
  (lookback 3x, 2x for 500), no shrinkage.
* ``block 0.5, hl 250``: both.

The saved core book (no overlays: they do not use the risk model), every variant from the
latest warmup among them, split 2017-07-26. Adopt only a variant that lifts Sharpe by at least
0.02 over the window and in both halves. Reported also: effective number of positions
(``1 / sum(w^2)`` on the risky book), sector and Treasury weight, turnover, and the median
condition number of the correlation matrix.

**Result (2026-10-26): none adopted; the saved fast correlations are best.** 2011-02-18 ..
2026-10-16 (the 500-day correlations set the start): saved 1.12 (1.06 / 1.16). Identity
0.1 / 0.25: 1.08 / 1.03. Block 0.25 / 0.5 / 1: 1.11 / 1.09 / 1.02, with shallower drawdowns
at 0.25-0.5 (-15.6%, -15.2% against -16.2%) and more in Treasuries, but lower in the test
half. Correlation halflives 120 / 250 / 500: 1.05-1.06, test 1.05-1.06, drawdowns -18% to
-20%; with block 0.5 as well, 1.06. No variant is within 0.01 of the saved Sharpe in the
test half; the differences have t -0.1 to -1.5. Shrinking or slowing correlations cuts the
condition number (639 -> 25-139) and turnover (1.0 -> 0.6-0.9) and spreads the book a
little (effective N 7.0 -> 7.3-8.3), but the fast correlations carry information the book
uses (probably: slower ones keep leaning on a stock-bond hedge after it has changed, as in
2022; not checked by period). With priors equal
within each class, covariance noise between sectors is not what costs the book.

Usage:
    uv run python research/research_corr_block.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data.bundle import as_bundle
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.risk import SampleCovariance
from quantzy.risk.base import RiskModel
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
CLASS_OF = {t: c for c, ts in core.CLASSES.items() for t in ts}
MODELS = {
    "saved": (None, 0.0, None),
    "identity 0.1": ("identity", 0.1, None),
    "identity 0.25": ("identity", 0.25, None),
    "block 0.25": ("block", 0.25, None),
    "block 0.5": ("block", 0.5, None),
    "block 1": ("block", 1.0, None),
    "corr hl 120": (None, 0.0, (120, 360)),
    "corr hl 250": (None, 0.0, (250, 750)),
    "corr hl 500": (None, 0.0, (500, 1000)),
    "block 0.5, hl 250": ("block", 0.5, (250, 750)),
}


def block_target(rho: np.ndarray, cols: list[str]) -> np.ndarray:
    """Each off-diagonal entry replaced by the mean over its pair of classes; diagonal 1."""
    cls = np.array([CLASS_OF[c] for c in cols])
    off = ~np.eye(len(cols), dtype=bool)
    t = np.eye(len(cols))
    for a in np.unique(cls):
        for b in np.unique(cls):
            m = np.outer(cls == a, cls == b) & off
            if m.any():
                t[m] = rho[m].mean()
    return t


def nearest_psd(rho: np.ndarray) -> np.ndarray:
    ev, vec = np.linalg.eigh(rho)
    if ev.min() > 1e-8:
        return rho
    x = vec @ np.diag(np.clip(ev, 1e-8, None)) @ vec.T
    d = np.sqrt(np.diag(x))
    return x / np.outer(d, d)


@dataclass
class ShrunkCorrelation:
    """``base``'s vols; correlations from ``corr_model`` (or ``base``), shrunk toward a target."""

    base: RiskModel
    target: str | None = None  # "identity", "block" or None
    delta: float = 0.0
    corr_model: RiskModel | None = None

    def correlation(self, bundle, asof) -> tuple[pd.DataFrame, list[str], np.ndarray]:
        cov = self.base.covariance(bundle, asof)
        cols = [c for c in cov.index if c != CASH]
        src = self.corr_model.covariance(bundle, asof) if self.corr_model is not None else cov
        s = src.loc[cols, cols].to_numpy()
        sd = np.sqrt(np.clip(np.diag(s), 1e-18, None))
        rho = s / np.outer(sd, sd)
        if self.target == "identity":
            rho = (1 - self.delta) * rho + self.delta * np.eye(len(cols))
        elif self.target == "block":
            rho = nearest_psd((1 - self.delta) * rho + self.delta * block_target(rho, cols))
        return cov, cols, rho

    def covariance(self, data, asof=None) -> pd.DataFrame:
        bundle = as_bundle(data)
        cov, cols, rho = self.correlation(bundle, asof)
        vol = np.sqrt(np.clip(np.diag(cov.loc[cols, cols].to_numpy()), 0, None))
        out = cov.copy()
        out.loc[cols, cols] = rho * np.outer(vol, vol)
        return out

    @property
    def min_history(self) -> int:
        need = [self.base.min_history]
        if self.corr_model is not None:
            need.append(self.corr_model.min_history)
        return max(need)


def strategy(name: str):
    target, delta, corr = MODELS[name]
    s = core.build()
    if name != "saved":
        cm = SampleCovariance(halflife=corr[0], lookback=corr[1]) if corr else None
        s.risk_model = ShrunkCorrelation(core.PORTFOLIO.risk_model(), target, delta, cm)
    return s


def run_one(args):
    name, start = args
    b = latest_complete(core.load(), core.RISKY)
    s = strategy(name)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    conds = []
    if isinstance(s.risk_model, ShrunkCorrelation):
        for asof in r.resample("ME").last().index:
            a = b.index[b.index.searchsorted(asof, "right") - 1]
            ev = np.linalg.eigvalsh(s.risk_model.correlation(b, a)[2])
            conds.append(ev.max() / ev.min())
    else:
        for asof in r.resample("ME").last().index:
            a = b.index[b.index.searchsorted(asof, "right") - 1]
            c = s.risk_model.covariance(b, a).drop(index=CASH, columns=CASH).to_numpy()
            sd = np.sqrt(np.diag(c))
            ev = np.linalg.eigvalsh(c / np.outer(sd, sd))
            conds.append(ev.max() / ev.min())
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, res.weights, turnover, float(np.median(conds))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    start = max(core.warmup(b, strategy(n)) for n in MODELS)
    print(f"common start {b.index[start].date()}")
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, [(n, start) for n in MODELS])}
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "sharpe": t["full"]["sharpe"], "tr_sharpe": t["train"]["sharpe"],
        "te_sharpe": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[list(MODELS)]
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS})
    m["diff t"] = ex.mean() / ex.std() * np.sqrt(len(ex))
    m.loc["saved", "diff t"] = np.nan
    w = {n: v[1].drop(columns=CASH) for n, v in out.items()}
    m["eff N"] = [(1 / (w[n].div(w[n].sum(axis=1), axis=0) ** 2).sum(axis=1)).mean()
                  for n in MODELS]
    m["sectors"] = [w[n][core.CLASSES["US sectors"]].sum(axis=1).mean() for n in MODELS]
    m["Treasuries"] = [w[n][core.CLASSES["Treasuries"]].sum(axis=1).mean() for n in MODELS]
    m["turnover"] = [out[n][2] for n in MODELS]
    m["median cond"] = [out[n][3] for n in MODELS]
    idx = runs["saved"].index
    pct = "{:.1%}".format
    print(f"\nCORE BOOK BY CORRELATION MODEL, {idx[0].date()} .. {idx[-1].date()}, split "
          f"{SPLIT.date()} | vols from the saved blend in every model")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "sectors",
                                                   "Treasuries")}
                      | {c: "{:.2f}".format for c in ("sharpe", "tr_sharpe", "te_sharpe")}
                      | {"diff t": "{:+.1f}".format, "eff N": "{:.1f}".format,
                         "turnover": "{:.1f}".format, "median cond": "{:.0f}".format}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
