"""Factor-cleaned correlations: keep the real factors fast, drop the noise beneath them.

The shared risk model (5/20/60-day EWMA blend over 250 days) rests on an effective ~48 days
for 17 assets (q = N / T = 0.36). On weekly dates since 2008-09 only 1-2 eigenvalues of its
correlation matrix sit above the Marchenko-Pastur noise edge (2.55); ~40% of the trace is in
eigenvalues pure noise could produce, and the median condition number is ~610 against ~16 for
pure noise of this size: the smallest directions look nearly riskless. The off-diagonals are
not too *heavy* (mean |rho| 0.45 against 0.47 for 3-year correlations); they are noisy below
the first factor or two.

Earlier studies shrank the whole matrix (``research_shrinkage``: identity, Ledoit-Wolf;
``research_corr_block``: class blocks, slower EWMAs; ``research_corr_anchor``: 3-5 year
correlations) and lost Sharpe: the fast correlations carry information, the stock-bond hedge
above all. Cleaning differs: the leading eigenvectors -- where that hedge lives -- keep their
fast estimate; only the noise band is replaced. **Fixed before any test.**

Vols always from the saved blend; only the correlation of the 17 risky ETFs changes:

* ``saved``.
* ``rmt clip``: eigenvalues below the noise edge ``(1 + sqrt(N / T))^2``, ``T`` = 48, are
  replaced by their average (the trace is kept), eigenvectors unchanged, rescaled to a unit
  diagonal (Laloux, Cizeau, Bouchaud and Potters, 1999).
* ``pca 1``, ``pca 2``, ``pca 3``: a statistical factor model, ``rho = L L' + D``, where ``L``
  is the top ``k`` eigenvectors times the square roots of their eigenvalues and ``D`` is
  diagonal, making the diagonal 1. Pairs are correlated only through the ``k`` factors.

Class factors (equity, rates, real assets, dollar) are left out: a constant correlation within
each pair of classes is close to ``block 1``, already tested (1.02 against 1.12).

Reported, independent of the alphas: each month, the global minimum-variance portfolio of
the 17 (budget 1, unconstrained, the portfolio most sensitive to covariance noise) under each
model; its realized vol over the next 21 days, and realized over predicted vol (1 = calibrated,
above 1 = the model thought the portfolio safer than it was). Then the saved core book (no
overlays: they do not use the risk model) under each model, from a common start, split
2017-07-26: Sharpe, both halves, vol, drawdown, turnover, effective number of positions, and
the median condition number.

Adoption rule (as ``research_corr_block``): a model must lift the book's Sharpe by at least
0.02 over the window *and* in both halves. Better minimum-variance numbers alone do not count.

**Result (2026-10-31): none adopted; the small eigenvalues are structure, not noise.**
2008-09-10 .. 2026-10-30. The minimum-variance portfolio is safest under the *saved* model
(realized vol 2.7% against 3.2-3.8% cleaned); none is better calibrated (realized / predicted
1.26 saved, 1.23-1.36 cleaned). Book: saved 1.21 (train 1.24 / test 1.20); rmt clip 1.12
(1.21 / 1.05); pca 1 1.15 (1.21 / 1.10); pca 2 1.22 (1.28 / 1.17); pca 3 1.21 (1.26 / 1.18).
pca 2 gains in the train half and loses in the test half (difference t +0.2). Cleaning cuts
the condition number (580 -> 23-88) but not risk.

Why the noise diagnosis misled: Marchenko-Pastur assumes assets with no structure, and these
17 overlap. On 2009-2026 daily correlations (T = 4,485, noise floor 0.88) the smallest
eigenvalues are 0.02 (QQQ against XLK and XLY; five sector SPDRs explain 96.5% of QQQ's
variance), 0.08 (TLT against IEF) and 0.09 (EFA against EEM and XLI), and the condition
number is 434, close to the fast model's. Those near-riskless directions are real near-
duplicates, which the long-only book cannot trade as spreads anyway; replacing them with an
average eigenvalue invents risk that is not there. With this universe, the covariance is not
the weak link -- consistent with ``research_shrinkage``, ``research_corr_anchor`` and
``research_corr_block``. A factor model would matter for a much larger universe (hundreds of
names), where N / T, not overlap, sets the conditioning.

Usage:
    uv run python research/research_corr_factor.py [--workers 6]
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
from quantzy.risk.base import RiskModel
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
EFFECTIVE_T = 48  # effective sample of the 5/20/60 blend over 250 days
MODELS = {"saved": None, "rmt clip": "rmt", "pca 1": 1, "pca 2": 2, "pca 3": 3}
GMV_HORIZON = 21


def clean(rho: np.ndarray, how: str | int | None) -> np.ndarray:
    """``rho`` cleaned by eigenvalue clipping (``"rmt"``) or a ``k``-factor model."""
    if how is None:
        return rho
    ev, vec = np.linalg.eigh(rho)  # ascending
    if how == "rmt":
        edge = (1 + np.sqrt(len(rho) / EFFECTIVE_T)) ** 2
        noise = ev < edge
        if noise.any():
            ev = ev.copy()
            ev[noise] = ev[noise].mean()
        x = vec @ np.diag(ev) @ vec.T
    else:
        load = vec[:, -how:] * np.sqrt(np.clip(ev[-how:], 0, None))
        x = load @ load.T
        np.fill_diagonal(x, 1.0)  # D = 1 - diag(L L'): residual variance, no residual correlation
    d = np.sqrt(np.diag(x))
    return x / np.outer(d, d)


@dataclass
class CleanedCorrelation:
    """``base``'s covariance with the risky block's correlation cleaned; cash untouched."""

    base: RiskModel
    how: str | int | None = None

    def covariance(self, data, asof=None) -> pd.DataFrame:
        cov = self.base.covariance(as_bundle(data), asof)
        cols = [c for c in cov.index if c != CASH]
        s = cov.loc[cols, cols].to_numpy()
        vol = np.sqrt(np.clip(np.diag(s), 1e-18, None))
        out = cov.copy()
        out.loc[cols, cols] = clean(s / np.outer(vol, vol), self.how) * np.outer(vol, vol)
        return out

    @property
    def min_history(self) -> int:
        return self.base.min_history


def strategy(name: str):
    s = core.build()
    if MODELS[name] is not None:
        s.risk_model = CleanedCorrelation(core.PORTFOLIO.risk_model(), MODELS[name])
    return s


def condition(cov: pd.DataFrame) -> float:
    c = cov.drop(index=CASH, columns=CASH).to_numpy()
    sd = np.sqrt(np.diag(c))
    ev = np.linalg.eigvalsh(c / np.outer(sd, sd))
    return float(ev.max() / ev.min())


def run_one(args):
    name, start = args
    b = latest_complete(core.load(), core.RISKY)
    s = strategy(name)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    month_ends = [b.index[b.index.searchsorted(a, "right") - 1]
                  for a in r.resample("ME").last().index]
    cond = float(np.median([condition(s.risk_model.covariance(b, a)) for a in month_ends]))
    w = res.weights.reindex(r.index).fillna(0.0)[core.RISKY]
    eff_n = float((1 / (w.pow(2).sum(axis=1).where(lambda x: x > 0))).median())
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, turnover, eff_n, cond


def gmv_report(start: int) -> None:
    """Global minimum-variance portfolio of the 17 under each model, monthly, out of sample."""
    b = latest_complete(core.load(), core.RISKY)
    rets = b.prices[core.RISKY].pct_change(fill_method=None)
    models = {n: strategy(n).risk_model for n in MODELS}
    dates = b.index[start:].to_series().resample("ME").last().dropna()
    rows = {n: {"realized": [], "ratio": []} for n in MODELS}
    for d in dates:
        i = b.index.get_loc(d)
        fwd = rets.iloc[i + 1: i + 1 + GMV_HORIZON]
        if len(fwd) < GMV_HORIZON:
            continue
        for n, m in models.items():
            c = m.covariance(b, d).loc[core.RISKY, core.RISKY].to_numpy()
            x = np.linalg.solve(c, np.ones(len(c)))
            w = x / x.sum()
            pred = np.sqrt(w @ c @ w)  # annualized by the risk model
            real = (fwd.to_numpy() @ w).std(ddof=1) * np.sqrt(TRADING_DAYS)
            rows[n]["realized"].append(real)
            rows[n]["ratio"].append(real / pred)
    t = pd.DataFrame({n: {"realized vol": np.mean(v["realized"]),
                          "realized / predicted": np.median(v["ratio"]),
                          "months": len(v["ratio"])} for n, v in rows.items()}).T
    print(f"\n1. GLOBAL MIN-VARIANCE PORTFOLIO of the 17, monthly, next {GMV_HORIZON} days "
          "(mean realized vol; median realized / predicted)")
    print(t.to_string(float_format="{:.3f}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    start = max(core.warmup(b, strategy(n)) for n in MODELS)
    print(f"common start {b.index[start].date()} .. {b.index[-1].date()}")
    gmv_report(start)
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
    m["eff_n"] = [out[n][2] for n in m.index]
    m["cond"] = [out[n][3] for n in m.index]
    print(f"\n2. CORE BOOK (no overlays), split {SPLIT.date()} | {core.PORTFOLIO}")
    print(m.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS if n != "saved"})
    t_diff = ex.mean() / ex.std() * np.sqrt(len(ex))
    print("\n   difference from saved, t-stat: "
          + ", ".join(f"{n} {v:+.1f}" for n, v in t_diff.items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
