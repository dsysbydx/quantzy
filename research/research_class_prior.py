"""Class-level (equilibrium) Sharpe prior for equities, instead of one prior per ETF.

The prior gives every equity ETF ``mu_i = 0.30 * vol_i``. Under it an index always loses to
its constituents (``research_universe_broad.py``: SPY and ACWI are never held; QQQ ~3%):
a basket of sectors expects 0.30 x their average vol at a lower vol, so diversification
looks like alpha, and the book leans to the sectors (65% of NAV). The professional fix is
Black-Litterman's equilibrium prior, ``mu = lambda * Sigma * w``: returns implied by a
benchmark, so the benchmark needs no tilt and only the alphas move away from it. Within a
class: ``mu_i = beta_i * mu_class``, ``beta_i`` the ETF's beta to the class benchmark.
**Fixed before any test.**

Under the saved priors the equal-weight equity portfolio already expects ~0.35 Sharpe, not
0.30 (diversification), so changing to a class prior at 0.30 would also cut the *level* of
equity returns -- which earlier studies show mostly resizes the book. So the candidate keeps
the level:

* ``saved``.
* ``A: equity class, level-neutral`` (**the candidate**): the 12 equity ETFs as one class,
  equal-weight benchmark ``w``. Each date, ``mu_class = w' prior`` (the saved priors' own
  expected return for ``w``), then ``mu_i = beta_i * mu_class`` with ``beta = Sigma w / (w'
  Sigma w)`` from the saved risk model at the same date. Non-equity priors, every alpha,
  ``PORTFOLIO`` unchanged.
* ``B: US sectors only, level-neutral`` (diagnostic): as A on the 9 sector SPDRs; QQQ, EFA,
  EEM keep per-asset priors.
* ``C: equity class at 0.30`` (sensitivity): as A, but ``mu_class = 0.30 * vol(w)``: the
  benchmark gets exactly the judgment prior, a lower level.

Saved core book (no overlays) and with overlays, split 2017-07-26. Rule: +0.02 Sharpe over
the window and in both halves. Reported also: concentration (effective number of risky
positions ``1 / sum w^2``, top-four weight), US sectors / equity / Treasuries weight,
turnover.

**Result (2026-11-01): none adopted; per-asset priors are not what concentrates the book.**
2008-09-10 .. 2026-10-30. Saved 1.211 (train 1.235 / test 1.198). A, the candidate: 1.237
(1.353 / 1.143), max drawdown -16.4% against -14.8%; +0.026 overall, all of it in the train
half, -0.055 in the test half (difference t +0.2). B: 1.217 (1.310 / 1.142). C: 1.284 (1.366 /
1.206) at 9.0% vol against 10.0% -- a smaller book, the effect ``research_prior_weight``
found, with +0.008 in the test half. With overlays: 1.401 / 1.381 / 1.442 against 1.371,
the test half 1.330 / 1.330 / 1.390 against 1.379. Every variant moves ~9-13 points from the
sectors into QQQ / EFA / EEM and Treasuries, but concentration does not move: effective N
4.3 and top-four weight 83-86% in all four. Equal per-asset priors explain why an index is
never held next to its constituents; the concentration itself comes from the alphas and the
fast correlations (``research_corr_anchor``).

Usage:
    uv run python research/research_class_prior.py
"""

from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from types import MethodType

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.backtest import WalkForwardBacktester  # noqa: E402
from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data.bundle import as_bundle  # noqa: E402
from quantzy.jobs.daily import latest_complete  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

SPLIT = pd.Timestamp("2017-07-26")
SECTORS = core.CLASSES["US sectors"]
# name -> (class members, level: "neutral" or a Sharpe for the benchmark)
MODELS = {
    "saved": None,
    "A: equity class, level-neutral": (list(core.EQUITY), "neutral"),
    "B: US sectors only, level-neutral": (list(SECTORS), "neutral"),
    "C: equity class at 0.30": (list(core.EQUITY), 0.30),
}
CANDIDATE = "A: equity class, level-neutral"


def strategy(name: str):
    s = core.build()
    spec = MODELS[name]
    if spec is None:
        return s
    members, level = spec
    plain_mu, prior, k = s.mu, s.signals["prior"], s.weights["prior"]
    w = np.full(len(members), 1.0 / len(members))

    def mu(self, data, asof=None):
        bundle = as_bundle(data)
        m = plain_mu(bundle, asof).copy()
        p = prior.forecast(bundle, asof).reindex(members) * k
        cov = self.risk_model.covariance(bundle, asof).loc[members, members].to_numpy()
        var = float(w @ cov @ w)
        if not np.isfinite(var) or var <= 0 or p.isna().any():
            return m
        beta = cov @ w / var
        mu_class = float(w @ p.to_numpy()) if level == "neutral" else level * np.sqrt(var)
        m[members] = m[members] - p.to_numpy() + beta * mu_class
        return m

    s.mu = MethodType(mu, s)
    return s


def run_one(name: str):
    b = latest_complete(core.load(), core.RISKY)
    start = core.warmup(b, core.build())
    s = strategy(name)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, res.weights.reindex(r.index).fillna(0.0), turnover


def main() -> None:
    pd.set_option("display.width", 240)
    with ProcessPoolExecutor(max_workers=4) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, list(MODELS))}
    b = core.load()
    runs = {n: v[0] for n, v in out.items()}
    with_ov = {f"{n} + overlays": export.with_overlays(r, b) for n, r in runs.items()}
    t = compare({**runs, **with_ov}, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    print(f"{runs['saved'].index[0].date()} .. {runs['saved'].index[-1].date()}, split "
          f"{SPLIT.date()}; candidate: {CANDIDATE}")
    print(m.to_string(float_format="{:.3f}".format))

    rows = {}
    for n, (_, w, turnover) in out.items():
        risky = w[core.RISKY].clip(lower=0)
        eff = 1 / risky.pow(2).sum(axis=1).where(lambda x: x > 0)
        top4 = risky.apply(lambda x: x.nlargest(4).sum(), axis=1)
        rows[n] = {"eff N": eff.median(), "top-4": top4.mean(),
                   "US sectors": risky[SECTORS].sum(axis=1).mean(),
                   "QQQ/EFA/EEM": risky[["QQQ", "EFA", "EEM"]].sum(axis=1).mean(),
                   "Treasuries": risky[["IEF", "TLT"]].sum(axis=1).mean(),
                   "gross": risky.sum(axis=1).mean(), "turnover": turnover}
    print("\nConcentration and weights (x NAV, averages; eff N median)")
    print(pd.DataFrame(rows).T.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS if n != "saved"})
    print("\n   difference from saved, t-stat: " + ", ".join(
        f"{n} {v:+.1f}" for n, v in (ex.mean() / ex.std() * np.sqrt(len(ex))).items()))


if __name__ == "__main__":
    main()
