"""Implied vol in both places: Sigma's vols and the vol that scales ``mu``.

``research_implied_vol_risk.py`` put VIX / MOVE-based implied vol into the risk model only.
Forecasts improved, the book got worse (1.16 against 1.21, down in the test half), and the
likely reason is an inconsistency it created: every alpha is ``vol_i * Sharpe view``, with
``vol_i`` the ETF's trailing 60-day vol, so raising Sigma's vol by a factor ``s`` while
``mu`` stays put cuts the weight by ``1 / s^2`` instead of ``1 / s`` -- hardest after a
selloff, when implied vol is high and the book needs to re-enter. This makes the two
consistent. **Fixed before any test; no parameter is re-chosen.**

Each ETF's multiplier ``s_i = vol_new / vol_saved`` is the factor the earlier candidate
applied to its risk-model vol (same mapping, same 0.5 weight, same assets). Here ``mu_i`` is
multiplied by the same ``s_i`` at the same date, so ``mu_i / vol_i`` -- the Sharpe view --
is unchanged and implied vol only resizes positions, as vol targeting would.

* ``saved``.
* ``iv 0.5 sigma`` (the earlier candidate, for reference).
* ``iv 0.5 both`` (**the candidate**): equities and Treasuries, Sigma and ``mu``.
* Sensitivity only, not candidates: ``iv 1.0 both``, ``iv 0.5 both equity``.

The premium-kept ``raw`` variant scored best last time; it is left out so the result is
not picked from it.

Adoption rule, as before: +0.02 Sharpe over the window and in both halves, ``PORTFOLIO``
fixed, saved core book (no overlays). Reported also: return, vol, drawdown, turnover,
average equity weight, the 2008, 2020 and 2022 episodes, and the equity weight three months
after the March 2020 low.

**Result (2026-10-31): not adopted; the inconsistency was not the cause.** 2008-09-10 ..
2026-10-30. Saved 1.21 (train 1.24 / test 1.20). The candidate, ``iv 0.5 both``: 1.15
(1.23 / 1.09), return difference t -3.0, max drawdown -15.3% against -14.8%, worse in 2008,
2020 and 2022. Sensitivities: ``iv 1.0 both`` 1.12, ``iv 0.5 both equity`` 1.16. Scaling
``mu`` restored the average equity weight (0.70 against 0.71; 0.67 with Sigma alone) but not
the re-entry: three months after the March 2020 low equities were 38% of capital against 46%
saved. Sizing on implied vol cuts exposure when implied vol is high, after selloffs -- when
forward equity returns have been high, which is the variance risk premium the ``vrp`` alpha
already buys. Implied vol in sizing fights that alpha. Idea closed.

Usage:
    uv run python research/research_implied_vol_mu.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from types import MethodType

import numpy as np
import pandas as pd
from research_implied_vol_risk import (
    EPISODES,
    SPLIT,
    TREASURIES,
    YEAR,
    ImpliedVolRisk,
    implied_vols,
)
from research_implied_vol_risk import load as load_bundle

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data.bundle import as_bundle
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
BOTH = (*core.EQUITY, *TREASURIES)
# name -> (weight on implied vol, assets, scale mu too)
MODELS = {
    "saved": (0.0, (), False),
    "iv 0.5 sigma": (0.5, BOTH, False),
    "iv 0.5 both": (0.5, BOTH, True),
    "iv 1.0 both": (1.0, BOTH, True),
    "iv 0.5 both equity": (0.5, tuple(core.EQUITY), True),
}
CANDIDATE = "iv 0.5 both"


def strategy(name: str, b):
    w, assets, scale_mu = MODELS[name]
    s = core.build()
    if not w:
        return s
    base = core.PORTFOLIO.risk_model()
    risk = ImpliedVolRisk(base, implied_vols(b, True), w, assets)
    s.risk_model = risk
    if scale_mu:
        plain_mu = s.mu

        def mu(self, data, asof=None):
            bundle = as_bundle(data)
            old = np.diag(base.covariance(bundle, asof).to_numpy())
            new = np.diag(risk.covariance(bundle, asof).to_numpy())
            cols = base.covariance(bundle, asof).index
            ratio = pd.Series(np.sqrt(new / np.where(old > 0, old, np.nan)), index=cols)
            m = plain_mu(bundle, asof)
            return m * ratio.reindex(m.index).fillna(1.0)

        s.mu = MethodType(mu, s)
    return s


def run_one(args):
    name, start = args
    b = load_bundle()
    s = strategy(name, b)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, res.weights, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    b = load_bundle()
    start = max(core.warmup(b, core.build()), core.PORTFOLIO.risk_model().min_history + YEAR,
                int(b["move"].iloc[:, 0].notna().values.argmax()) + YEAR + 1)
    print(f"common start {b.index[start].date()} .. {b.index[-1].date()}")
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, [(n, start) for n in MODELS])}
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "sharpe": t["full"]["sharpe"], "tr_sharpe": t["train"]["sharpe"],
        "te_sharpe": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[list(MODELS)]
    m["turnover"] = [out[n][2] for n in m.index]
    for e, (a, z) in EPISODES.items():
        m[e] = [float((1 + runs[n].loc[a:z]).prod() - 1) for n in m.index]
    eq = {n: out[n][1][core.EQUITY].sum(axis=1) for n in MODELS}
    m["equity w"] = [float(eq[n].mean()) for n in m.index]
    m["eq w 2020-06-23"] = [float(eq[n].loc[:"2020-06-23"].iloc[-1]) for n in m.index]
    print(f"\nCORE BOOK (no overlays), split {SPLIT.date()}, PORTFOLIO fixed; "
          f"candidate: {CANDIDATE}")
    print(m.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS if n != "saved"})
    print("\n   difference from saved, t-stat: " + ", ".join(
        f"{n} {v:+.1f}" for n, v in (ex.mean() / ex.std() * np.sqrt(len(ex))).items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
