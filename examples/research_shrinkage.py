"""Correlation shrinkage in the shared risk model, on the saved 17 and on 65 ETFs.

The risk model blends EWMA covariances with 5, 20 and 60-day halflives over 250 days. An EWMA
with halflife ``h`` behaves like about ``2.9 h`` observations, so the 5-day part rests on
about 14 days: too few to estimate a 17 x 17 correlation matrix, let alone 65 x 65. On the
37 equity ETFs of the broad universe the correlation matrix's condition number reached
4,000-14,000, and with equal 0.30 priors the optimizer saw a sleeve Sharpe of 1.0-1.5:
near-riskless spreads that are estimation noise. **Fixed before any test.**

Risk models (vols always from the saved blend; only correlations change):

* ``none``: the saved model.
* ``delta 0.1``: correlations ``0.9 * rho + 0.1 * I`` (the candidate).
* ``delta 0.25``: ``0.75 * rho + 0.25 * I``.
* ``Ledoit-Wolf``: the same target, with the intensity estimated each day (Ledoit and Wolf,
  2004) from the last 250 days of returns scaled by their own vol.
* ``delta 0.1, slow corr``: correlations from a 250-day halflife EWMA over 750 days,
  then shrunk 0.1; vols still from the fast blend.

Each on the saved 17 (``core_multiasset`` as is) and on the 65 of
``research_broad_universe.py`` (equal priors, class limits; its unshrunk result was 0.88).
Reported: Sharpe in both halves, vol, drawdown, turnover, weight by class, and the average
condition number of the correlation matrix the optimizer used.

**Result (2026-10-17): no shrinkage.** It cuts the median condition number by 90-99% but
does not help: on the 17 (2010-02 ..) Sharpe 1.22 unshrunk against 1.18 (delta 0.1), 1.14
(0.25), 1.21 (Ledoit-Wolf, which picks only ~0.04 from the 250-day sample) and 1.18 (slow
correlations); on the 65 (2012-01 ..) 0.81 against 0.77-0.81. EM stays at 22% of capital
under every model, so its pile-up comes from equal priors, not from covariance noise: the
long-only rule already blocks the spurious long/short directions. Shrinking toward identity
also weakens the stock-bond hedge (rates 25% -> 19% at 0.25), raising vol.

Usage:
    uv run python examples/research_shrinkage.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import research_broad_universe as broad

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.data.bundle import as_bundle
from quantzy.research import compare
from quantzy.risk import SampleCovariance
from quantzy.risk.base import RiskModel
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
MODELS = ("none", "delta 0.1", "delta 0.25", "Ledoit-Wolf", "delta 0.1, slow corr")


@dataclass
class ShrunkCorrelation:
    """Keep ``base``'s vols; shrink its correlations (or ``corr_model``'s) toward identity."""

    base: RiskModel
    delta: float | None = 0.1  # None: Ledoit-Wolf intensity each day
    corr_model: RiskModel | None = None
    lw_window: int = 250
    _rets: tuple | None = field(default=None, init=False, repr=False, compare=False)

    def _lw_delta(self, bundle: DataBundle, asof, cols) -> float:
        key = bundle.fingerprint()
        if self._rets is None or self._rets[0] != key:
            self._rets = (key, bundle.prices.pct_change(fill_method=None))
        r = self._rets[1][cols].loc[:asof].iloc[:-1].iloc[-self.lw_window:].dropna()
        x = (r - r.mean()) / r.std()
        x = x.to_numpy()
        n, p = x.shape
        sample = x.T @ x / n
        x2 = x ** 2
        phi = (x2.T @ x2) / n - sample ** 2
        gamma = np.linalg.norm(sample - np.eye(p), "fro") ** 2
        return float(np.clip((phi.sum() - np.trace(phi)) / gamma / n, 0, 1)) if gamma > 0 else 0.0

    def covariance(self, data, asof=None) -> pd.DataFrame:
        bundle = as_bundle(data)
        cov = self.base.covariance(bundle, asof)
        cols = [c for c in cov.index if c != CASH]
        src = (self.corr_model.covariance(bundle, asof) if self.corr_model is not None else cov)
        s = src.loc[cols, cols].to_numpy()
        sd = np.sqrt(np.clip(np.diag(s), 1e-18, None))
        rho = s / np.outer(sd, sd)
        delta = self._lw_delta(bundle, asof, cols) if self.delta is None else self.delta
        rho = (1 - delta) * rho + delta * np.eye(len(cols))
        vol = np.sqrt(np.clip(np.diag(cov.loc[cols, cols].to_numpy()), 0, None))
        out = cov.copy()
        out.loc[cols, cols] = rho * np.outer(vol, vol)
        out.loc[cols, CASH] = cov.loc[cols, CASH]  # cash's tiny covariances left as they are
        out.loc[CASH, cols] = cov.loc[CASH, cols]
        return out

    @property
    def min_history(self) -> int:
        need = [self.base.min_history, self.lw_window + 2]
        if self.corr_model is not None:
            need.append(self.corr_model.min_history)
        return max(need)


def risk_model(name: str) -> RiskModel:
    base = core.PORTFOLIO.risk_model()
    if name == "none":
        return base
    if name == "delta 0.1":
        return ShrunkCorrelation(base, 0.1)
    if name == "delta 0.25":
        return ShrunkCorrelation(base, 0.25)
    if name == "Ledoit-Wolf":
        return ShrunkCorrelation(base, None)
    return ShrunkCorrelation(base, 0.1, corr_model=SampleCovariance(halflife=250, lookback=750))


def strategy(universe: str, model: str, costs: dict[str, float]):
    s = core.build() if universe == "17" else broad.strategy(broad.ALL, True, costs)
    s.risk_model = risk_model(model)
    return s


def bundle_for(universe: str) -> DataBundle:
    full = broad.load()
    tickers = list(core.RISKY) if universe == "17" else broad.ALL
    px = full.prices[[*tickers, CASH]]
    return DataBundle.build(px, **{k: full[k][px.columns] for k in ("vix", "market", "carry")})


def run_one(args):
    universe, model, costs, start = args
    bundle = bundle_for(universe)
    s = strategy(universe, model, costs)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    years = len(r) / TRADING_DAYS
    # condition number of the correlation matrix used, at month-ends
    conds = []
    for asof in r.resample("ME").last().index:
        a = bundle.index[bundle.index.searchsorted(asof, "right") - 1]
        c = s.risk_model.covariance(bundle, a).drop(index=CASH, columns=CASH).to_numpy()
        sd = np.sqrt(np.diag(c))
        ev = np.linalg.eigvalsh(c / np.outer(sd, sd))
        conds.append(ev.max() / ev.min())
    return (universe, model, r, res.weights, sum(d.turnover for d in res.decisions) / years,
            float(np.median(conds)))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    costs = broad.cost_tiers()
    b = bundle_for("65")
    probe = broad.strategy(broad.ALL, True, costs)
    probe.risk_model = risk_model("delta 0.1, slow corr")
    start65 = broad.warmup(b, probe, broad.ALL)
    b17 = bundle_for("17")
    s17 = core.build()
    s17.risk_model = risk_model("delta 0.1, slow corr")
    start17 = core.warmup(b17, s17)
    print(f"start: 17 from {b17.index[start17].date()}, 65 from {b.index[start65].date()}")
    jobs = [("17", m, costs, start17) for m in MODELS] + [("65", m, costs, start65)
                                                         for m in MODELS]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {(u, m): rest for u, m, *rest in pool.map(run_one, jobs)}
    pct = "{:.1%}".format
    cls = {t: c for c, ts in broad.CLASSES.items() for t in ts}
    for u in ("17", "65"):
        runs = {m: out[(u, m)][0] for m in MODELS}
        s0 = max(r.index[0] for r in runs.values())
        runs = {m: r.loc[s0:] for m, r in runs.items()}
        t = compare(runs, split_date=SPLIT)
        tab = pd.DataFrame({
            "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
            "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
            "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
        }).loc[list(MODELS)]
        tab["turnover"] = [out[(u, m)][2] for m in MODELS]
        tab["median cond"] = [out[(u, m)][3] for m in MODELS]
        ex = pd.DataFrame({m: runs[m] - runs["none"] for m in MODELS[1:]})
        tab["diff t vs none"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
        print(f"\n{u} ETFs, {s0.date()} .. {runs['none'].index[-1].date()}, split {SPLIT.date()}")
        print(tab.to_string(formatters={
            **{c: pct for c in ("ret", "vol", "max_dd")},
            **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t vs none")},
            "turnover": "{:.1f}".format, "median cond": "{:,.0f}".format}))
        w = {m: out[(u, m)][1].drop(columns=CASH).loc[s0:] for m in MODELS}
        print("   average weight by class, % of capital")
        print(pd.DataFrame({m: w[m].T.groupby(lambda x: cls[x]).sum().T.mean()
                            for m in MODELS}).T.reindex(columns=list(broad.CLASSES)).dropna(
            axis=1, how="all").to_string(float_format="{:.0%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
