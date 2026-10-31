"""Implied vol in the risk model: VIX for the equity ETFs, MOVE for Treasuries.

``research_downturn_corr.py`` found the risk model under-forecasts the book's vol in crash
months (1.31x), mostly through vols (1.28x) rather than correlations (1.07x): the 5/20/60
EWMA blend sees a vol spike only after it happens. Implied vol is forward-looking and moves
the same day. VIX was rejected as an *alpha* (``research_reentry``: log VIX has no IC) but has
not been tried in Sigma (``research_qqq_volmodel`` tried EWMA halflives and trend scaling).
**Fixed before any test.**

Implied vol per ETF. One index per class -- VIX (S&P 500, 30 days) for the 12 equity ETFs,
MOVE (Treasury yield vol, 1 month, in bp) for IEF and TLT -- mapped to each ETF by
``iv_i(t) = IV(t) * RV_i(1y) / mean(IV over 1y)``, where ``RV_i(1y)`` is the ETF's realized
vol over the trailing 252 days. The scale makes the ETF's implied vol average its own
realized vol over the past year, so it keeps implied vol's timing but not its level: the
variance premium (VIX above realized on average) is taken out, and so are the ETF's beta to
the index and, for MOVE, duration. Everything through row ``t``, then lagged one row like the
risk model, so both rest on the same information.

Models -- vols change, correlations stay those of the saved blend, other assets untouched:

* ``saved``: the 5/20/60 EWMA blend.
* ``iv 0.5`` (the candidate): each vol ``0.5 * EWMA + 0.5 * iv``, equities and Treasuries.
* ``iv 1.0``: implied vol alone.
* ``iv 0.5 equity``: the candidate on the equity ETFs only.
* ``iv 0.5 raw``: equities only, ``VIX * RV_i(1y) / RV_SPY(1y)``, premium kept: the naive
  version, as a check on the premium adjustment.

Tests, in order:

1. Forecast quality, weekly, next 21 days (``evaluate_vol``), averaged over the ETFs of each
   class: QLIKE (lower is better), bias (realized / forecast variance, 1 = unbiased), left-
   tail rate of returns beyond -2.33 forecast sds (1% if calibrated); on all dates, train /
   test (split 2017-07-26), and on SPY's worst 10% of forward months.
2. The saved core book (no overlays: they do not use the risk model) under each model,
   ``PORTFOLIO`` fixed, from a common start: Sharpe, both halves, vol, drawdown, turnover,
   and returns over 2008-09..11, 2020-02-19..04-30 and 2022.

Adoption rule: +0.02 Sharpe over the window and in both halves, as for the covariance
studies. A better forecast alone does not count.

**Result (2026-10-31): none adopted; implied vol forecasts better but sizes worse.**
2008-09-10 .. 2026-10-30. Forecasts: every implied-vol model beats the saved blend on
QLIKE, most in SPY's worst months (equities 0.13 for ``iv 0.5`` against 0.35 saved; -0.33
for ``iv 0.5 raw``; Treasuries -2.78 against -2.66), but all still under-forecast those
months (variance bias 2.0-2.2 against 2.3): implied vol does not see a crash coming much
sooner. Book: saved 1.21 (train 1.24 / test 1.20); ``iv 0.5`` 1.16 (1.27 / 1.07), ``iv 1.0``
1.12 (1.27 / 0.99), ``iv 0.5 equity`` 1.15 (1.28 / 1.05), the return difference significant
(t -2.2 to -2.7). ``iv 0.5 raw`` 1.23 (1.29 / 1.17) gains only in the train half, at lower
vol (9.0%) and return; it lost most in 2022 (-7.4% against -2.8%). Every variant gains in
train and loses in test, and holds less equity (63-68% against 71%).

Likely why (not tested): ``mu`` scales each prior by the ETF's own trailing 60-day vol, so
putting implied vol into Sigma alone lowers ``mu / var`` exactly when implied vol is high --
after selloffs, when the book most needs to re-enter -- and the 1-year realized-vol scale
keeps that effect for a year. Implied vol in ``mu``'s vol as well is the consistent version;
it is a new candidate, not a rescue of this one. Tested in
``research_implied_vol_mu.py``: it was not the cause (1.15).

Usage:
    uv run python research/research_implied_vol_risk.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare, evaluate_vol
from quantzy.risk.base import RiskModel
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
YEAR = TRADING_DAYS
TREASURIES = ["IEF", "TLT"]
GROUPS = {"equity": core.EQUITY, "Treasuries": TREASURIES}
# name -> (weight on implied vol, assets, premium-adjusted)
MODELS = {
    "saved": (0.0, (), True),
    "iv 0.5": (0.5, (*core.EQUITY, *TREASURIES), True),
    "iv 1.0": (1.0, (*core.EQUITY, *TREASURIES), True),
    "iv 0.5 equity": (0.5, tuple(core.EQUITY), True),
    "iv 0.5 raw": (0.5, tuple(core.EQUITY), False),
}
H = 21
EPISODES = {"2008-09..11": ("2008-09-10", "2008-11-30"),
            "2020-02-19..04-30": ("2020-02-19", "2020-04-30"), "2022": ("2022-01-01", "2022-12-31")}


def load() -> DataBundle:
    """The saved bundle plus a ``move`` panel (the MOVE index, broadcast)."""
    b = latest_complete(core.load(), core.RISKY)
    move = CachedReader(YFinanceReader()).history(["^MOVE"], start="2002-01-01")["^MOVE"]
    move = move.reindex(b.index).ffill()
    return DataBundle({**b, "move": pd.DataFrame({t: move for t in b.prices.columns})})


def implied_vols(b: DataBundle, adjusted: bool = True) -> pd.DataFrame:
    """Annualized implied vol per ETF (NaN where it has no index), through row ``t``."""
    rets = b.prices.pct_change(fill_method=None)
    rv1y = np.sqrt(rets.pow(2).rolling(YEAR, min_periods=YEAR).mean() * YEAR)
    vix = b["vix"].iloc[:, 0].ffill() / 100.0
    move = b["move"].iloc[:, 0]
    out = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
    if adjusted:
        for idx, assets in ((vix, core.EQUITY), (move, TREASURIES)):
            mean_iv = idx.rolling(YEAR, min_periods=YEAR).mean()
            for t in assets:
                out[t] = idx * rv1y[t] / mean_iv
    else:
        spy = b["market"].iloc[:, 0].ffill().pct_change(fill_method=None)
        rv_spy = np.sqrt(spy.pow(2).rolling(YEAR, min_periods=YEAR).mean() * YEAR)
        for t in core.EQUITY:
            out[t] = vix * rv1y[t] / rv_spy
    return out


@dataclass
class ImpliedVolRisk:
    """``base``'s covariance with chosen vols blended toward implied vol; correlations kept."""

    base: RiskModel
    iv: pd.DataFrame  # annualized, through row t
    weight: float
    assets: tuple[str, ...]
    lag: int = 1

    def covariance(self, data, asof=None) -> pd.DataFrame:
        bundle = as_bundle(data)
        cov = self.base.covariance(bundle, asof)
        if not self.weight or not self.assets:
            return cov
        idx = bundle.index
        pos = len(idx) - 1 if asof is None else idx.searchsorted(asof, "right") - 1
        iv = self.iv.iloc[pos - self.lag] if pos - self.lag >= 0 else None
        vol = pd.Series(np.sqrt(np.clip(np.diag(cov.to_numpy()), 0, None)), index=cov.index)
        new = vol.copy()
        for t in self.assets:
            if iv is not None and np.isfinite(iv.get(t, np.nan)) and vol[t] > 0:
                new[t] = (1 - self.weight) * vol[t] + self.weight * iv[t]
        scale = (new / vol.where(vol > 0)).fillna(1.0)
        return cov * np.outer(scale, scale)

    @property
    def min_history(self) -> int:
        return max(self.base.min_history, YEAR + self.lag)


def model(name: str, b: DataBundle) -> RiskModel:
    w, assets, adjusted = MODELS[name]
    base = core.PORTFOLIO.risk_model()
    return base if not w else ImpliedVolRisk(base, implied_vols(b, adjusted), w, assets)


def forecast_report(b: DataBundle, start: int) -> None:
    rets = b.prices.pct_change(fill_method=None)
    dates = b.index[start::5]
    spy = b["market"].iloc[:, 0].ffill()
    fwd = (spy.shift(-H) / spy - 1).reindex(dates)
    worst = fwd <= fwd.quantile(0.10)
    label = pd.Series(np.where(dates < SPLIT, "train", "test"), index=dates)
    crash = pd.Series(np.where(worst, "SPY worst 10%", "other"), index=dates)
    vols = {}
    for n in MODELS:
        m = model(n, b)
        rows = {d: np.sqrt(np.clip(np.diag(m.covariance(b, d).to_numpy()), 0, None)) for d in dates}
        vols[n] = pd.DataFrame(rows, index=b.prices.columns).T
    cols = ["qlike", "bias", "left_tail"]
    for g, assets in GROUPS.items():
        print(f"\n1. FORECAST QUALITY, {g} ETFs, next {H} days, mean over {len(assets)} ETFs "
              "(qlike lower is better; bias 1 = unbiased; left tail 1% = calibrated)")
        parts = []
        for by, tag in ((None, "all"), (label, None), (crash, None)):
            per = []
            for t in assets:
                f = {n: vols[n][t].dropna() for n in MODELS}
                e = evaluate_vol(f, rets[t].dropna(), horizon=H, by=by)
                per.append(e[cols])
            m = sum(per) / len(per)
            if tag:
                m.index = pd.MultiIndex.from_product([m.index, [tag]])
            parts.append(m)
        t = pd.concat(parts).sort_index()
        print(t.unstack().to_string(float_format="{:.3f}".format))


def run_one(args):
    name, start = args
    b = load()
    s = core.build()
    s.risk_model = model(name, b)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, res.weights, turnover


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    b = load()
    probe = core.build()
    start = max(core.warmup(b, probe), model("iv 0.5", b).min_history,
                int(b["move"].iloc[:, 0].notna().values.argmax()) + YEAR + 1)
    print(f"common start {b.index[start].date()} .. {b.index[-1].date()}")
    forecast_report(b, start)
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
    m["equity w"] = [float(out[n][1][core.EQUITY].sum(axis=1).mean()) for n in m.index]
    print(f"\n2. CORE BOOK (no overlays), split {SPLIT.date()}, PORTFOLIO fixed")
    print(m.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved"] for n in MODELS if n != "saved"})
    print("\n   difference from saved, t-stat: " + ", ".join(
        f"{n} {v:+.1f}" for n, v in (ex.mean() / ex.std() * np.sqrt(len(ex))).items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
