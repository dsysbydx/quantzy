"""Re-entry after equity drawdowns: slower sizing vol (A) and a VIX-based equity view (B).

The saved book cuts equities hard in a crash and adds them back slowly: about 25% of capital
at the March 2020 trough, 50% three months later, while US sectors rose 48% in six months.
The prior-only book does the same, so this is not the trend alpha. It is sizing: forecast
vol jumps, the prior's ``mu`` rises with vol but the variance with vol squared, so weights
fall roughly as 1 / vol (correlation of equity weight with equity vol: -0.63).

**Fixed before any test.**

A. *Long-run vol in the risk model* (Carver; also the Pine script a friend shared). Each
   asset's forecast vol becomes ``(1 - w) * vol_model + w * vol_long``, where ``vol_long`` is
   the trailing 10-year (2520-row) mean of its 60-day realized vol, from 2 years of history,
   known at t-1. Correlations are unchanged. ``w = 0.3`` (Carver's value) is the test;
   ``w = 0.5`` is a sensitivity check. This changes a portfolio-level setting, so it is
   judged on the book, not on IC.
B. *VIX-based equity expected return.* The variance risk premium,
   ``VRP = (VIX / 100)^2 - realized variance of SPY over 21 days (annualized)``, has been
   found to predict equity returns over the next one to three months (Bollerslev, Tauchen
   and Zhou, 2009), and it is highest just after crashes. Its score is the z-score against
   the trailing 5 years (from 2 years of history), capped at +/-2, the same for every equity
   ETF (US sectors, QQQ, EFA, EEM) and absent elsewhere. A second rule uses the z-score of
   log VIX instead. Each enters ``mu`` like the trend alpha, score times vol times ``k``,
   with ``k`` matched to the trend alpha's train-window dispersion, added on top of
   ``ALPHAS``.

Tests: IC of B on the equity ETFs and on SPY back to 1993 (1, 21 and 63 days, both fold
directions); then the book for A, B and A + B, with equity weight and returns around the
troughs.

**Result (2026-10-16).** A: long-run vol re-enters faster (equity weight three months after
the 2020 low 50% -> 63% at w = 0.3) but also de-risks slower into crashes; Sharpe 1.05 and
0.99 against 1.06, not adopted. B: VRP has IC (SPY 1-day +0.023, t 2.4) and lifted the book
to 1.09 with a smaller drawdown; log VIX has no IC. VRP was at -2 at the March 2020 low, so it
does not fix that re-entry. VRP went on to ``research_vrp.py`` and was adopted.

Usage:
    uv run python examples/research_reentry.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.data.bundle import as_bundle
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.risk.base import RiskModel
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core
from quantzy.strategy import Strategy

EQUITY = [*core.CLASSES["US sectors"], *core.CLASSES["equity index"]]
SPLIT = pd.Timestamp("2017-07-26")
CHUNK, NW_LAGS, VOL_IC = 47, 21, 60
Z_WINDOW, Z_MIN = 1260, 504
LONG_VOL_WINDOW, LONG_VOL_MIN = 2520, 504
TROUGHS = {"2009-03": "2009-03-09", "2011-10": "2011-10-03", "2018-12": "2018-12-24",
           "2020-03": "2020-03-23", "2022-10": "2022-10-12"}


# --- data --------------------------------------------------------------------------------------

def vix_scores(start: str = "1990-01-01") -> pd.DataFrame:
    """Daily VRP and log-VIX z-scores (capped at +/-2), on SPY's dates, known at that close."""
    raw = CachedReader(YFinanceReader()).history(["^VIX", "SPY"], start=start)
    vix, spy = raw["^VIX"].ffill(), raw["SPY"]
    rv = (spy.pct_change() ** 2).rolling(21, min_periods=21).mean() * TRADING_DAYS
    raw_sig = pd.DataFrame({"vrp": (vix / 100) ** 2 - rv, "vix": np.log(vix)})
    mean = raw_sig.rolling(Z_WINDOW, min_periods=Z_MIN).mean()
    sd = raw_sig.rolling(Z_WINDOW, min_periods=Z_MIN).std()
    out = ((raw_sig - mean) / sd).clip(-2, 2)
    out["spy"] = spy
    return out[spy.notna()]


def load() -> DataBundle:
    """The saved book's prices, plus ``vrp`` and ``vix`` score panels on the equity ETFs."""
    px = core.load().prices
    s = vix_scores().reindex(px.index).ffill()
    panels = {}
    for name in ("vrp", "vix"):
        p = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
        for t in EQUITY:
            p[t] = s[name]
        panels[name] = p.where(px.notna())
    return DataBundle.build(px, **panels)


# --- B: the signal -----------------------------------------------------------------------------

@dataclass
class MarketStateView(Signal):
    """A bundle score panel (``vrp`` or ``vix``) times annualized vol: Sharpe units."""

    name: ClassVar[str] = "market_state"
    panel: str = "vrp"
    vol_window: int = 60

    def raw(self, data: DataBundle) -> pd.DataFrame:
        return data[self.panel] * realized_vol(data.prices, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return self.vol_window + 1


# --- A: the risk model -------------------------------------------------------------------------

@dataclass
class LongRunVolBlend:
    """Blend each asset's forecast vol with its long-run average; correlations unchanged."""

    base: RiskModel
    weight: float = 0.3
    vol_window: int = 60
    _cache: tuple[tuple, pd.DataFrame] | None = field(
        default=None, init=False, repr=False, compare=False)

    def long_vol(self, bundle: DataBundle) -> pd.DataFrame:
        key = bundle.fingerprint()
        if self._cache is None or self._cache[0] != key:
            v = realized_vol(bundle.prices, self.vol_window, annualize=True)
            lv = v.rolling(LONG_VOL_WINDOW, min_periods=LONG_VOL_MIN).mean().shift(1)
            self._cache = (key, lv)
        return self._cache[1]

    def covariance(self, data, asof=None) -> pd.DataFrame:
        bundle = as_bundle(data)
        cov = self.base.covariance(bundle, asof)
        lv = self.long_vol(bundle)
        pos = len(lv) - 1 if asof is None else lv.index.searchsorted(asof, "right") - 1
        long = lv.iloc[pos].reindex(cov.index).to_numpy()
        vol = np.sqrt(np.clip(np.diag(cov.to_numpy()), 0, None))
        blend = (1 - self.weight) * vol + self.weight * long
        m = np.where(np.isfinite(blend) & (vol > 1e-4), blend / np.where(vol > 1e-4, vol, 1), 1.0)
        return cov * np.outer(m, m)

    @property
    def min_history(self) -> int:
        return self.base.min_history


# --- 1. IC -------------------------------------------------------------------------------------

def fmt(s: pd.Series, lags: int) -> str:
    s = s.dropna()
    return f"{s.mean():+.4f} ({newey_west_t(s, lags):+.1f})"


def daily_ic(scores: pd.DataFrame, px: pd.DataFrame, h: int,
             fit: pd.Series | None = None) -> pd.Series:
    zx, zy = _ts_pairs(scores, px, h, "spearman", VOL_IC, fit=fit)
    return (zx * zy).mean(axis=1)


def ic_table(scores: pd.DataFrame, px: pd.DataFrame, index: pd.Index) -> dict[str, str]:
    folds = chunk_folds(index, CHUNK)
    a, b = folds == 0, folds == 1
    row = {}
    for h in (1, 21, 63):
        lags = max(NW_LAGS, 2 * h)
        if h == 1:
            held = pd.concat([daily_ic(scores, px, 1, a)[b], daily_ic(scores, px, 1, b)[a]])
            row["1d held out, both"] = fmt(held, lags)
        full = daily_ic(scores, px, h)
        row[f"h={h}"] = fmt(full, lags)
        row[f"h={h} 2010+"] = fmt(full.loc["2010":], lags)
    return row


def ic_report(bundle: DataBundle) -> None:
    px = bundle.prices[EQUITY]
    s = vix_scores()
    spy = s[["spy"]]
    rows = {}
    for name in ("vrp", "vix"):
        sc = bundle[name][EQUITY].shift(1)
        rows[(name, "12 equity ETFs")] = ic_table(sc, px, bundle.index)
        sspy = s[[name]].rename(columns={name: "spy"}).shift(1)
        rows[(name, "SPY 1993+")] = ic_table(sspy, spy, s.index)
    print("\n1. TIME-SERIES RANK IC of the market-state scores vs forward return / trailing vol "
          f"* sqrt(h). {CHUNK}-day folds for 1 day; Newey-West t with max(21, 2h) lags")
    print(pd.DataFrame(rows).T.to_string())
    print("\n   score at the troughs (vrp, vix):")
    for lab, t in TROUGHS.items():
        r = s.loc[:t].iloc[-1]
        print(f"     {lab}: vrp {r['vrp']:+.2f}  vix {r['vix']:+.2f}")


# --- 2. portfolio ------------------------------------------------------------------------------

def strategy(variant: str, k: float) -> Strategy:
    alphas = dict(core.ALPHAS)
    risk = core.PORTFOLIO.risk_model()
    if variant.startswith("A"):
        risk = LongRunVolBlend(risk, weight=0.5 if "0.5" in variant else 0.3)
    if "VRP" in variant:
        alphas["vrp"] = (MarketStateView("vrp", normalizer=Identity()), k)
    if "log VIX" in variant:
        alphas["vix"] = (MarketStateView("vix", normalizer=Identity()), k)
    return Strategy(universe=core.UNIVERSE,
                    signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=risk, optimizer=core.PORTFOLIO.optimizer(core.CASH),
                    name=variant)


def run_one(args: tuple[str, float]) -> tuple[str, pd.Series, pd.DataFrame, float]:
    variant, k = args
    bundle = load()
    s = strategy(variant, k)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle, core.build())
                                ).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return variant, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def portfolio_report(bundle: DataBundle, workers: int) -> None:
    trend_sd = (core.ALPHAS["trend"][0].raw(bundle)
                / realized_vol(bundle.prices, 60, annualize=True))[EQUITY].loc[:SPLIT].stack().std()
    k = {n: 0.1 * trend_sd / bundle[n][EQUITY].loc[:SPLIT].stack().std()
         for n in ("vrp", "vix")}
    print(f"\n   k matched to the full trend budget's dispersion (0.1 on EMA-200): "
          f"VRP {k['vrp']:.3f}, log VIX {k['vix']:.3f}")
    jobs = {"saved book": 0.0, "A: long-run vol 0.3": 0.0, "A: long-run vol 0.5": 0.0,
            "B: + VRP": k["vrp"], "B: + log VIX": k["vix"], "A 0.3 + B VRP": k["vrp"]}
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, jobs.items())}
    start = max(v[0].index[0] for v in out.values())
    runs = {n: v[0].loc[start:] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[list(jobs)]
    m["turnover"] = [out[n][2] for n in m.index]
    m["avg eq wt"] = [out[n][1][EQUITY].loc[start:].sum(axis=1).mean() for n in m.index]
    m["gross"] = [out[n][1][core.RISKY].loc[start:].sum(axis=1).mean() for n in m.index]
    pct = "{:.1%}".format
    print(f"\n2. PORTFOLIO, {start.date()} .. {runs['saved book'].index[-1].date()}, split "
          f"{SPLIT.date()} | other settings as PORTFOLIO")
    print(m.to_string(formatters={**{c: pct for c in ("ret", "vol", "max_dd", "avg eq wt")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe", "gross")},
                                  "turnover": "{:.1f}".format}))

    idx = runs["saved book"].index
    rows = {}
    for lab, day in TROUGHS.items():
        i = idx.get_indexer([pd.Timestamp(day)], method="nearest")[0]
        for n in jobs:
            r, w = runs[n], out[n][1].reindex(idx)[EQUITY].sum(axis=1)
            rows[(lab, n)] = {"eq wt trough": w.iloc[i],
                              "eq wt +3m": w.iloc[min(i + 63, len(w) - 1)],
                              "ret 6m after": (1 + r.iloc[i + 1:i + 127]).prod() - 1,
                              "ret 6m before": (1 + r.iloc[max(i - 126, 0):i + 1]).prod() - 1}
    print("\n   around US equity troughs: equity weight (share of capital) and book return")
    print(pd.DataFrame(rows).T.to_string(float_format="{:.0%}".format))
    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    print("\n   calendar years")
    print(yr[list(jobs)].to_string(float_format="{:+.1%}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    bundle = load()
    ic_report(bundle)
    portfolio_report(bundle, workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
