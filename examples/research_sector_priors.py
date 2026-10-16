"""Different expected returns across US sectors: relative momentum (C) and history (D).

Every US sector ETF gets the same prior Sharpe, 0.30, so the optimizer weights them by vol and
correlation alone: defensive XLP and XLU near 11-14% of capital, XLK 7% and QQQ 3%, while
realized Sharpe ran from 0.39 (XLE) to 0.85 (XLK) over 2008-2026. Across the 10 US equity
ETFs, realized Sharpe in the first half of the sample was 0.59 correlated with the second.
Point-in-time sector valuation and growth estimates are not in free data, so this tests the
two price-based ways to let sector expected returns differ.

**Fixed before any test.** The "US group" is the nine sector SPDRs plus QQQ.

C. *Relative momentum* (Moskowitz and Grinblatt's industry momentum): each ETF's return over
   the past 12 months skipping the latest one, ranked within the US group and mapped to a
   normal score (mean zero, so the group's total tilt is zero). Enters ``mu`` as score times
   vol times ``k``, ``k`` matched to the trend budget's train-window dispersion (0.1 on
   EMA-200), added on top of ``ALPHAS``.
D. *Long-run Sharpe prior*: ``SR_i = 0.25 * SR_history_i + 0.75 * SR_prior_i``, where the
   history is the asset's expanding-window Sharpe through t-1, used from 5 years of history
   (before that, the prior alone). It replaces the judgment prior. Two versions: the US group
   only (other assets keep their prior), and every risky asset (bonds, gold and the dollar
   too, which also tests the stale Treasury prior).

Tests: cross-sectional IC within the US group (1, 21 and 252 days, interleaved folds), then
the book with ``PORTFOLIO`` fixed.

**Result (2026-10-16): both rejected.** C has the strongest IC found so far (1-day +0.026,
t 4.8, both folds and since 2010) but in the long-only book it only concentrates risk:
Sharpe 0.95 against 1.06, vol 12.0%. A rotation that holds total US equity weight fixed, or a
long/short sleeve, would be a separate test. D's history has the wrong sign (1-year IC
-0.15, -0.23 since 2010: long-horizon reversal), Sharpe 0.94 (US group) and 0.86 (all).

Usage:
    uv run python examples/research_sector_priors.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd
from scipy.stats import norm

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import information_coefficient
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

US_GROUP = [*core.CLASSES["US sectors"], "QQQ"]
SPLIT = pd.Timestamp("2017-07-26")
CHUNK, NW_LAGS = 47, 21
HIST_WEIGHT, HIST_MIN = 0.25, 1260


def rank_normal(panel: pd.DataFrame) -> pd.DataFrame:
    """Per row, ranks mapped to standard normal quantiles (mean zero)."""
    r = panel.rank(axis=1)
    n = panel.notna().sum(axis=1)
    return pd.DataFrame(norm.ppf(((r.sub(0.5)).div(n, axis=0)).to_numpy()),
                        index=panel.index, columns=panel.columns).where(panel.notna())


@dataclass
class RelativeMomentum(Signal):
    """Normal score of 12-1 month return within ``group``, times annualized vol."""

    name: ClassVar[str] = "rel_mom"
    group: tuple[str, ...] = tuple(US_GROUP)
    lookback: int = 252
    skip: int = 21
    vol_window: int = 60

    def score(self, px: pd.DataFrame) -> pd.DataFrame:
        g = px[list(self.group)]
        mom = g.shift(self.skip) / g.shift(self.lookback) - 1
        return rank_normal(mom).reindex(columns=px.columns)

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        return self.score(px) * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return max(self.lookback + 1, self.vol_window + 1)


@dataclass
class HistorySharpePrior(Signal):
    """``(w * expanding Sharpe + (1 - w) * prior) * vol`` for ``assets``; the prior elsewhere."""

    name: ClassVar[str] = "prior"
    sr: dict[str, float] | None = None
    assets: tuple[str, ...] = ()
    weight: float = HIST_WEIGHT
    min_rows: int = HIST_MIN
    vol_window: int = 60

    def sharpe(self, px: pd.DataFrame) -> pd.DataFrame:
        prior = pd.DataFrame({t: float(v) for t, v in (self.sr or {}).items()},
                             index=px.index).reindex(columns=px.columns)
        r = px.pct_change()
        e = r.expanding(min_periods=self.min_rows)
        hist = e.mean() / e.std() * np.sqrt(TRADING_DAYS)
        blend = self.weight * hist + (1 - self.weight) * prior
        use = pd.DataFrame(False, index=px.index, columns=px.columns)
        use[[a for a in self.assets if a in px.columns]] = True
        return prior.where(~use | hist.isna(), blend)

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        return self.sharpe(px) * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return self.vol_window + 1


# --- 1. IC -------------------------------------------------------------------------------------

def fmt(s: pd.Series, lags: int) -> str:
    s = s.dropna()
    return f"{s.mean():+.4f} ({newey_west_t(s, lags):+.1f})"


def ic_report(bundle: DataBundle) -> None:
    px = bundle.prices[US_GROUP]
    folds = chunk_folds(bundle.index, CHUNK)
    hist = HistorySharpePrior(core.PRIOR_SR, assets=tuple(US_GROUP)).sharpe(bundle.prices)
    rules = {"C relative momentum 12-1": RelativeMomentum().score(bundle.prices)[US_GROUP],
             "D expanding Sharpe": hist[US_GROUP].where(hist[US_GROUP].ne(0.3))}
    rows = {}
    for name, s in rules.items():
        s = s.shift(1)
        row = {}
        for h in (1, 21, 252):
            lags = max(NW_LAGS, 2 * h)
            ic = information_coefficient(s, px, h, "spearman", min_names=5)
            if h == 1:
                row["1d fold A"] = fmt(ic[folds.reindex(ic.index) == 0], lags)
                row["1d fold B"] = fmt(ic[folds.reindex(ic.index) == 1], lags)
            row[f"h={h}"] = fmt(ic, lags)
            row[f"h={h} 2010+"] = fmt(ic.loc["2010":], lags)
        rows[name] = row
    print("\n1. CROSS-SECTIONAL RANK IC within the US group (9 sectors + QQQ), mean "
          "(Newey-West t, max(21, 2h) lags). Folds need no fit: ranks are per date")
    print(pd.DataFrame(rows).T.to_string())
    print("\n   D: the history-blended Sharpe prior at a few dates")
    full = HistorySharpePrior(core.PRIOR_SR, assets=tuple(core.RISKY)).sharpe(bundle.prices)
    days = ["2008-09-10", "2012-12-31", "2017-07-26", "2022-01-03", full.index[-1]]
    rows = [full.index[full.index.searchsorted(pd.Timestamp(d), "right") - 1] for d in days]
    print(full.loc[rows, core.RISKY].T.to_string(float_format="{:.2f}".format))


# --- 2. portfolio ------------------------------------------------------------------------------

def alphas(variant: str, k: float) -> dict:
    a = dict(core.ALPHAS)
    if "C" in variant:
        a["rel_mom"] = (RelativeMomentum(normalizer=Identity()), k)
    if "D" in variant:
        assets = tuple(core.RISKY) if "all" in variant else tuple(US_GROUP)
        a["prior"] = (HistorySharpePrior(core.PRIOR_SR, assets=assets, normalizer=Identity()), 1.0)
    return a


def run_one(args: tuple[str, float]) -> tuple[str, pd.Series, pd.DataFrame, float]:
    variant, k = args
    bundle = core.load()
    s = core.build(alphas=alphas(variant, k))
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle, core.build())
                                ).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return variant, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def portfolio_report(bundle: DataBundle, workers: int) -> None:
    vol = realized_vol(bundle.prices, 60, annualize=True)
    trend_sd = (core.ALPHAS["trend"][0].raw(bundle) / vol)[US_GROUP].loc[:SPLIT].stack().std()
    mom_sd = RelativeMomentum().score(bundle.prices)[US_GROUP].loc[:SPLIT].stack().std()
    k = 0.1 * trend_sd / mom_sd
    print(f"\n   C weight matched to the trend budget's dispersion: k = {k:.3f}")
    jobs = {"saved book": 0.0, "C: + relative momentum": k, "D: history prior, US group": 0.0,
            "D: history prior, all": 0.0, "C + D US group": k}
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
    pct = "{:.1%}".format
    print(f"\n2. PORTFOLIO, {start.date()} .. {runs['saved book'].index[-1].date()}, split "
          f"{SPLIT.date()} | {core.PORTFOLIO}")
    print(m.to_string(formatters={**{c: pct for c in ("ret", "vol", "max_dd")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe")},
                                  "turnover": "{:.1f}".format}))
    print("\n   average weight, % of capital (US group and Treasuries)")
    w = pd.DataFrame({n: out[n][1].loc[start:, [*US_GROUP, "IEF", "TLT"]].mean() for n in jobs})
    print(w.T.to_string(float_format="{:.0%}".format))
    ex = pd.DataFrame({n: runs[n] - runs["saved book"] for n in jobs if n != "saved book"})
    g = ex.groupby(ex.index.year // 5 * 5)
    ir = (g.mean() / g.std() * np.sqrt(TRADING_DAYS)).rename(lambda y: f"{y}-{y + 4}").T
    ir["all"] = ex.mean() / ex.std() * np.sqrt(TRADING_DAYS)
    print("\n   difference from the saved book: annualized return / tracking vol, by period")
    print(ir.to_string(float_format="{:+.2f}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    bundle = core.load()
    ic_report(bundle)
    portfolio_report(bundle, workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
