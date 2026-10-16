"""Valuation and carry priors: replace judgment Sharpe priors with building-block forecasts.

The saved book's ``"prior"`` alpha is a fixed Sharpe ratio per asset class, set by judgment.
It does not move: equities get 0.30 whether the market yields 3% (2009) or 1.2% with cash at
5% (2024). This replaces it, for the assets where a clean point-in-time measure exists, with
the textbook building blocks of expected excess return:

    US equity (sectors, QQQ)  (dividend yield of SPY + G_US   - T-bill) / VOL_US
    EFA, EEM                  (own dividend yield     + G_INTL - T-bill) / own VOL
    IEF                       (10-year Treasury yield  - T-bill) / VOL_IEF
    TLT                       (30-year Treasury yield  - T-bill) / VOL_TLT
    GLD, DBC, UUP             unchanged judgment priors (no clean carry or valuation here)

That gives a Sharpe ratio per class through time; each asset's ``mu`` is that Sharpe times
its trailing 60-day vol, exactly as the judgment prior does. Dividend yield is the trailing
12-month yield implied by adjusted versus unadjusted closes, so it only uses dividends paid
by date t. Yields are Yahoo's ^TNX, ^TYX and ^IRX.

**Fixed before any test** (long-run, not fitted):

* ``G_US = 5.5%``: about 2% real growth, 2.5% inflation and 1% net buybacks.
* ``G_INTL = 4.5%``: the same without buybacks.
* Long-run vols used to turn excess return into a Sharpe: US equity 16%, EFA 17%, EEM 22%,
  IEF 7%, TLT 14%. Constant, so the prior does not double as a vol-timing signal.

Tests, with the saved strategy's portfolio settings and its 0.1 trend alpha unchanged:

1. Time-series IC of the valuation Sharpe, per class, against the next day's return over
   trailing vol (47-day interleaved folds, Newey-West t), and against 1-year returns.
2. Backtests: judgment prior (the saved book), valuation prior, and a 50/50 blend.

**Result (2026-10-16): negative, not adopted.** Rank IC was right-signed for US equity
(1-day +0.017, t 2.1) and Treasury carry (TLT +0.024, t 2.0), but wrong-signed for EFA and
EEM (1-year -0.29 and -0.27): their dividend yields rose as they kept underperforming. In the
book (2008-05 .. 2026-10) the valuation prior gave Sharpe 0.98 against 0.99 for judgment
(train 0.83 vs 0.92, test 1.14 vs 1.05), at 12.3% vol against 10.8% and a deeper drawdown
(-28.4% vs -25.3%); the 50/50 blend gave 1.03. That is within noise, inconsistent across the
halves, and partly a larger book. Treasury carry alone looked cleanest, but it was picked
after seeing these results. Better measures (earnings yield or CAPE by region, commodity
futures curves) need data outside Yahoo.

Usage:
    uv run python examples/research_valuation_priors.py [--workers 3]
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
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

G_US, G_INTL = 0.055, 0.045
LONG_RUN_VOL = {"US": 0.16, "EFA": 0.17, "EEM": 0.22, "IEF": 0.07, "TLT": 0.14}
US_EQUITY = [*core.CLASSES["US sectors"], "QQQ"]
VALUED = [*US_EQUITY, "EFA", "EEM", "IEF", "TLT"]
DY_WINDOW = 252
CHUNK, NW_LAGS = 47, 21


def trailing_dividend_yield(adj: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    """Trailing 12-month dividend yield: total return over price return, minus one."""
    return (adj / adj.shift(DY_WINDOW)) / (raw / raw.shift(DY_WINDOW)) - 1.0


def load() -> DataBundle:
    """The saved book's prices, plus a ``val_sr`` panel: the valuation Sharpe per asset."""
    bundle = core.load()
    px = bundle.prices
    adj_r = CachedReader(YFinanceReader())
    raw_r = CachedReader(YFinanceReader(auto_adjust=False))
    eq = ["SPY", "EFA", "EEM"]
    dy = trailing_dividend_yield(adj_r.history(eq, start=core.START),
                                 raw_r.history(eq, start=core.START, field="close"))
    yld = adj_r.history(["^TNX", "^TYX", "^IRX"], start=core.START).ffill() / 100.0
    dy, yld = dy.reindex(px.index).ffill(), yld.reindex(px.index).ffill()
    rf = yld["^IRX"]
    sr = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
    us = (dy["SPY"] + G_US - rf) / LONG_RUN_VOL["US"]
    for t in US_EQUITY:
        sr[t] = us
    sr["EFA"] = (dy["EFA"] + G_INTL - rf) / LONG_RUN_VOL["EFA"]
    sr["EEM"] = (dy["EEM"] + G_INTL - rf) / LONG_RUN_VOL["EEM"]
    sr["IEF"] = (yld["^TNX"] - rf) / LONG_RUN_VOL["IEF"]
    sr["TLT"] = (yld["^TYX"] - rf) / LONG_RUN_VOL["TLT"]
    for t in ("GLD", "DBC", "UUP"):  # no valuation measure: keep the judgment prior
        sr[t] = core.PRIOR_SR[t]
    sr = sr.where(px.notna())  # nothing before an ETF exists
    return DataBundle.build(px, val_sr=sr)


@dataclass
class ValuationPrior(Signal):
    """``val_sr_i(t) * annualized trailing vol_i``: a time-varying Sharpe prior in return
    units. The Sharpe comes from the bundle's ``val_sr`` panel, built by :func:`load`."""

    name: ClassVar[str] = "valuation"
    vol_window: int = 60

    def raw(self, data: DataBundle) -> pd.DataFrame:
        return data["val_sr"] * realized_vol(data.prices, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return max(self.vol_window + 1, DY_WINDOW + 1)


# --- 1. IC -------------------------------------------------------------------------------------

def ic_report(bundle: DataBundle) -> None:
    px = bundle.prices[VALUED]
    sr = bundle["val_sr"][VALUED].shift(1)  # tradeable at t: known at t-1's close
    folds = chunk_folds(bundle.index, CHUNK)
    in_a = folds == 0
    groups = {"US equity (one series)": US_EQUITY, "EFA": ["EFA"], "EEM": ["EEM"],
              "IEF": ["IEF"], "TLT": ["TLT"]}

    def fmt(x: pd.Series) -> str:
        return f"{x.mean():+.4f} ({newey_west_t(x.dropna(), NW_LAGS):+.1f})"

    print(f"\n1. TIME-SERIES IC of the valuation Sharpe. 1-day: next-day return / trailing 60d "
          f"vol, {CHUNK}-day interleaved folds, z-stats from fold A, Newey-West t "
          f"({NW_LAGS} lags). 1-year: 252-day forward return, Newey-West t with 252 lags "
          f"(overlap)")
    rows = {}
    for method in ("spearman", "pearson"):
        zx, zy = _ts_pairs(sr, px, 1, method, 60, fit=in_a)
        zx1, zy1 = _ts_pairs(sr, px, 252, method, 60)
        for g, ts in groups.items():
            d, y = (zx[ts] * zy[ts]).mean(axis=1), (zx1[ts] * zy1[ts]).mean(axis=1).dropna()
            rows[(g, method)] = {"1d fold A": fmt(d[in_a]), "1d fold B": fmt(d[~in_a]),
                                 "1d all": fmt(d),
                                 "1y all": f"{y.mean():+.3f} ({newey_west_t(y, 252):+.1f})"}
    print(pd.DataFrame(rows).T.to_string())

    print("\n   the valuation Sharpe over time (year-end values)")
    snap = bundle["val_sr"][["XLK", "EFA", "EEM", "IEF", "TLT"]].rename(
        columns={"XLK": "US equity"})
    print(snap.groupby(snap.index.year).last().loc[2000:].to_string(
        float_format="{:+.2f}".format))


# --- 2. portfolio ------------------------------------------------------------------------------

def alphas(prior: str) -> dict:
    judgment, trend = core.ALPHAS["prior"][0], core.ALPHAS["trend"]
    valuation = ValuationPrior(normalizer=Identity())
    base = {"judgment": {"prior": (judgment, 1.0)},
            "valuation": {"valuation": (valuation, 1.0)},
            "blend 50/50": {"prior": (judgment, 0.5), "valuation": (valuation, 0.5)}}[prior]
    return {**base, "trend": trend}


def run_one(prior: str) -> tuple[str, pd.Series, pd.DataFrame, float]:
    bundle = load()
    s = core.build(alphas=alphas(prior))
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle, s)).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return prior, r, res.weights, turnover


def portfolio_report(workers: int) -> None:
    priors = ["judgment", "valuation", "blend 50/50"]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, priors)}
    start = max(v[0].index[0] for v in out.values())
    runs = {n: v[0].loc[start:] for n, v in out.items()}
    split = runs["judgment"].index[len(runs["judgment"]) // 2]
    t = compare(runs, split_date=split)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    })
    m["turnover"] = [out[n][2] for n in m.index]
    pct = "{:.1%}".format
    print(f"\n2. PORTFOLIO, {start.date()} .. {runs['judgment'].index[-1].date()}, split "
          f"{split.date()} | trend 0.1 kept | {core.PORTFOLIO}")
    print(m.to_string(formatters={"ret": pct, "vol": pct, "max_dd": pct,
                                  "tr_sharpe": "{:.2f}".format, "te_sharpe": "{:.2f}".format,
                                  "sharpe": "{:.2f}".format, "turnover": "{:.1f}".format}))
    cls = {t: c for c, ts in core.CLASSES.items() for t in ts}
    for n in priors:
        w = out[n][1].loc[start:]
        g = w[core.RISKY].T.groupby(cls).sum().T
        print(f"\n   {n}: average weight by class (% of capital), by period")
        print(g.groupby(g.index.year // 5 * 5).mean().rename(lambda y: f"{y}-{y + 4}").T
              .to_string(float_format="{:.0%}".format))
    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    print("\n   calendar years")
    print(yr.to_string(float_format="{:+.1%}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    ic_report(load())
    portfolio_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    main(ap.parse_args().workers)
