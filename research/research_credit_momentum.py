"""Twelve-month credit momentum as an equity tilt.

``research_credit_breadth.py`` found the 252-day credit excess return predicting SPY with the
opposite sign to its contrarian hypothesis: a strong year for high yield over Treasuries came
before better equity returns (VWEHX/VFITX 1993 on, IC t -3.4 at 21 days, residual after trend
and VRP t -3.5). That is a sign flip found after the fact, so this tests it fresh. **Fixed
before any test.**

* C3: the 252-day credit excess return (high yield minus a lagged rolling one-year beta times
  intermediate Treasuries) over (60-day daily vol times sqrt 252), capped +/-2: the C2 series
  with its sign reversed. The book uses VWEHX/VFITX (daily since 1993, covering the whole
  backtest); HYG/IEF is the check.
* Added to the saved book as a tilt on the 12 equity ETFs at 0.10 Sharpe per unit,
  ``PORTFOLIO`` unchanged; the signal lags a day.

Judged on the saved book (core plus overlays): full-period and test-half Sharpe (split
2017-07-26), drawdown and the sell-offs. Adopted only if the test half improves as well.

**Result (2026-10-19): rejected.** The tilt lowers the saved book's Sharpe from 1.231 to
1.063 (VWEHX) / 1.081 (HYG), test half 1.184 -> 1.015 / 1.028, max drawdown -17.2% -> -18.5%,
2020 -8.4% -> -13.0% and 2022 -3.8% -> -9.4%. The score is not centred: credit excess returns
are mostly positive, so C3 averages +1.23 and sits at its +2 cap over half the time. In the
book it acts as a standing extra equity budget (average equity weight 0.79 -> 0.93), not as
timing. The IC (a rank correlation) measures only the timing part.

**C4, the centred version (fixed before its test).** The same uncapped 252-day credit score,
z-scored against its own trailing 5 years (at least 3), capped +/-2; a trailing-10-year
window as the sensitivity, HYG/IEF as the check (from ~2011, when its window fills; the tilt
is zero before). Same tilt, weight and judging rule. Also reported: the IC of each version vs
SPY's next 21 / 63 days over vol (Newey-West, 252 lags), in both folds and since 2009.

**Result (2026-10-19): C4 rejected too.** Centring works as intended: the mean falls from
+1.22 to -0.05 / -0.09, the share of days at the cap from 50% to 5-6%, and average equity
weight returns to 0.75-0.77 (book 0.79). The timing IC is unchanged (21-day t +2.9, 63-day
+2.5-2.7, both folds positive) but weaker since 2009 (t +1.1 to +1.9). In the book it still
costs: Sharpe 1.231 -> 1.186-1.202, test half 1.184 -> 1.072-1.115. It halves the 2008
loss (-14.9% -> -7.4%) but loses more in the fast reversals (2020 -8.4% -> -9.5% to -11.1%,
2022 -3.8% -> -6.8% to -7.8%), where a one-year credit return turns too late: what it adds
in slow bear markets the EMA-200 and EWMAC trend already capture.

Usage:
    uv run python research/research_credit_momentum.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from research_bounce_filtered import stats
from research_credit_breadth import credit_signals, fmt, ic_series
from research_reentry import MarketStateView

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds
from quantzy.signals import Identity
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme

TILT = 0.10
PAIRS = {"VWEHX/VFITX": ("VWEHX", "VFITX"), "HYG/IEF": ("HYG", "IEF")}


def c3(pair: str) -> pd.Series:
    hy, gov = PAIRS[pair]
    px = CachedReader(YFinanceReader()).history([hy, gov], start="1990-01-01")
    return -credit_signals(px[hy], px[gov])["C2 credit contrarian 252d"]


def credit_score(pair: str) -> pd.Series:
    """The uncapped 252-day credit excess return over vol (C3 before its cap)."""
    hy, gov = PAIRS[pair]
    px = CachedReader(YFinanceReader()).history([hy, gov], start="1990-01-01")
    rh, rg = px[hy].pct_change(fill_method=None), px[gov].pct_change(fill_method=None)
    beta = (rh.rolling(252).cov(rg) / rg.rolling(252).var()).shift(1)
    ex = rh - beta * rg
    return ex.rolling(252).sum() / (ex.rolling(60, min_periods=60).std() * np.sqrt(252))


def c4(pair: str, years: int) -> pd.Series:
    x = credit_score(pair)
    r = x.rolling(252 * years, min_periods=252 * 3)
    return ((x - r.mean()) / r.std()).clip(-2, 2)


VARIANTS = {
    "C3 capped (VWEHX/VFITX)": lambda: c3("VWEHX/VFITX"),
    "C4 z 5y (VWEHX/VFITX)": lambda: c4("VWEHX/VFITX", 5),
    "C4 z 10y (VWEHX/VFITX)": lambda: c4("VWEHX/VFITX", 10),
    "C4 z 5y (HYG/IEF)": lambda: c4("HYG/IEF", 5),
}


def run_one(name: str | None) -> tuple[str, pd.Series, pd.DataFrame]:
    b = latest_complete(core.load(), core.RISKY)
    alphas = dict(core.ALPHAS)
    if name is not None:
        z = VARIANTS[name]().reindex(b.index).ffill().fillna(0.0)
        panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
        for t in core.EQUITY:
            panel[t] = z
        b = DataBundle.build(b.prices, vix=b["vix"], market=b["market"], carry=b["carry"],
                             dollar_value=b["dollar_value"], credit=panel)
        alphas["credit"] = (MarketStateView("credit", normalizer=Identity()), TILT)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(
        b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    r = r + tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None),
                               cash).reindex(r.index).fillna(0)
    return name or "saved book", r, res.weights


def main() -> None:
    pd.set_option("display.width", 300)
    spy = CachedReader(YFinanceReader()).history(["SPY"], start="1993-01-01")["SPY"].dropna()
    folds = chunk_folds(spy.index, 47)
    ics = {}
    for name, f in VARIANTS.items():
        z = f().shift(1)
        row = {"mean": z.loc["2008-09":].mean(), "% at cap": (z.loc["2008-09":].abs() >= 2).mean()}
        for h in (21, 63):
            ic = ic_series(z, spy, h)
            fo = folds.reindex(ic.index)
            row[f"{h}d"] = fmt(ic, 252)
            row[f"{h}d fold A"], row[f"{h}d fold B"] = fmt(ic[fo == 0], 252), fmt(ic[fo == 1], 252)
            row[f"{h}d 2009 on"] = fmt(ic.loc["2009":], 252)
        ics[name] = row
    print("1. IC vs SPY's forward return / vol (NW t, 252 lags); mean and cap since 2008-09")
    print(pd.DataFrame(ics).T.to_string(float_format=lambda v: f"{v:+.2f}"))

    jobs = [None, *VARIANTS]
    with ProcessPoolExecutor(max_workers=5) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, jobs)}
    base_eq = out["saved book"][1][core.EQUITY].sum(axis=1)
    rows = {}
    for name, (r, w) in out.items():
        eq = w[core.EQUITY].sum(axis=1)
        rows[name if name == "saved book" else f"+ {name}"] = {
            **stats(r), "vol": r.std() * np.sqrt(252), "avg equity": eq.mean(),
            "corr eq wt vs book": eq.corr(base_eq)}
    print(f"\n2. SAVED BOOK (core + overlays) PLUS EACH TILT AT {TILT}, "
          f"{out['saved book'][0].index[0].date()} on")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))
    last = {n: f().dropna().iloc[-1] for n, f in VARIANTS.items()}
    print("\nlatest: " + ", ".join(f"{n} {v:+.2f}" for n, v in last.items()))


if __name__ == "__main__":
    main()
