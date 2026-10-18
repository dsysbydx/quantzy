"""Short-term mean reversion: last week's or month's losers among sectors and countries.

``research_multi_horizon.py`` found 1-month TSMOM is *reversal* on US sectors (IC -0.016,
t -2.3). Short-term reversal is usually explained by liquidity provision: prices overshoot on
flows and come back. It needs fast trading the core's no-trade band will not do, so it is
tested as its own sleeve. **Fixed before any test.**

Universes: the 9 sector SPDRs plus QQQ (from 1999); 23 developed and emerging country ETFs
from ``research_broad_universe.py`` (EFA and EEM left out); the 17 for time-series tests.
Signals skip a day: they use returns through the prior close, positions start at the close.

1. *Cross-sectional reversal*, lookback L of 5 or 21 days: rank IC of minus the past return
   with the forward 1, 5 and 21-day returns, by fold and period; and a sleeve long the bottom
   third and short the top third (equal weight, dollar-neutral), formed every L days and
   held L days, charged 2 bp (sectors) or 5 bp (countries) a trade; and a long-only version,
   the bottom third minus the equal-weight universe (a tilt inside an equity budget).
2. *Time-series reversal* on each of the 17: IC of ``-(5-day return / (daily vol sqrt 5))``
   with the next 1 and 5 days, by class.
3. *Pullback in an uptrend*: hold an ETF for 5 days after a 5-day fall of more than one
   sigma while its EMA-200 trend is positive (the 17), against holding it unconditionally.

**Result (2026-10-18).** Weekly (5-day) cross-sectional reversal has faded: sectors 5-day IC
+0.002 and countries +0.001 since 2017, and at ~140x turnover the sleeves need under 5-7 bp
to break even (net Sharpe 2017-26: -0.01 and -0.31). Two pieces survive: (1) time-series
5-day reversal in equity ETFs (US sectors fold B +0.050, t 2.8; equity index +0.056, t 2.3;
both t 2.6-2.8 since 2010; commodities show short-term momentum instead); (2) 21-day
cross-sectional reversal across country ETFs, long/short net Sharpe 0.34 (break-even 18 bp,
2008-16 0.73, 2017-26 0.41), correlation with the core 0.04. Pullback-in-uptrend is not
robust (t 0.6 since 2010). Both survivors go on to ``research_reversal_sleeves.py``.

Usage:
    uv run python research/research_short_reversal.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import research_broad_universe as broad

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, newey_west_t
from quantzy.research.ic import _ts_pairs, information_coefficient
from quantzy.strategies import core_multiasset as core

SECTORS = [*core.CLASSES["US sectors"], "QQQ"]
COUNTRIES = [t for c in ("developed equity", "emerging equity") for t in broad.CLASSES[c]
             if t not in ("EFA", "EEM")]
COST = {"sectors": 2e-4, "countries": 5e-4}
PERIODS = [("1999", "2007"), ("2008", "2016"), ("2017", "2026")]


def fmt(x: pd.Series, lags: int) -> str:
    x = x.dropna()
    return f"{x.mean():+.4f} ({newey_west_t(x, lags):+.1f})"


def cs_ic(px: pd.DataFrame, lookback: int) -> dict[str, str]:
    score = -(px.shift(1) / px.shift(1 + lookback) - 1)  # through the prior close
    folds = chunk_folds(px.index, 47)
    row = {}
    for h in (1, 5, 21):
        ic = information_coefficient(score, px, h, "spearman", min_names=5)
        lags = max(21, 2 * h)
        if h == 1:
            row["1d fold A"] = fmt(ic[folds.reindex(ic.index) == 0], lags)
            row["1d fold B"] = fmt(ic[folds.reindex(ic.index) == 1], lags)
        row[f"{h}d"] = fmt(ic, lags)
        if h == lookback:
            for a, b in PERIODS:
                row[f"{h}d {a}-{b[2:]}"] = fmt(ic.loc[a:b], lags)
    return row


def sleeves(px: pd.DataFrame, lookback: int, cost: float) -> dict[str, pd.Series]:
    """Long-short terciles and long-only bottom tercile minus equal weight, net of cost."""
    rets = px.pct_change(fill_method=None)
    past = px.shift(1) / px.shift(1 + lookback) - 1
    dates = px.index[lookback + 2::lookback]
    ls_w, lo_w = [], []
    for d in dates:
        p = past.loc[d].dropna()
        if len(p) < 6:
            ls_w.append(pd.Series(dtype=float, name=d))
            lo_w.append(pd.Series(dtype=float, name=d))
            continue
        n = len(p) // 3
        losers, winners = p.nsmallest(n).index, p.nlargest(n).index
        w = pd.Series(0.0, index=p.index)
        w[losers], w[winners] = 1.0 / n, -1.0 / n
        ls_w.append(w.rename(d))
        lo = pd.Series(-1.0 / len(p), index=p.index)
        lo[losers] += 1.0 / n
        lo_w.append(lo.rename(d))
    out = {}
    for name, ws in (("long-short", ls_w), ("long-only tilt", lo_w)):
        w = pd.DataFrame(ws).reindex(columns=px.columns).fillna(0.0)
        held = w.reindex(px.index).ffill().fillna(0.0).shift(1).fillna(0.0)  # from that close
        gross = (held * rets.fillna(0.0)).sum(axis=1)
        turnover = w.diff().abs().sum(axis=1).reindex(px.index).fillna(0.0)
        out[name] = (gross, turnover)
    return {k: (g, t, g - t * cost) for k, (g, t) in out.items()}


def perf(gross: pd.Series, turnover: pd.Series, net: pd.Series, core_r: pd.Series) -> dict:
    g, n = gross.loc[gross.ne(0).idxmax():], net.loc[gross.ne(0).idxmax():]
    ann = np.sqrt(TRADING_DAYS)
    to_year = turnover.loc[g.index].sum() / (len(g) / TRADING_DAYS)
    breakeven = g.mean() * TRADING_DAYS / to_year * 1e4 if to_year else np.nan
    row = {"from": f"{g.index[0]:%Y}", "Sharpe gross": g.mean() / g.std() * ann,
           "Sharpe net": n.mean() / n.std() * ann, "return net": n.mean() * TRADING_DAYS,
           "vol": n.std() * ann, "turnover/yr": to_year, "break-even bp": breakeven,
           "corr with core": n.corr(core_r)}
    for a, b in PERIODS:
        x = n.loc[a:b]
        row[f"Sharpe {a}-{b[2:]}"] = x.mean() / x.std() * ann if len(x) > 250 else np.nan
    return row


def main() -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, core.build())
    core_r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    raw = CachedReader(YFinanceReader()).history([*SECTORS, *COUNTRIES], start="1998-12-22")
    raw = raw[raw.notna().any(axis=1)]
    univ = {"sectors": raw[SECTORS], "countries": raw[COUNTRIES]}

    print("1a. CROSS-SECTIONAL RANK IC of minus the past L-day return (Newey-West t)")
    rows = {(u, f"L={L}"): cs_ic(px, L) for u, px in univ.items() for L in (5, 21)}
    print(pd.DataFrame(rows).T.to_string())

    print("\n1b. SLEEVES (formed every L days, held L days), net of cost")
    prow = {}
    for u, px in univ.items():
        for L in (5, 21):
            for kind, (g, t, n) in sleeves(px, L, COST[u]).items():
                prow[(u, f"L={L}", kind)] = perf(g, t, n, core_r)
    print(pd.DataFrame(prow).T.to_string(float_format=lambda v: f"{v:+.2f}"))

    print("\n2. TIME-SERIES REVERSAL, IC of -(5-day return / vol sqrt 5), by class")
    px = b.prices[core.RISKY]
    vol = px.pct_change(fill_method=None).rolling(60, min_periods=60).std()
    score = -((px / px.shift(5) - 1) / (vol * np.sqrt(5)))
    folds = chunk_folds(b.index, 47)
    a, bb = folds == 0, folds == 1
    trows = {}
    for h in (1, 5):
        zx, zy = _ts_pairs(score.shift(1), px, h, "spearman", 60, fit=a)
        prod = zx * zy
        for c, ts in core.CLASSES.items():
            x = prod[ts].mean(axis=1)
            trows[(c, f"h={h}")] = {"fold A": fmt(x[a], 21), "fold B (held out)": fmt(x[bb], 21),
                                    "2010+": fmt(x.loc["2010":], 21)}
    print(pd.DataFrame(trows).T.to_string())

    print("\n3. PULLBACK IN AN UPTREND (hold 5 days after a >1 sigma 5-day fall, EMA-200 trend "
          "> 0), daily excess over T-bills while held, against holding unconditionally")
    rets = px.pct_change(fill_method=None)
    rf = b.prices[core.CASH].pct_change(fill_method=None)
    trend = core.ALPHAS["trend"][0].raw(b)[core.RISKY].shift(1)  # raw is unlagged
    fall = ((px / px.shift(5) - 1) / (vol * np.sqrt(5))).shift(1) < -1.0
    trigger = fall & (trend > 0)
    held = trigger.astype(float).rolling(5, min_periods=1).max().shift(1).fillna(0) > 0
    ex = rets.sub(rf, axis=0)
    prow = {}
    for c, ts in core.CLASSES.items():
        x = ex[ts].where(held[ts]).stack().dropna()
        allx = ex[ts].stack().dropna()
        t = (x.mean() - allx.mean()) / np.sqrt(x.var() / len(x) + allx.var() / len(allx))
        prow[c] = {"days held": len(x), "excess a day, held (%)": x.mean() * 100,
                   "excess a day, always (%)": allx.mean() * 100, "t (held - always)": t}
    print(pd.DataFrame(prow).T.to_string(float_format=lambda v: f"{v:+.3f}"))


if __name__ == "__main__":
    main()
