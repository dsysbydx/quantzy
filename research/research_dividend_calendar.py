"""The dividend-calendar effect across countries (idea 17, step 2 of the user's idea).

The dividend month premium (Hartzmark and Solomon 2013): investors buy, or hold off selling,
ahead of an expected dividend, so prices run up into it. Where a market's dividends cluster
in known months, the market should do better than others in the run-up. Unlike generic
seasonality (``research_country_seasonality.py``, which failed), the months here come from
each market's dividend calendar, drafted from known patterns and confirmed by the user
(2026-11-01). **Fixed before any test.**

Run-up window per market: the month before each main ex-dividend season and its first month.
Thailand Feb-Mar, Jul-Aug; Japan Feb-Mar, Aug-Sep; Korea Nov-Dec; Taiwan Jun-Jul; Hong Kong
and China Apr-May, Jul-Aug; Singapore and Malaysia Mar-Apr, Jul-Aug; Australia Jan-Feb,
Jul-Aug; continental Europe (Germany, France, Italy, Netherlands, Switzerland, Sweden)
Mar-Apr; India Jun-Jul. Controls with dividends spread through the year: US, Canada, UK.

Each market's monthly total return (ETF, over T-bills) is taken relative to the equal-weight
mean of all these markets that month, which removes calendar effects common to all of them.

1. Pooled (the decision): each month, the mean relative return of markets in their window
   minus that of the others -- a long/short spread. Rule: mean > 0 with Newey-West t >= 2
   (3 lags) and positive in each period (2001-08, 2009-16, 2017-26). The pre-season month
   and the season's first month are also reported apart.
2. If it passes, the sleeve (long the markets in their window, short the others, equal weight
   per leg, costs as in the seasonality study, 1% borrow, 10% vol by trailing 36 months),
   judged like the other sleeves: Sharpe above 0.3 since 2008-09 and the book with 0.2x up at
   least 0.02 over the window and in both halves.
3. Reported, no rule (THD from 2008, ~18 years): Thailand alone in its window; and Thai tax-
   saving fund buying (LTF / SSF / RMF) before the year-end deadline, THD in Nov-Dec.

**Result (2026-11-01): the effect is there, but not tradeable as a sleeve.** 2001-01 ..
2026-10, 19 markets. (1) Pooled spread, markets in their window minus the others: +52 bp a
month (t 2.2), positive in each period (+102 bp t 2.0 / +18 bp t 0.4 / +39 bp t 1.2) -- the
rule passes, mostly on 2001-08; pre-season month +42 bp (t 1.5), first season month +39 bp
(t 1.3). 12 of 16 markets go the right way: Japan +56 against -69 bp (t 2.8), Thailand +64
against -33 bp (t 1.4), India, Germany, Singapore and all of continental Europe positive;
Taiwan and China the other way. (2) The sleeve: Sharpe +0.29 / -0.04 / +0.10 by period, +0.03
since 2008-09 (t 0.2) at 1.8x one-way turnover a month -- costs take the post-2008 edge; the
book with 0.2x 1.424 against 1.443. (3) Thai tax-fund buying: Nov-Dec relative -11 against
+1 bp (t -0.2); THD's strong Nov-Dec (+143 against +32 bp absolute) is the year-end strength
every market shares. A real, faded mechanism, usable at no extra cost to time contributions
or rebalancing into markets already held (e.g. Thai equities in Feb-Mar and Jul-Aug), not as
a separate sleeve.

Usage:
    uv run python research/research_dividend_calendar.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402
from research_country_seasonality import COST_BPS  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.research import compare, newey_west_t  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

EUROPE = ["EWG", "EWQ", "EWI", "EWN", "EWL", "EWD"]
# market -> [(pre-season month, first season month), ...]
WINDOWS = {
    "THD": [(2, 3), (7, 8)], "EWJ": [(2, 3), (8, 9)], "EWY": [(11, 12)], "EWT": [(6, 7)],
    "EWH": [(4, 5), (7, 8)], "FXI": [(4, 5), (7, 8)], "EWS": [(3, 4), (7, 8)],
    "EWM": [(3, 4), (7, 8)], "EWA": [(1, 2), (7, 8)], "INDA": [(6, 7)],
    **{t: [(3, 4)] for t in EUROPE},
}
CONTROLS = ["SPY", "EWC", "EWU"]
MARKETS = list(WINDOWS) + CONTROLS
BORROW, TARGET_VOL = 0.01, 0.10
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("2001", "2008"), ("2009", "2016"), ("2017", "2026"))


def flags(index: pd.DatetimeIndex, part: str = "both") -> pd.DataFrame:
    """True where a market is in its window (``part``: "pre", "first" or "both")."""
    out = pd.DataFrame(False, index=index, columns=MARKETS)
    for t, wins in WINDOWS.items():
        months = {m for pre, first in wins for m in
                  ((pre, first) if part == "both" else (pre,) if part == "pre" else (first,))}
        out[t] = index.month.isin(sorted(months))
    return out


def spread(rel: pd.DataFrame, inside: pd.DataFrame) -> pd.Series:
    """Per month: mean relative return of markets in their window minus the others'."""
    ok = rel.notna()
    a = rel.where(inside & ok).mean(axis=1)
    b = rel.where(~inside & ok).mean(axis=1)
    return (a - b).where((inside & ok).any(axis=1) & (~inside & ok).any(axis=1)).dropna()


def fmt(x: pd.Series) -> str:
    return f"{x.mean() * 1e4:+.1f} bp ({newey_west_t(x, 3):+.1f}), {len(x)}"


def main() -> None:
    pd.set_option("display.width", 220)
    px = CachedReader(YFinanceReader()).history(MARKETS, start="1995-01-01")
    b = core.load()
    rf_d = b.prices[core.CASH].pct_change(fill_method=None).reindex(px.index).fillna(0.0)
    mpx = px.resample("ME").last()
    mex = mpx.pct_change(fill_method=None).sub((1 + rf_d).resample("ME").prod() - 1, axis=0)
    mex = mex.where(mpx.notna() & mpx.shift(1).notna()).loc["2001":]
    rel = mex.sub(mex.mean(axis=1), axis=0)
    print(f"{mex.index[0].date()} .. {mex.index[-1].date()}, {len(MARKETS)} markets "
          f"({len(CONTROLS)} controls)")

    print("\n1. POOLED SPREAD, markets in their window minus the others, relative returns: "
          "mean a month (Newey-West t), months")
    rows = {}
    for part in ("both", "pre", "first"):
        s = spread(rel, flags(rel.index, part))
        row = {"all": fmt(s)} | {f"{a}-{z[2:]}": fmt(s.loc[a:z]) for a, z in PERIODS}
        rows[{"both": "window (decision)", "pre": "pre-season month",
              "first": "season's first month"}[part]] = row
    print(pd.DataFrame(rows).T.to_string())

    inside = flags(rel.index)
    per = {}
    for t in WINDOWS:
        x, w = rel[t].dropna(), inside[t].reindex(rel[t].dropna().index)
        if w.sum() > 3 and (~w).sum() > 3:
            per[t] = {"in window bp": x[w].mean() * 1e4, "other bp": x[~w].mean() * 1e4,
                      "t": stats.ttest_ind(x[w], x[~w], equal_var=False).statistic,
                      "years": len(x) / 12}
    print("\n   by market: relative return a month, in window vs other months (Welch t)")
    print(pd.DataFrame(per).T.to_string(float_format="{:+.2f}".format))

    th = rel["THD"].dropna()
    tax = th.index.month.isin([11, 12])
    win = inside["THD"].reindex(th.index)
    print(f"\n3. THAILAND (THD, {th.index[0].year}-): window {th[win].mean() * 1e4:+.1f}"
          f" bp vs {th[~win].mean() * 1e4:+.1f} bp a month relative; "
          f"tax-fund Nov-Dec {th[tax].mean() * 1e4:+.1f} vs {th[~tax].mean() * 1e4:+.1f} bp "
          f"(t {stats.ttest_ind(th[tax], th[~tax], equal_var=False).statistic:+.1f}); absolute "
          f"Nov-Dec {mex['THD'].dropna()[tax].mean() * 1e4:+.1f} vs "
          f"{mex['THD'].dropna()[~tax].mean() * 1e4:+.1f} bp")

    # 2. The sleeve, reported whatever the rule says.
    w = pd.DataFrame(0.0, index=mex.index, columns=MARKETS)
    for d in mex.index:
        ok = mex.loc[d].notna()
        a, z = (inside.loc[d] & ok), (~inside.loc[d] & ok)
        if a.any() and z.any():
            w.loc[d, a[a].index] = 1.0 / a.sum()
            w.loc[d, z[z].index] = -1.0 / z.sum()
    rets_d = px.pct_change(fill_method=None).sub(rf_d, axis=0).fillna(0.0).loc["2001":]
    w_d = w.reindex(rets_d.index.to_period("M").to_timestamp("M")).set_axis(rets_d.index)
    cost = pd.Series(COST_BPS).reindex(MARKETS).fillna(5.0) / 1e4
    trades = w.diff().abs().fillna(w.abs())
    cost_m = (trades * cost).sum(axis=1)
    first_day = rets_d.index.to_series().groupby(rets_d.index.to_period("M")).min()
    cost_d = pd.Series(0.0, index=rets_d.index)
    for d, c in cost_m.items():
        fd = first_day.get(d.to_period("M"))
        if fd is not None:
            cost_d[fd] += c
    borrow = w_d.clip(upper=0).abs().sum(axis=1) * BORROW / TRADING_DAYS
    raw = (w_d * rets_d).sum(axis=1) - cost_d - borrow
    vol = raw.rolling(756, min_periods=504).std().shift(1) * np.sqrt(TRADING_DAYS)
    sleeve = (raw * TARGET_VOL / vol).dropna()
    ann = np.sqrt(TRADING_DAYS)
    print(f"\n2. SLEEVE (10% vol), {sleeve.index[0].date()} on; one-way turnover "
          f"{trades.sum(axis=1).mean():.2f}x a month")
    for a, z in (("2001", "2008-08"), ("2008-09", "2016"), ("2017", "2026"), ("2008-09", "2026")):
        x = sleeve.loc[a:z]
        print(f"   {a}..{z}: Sharpe {x.mean() / x.std() * ann:+.2f}, "
              f"t {x.mean() / x.std() * np.sqrt(len(x)):+.2f}")
    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    s = sleeve.reindex(book.index).fillna(0.0)
    t = compare({"saved (core + all overlays)": book, "+ 0.2 x dividend-calendar sleeve":
                 book + 0.2 * s}, split_date=SPLIT)
    print("   book: " + "; ".join(f"{n} {t['full']['sharpe'][n]:.3f} ({t['train']['sharpe'][n]:.3f}"
                                    f" / {t['test']['sharpe'][n]:.3f})" for n in t["full"].index)
          + f"; corr {s.corr(book):+.2f}")


if __name__ == "__main__":
    main()
