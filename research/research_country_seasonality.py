"""Calendar-month seasonality across country equity markets (idea 6, step 1 of the user's
dividend-calendar idea, 2026-11-01).

Assets that did well in a calendar month tend to do well in the same month in later years
(Heston and Sadka 2008), across countries and industries too (Keloharju, Linnainmaa and
Nyberg 2016). Mechanisms proposed include dividend calendars (the dividend month premium,
Hartzmark and Solomon 2013), tax deadlines and fund-flow calendars -- the user's hypothesis
for Thailand. This tests the general pattern on country indices; step 2 tests the
dividend-calendar mechanism directly. **Fixed before any test.**

Universe: 28 markets (SPY and 27 country ETFs), total return, monthly, over T-bills; a
market enters once it has 5 years of history.

* Score: each market's mean excess return in the same calendar month over the previous 5-20
  years, prior years only. Control: its mean in the *other* months over the same years
  (seasonality net of momentum and of each market's average, per Heston and Sadka).
* IC: the cross-sectional rank IC of the score against that month's excess return, months
  with >= 10 markets; Newey-West t (3 lags); by period 2001-08, 2009-16, 2017-26.
* Sleeve: long the top third, short the bottom third, equal weight, dollar neutral, set at
  each month-end; costs one way 5 bp (developed), 15 bp (emerging), 20 bp (THD, EPHE, EIDO,
  EPU, TUR); 1% a year to borrow the shorts; scaled to 10% vol by its trailing 36 months.

Rule: mean IC > 0 with t >= 2 and positive in each period; the sleeve's Sharpe above 0.3
since 2008-09; and the saved book plus 0.2x the sleeve up at least 0.02 Sharpe over the
window and in both halves (split 2017-07-26).

**Result (2026-11-01): fails; generic calendar seasonality has no edge across countries.**
1995-01 .. 2026-10, 28 markets, 307 months with >= 10. Rank IC of the same-month score
-0.023 (t -1.6): -0.015 / -0.009 / -0.041 in 2001-08 / 2009-16 / 2017-26, negative in every
period; net of other months -0.014 (t -1.0); the other-month control -0.008. The sleeve
loses: Sharpe -0.62 / -0.52 / -1.07 by period, -0.82 (t -3.5) since 2008-09, with 2.8x
one-way turnover a month and ~3.2% a year of costs unscaled -- the ranks reshuffle every
month. The book with 0.2x falls to 1.233 against 1.443. With ~20 years per (market, month)
cell the trailing averages are mostly noise; step 2 fixes the months from dividend
calendars instead of estimating them.

Usage:
    uv run python research/research_country_seasonality.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.research import compare, newey_west_t  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

DEVELOPED = ["SPY", "EWJ", "EWA", "EWH", "EWS", "EWC", "EWG", "EWU", "EWQ", "EWI", "EWP",
             "EWN", "EWL", "EWD"]
EMERGING = ["EWM", "EWT", "EWY", "FXI", "EWZ", "EWW", "EZA", "ECH", "INDA"]
THIN = ["THD", "EPHE", "EIDO", "EPU", "TUR"]
MARKETS = DEVELOPED + EMERGING + THIN
COST_BPS = {**{t: 5.0 for t in DEVELOPED}, **{t: 15.0 for t in EMERGING},
            **{t: 20.0 for t in THIN}}
MIN_YEARS, MAX_YEARS, MIN_MARKETS = 5, 20, 10
BORROW, TARGET_VOL = 0.01, 0.10
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("2001", "2008"), ("2009", "2016"), ("2017", "2026"))


def scores(mex: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Same-calendar-month and other-month mean excess return over prior years, per row."""
    same = pd.DataFrame(np.nan, index=mex.index, columns=mex.columns)
    other = same.copy()
    for i, d in enumerate(mex.index):
        past = mex.iloc[:i]
        past = past[past.index.year >= d.year - MAX_YEARS]
        m = past[past.index.month == d.month]
        o = past[past.index.month != d.month]
        enough = m.notna().sum() >= MIN_YEARS
        same.iloc[i] = m.mean().where(enough)
        other.iloc[i] = o.mean().where(enough)
    return same, other


def rank_ic(score: pd.DataFrame, ret: pd.DataFrame) -> pd.Series:
    out = {}
    for d in score.index:
        x, y = score.loc[d], ret.loc[d]
        ok = x.notna() & y.notna()
        if ok.sum() >= MIN_MARKETS:
            out[d] = stats.spearmanr(x[ok], y[ok]).statistic
    return pd.Series(out)


def main() -> None:
    pd.set_option("display.width", 220)
    px = CachedReader(YFinanceReader()).history(MARKETS, start="1995-01-01")
    b = core.load()
    rf_d = b.prices[core.CASH].pct_change(fill_method=None).reindex(px.index).fillna(0.0)
    month_px = px.resample("ME").last()
    mret = month_px.pct_change(fill_method=None)
    rf_m = (1 + rf_d).resample("ME").prod() - 1
    mex = mret.sub(rf_m, axis=0)
    mex = mex.where(month_px.notna() & month_px.shift(1).notna())
    same, other = scores(mex)
    seasonal = same - other

    print(f"{mex.index[0].date()} .. {mex.index[-1].date()}, {len(MARKETS)} markets")
    print("\n1. CROSS-SECTIONAL RANK IC vs that month's excess return: mean (Newey-West t), months")
    rows = {}
    for name, sc in (("same month (score)", same), ("same minus other months", seasonal),
                     ("other months (control)", other)):
        ic = rank_ic(sc, mex).dropna()
        row = {"all": f"{ic.mean():+.4f} ({newey_west_t(ic, 3):+.1f}), {len(ic)}"}
        for a, z in PERIODS:
            x = ic.loc[a:z]
            row[f"{a}-{z[2:]}"] = f"{x.mean():+.4f} ({newey_west_t(x, 3):+.1f})"
        rows[name] = row
    print(pd.DataFrame(rows).T.to_string())

    # 2. The sleeve: weights set at month-end t-1 from scores known then, earning month t.
    w = pd.DataFrame(0.0, index=mex.index, columns=MARKETS)
    for d in mex.index:
        s = same.loc[d].dropna()
        s = s[mex.loc[d, s.index].notna()]  # markets with a return this month
        if len(s) < MIN_MARKETS:
            continue
        k = len(s) // 3
        top, bot = s.nlargest(k).index, s.nsmallest(k).index
        w.loc[d, top], w.loc[d, bot] = 1.0 / k, -1.0 / k
    # daily returns: month t's weights (decided at the end of t-1) over month t's days
    rets_d = px.pct_change(fill_method=None).sub(rf_d, axis=0).fillna(0.0)
    # each day takes its own month's row (decided at the previous month-end)
    w_d = w.reindex(rets_d.index.to_period("M").to_timestamp("M")).set_axis(rets_d.index)
    gross = (w_d * rets_d).sum(axis=1)
    trades = w.diff().abs().fillna(w.abs())
    cost_m = sum(trades[t] * COST_BPS[t] / 1e4 for t in MARKETS)
    first_day = rets_d.index.to_series().groupby(rets_d.index.to_period("M")).min()
    cost_d = pd.Series(0.0, index=rets_d.index)
    for d, c in cost_m.items():
        fd = first_day.get(d.to_period("M"))
        if fd is not None:
            cost_d[fd] += c
    short_d = w_d.clip(upper=0).abs().sum(axis=1) * BORROW / TRADING_DAYS
    raw = (gross - cost_d - short_d).loc[w_d.abs().sum(axis=1).gt(0).idxmax():]
    vol = raw.rolling(756, min_periods=504).std().shift(1) * np.sqrt(TRADING_DAYS)
    sleeve = (raw * (TARGET_VOL / vol)).dropna()
    ann = np.sqrt(TRADING_DAYS)
    print(f"\n2. SLEEVE (scaled to 10% vol), {sleeve.index[0].date()} on; monthly one-way turnover "
          f"{trades.sum(axis=1).loc[w.abs().sum(axis=1) > 0].mean():.2f}x, cost "
          f"{cost_m.sum() / (len(raw) / TRADING_DAYS):.2%}/yr unscaled")
    for a, z in (("2001", "2008-08"), ("2008-09", "2016"), ("2017", "2026"), ("2008-09", "2026")):
        x = sleeve.loc[a:z]
        print(f"   {a}..{z}: Sharpe {x.mean() / x.std() * ann:+.2f}, "
              f"t {x.mean() / x.std() * np.sqrt(len(x)):+.2f}")

    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    s = sleeve.reindex(book.index).fillna(0.0)
    t = compare({"saved (core + all overlays)": book, "+ 0.2 x seasonality sleeve": book + 0.2 * s},
                split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "vol": t["full"]["ann_vol"],
                      "max_dd": t["full"]["max_dd"]})
    print(f"\n3. BOOK, split {SPLIT.date()}; sleeve corr with the book {s.corr(book):+.2f}")
    print(m.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
