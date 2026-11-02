"""A seasonal equity tilt: the Halloween effect ("sell in May").

Equities earned far more from November to April than from May to October in most of 37
markets (Bouman and Jacobsen 2002), out of sample since (Andrade, Chhaochharia and Fuerst
2013) and over 300 years of UK data (Zhang and Jacobsen 2021). The user noticed November-
December strength in ``research_dividend_calendar.py``; to avoid fitting what was just seen,
this uses the literature's fixed split, November-April against May-October, not the months
observed. **Fixed before any test.**

1. Effect: monthly excess returns over T-bills, November-April against May-October.
   ^GSPC (price; dividends are spread evenly enough not to bias the split) from 1950, by
   period 1950-89 / 1990-2007 / 2008-26; and the 28 country ETFs (total return) from 1996: the
   share of markets where November-April beats May-October, and the pooled difference. The
   12-month profile is printed as a description only.
2. Overlay: +0.25x SPY over November-April and -0.25x over May-October, excess over cash, 1 bp
   each way, short borrow 0.5% a year while short. Net zero equity over a year: it moves *when*
   the book holds equities, not how much on average.

Rule: November-April above May-October in every ^GSPC period and in at least 70% of the
country markets, *and* the saved book (core plus its overlays) with the overlay up at least
0.02 Sharpe over the window and in both halves (split 2017-07-26).

**Result (2026-11-02): the effect is real; this overlay does not help the book.** ^GSPC,
Nov-Apr against May-Oct, bp a month: +82 / -14 (t 2.6) in 1950-89, +76 / +5 (t 1.3) in
1990-2007, +94 / +50 (t 0.7) in 2008-26 -- the right sign in every period, fading in the US.
Across the 28 country markets Nov-Apr wins in 27 (96%), pooled +134 bp a month, still +127 bp
since 2008: outside the US it is large and has not faded. The effect part of the rule passes.
The overlay, +/-0.25x SPY: +0.45% a year since 2008-09 (Sharpe 0.09, t 0.4); the book 1.305
against 1.443 (train 1.426 / test 1.215), vol 11.1%, max drawdown -20.7% -- long an extra
0.25x SPY through Nov 2008 - Mar 2009. Not adopted. Two follow-ups, shaped by this result:
the tilt on non-US equity (EFA / EEM), where the effect lives, and through the optimizer (a
seasonal equity prior), so the risk model and trend still cut equities in a crash.

Usage:
    uv run python research/research_halloween.py
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
from research_country_seasonality import MARKETS  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

WINTER = [11, 12, 1, 2, 3, 4]
SIZE, COST_BPS, BORROW = 0.25, 1.0, 0.005
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("1950", "1989"), ("1990", "2007"), ("2008", "2026"))


def monthly_excess(px: pd.DataFrame, rf_daily: pd.Series) -> pd.DataFrame:
    mpx = px.resample("ME").last()
    rf_m = (1 + rf_daily.reindex(px.index).fillna(0.0)).resample("ME").prod() - 1
    out = mpx.pct_change(fill_method=None).sub(rf_m, axis=0)
    return out.where(mpx.notna() & mpx.shift(1).notna())


def main() -> None:
    pd.set_option("display.width", 220)
    r = CachedReader(YFinanceReader())
    gspc = r.history(["^GSPC", "^IRX"], start="1949-01-01")
    irx = gspc["^IRX"].ffill() / 100
    rf_long = ((1 + irx) ** (1 / TRADING_DAYS) - 1).shift(1).fillna(0.0)  # ^IRX from 1960
    m = monthly_excess(gspc[["^GSPC"]], rf_long)["^GSPC"].dropna()
    win = m.index.month.isin(WINTER)
    print("1. EFFECT, ^GSPC monthly excess return (rf from ^IRX, 0 before 1960)")
    for a, z in (*PERIODS, ("1950", "2026")):
        x, w = m.loc[a:z], win[(m.index >= a) & (m.index <= f"{z}-12-31")]
        t_ = stats.ttest_ind(x[w], x[~w], equal_var=False).statistic
        print(f"   {a}-{z}: Nov-Apr {x[w].mean() * 1e4:+.0f} bp, "
              f"May-Oct {x[~w].mean() * 1e4:+.0f} bp a month (diff t {t_:+.1f})")
    prof = m.loc["1950":].groupby(m.loc["1950":].index.month).mean() * 1e4
    print("   12-month profile 1950-2026, bp (description only): "
          + " ".join(f"{k}:{v:+.0f}" for k, v in prof.items()))

    b = core.load()
    rf = b.prices[core.CASH].pct_change(fill_method=None)
    cm = monthly_excess(r.history(MARKETS, start="1995-01-01"), rf)
    cw = cm.index.month.isin(WINTER)
    diff = cm[cw].mean() - cm[~cw].mean()
    pooled = cm[cw].stack().mean() - cm[~cw].stack().mean()
    print(f"\n   28 country markets, 1996 on: Nov-Apr beats May-Oct in {int((diff > 0).sum())} of "
          f"{diff.notna().sum()} ({(diff > 0).mean():.0%}); pooled difference "
          f"{pooled * 1e4:+.0f} bp a month; median {diff.median() * 1e4:+.0f} bp")
    since = cm.loc["2008":]
    sw = since.index.month.isin(WINTER)
    d8 = since[sw].mean() - since[~sw].mean()
    print(f"   since 2008: {int((d8 > 0).sum())} of {d8.notna().sum()} ({(d8 > 0).mean():.0%}); "
          f"pooled {since[sw].stack().mean() * 1e4 - since[~sw].stack().mean() * 1e4:+.0f} bp")

    # 2. The overlay on SPY.
    spy = b["market"].iloc[:, 0]
    idx = b.index
    held = pd.Series(np.where(idx.month.isin(WINTER), SIZE, -SIZE), index=idx, dtype=float)
    ex = (spy.pct_change(fill_method=None) - rf).fillna(0.0)
    trades = held.diff().abs().fillna(held.abs())
    o = held.shift(1).fillna(0.0) * ex - trades * COST_BPS / 1e4 \
        - held.clip(upper=0).abs() * BORROW / TRADING_DAYS
    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    oo = o.reindex(book.index).fillna(0.0)
    ann = np.sqrt(TRADING_DAYS)
    sr = oo.mean() / oo.std()
    print(f"\n2. OVERLAY (+/-0.25x SPY), {book.index[0].date()} on: "
          f"{oo.mean() * TRADING_DAYS:+.2%}/yr, Sharpe {sr * ann:+.2f}, "
          f"t {sr * np.sqrt(len(oo)):+.2f}; corr with the book {oo.corr(book):+.2f}")
    t = compare({"saved (core + all overlays)": book, "+ Halloween overlay": book + oo},
                split_date=SPLIT)
    out = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                        "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    print(out.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
