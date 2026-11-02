"""Options-expiration week (idea 15).

Monthly equity options expire on the third Friday. Dealers' hedging of large option books
around the expiry has been linked to positive returns over expiration week and weaker ones
the week after (Stivers and Sun 2013, for optionable stocks). **Fixed before any test.**

* Window: the trading days of the week holding the month's third Friday (Monday to Friday;
  if that Friday is a holiday the expiry moves to Thursday, the week is the same).
* Effect: ^GSPC daily excess return over T-bills in the window against other days (Welch t),
  by period 1983-99 / 2000-12 / 2013-26 (the last when weekly and 0DTE options grew); the week
  after expiration week reported as a check.
* Overlay: +0.25x SPY over the window, cash otherwise, 1 bp each way.

Rule (pre-registered from the literature): the window's excess positive in every period, and
the saved book (core plus its overlays) with the overlay up at least 0.02 Sharpe over the
window and in both halves (split 2017-07-26).

**Result (2026-11-02): fails; the effect is gone.** ^GSPC daily excess, bp: expiration week
+7.53 against +2.49 on other days in 1983-99 (t 1.5; the week after -3.83, t -2.4), +0.81
against -0.23 in 2000-12 (t 0.2), +1.49 against +5.64 in 2013-26 (t -1.0, now below other
days, as weekly and 0DTE options spread the hedging over the month). The overlay earns +0.08%
a year since 2008-09 (Sharpe 0.03) and only adds equity beta (+0.25 correlation with the
book): 1.333 against 1.443, max drawdown -18.2%. Idea 15 closed.

Usage:
    uv run python research/research_opex_week.py
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
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

SIZE, COST_BPS = 0.25, 1.0
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("1983", "1999"), ("2000", "2012"), ("2013", "2026"))


def opex_weeks(index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    """True on trading days in expiration week, and in the week after it."""
    third_friday = {}
    for p in sorted(set(index.to_period("M"))):
        fridays = pd.date_range(p.start_time, p.end_time, freq="W-FRI")
        third_friday[p] = fridays[2]
    week = pd.Series(index.to_period("W-FRI"), index=index)
    opex = {third_friday[p].to_period("W-FRI") for p in third_friday}
    after = {w + 1 for w in opex}
    return week.isin(opex), week.isin(after)


def main() -> None:
    pd.set_option("display.width", 220)
    raw = CachedReader(YFinanceReader()).history(["^GSPC", "^IRX"], start="1982-01-01")
    rf = ((1 + raw["^IRX"].ffill() / 100) ** (1 / TRADING_DAYS) - 1).shift(1).fillna(0.0)
    ex = (raw["^GSPC"].pct_change(fill_method=None) - rf).dropna().loc["1983":]
    inside, after = opex_weeks(ex.index)
    print(f"^GSPC {ex.index[0].date()} .. {ex.index[-1].date()}; expiration week "
          f"{inside.mean():.0%} of days")
    print("\n1. EFFECT: mean excess return a day (bp), against all other days (Welch t)")
    for a, z in (*PERIODS, ("1983", "2026")):
        x, w, f = ex.loc[a:z], inside.loc[a:z], after.loc[a:z]
        t_w = stats.ttest_ind(x[w], x[~w], equal_var=False).statistic
        t_f = stats.ttest_ind(x[f], x[~f], equal_var=False).statistic
        print(f"   {a}-{z}: expiration week {x[w].mean() * 1e4:+.2f} (t {t_w:+.1f}); week after "
              f"{x[f].mean() * 1e4:+.2f} (t {t_f:+.1f}); other days {x[~w].mean() * 1e4:+.2f}")

    b = core.load()
    spy = b["market"].iloc[:, 0]
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    sx = (spy.pct_change(fill_method=None) - cash).fillna(0.0)
    held = SIZE * opex_weeks(b.index)[0].astype(float)
    trades = held.diff().abs().fillna(held.abs())
    o = held * sx - trades * COST_BPS / 1e4
    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    oo = o.reindex(book.index).fillna(0.0)
    sr = oo.mean() / oo.std()
    print(f"\n2. OVERLAY (+0.25x SPY in expiration week), {book.index[0].date()} on: "
          f"{oo.mean() * TRADING_DAYS:+.2%}/yr, Sharpe {sr * np.sqrt(TRADING_DAYS):+.2f}, "
          f"t {sr * np.sqrt(len(oo)):+.2f}; corr with the book {oo.corr(book):+.2f}")
    t = compare({"saved (core + all overlays)": book, "+ expiration-week overlay": book + oo},
                split_date=SPLIT)
    out = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                        "test": t["test"]["sharpe"], "vol": t["full"]["ann_vol"],
                        "max_dd": t["full"]["max_dd"]})
    print(out.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
