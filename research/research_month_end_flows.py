"""More month-end flows: other bond indexes, and pension rebalancing between stocks and bonds.

``research_calendar_flows.py`` adopted the Treasury month-end overlay (0.5x TLT over the last
two trading days; bond index funds extend duration at month-end). Two more flows hit the same
days, each with a mechanism and a publication. **Fixed before any test.**

A. *Duration extension beyond long Treasuries.* Index extension happens across the bond
   index, so the same window in IEF (7-10 year Treasuries) and LQD (investment-grade
   corporates). Each as an overlay like TLT's: 0.5x in the window, T-bills otherwise.
   Asked separately: does it add on top of the TLT overlay already in the book?
B. *Pension rebalancing* (Harvey, Mazzoleni and Melone, 2025). Balanced funds rebalance to
   fixed weights near month-end, so if stocks beat bonds this month they sell stocks into the
   last days, and buy if stocks lagged. The signal is SPY minus IEF total return from the
   previous month's last close to the close three trading days before month-end (the TLT
   overlay's entry); over the last two trading days hold ``-0.5 x sign(signal)`` of SPY,
   excess over T-bills. A signed overlay: shorting SPY needs futures or a short ETF; the test
   reads its return either way. Also the continuous slope of SPY's window excess return on
   the signal.
C. *Quarter-ends*, diagnostic only: each effect's window return at quarter-end months against
   the others, where rebalancing should be heaviest.

Costs as the TLT overlay: 1 bp each way. Effects from 2002-08 (LQD's and IEF's first full
month); the book from 2008-09 as saved.

Per effect: mean excess return a day inside the window against other days (Welch t), by
period (2002-08, 2008-16, 2017-26); the overlay's Sharpe, return, vol and correlation with
the saved book (core plus both overlays). Adoption rule, as for the TLT overlay: an effect
positive in every period, and the saved book plus the candidate overlay up at least 0.02
Sharpe over the window and in both halves (split 2017-07-26).

**Result (2026-10-31): none adopted; pension rebalancing is real in magnitude but the
pre-registered sign rule does not survive a one-day lag.** 2002-08 .. 2026-10; book from
2008-09, saved 1.340 (train 1.316 / test 1.364).

* IEF month-end: t 3.9 / 2.6 / 2.5 by period; book 1.371 (1.359 / 1.385), passes, but it is
  0.92 correlated with the TLT overlay: more of the same trade, so a question of the TLT
  overlay's size (capped at 0.5x for decay and Reg T), not a new source. Not adopted as one.
* LQD month-end: t 3.4 / 1.2 / 1.9; book 1.363 (1.357 / 1.371), +0.007 in the test half: fails.
* Pension rebalancing (sign rule, SPY, 0.5x): positive in every period (t 1.0 / 1.5 / 1.6),
  slope on the signal -7.1 bp per 1% (t -4.2; -1.1 / -3.5 / -2.0), correlation with the book
  0.03; book 1.407 (1.402 / 1.415). But with the signal one day earlier the gain is gone
  (1.341; two days earlier 1.321), the best five months give 45% of it (mostly late 2008),
  and the max drawdown deepens (-17.9%, Dec 2008 - Mar 2009). Split, the window return loads
  on the entry day's own SPY-IEF move (t -2.9: short-term reversal) *and* on the month to date
  before it (t -4.0, negative in every period). The month-to-date information is real, but a
  sign rule throws away its magnitude once lagged. Not adopted: trading at the close the
  signal is measured on cannot be checked on daily data.
* Quarter-ends: no effect concentrates there (all |t| <= 1.0).

Follow-up (a new pre-registration, shaped by this result, so it takes the stricter bar):
position proportional to the month-to-date deviation through the day *before* the entry
close, capped, so it is executable on daily data.

Usage:
    uv run python research/research_month_end_flows.py
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
from quantzy.strategies import treasury_month_end as tme  # noqa: E402

START = "2002-07-31"
SIZE = 0.5
COST_BPS = 1.0
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("2002-08", "2008-08"), ("2008-09", "2016-12"), ("2017-01", "2026-12"))


def load() -> tuple[pd.DataFrame, pd.Series]:
    px = CachedReader(YFinanceReader()).history(["SPY", "IEF", "TLT", "LQD", "^IRX"],
                                                start="2002-06-01")
    px = px[px[["SPY", "IEF", "TLT", "LQD"]].notna().all(axis=1)]
    rets = px[["SPY", "IEF", "TLT", "LQD"]].pct_change(fill_method=None)
    rf = ((1 + px["^IRX"].ffill() / 100) ** (1 / TRADING_DAYS) - 1).shift(1)
    keep = rets.index >= START
    return rets[keep], rf[keep].fillna(0.0)


def entry_rows(index: pd.DatetimeIndex) -> pd.Series:
    """True on each month's entry close: the day before its last ``tme.DAYS`` trading days."""
    window = tme.in_window(index)
    return window.shift(-1, fill_value=False) & ~window


def pension_signal(rets: pd.DataFrame) -> pd.Series:
    """SPY minus IEF return month to date, through each entry close (NaN elsewhere)."""
    month = rets.index.to_period("M")
    growth = (1 + rets[["SPY", "IEF"]]).groupby(month).cumprod()
    rel = growth["SPY"] - growth["IEF"]
    return rel.where(entry_rows(rets.index))


def overlay(asset: pd.Series, rf: pd.Series, held: pd.Series) -> pd.Series:
    """``held`` (a weight per day) times the asset's excess return, less 1 bp a side traded."""
    trades = held.diff().abs().fillna(held.abs())
    return held * (asset - rf) - trades * COST_BPS / 1e4


def effect(excess: pd.Series, inside: pd.Series) -> dict[str, float]:
    a, b = excess[inside], excess[~inside]
    return {"in bp/day": a.mean() * 1e4, "out bp/day": b.mean() * 1e4,
            "t": stats.ttest_ind(a, b, equal_var=False).statistic}


def perf(r: pd.Series) -> dict[str, float]:
    ann = np.sqrt(TRADING_DAYS)
    return {"ret": r.mean() * TRADING_DAYS, "vol": r.std() * ann,
            "sharpe": r.mean() / r.std() * ann if r.std() > 0 else np.nan}


def main() -> None:
    pd.set_option("display.width", 220)
    rets, rf = load()
    idx = rets.index
    window = tme.in_window(idx)
    quarter = pd.Series(idx.month % 3 == 0, index=idx)

    # Pension: hold -SIZE * sign(signal) of SPY through the window days that follow each entry.
    sig = pension_signal(rets).ffill().where(window)
    pension_w = (-SIZE * np.sign(sig)).fillna(0.0)
    overlays = {
        "IEF month-end": overlay(rets["IEF"], rf, window * SIZE),
        "LQD month-end": overlay(rets["LQD"], rf, window * SIZE),
        "pension (SPY)": overlay(rets["SPY"], rf, pension_w),
        "TLT month-end (saved)": overlay(rets["TLT"], rf, window * SIZE),
    }

    print(f"{idx[0].date()} .. {idx[-1].date()}; window = last {tme.DAYS} trading days")
    print("\n1. EFFECT: mean excess return a day, in the window vs other days (Welch t), by period")
    rows = {}
    for name, asset in (("IEF", "IEF"), ("LQD", "LQD"), ("TLT", "TLT")):
        ex = rets[asset] - rf
        rows[f"{name} month-end"] = {f"{a[:4]}-{b[2:4]} {k}": v
                                     for a, b in PERIODS
                                     for k, v in effect(ex.loc[a:b], window.loc[a:b]).items()}
    signed = (-np.sign(sig) * (rets["SPY"] - rf)).where(window)
    for a, b in PERIODS:
        x = signed.loc[a:b].dropna()
        rows.setdefault("pension (SPY, signed)", {})[f"{a[:4]}-{b[2:4]} in bp/day"] = x.mean() * 1e4
        rows["pension (SPY, signed)"][f"{a[:4]}-{b[2:4]} t"] = x.mean() / x.std() * np.sqrt(len(x))
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.2f}".format))

    print("\n   pension slope: SPY window excess return (2 days, bp) on the signal (%), by period")
    ent = pension_signal(rets).dropna()
    win_ret = ((rets["SPY"] - rf).where(window)).groupby(idx.to_period("M")).sum()
    df = pd.DataFrame({"signal": ent.groupby(ent.index.to_period("M")).last(),
                       "window": win_ret}).dropna()
    df["q_end"] = df.index.month % 3 == 0
    for a, b in (*PERIODS, (PERIODS[0][0], PERIODS[-1][1])):
        d = df.loc[a:b]
        res = stats.linregress(d["signal"] * 100, d["window"] * 1e4)
        print(f"   {a}..{b}: slope {res.slope:+.2f} bp per 1% (t {res.slope / res.stderr:+.1f}), "
              f"{len(d)} months")

    print("\n2. QUARTER-END vs OTHER MONTHS: mean window return per month (bp), diagnostic")
    q = {}
    for name, ser in (("IEF", rets["IEF"] - rf), ("LQD", rets["LQD"] - rf),
                      ("TLT", rets["TLT"] - rf), ("pension signed", signed.fillna(0.0))):
        m = ser.where(window).groupby(idx.to_period("M")).sum()
        qe = m.index.month % 3 == 0
        q[name] = {"quarter-end": m[qe].mean() * 1e4, "other months": m[~qe].mean() * 1e4,
                   "t (diff)": stats.ttest_ind(m[qe], m[~qe], equal_var=False).statistic}
    print(pd.DataFrame(q).T.to_string(float_format="{:+.2f}".format))
    _ = quarter

    # 3. The book: saved core + both overlays, plus each candidate.
    r_core, *_ = export.run("strategy")
    bundle = core.load()
    saved = export.with_overlays(r_core, bundle)
    books = {"saved (core + overlays)": saved}
    for name in ("IEF month-end", "LQD month-end", "pension (SPY)"):
        books[f"saved + {name}"] = saved + overlays[name].reindex(saved.index).fillna(0.0)
    books["saved + pension + LQD"] = (saved + overlays["pension (SPY)"].reindex(saved.index)
                                      .fillna(0.0) + overlays["LQD month-end"]
                                      .reindex(saved.index).fillna(0.0))
    print("\n3. OVERLAYS on their own (2002-08 on) and correlation with the saved book")
    o = pd.DataFrame({k: perf(v) for k, v in overlays.items()}).T
    o["corr w/ book"] = [v.reindex(saved.index).corr(saved) for v in overlays.values()]
    o["corr w/ TLT ME"] = [v.corr(overlays["TLT month-end (saved)"]) for v in overlays.values()]
    print(o.to_string(float_format="{:.3f}".format))
    t = compare(books, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    print(f"\n4. BOOK, {saved.index[0].date()} .. {saved.index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
