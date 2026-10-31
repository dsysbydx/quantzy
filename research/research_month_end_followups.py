"""Month-end follow-ups: Treasury overlay split between TLT and IEF; pension rebalancing by size.

Both follow ``research_month_end_flows.py`` and were shaped by it. **Fixed before any test.**

A. *TLT, IEF or both, at the same risk.* IEF's month-end effect is 0.92 correlated with
   TLT's, so adding it is mostly a bigger overlay. The question is how to spend the overlay's
   *fixed* risk: TLT only (saved, 0.5x), IEF only, or half the risk in each. IEF's size per
   unit of TLT risk is the TLT / IEF daily-vol ratio over 2002-08 (before the book's sample),
   so nothing is fitted to the test. Reported with gross exposure on window days: IEF needs
   more notional for the same risk, which binds under Reg T in ETFs but not with futures.
   Rule: a split is preferred only if it lifts the book +0.02 in both halves.
B. *Pension rebalancing by magnitude.* The sign rule's gain needed the signal measured at the
   trading close; the month-to-date SPY-IEF deviation before the entry day still predicted the
   window (t -4.0). Here the signal is ``z = MTD / (sd_daily * sqrt(n))``: SPY minus IEF return
   from the previous month-end through the close *before* the entry close (``n`` days),
   over its expected spread, ``sd_daily`` the trailing 252-day sd of daily SPY-IEF returns.
   Over the last two trading days hold ``-0.5 * clip(z / 2, -1, 1)`` of SPY (full size at
   |z| >= 2), 1 bp each way. One candidate. Stricter rule, as it was shaped by the first
   result: +0.02 to the book in both halves *and* the overlay's own return t >= 2.

**Result (2026-10-31): A, no change; B passes, a candidate for a third overlay.**
2008-09-10 .. 2026-10-30; saved 1.340 (train 1.316 / test 1.364).

A. IEF per unit of TLT risk: 1.63x. IEF only 1.343 (1.293 / 1.390), half / half 1.343
   (1.306 / 1.378): +0.003, short of the rule, at more gross on window days (2.08x / 1.92x
   against 1.76x). IEF has decayed less (2017-26 overlay Sharpe 0.57 against 0.38) and draws
   down less (-14.5%); revisit only with futures, where gross does not bind.
B. Pension rebalancing by magnitude: book 1.396 (1.410 / 1.388), max drawdown unchanged
   (-15.5%); the overlay's own return t 2.96 since 2002-08 (2.39 since 2008-09), Sharpe 0.60,
   correlation with the book +0.04. Both parts of the stricter rule pass. Checks run after:
   the signal through t-2 or t-3 gives 1.401 / 1.403 (a slow flow, as it should be; the sign
   rule collapsed here); with 2008-09 - 2009-06 zeroed, 1.387; 13 of 19 years positive.
   Against it: the best five months give 58% of the gain (full size in 3% of months), and it
   is fading (overlay Sharpe 0.91 / 0.77 / 0.35 by period, 2017-26 t 1.1; 2025-26 slightly
   negative) since publication in 2025. It is short SPY 61% of the time: an ETF short or
   ES / MES futures.

Usage:
    uv run python research/research_month_end_followups.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402
from research_month_end_flows import SIZE, SPLIT, entry_rows, load, overlay, perf  # noqa: E402

from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402
from quantzy.strategies import treasury_month_end as tme  # noqa: E402

RATIO_FIT = ("2002-08-01", "2008-08-31")
PERIODS = (("2002-08", "2008-08"), ("2008-09", "2016-12"), ("2017-01", "2026-12"))


def pension_z(rets: pd.DataFrame) -> pd.Series:
    """The month-to-date SPY-IEF deviation before each entry close, in expected sds."""
    rel = rets["SPY"] - rets["IEF"]
    month = rets.index.to_period("M")
    growth = (1 + rets[["SPY", "IEF"]]).groupby(month).cumprod()
    mtd = (growth["SPY"] - growth["IEF"]).groupby(month).shift(1).fillna(0.0)  # through t-1
    n = rel.groupby(month).cumcount()  # days in the month before row t
    sd = rel.rolling(252, min_periods=252).std().shift(1)
    z = mtd / (sd * np.sqrt(n.where(n > 0)))
    return z.where(entry_rows(rets.index))


def main() -> None:
    pd.set_option("display.width", 220)
    rets, rf = load()
    idx = rets.index
    window = tme.in_window(idx)
    a, z = RATIO_FIT
    ratio = float(rets["TLT"].loc[a:z].std() / rets["IEF"].loc[a:z].std())
    print(f"TLT / IEF daily vol ratio, {a} .. {z}: {ratio:.2f} (IEF per unit of TLT risk)")

    tlt = overlay(rets["TLT"], rf, window * SIZE)
    ief_only = overlay(rets["IEF"], rf, window * SIZE * ratio)
    split = (overlay(rets["TLT"], rf, window * SIZE / 2)
             + overlay(rets["IEF"], rf, window * SIZE / 2 * ratio))
    zsig = pension_z(rets).ffill().where(window)
    pension_w = (-SIZE * np.clip(zsig / 2, -1, 1)).fillna(0.0)
    pension = overlay(rets["SPY"], rf, pension_w)

    r_core, w, *_ = export.run("strategy")
    bundle = core.load()
    saved = export.with_overlays(r_core, bundle)  # core + TLT 0.5x + VIX hedge
    tlt_in_saved = tlt.reindex(saved.index).fillna(0.0)

    def swap(new: pd.Series) -> pd.Series:
        return saved - tlt_in_saved + new.reindex(saved.index).fillna(0.0)

    books = {"saved (TLT 0.5x)": saved, "IEF only, same risk": swap(ief_only),
             "half TLT / half IEF risk": swap(split),
             "saved + pension (magnitude)": saved + pension.reindex(saved.index).fillna(0.0)}
    t = compare(books, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    core_gross = w[core.RISKY].sum(axis=1).reindex(saved.index).fillna(0.0)
    win_b = window.reindex(saved.index, fill_value=False)
    extra = {"saved (TLT 0.5x)": SIZE, "IEF only, same risk": SIZE * ratio,
             "half TLT / half IEF risk": SIZE / 2 * (1 + ratio),
             "saved + pension (magnitude)": SIZE}
    m["gross on window days"] = [float((core_gross[win_b] + extra[k]).mean()) for k in m.index]
    print(f"\n1. BOOK, {saved.index[0].date()} .. {saved.index[-1].date()}, split {SPLIT.date()} "
          "(VIX hedge in every row)")
    print(m.to_string(float_format="{:.3f}".format))

    print("\n2. OVERLAYS on their own, 2002-08 on; by period: Sharpe (t of the mean)")
    rows = {}
    for name, o in (("TLT 0.5x (saved)", tlt), ("IEF, same risk", ief_only),
                    ("half / half", split), ("pension (magnitude)", pension)):
        row = perf(o)
        row["t"] = o.mean() / o.std() * np.sqrt(len(o))
        for p0, p1 in PERIODS:
            x = o.loc[p0:p1]
            row[f"{p0[:4]}-{p1[2:4]}"] = (f"{x.mean() / x.std() * np.sqrt(252):+.2f} "
                                          f"({x.mean() / x.std() * np.sqrt(len(x)):+.1f})")
        rows[name] = row
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.3f}".format))
    full = (zsig.abs() >= 2).groupby(idx.to_period("M")).any().mean()
    corr = pension.reindex(saved.index).corr(saved)
    print(f"\n   pension: |z| >= 2 (full size) in {full:.0%} of months; "
          f"corr with the saved book {corr:+.2f}")


if __name__ == "__main__":
    main()
