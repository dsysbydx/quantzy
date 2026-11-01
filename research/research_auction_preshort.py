"""Short Treasuries into auctions only: the follow-up to ``research_auction_cycle.py``.

The auction cycle overlay failed because the post-auction recovery is not reliable, but the
concession *before* auctions has been significant since 2017 (IEF t -2.8, TLT t -2.4),
consistent with heavier supply. This keeps only that leg. It was shaped by the first result,
so it is one candidate under a stricter rule. **Fixed before the test.**

* Candidate: -0.25x IEF over the 5 trading days into each nominal 10-year auction close and
  -0.25x TLT into each 30-year auction close; flat otherwise. 1 bp each way and 0.5% a year
  short borrow while short.

Rule, all three: (1) the pre window negative in both halves of the book's window (2008-09 ..
2017-07 and 2017-07 .. 2026-10), for both ETFs; (2) the overlay's own return t >= 2 over the
book's window; (3) the saved book (core plus its three overlays) with the candidate up at
least 0.02 Sharpe over the window and in both halves. 2002-08 .. 2008-08, before the book's
window, is reported as an earlier check.

**Result (2026-11-01): passes all three, with caveats; a candidate pending a decision.**
(1) Pre window, bp a day (t): IEF -0.11 (-1.1) / -4.29 (-2.4), TLT -5.86 (-2.2) / -8.44 (-2.0)
in the two halves: negative in both, IEF's first half only just. (2) The overlay: +1.22% a
year at 2.6% vol over the book's window, Sharpe 0.47, t 2.02 (train 0.27, test 0.67).
(3) Book 1.489 against 1.371 (train 1.430 / test 1.543), correlation -0.11, max drawdown
unchanged; 15 of 19 years positive. Checks run after: the same shorts 10 or 15 days away from
the auctions *lose* (-1.1% / -1.0% a year, t -2.0 / -1.7), so the timing is what pays, not
bond drift; without 2022 +1.04% a year (t 1.70). Against it: 2002-08, before the book's
window, goes the other way (Sharpe -0.40, t -1.0; thin, 30-year auctions were suspended in
2001-06); it was chosen after the full-cycle result; t sits at the bar; and the concession
grew with Treasury supply, so it may fade if issuance does. Expect less than +0.12 live.

Usage:
    uv run python research/research_auction_preshort.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent))
from research_auction_cycle import TERMS, auction_dates, windows  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

SIZE, COST_BPS, BORROW = 0.25, 1.0, 0.005
SPLIT = pd.Timestamp("2017-07-26")
HALVES = (("2002-08", "2008-09-09", "2002-08 (earlier)"), ("2008-09-10", "2017-07-25", "train"),
          ("2017-07-26", "2026-12-31", "test"))


def short_overlay(b, idx, rf, dates, shift: int, rows: dict | None = None):
    """The overlay, with every window moved ``shift`` trading days later (0: the candidate).
    With ``rows``, also fills the pre-window effect table."""
    legs = []
    for term, (_, _, etf) in TERMS.items():
        ex = (b.prices[etf].pct_change(fill_method=None).reindex(idx) - rf).fillna(0.0)
        pre, _ = windows(idx, dates[term])
        pre = pre.shift(shift, fill_value=False)
        held = -SIZE * pre.astype(float)
        trades = held.diff().abs().fillna(held.abs())
        legs.append(held * ex - trades * COST_BPS / 1e4 - held.abs() * BORROW / TRADING_DAYS)
        if rows is not None:
            row = {}
            for a, z, lab in HALVES:
                e, w = ex.loc[a:z], pre.loc[a:z]
                t_ = stats.ttest_ind(e[w], e[~w], equal_var=False).statistic
                row[f"{lab} bp (t)"] = f"{e[w].mean() * 1e4:+.2f} ({t_:+.1f})"
            rows[f"{term} ({etf})"] = row
    return sum(legs), rows


def main() -> None:
    pd.set_option("display.width", 220)
    dates = auction_dates()
    b = core.load()
    idx = b.index[b.index >= "2002-08-01"]
    rf = b.prices[core.CASH].pct_change(fill_method=None).reindex(idx).fillna(0.0)
    o, rows = short_overlay(b, idx, rf, dates, 0, rows={})
    print(f"{idx[0].date()} .. {idx[-1].date()}")
    print("\n1. PRE WINDOW: mean excess return a day, against other days (Welch t)")
    print(pd.DataFrame(rows).T.to_string())

    print("\n2. OVERLAY on its own")
    for a, z, lab in HALVES + (("2008-09-10", "2026-12-31", "book window"),):
        x = o.loc[a:z]
        ann, sr = np.sqrt(TRADING_DAYS), x.mean() / x.std()
        print(f"   {lab}: {x.mean() * TRADING_DAYS:+.2%}/yr, vol {x.std() * ann:.2%}, "
              f"Sharpe {sr * ann:+.2f}, t {sr * np.sqrt(len(x)):+.2f}")

    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    oo = o.reindex(book.index).fillna(0.0)
    t = compare({"saved (core + 3 overlays)": book, "+ pre-auction short": book + oo},
                split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    # Checks added after the first run: does the timing matter, or is it bond drift?
    print("\n   placebo (same shorts, windows moved later), book window 2008-09 on:")
    for k in (10, 15):
        pl, _ = short_overlay(b, idx, rf, dates, k)
        x = pl.loc["2008-09-10":]
        print(f"     +{k} days: {x.mean() * TRADING_DAYS:+.2%}/yr, "
              f"t {x.mean() / x.std() * np.sqrt(len(x)):+.2f}")
    x = o.loc["2008-09-10":]
    x = x[x.index.year != 2022]
    print(f"   candidate without 2022: {x.mean() * TRADING_DAYS:+.2%}/yr, "
          f"t {x.mean() / x.std() * np.sqrt(len(x)):+.2f}")

    print(f"\n3. BOOK, {book.index[0].date()} .. {book.index[-1].date()}, split {SPLIT.date()}; "
          f"correlation {oo.corr(book):+.2f}")
    print(m.to_string(float_format="{:.3f}".format))
    yr = oo.groupby(oo.index.year).sum() * 100
    print("\n   overlay by year (%): " + " ".join(f"{y}:{v:+.2f}" for y, v in yr.items())
          + f" | positive {int((yr > 0).sum())}/{len(yr)}")


if __name__ == "__main__":
    main()
