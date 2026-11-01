"""The Treasury auction cycle as an overlay (idea 13).

Dealers absorb new Treasury supply at each auction, so prices cheapen in the days before and
recover after (Lou, Yan and Zhang, 2013) -- a forced flow, like the month-end flows behind
the two adopted month-end overlays. **Fixed before any test.**

Events: every nominal 10-year auction (new issues and reopenings, TIPS excluded), traded in
IEF, and every nominal 30-year auction, traded in TLT; dates from TreasuryDirect
(``competitiveAuctionCloseDate``, bids close 1 pm ET). The schedule is public days ahead, so
positions set at the closes around an auction use no future information.

* Pre window: the 5 daily returns into the auction-day close (the concession building).
* Post window: the 5 daily returns after it (the recovery).
* Effect: mean excess return a day in each window against other days (Welch t), by period
  (2002-08 / 2008-16 / 2017-26).
* The one candidate, ``auction cycle``: -0.5x the auctioned ETF over the pre window, +0.5x
  over the post window, 1 bp each way; IEF and TLT legs add when windows overlap.

Rule, as for the month-end overlays: the pre window negative and the post window positive in
every period, *and* the saved book (core plus its three overlays) with the candidate up at
least 0.02 Sharpe over the window and in both halves (split 2017-07-26). The post window
alone (long only) is reported as a diagnostic.

**Result (2026-11-01): not adopted; the concession is there lately, the recovery is not.**
2002-08 .. 2026-10; 263 ten-year and 217 thirty-year auctions. Pre window (bp a day, t):
IEF +3.3 (1.0) / -0.3 (-0.9) / -3.9 (-2.8), TLT -3.9 (-0.4) / -6.1 (-1.9) / -8.1 (-2.4) in
2002-08 / 2008-16 / 2017-26; post window IEF +5.6 (1.9) / +3.7 (0.7) / -1.8 (-1.6), TLT -13.4
(-1.3) / +8.0 (0.7) / -3.1 (-1.4). The sign rule fails (pre positive for IEF in 2002-08; the
post window not reliably positive). The cycle overlay alone: +2.3% a year at 6.2% vol (Sharpe
0.37, t 1.8), correlation +0.01 with the book; the book 1.361 against 1.371 (train 1.495 /
test 1.246), vol 12.2% against 9.8%, max drawdown -24.1%: the windows cover about half the
trading days, so at 0.5x it is a large, volatile bond position. Post window only: 1.201. The
pre-auction concession has strengthened since 2017 (t -2.8 / -2.4), consistent with heavier
supply; a short-before-auctions overlay would be a new, post-hoc candidate.

Usage:
    uv run python research/research_auction_cycle.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402
from quantzy.strategies import treasury_month_end as tme  # noqa: E402

API = "https://www.treasurydirect.gov/TA_WS/securities/search"
# original term -> (security type, the terms its auctions are listed under, ETF)
TERMS = {
    "10-Year": ("Note", ["10-Year", "9-Year 11-Month", "9-Year 10-Month", "9-Year 9-Month"],
                "IEF"),
    "30-Year": ("Bond", ["30-Year", "29-Year 11-Month", "29-Year 10-Month", "29-Year 9-Month"],
                "TLT"),
}
CACHE = Path(".cache/auctions.json")
PRE, POST, SIZE, COST_BPS = 5, 5, 0.5, 1.0
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("2002-08", "2008-08"), ("2008-09", "2016-12"), ("2017-01", "2026-12"))


def auction_dates() -> dict[str, pd.DatetimeIndex]:
    """Nominal auction dates per original term (TIPS and floaters excluded), cached."""
    if CACHE.exists():
        rows = json.loads(CACHE.read_text())
    else:
        rows = []
        for sec_type, listed, _ in TERMS.values():
            for term in listed:
                # curl, for the system's certificate store (a proxy may re-sign TLS)
                out = subprocess.run(
                    ["curl", "-sfG", API, "--data-urlencode", f"securityType={sec_type}",
                     "--data-urlencode", f"securityTerm={term}", "--data-urlencode",
                     "format=json"], check=True, capture_output=True, timeout=120)
                rows += json.loads(out.stdout)
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(rows))
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["competitiveAuctionCloseDate"], errors="coerce")
    df["orig"] = df["originalSecurityTerm"].where(df["originalSecurityTerm"].astype(str) != "",
                                                  df["securityTerm"])
    nominal = (df["inflationProtected"].astype(str) != "Yes") & \
              (df["floatingRate"].astype(str) != "Yes")
    df = df[nominal & df["date"].notna()].drop_duplicates(["cusip", "date"])
    return {t: pd.DatetimeIndex(sorted(df.loc[df["orig"] == t, "date"].unique()))
            for t in TERMS}


def windows(index: pd.DatetimeIndex, dates: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    """Boolean per trading day: in a pre window, in a post window."""
    pre = pd.Series(False, index=index)
    post = pd.Series(False, index=index)
    for d in dates:
        i = index.searchsorted(d)  # the auction day (or the next trading day)
        if i >= len(index) or i < PRE:
            continue
        pre.iloc[i - PRE + 1: i + 1] = True
        post.iloc[i + 1: i + 1 + POST] = True
    return pre, post


def overlay(asset_ex: pd.Series, held: pd.Series) -> pd.Series:
    trades = held.diff().abs().fillna(held.abs())
    return held * asset_ex - trades * COST_BPS / 1e4


def main() -> None:
    pd.set_option("display.width", 220)
    dates = auction_dates()
    b = core.load()
    idx = b.index[b.index >= "2002-08-01"]
    rf = b.prices[core.CASH].pct_change(fill_method=None).reindex(idx).fillna(0.0)
    print(f"{idx[0].date()} .. {idx[-1].date()}; auctions in the window: " + ", ".join(
        f"{t} {int((dates[t] >= idx[0]).sum())}" for t in TERMS))

    rows, legs, post_only = {}, [], []
    for term, (_, _, etf) in TERMS.items():
        ex = (b.prices[etf].pct_change(fill_method=None).reindex(idx) - rf).fillna(0.0)
        pre, post = windows(idx, dates[term])
        other = ~(pre | post)
        row = {}
        for a, z in PERIODS:
            e, p, q, o = ex.loc[a:z], pre.loc[a:z], post.loc[a:z], other.loc[a:z]
            for lab, m in (("pre", p), ("post", q)):
                row[f"{a[:4]}-{z[2:4]} {lab} bp"] = e[m].mean() * 1e4
                row[f"{a[:4]}-{z[2:4]} {lab} t"] = stats.ttest_ind(
                    e[m], e[o], equal_var=False).statistic
        row["other days bp"] = ex[other].mean() * 1e4
        rows[f"{term} ({etf})"] = row
        legs.append(overlay(ex, (-SIZE * pre + SIZE * post).astype(float)))
        post_only.append(overlay(ex, (SIZE * post).astype(float)))
    print("\n1. EFFECT: mean excess return a day, window vs other days (Welch t)")
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.2f}".format))

    cycle, longs = sum(legs), sum(post_only)
    ann = np.sqrt(TRADING_DAYS)
    for name, o in (("auction cycle (candidate)", cycle), ("post window only", longs)):
        sr, t_ = o.mean() / o.std() * ann, o.mean() / o.std() * np.sqrt(len(o))
        print(f"   {name}: excess {o.mean() * TRADING_DAYS:+.2%}/yr, vol {o.std() * ann:.2%}, "
              f"Sharpe {sr:+.2f}, t {t_:+.1f}")
    month_end = tme.in_window(idx)
    pre10, post10 = windows(idx, dates["10-Year"])
    shared = int(((pre10 | post10) & month_end).sum())
    print(f"   overlap: {shared} of {int(month_end.sum())} month-end days fall in a 10-year "
          "auction window")

    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    books = {"saved (core + 3 overlays)": book,
             "+ auction cycle": book + cycle.reindex(book.index).fillna(0.0),
             "+ post window only": book + longs.reindex(book.index).fillna(0.0)}
    t = compare(books, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    print(f"\n2. BOOK, {book.index[0].date()} .. {book.index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(float_format="{:.3f}".format))
    print(f"   correlation with the book: cycle {cycle.reindex(book.index).corr(book):+.2f}, "
          f"post only {longs.reindex(book.index).corr(book):+.2f}")


if __name__ == "__main__":
    main()
