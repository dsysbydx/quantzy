"""Short into the 2-, 5-, 7- and 20-year Treasury auctions: the adopted overlay's mechanism
on the tenors it does not trade.

The question (2026-11-03): ``treasury_auction_10y`` / ``_30y`` short IEF / TLT over the 5
days into each 10- and 30-year auction, adopted at 0.125x after a failed first test with t
at the bar (``research_auction_preshort``). If dealers' absorption of new supply is real, the
other coupon auctions should show the same concession: a fresh test of the mechanism, and a
possible second overlay. **Fixed before any run.**

Events: nominal auctions from TreasuryDirect by original term (TIPS and floaters excluded):
2-, 5- and 7-year notes (new issues; held monthly in one late-month week) and the 20-year
bond (new and reopened, since 2020-05). Window as adopted: the 5 daily returns into the
auction-day close.

1. Effect: mean excess return a day in the window against other days (Welch t), 2002-08 ..
   2008-09 (earlier), train 2008-09-10 .. 2017-07-25 and test 2017-07-26 .. 2026-10-30, on
   the matching ETF (2y SHY, 5y IEI, 7y IEF, 20y TLT) and on IEF for the notes.
2. The one candidate, ``coupon auctions``: -0.125x IEF over the union of the 5- and 7-year
   windows (1-2 days apart, so not stacked) and -0.125x TLT over each 20-year window; 1 bp
   each way, 0.5% a year short borrow. The 2-year is a diagnostic only (SHY is not held and
   its duration is far from IEF's). Legs reported, never picked after the fact.
3. Placebo: the same shorts 10 and 15 trading days later.

Rule, all three: (1) the 5- and 7-year window negative on IEF in train and test; (2) the
candidate's own return t >= 2 over 2008-09-10 .. 2026-10-30; (3) the saved book (core plus
its five overlays) with the candidate up at least 0.02 Sharpe over the window and in train
and test. The month-end overlay holds TLT long over the last two days of each month, close
to the late-month note auctions, so its overlap is reported.

**Result (2026-11-03): fails every part; the concession does not show at the other
auctions.** 2-year 546, 5-year 376, 7-year 261 (resumed 2009), 20-year 79 auctions. Window
effect on IEF, bp a day (t) in 2002-08 / train / test: 5-year +0.67 (-0.4) / -2.20 (-2.4) /
+0.06 (+0.4); 7-year - / +0.59 (-0.7) / +1.86 (+1.6). On their own ETFs the 2- and 5-year
show a concession in train only (SHY t -3.1, IEI t -3.3), none in test; the 20-year on TLT
-2.90 (-0.4) since 2020. The candidate: -0.02% a year, t -0.11 over 2008-09 .. 2026-10
(5y+7y leg t -0.80, 20y leg +0.48); placebo windows 10 / 15 days later do no worse (t +0.48 /
+0.71); 9 of 19 years positive. Book 1.446 against 1.443 (train 1.423 / test 1.470 against
1.412 / 1.473). Only 7% of the note windows fall on month-end overlay days, so that is not
what hides it. For the adopted 10- / 30-year overlays this is evidence against a general
supply mechanism: their concession (test t -2.4 / -2.0) is not shared by the late-month
coupon auctions, which leaves it either specific to the mid-month long-end auctions or noise.
Keep them at 0.125x under the existing live review.

Usage:
    uv run python research/research_auction_more.py
"""

from __future__ import annotations

import json
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
from quantzy.strategies import treasury_auction as ta  # noqa: E402
from quantzy.strategies import treasury_month_end as tme  # noqa: E402

#: original term -> (security type, terms its auctions are listed under, start)
TERMS = {
    "2-Year": ("Note", ["2-Year"], None),
    "5-Year": ("Note", ["5-Year"], None),
    "7-Year": ("Note", ["7-Year"], None),
    "20-Year": ("Bond", ["20-Year", "19-Year 11-Month", "19-Year 10-Month"], "2020-05-01"),
}
OWN_ETF = {"2-Year": "SHY", "5-Year": "IEI", "7-Year": "IEF", "20-Year": "TLT"}
SIZE, COST_BPS, BORROW = 0.125, 1.0, 0.005
CACHE = Path(".cache/auctions/more.json")
PERIODS = (("2002-08-01", "2008-09-09", "2002-08"), ("2008-09-10", "2017-07-25", "train"),
           ("2017-07-26", "2026-10-30", "test"))
BOOK = ("2008-09-10", "2026-10-30")
SPLIT = pd.Timestamp("2017-07-26")


def auction_dates() -> dict[str, pd.DatetimeIndex]:
    if not CACHE.exists():
        rows = {}
        for term, (sec_type, listed, _) in TERMS.items():
            rows[term] = []
            for t in listed:
                rows[term] += json.loads(ta._curl(
                    "-G", ta.HISTORY_API, "--data-urlencode", f"securityType={sec_type}",
                    "--data-urlencode", f"securityTerm={t}", "--data-urlencode", "format=json"))
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(rows))
    out = {}
    for term, rows in json.loads(CACHE.read_text()).items():
        df = pd.DataFrame(rows)
        orig = df["originalSecurityTerm"].astype(str)
        df = df[(orig.where(orig != "", df["securityTerm"]) == term)
                & (df["inflationProtected"].astype(str) != "Yes")
                & (df["floatingRate"].astype(str) != "Yes")]
        d = pd.to_datetime(df["competitiveAuctionCloseDate"], errors="coerce").dropna()
        start = TERMS[term][2]
        out[term] = pd.DatetimeIndex(sorted(d[d >= start].unique() if start else d.unique()))
    return out


def window(idx: pd.DatetimeIndex, dates, shift: int = 0) -> pd.Series:
    return ta.in_pre_window(idx, dates).shift(shift, fill_value=False)


def short_leg(ex: pd.Series, flag: pd.Series) -> pd.Series:
    held = -SIZE * flag.astype(float)
    trades = held.diff().abs().fillna(held.abs())
    return held * ex - trades * COST_BPS / 1e4 - held.abs() * BORROW / TRADING_DAYS


def candidate(ex: pd.DataFrame, dates: dict, shift: int = 0) -> tuple[pd.Series, dict]:
    idx = ex.index
    notes = window(idx, dates["5-Year"], shift) | window(idx, dates["7-Year"], shift)
    legs = {"5y+7y on IEF": short_leg(ex["IEF"], notes),
            "20y on TLT": short_leg(ex["TLT"], window(idx, dates["20-Year"], shift))}
    return sum(legs.values()), legs


def tstat(x: pd.Series) -> str:
    sr = x.mean() / x.std()
    return (f"{x.mean() * TRADING_DAYS:+.2%}/yr, Sharpe {sr * np.sqrt(TRADING_DAYS):+.2f}, "
            f"t {sr * np.sqrt(len(x)):+.2f}")


def main() -> None:
    pd.set_option("display.width", 200)
    dates = auction_dates()
    for k, d in dates.items():
        print(f"{k}: {len(d)} auctions, {d[0].date()} .. {d[-1].date()}")
    b = core.load()
    idx = b.index[b.index >= PERIODS[0][0]]
    px = CachedReader(YFinanceReader()).history(["SHY", "IEI", "IEF", "TLT"], start="2002-07-01")
    rf = b.prices[core.CASH].pct_change(fill_method=None).reindex(idx).fillna(0.0)
    ex = px.pct_change(fill_method=None).reindex(idx).sub(rf, axis=0)

    print("\n1. WINDOW EFFECT: mean excess return a day in the 5 days into the auction, bp "
          "(Welch t vs other days)")
    rows = {}
    for term, d in dates.items():
        for etf in dict.fromkeys([OWN_ETF[term], "IEF"] if term != "20-Year" else ["TLT"]):
            w = window(idx, d)
            row = {}
            for a, z, lab in PERIODS:
                e, f = ex[etf].loc[a:z], w.loc[a:z]
                if f.sum() < 10 or e.dropna().empty:
                    row[lab] = ""
                    continue
                t_ = stats.ttest_ind(e[f].dropna(), e[~f].dropna(), equal_var=False).statistic
                row[lab] = f"{e[f].mean() * 1e4:+.2f} ({t_:+.1f})"
            rows[f"{term} on {etf}"] = row
    print(pd.DataFrame(rows).T.to_string())

    exf = ex.fillna(0.0)
    o, legs = candidate(exf, dates)
    me = tme.in_window(idx)
    notes = window(idx, dates["5-Year"]) | window(idx, dates["7-Year"])
    print(f"\n   overlap: {(notes & me).sum() / notes.sum():.0%} of 5y/7y window days are "
          f"month-end overlay days")

    print("\n2. CANDIDATE on its own")
    for a, z, lab in PERIODS + (BOOK + ("book window",),):
        print(f"   {lab:12s} {tstat(o.loc[a:z])}")
        for n, leg in legs.items():
            x = leg.loc[a:z]
            if x.abs().sum():
                print(f"     {n:13s} {tstat(x)}")
    print("   placebo, book window:")
    for k in (10, 15):
        print(f"     +{k} days    {tstat(candidate(exf, dates, k)[0].loc[slice(*BOOK)])}")

    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b).loc[slice(*BOOK)]
    oo = o.reindex(book.index).fillna(0.0)
    t = compare({"saved (core + 5 overlays)": book, "+ coupon auctions": book + oo},
                split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    adopted = sum(ov.backtest_returns(b) for ov in export.overlays(core)
                  if ov.NAME.startswith("treasury_auction")).reindex(book.index)
    print(f"\n3. BOOK, {book.index[0].date()} .. {book.index[-1].date()}; correlation "
          f"{oo.corr(book):+.2f}, with the adopted auction overlays {oo.corr(adopted):+.2f}")
    print(m.to_string(float_format="{:.3f}".format))
    yr = oo.groupby(oo.index.year).sum() * 100
    print("\n   candidate by year (%): " + " ".join(f"{y}:{v:+.2f}" for y, v in yr.items())
          + f" | positive {int((yr > 0).sum())}/{len(yr)}")


if __name__ == "__main__":
    main()
