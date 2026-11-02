"""Fast sleeves on top of the book: relative sector momentum and two other fast signals
(ideas 21, from SYNTHESIS.md).

The book's optimizer charges every trade 5 bp x 48 -- a noise filter for the backbone, not a
cost -- so any alpha whose edge lasts days is taxed out of it. Relative sector momentum had
the strongest IC found (1-day t 4.7) and no use in the long-only book. Run as separate
sleeves with their own real costs and their own clock, dollar-neutral where the signal is
relative. Each signal comes from an earlier study, so: one study, three sleeves and their
combination, a strict rule. **Fixed before any test.**

* A. Sector relative momentum (``research_sector_priors`` C): each of the 9 sector SPDRs and
  QQQ by its 12-month return skipping the latest month; long the top third, short the bottom
  third, equal weight, dollar neutral.
* B. Sector 5-day reversal (``research_short_reversal``): the same 10 by minus their past
  5-day return; long the bottom third, short the top third.
* C. Commodity 5-day momentum (``research_short_reversal``: commodities show short-term
  momentum): GLD, SLV, USO, UNG, DBA, DBB, DBC, each long or short by its 5-day return over
  its 5-day vol, capped +/-2, sized to equal risk.
* Combined: A, B and C at equal risk.

All signals use closes through the day before; positions are set every 5 trading days and
held in between. One-way cost: sector SPDRs 2 bp, QQQ and GLD 1 bp, the other commodity ETFs
5 bp; short borrow 0.5% a year (sector SPDRs, QQQ), 1% (commodity ETFs). Each sleeve is
scaled to 10% vol by its trailing 252-day vol, lagged.

Rule, per sleeve and for the combination: t >= 2 since 2008-09, *and* the saved book (core
plus its overlays) with 0.2x the sleeve up at least 0.02 Sharpe over the window and in both
halves (split 2017-07-26).

**Result (2026-11-02): none passes; relative momentum's "strongest IC" is mostly an artifact
of averaging days equally.** Since 2008-09, net, at 10% vol: A +0.08 (t 0.3), B -0.02, C -0.06
(C was +1.11 in 2001-08, then gone), combined -0.03; the book falls with each at 0.2x (1.399 /
1.376 / 1.398 / 1.366 against 1.443). A's turnover is low (23x a year), so costs are not the
reason: even gross and rebalanced daily its terciles earn Sharpe +0.06. Its daily IC is real
(Pearson t 4.8, since 2008-09 t 3.6; no leak) but sits on calm days: by daily cross-sectional
dispersion, mean IC +0.058 in the calmest fifth and -0.018 in the wildest, where it loses
4.9 bp a day; weighted by dispersion the IC is +0.005, and the payoff (signal z times
demeaned return) has t 0.6. A follow-up run after (one candidate, the published fix, Barroso
and Santa-Clara 2015): A scaled daily by its trailing 126-day realized vol, leverage <= 3x --
Sharpe +0.11 (t 0.5), book 1.402, max drawdown -25.8%; six-month vol cannot see one-day
dispersion spikes. Screening lesson: judge a signal by its payoff-weighted IC (the return of
the portfolio it implies), not by an equal-weight average of daily ICs.

Usage:
    uv run python research/research_fast_sleeves.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

SECTORS = [*core.CLASSES["US sectors"], "QQQ"]
COMMOD = ["GLD", "SLV", "USO", "UNG", "DBA", "DBB", "DBC"]
COST_BPS = {**{t: 2.0 for t in SECTORS}, "QQQ": 1.0, **{t: 5.0 for t in COMMOD}, "GLD": 1.0}
BORROW = {**{t: 0.005 for t in SECTORS}, **{t: 0.01 for t in COMMOD}}
REBAL, TARGET_VOL, SHARE = 5, 0.10, 0.2
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("2001", "2008-08"), ("2008-09", "2016"), ("2017", "2026"))


def thirds(score: pd.DataFrame, long_top: bool) -> pd.DataFrame:
    """Equal-weight, dollar-neutral long and short thirds of each row's live names."""
    w = pd.DataFrame(0.0, index=score.index, columns=score.columns)
    for d, row in score.iterrows():
        s = row.dropna()
        k = len(s) // 3
        if k < 1:
            continue
        top, bot = s.nlargest(k).index, s.nsmallest(k).index
        lo, sh = (top, bot) if long_top else (bot, top)
        w.loc[d, lo], w.loc[d, sh] = 1.0 / k, -1.0 / k
    return w


def run_sleeve(target: pd.DataFrame, px: pd.DataFrame, rf: pd.Series) -> pd.Series:
    """Hold ``target`` (set every REBAL days from signals through t-1), net of costs and
    borrow, scaled to TARGET_VOL by its own trailing vol."""
    keep = pd.Series(np.arange(len(target)) % REBAL == 0, index=target.index)
    w = target.where(keep).ffill().fillna(0.0)
    held = w.shift(1).fillna(0.0)  # set at the close, earns from the next day
    ex = px[w.columns].pct_change(fill_method=None).sub(rf, axis=0).fillna(0.0)
    trades = w.diff().abs().fillna(w.abs())
    cost = (trades * pd.Series(COST_BPS).reindex(w.columns) / 1e4).sum(axis=1)
    borrow = (held.clip(upper=0).abs() * pd.Series(BORROW).reindex(w.columns)).sum(axis=1)
    raw = (held * ex).sum(axis=1) - cost - borrow / TRADING_DAYS
    raw = raw.loc[w.abs().sum(axis=1).gt(0).idxmax():]
    vol = raw.rolling(TRADING_DAYS, min_periods=TRADING_DAYS).std().shift(1)
    vol = vol * np.sqrt(TRADING_DAYS)
    return (raw * TARGET_VOL / vol).dropna(), trades.sum(axis=1)


def main() -> None:
    pd.set_option("display.width", 220)
    px = CachedReader(YFinanceReader()).history([*SECTORS, *COMMOD], start="1998-01-01")
    b = core.load()
    rf = b.prices[core.CASH].pct_change(fill_method=None).reindex(px.index).fillna(0.0)
    rets = px.pct_change(fill_method=None)
    lag = px.shift(1)  # closes through the day before

    mom = (lag.shift(21) / lag.shift(252) - 1)[SECTORS]
    rev = (lag / lag.shift(5) - 1)[SECTORS]
    a_w = thirds(mom.where(px[SECTORS].notna()), long_top=True)
    b_w = thirds(rev.where(px[SECTORS].notna()), long_top=False)
    vol5 = rets[COMMOD].rolling(60, min_periods=60).std().shift(1)
    z5 = (lag / lag.shift(5) - 1)[COMMOD] / (vol5 * np.sqrt(5))
    live = z5.notna()
    c_raw = (z5.clip(-2, 2) / 2) / (vol5 * np.sqrt(TRADING_DAYS))  # equal risk per name
    c_w = c_raw.div(live.sum(axis=1).replace(0, np.nan), axis=0).fillna(0.0)

    sleeves, turn = {}, {}
    for name, w in (("A: sector relative momentum", a_w), ("B: sector 5-day reversal", b_w),
                    ("C: commodity 5-day momentum", c_w)):
        sleeves[name], turn[name] = run_sleeve(w, px, rf)
    common = pd.concat(sleeves, axis=1).dropna()
    sleeves["combined (equal risk)"] = common.mean(axis=1) * TARGET_VOL / (
        common.mean(axis=1).rolling(TRADING_DAYS, min_periods=TRADING_DAYS).std().shift(1)
        * np.sqrt(TRADING_DAYS))
    sleeves["combined (equal risk)"] = sleeves["combined (equal risk)"].dropna()

    ann = np.sqrt(TRADING_DAYS)
    print("1. SLEEVES ALONE (10% vol, net of costs and borrow): Sharpe (t) by period")
    rows = {}
    for n, s in sleeves.items():
        row = {}
        for a, z in (*PERIODS, ("2008-09", "2026")):
            x = s.loc[a:z]
            sr = x.mean() / x.std()
            row[f"{a}..{z}"] = (f"{sr * ann:+.2f} ({sr * np.sqrt(len(x)):+.1f})"
                                if len(x) > 60 else "-")
        rows[n] = row
    print(pd.DataFrame(rows).T.to_string())
    for n, t_ in turn.items():
        print(f"   {n}: one-way turnover {t_.loc['2008-09':].sum() / 18.1:.0f}x a year (unscaled)")
    print("   correlations since 2008-09:")
    print(pd.concat(sleeves, axis=1).loc["2008-09":].corr().round(2).to_string())

    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    books = {"saved (core + all overlays)": book}
    for n, s in sleeves.items():
        books[f"+ {SHARE} x {n}"] = book + SHARE * s.reindex(book.index).fillna(0.0)
    t = compare(books, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "vol": t["full"]["ann_vol"],
                      "max_dd": t["full"]["max_dd"]})
    m["corr w/ book"] = [np.nan] + [sleeves[n].reindex(book.index).fillna(0.0).corr(book)
                                    for n in sleeves]
    print(f"\n2. BOOK + {SHARE}x SLEEVE, {book.index[0].date()} .. {book.index[-1].date()}, "
          f"split {SPLIT.date()}")
    print(m.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
