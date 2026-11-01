"""A breadth trend sleeve: long/short trend across ~40 ETFs in five asset classes (idea 12).

Breadth helps a strategy whose risk the signal drives (IR ~ IC x sqrt(breadth)), not a small
tilt inside a long-only, prior-dominated book, which is where earlier universe expansions
went (``research_trend_global``, ``research_broad_universe``). The literature's version is a
long/short trend sleeve across many uncorrelated markets at equal risk, combined at the book
level. This builds it from ETFs, free data, as step 1 of idea 12; step 2 is futures.
**Fixed before any test.**

Universe, 42 ETFs, five classes (sector SPDRs left out: little breadth): country and index
equity (17), bonds (9), commodities (7), currencies (7), real estate (2); each enters when its
signals have warmed up.

* Signal: the mean of the book's two trend readings per market, EMA-200 ``tanh`` score and
  EWMAC 64/256 forecast (capped +/-2), both lagged one day.
* Position: ``signal * budget / vol60``, the budget equal per class and equal across a
  class's live markets; the whole sleeve scaled to 10% vol by its trailing 252-day vol
  (lagged), leverage capped at 4x. Rebalanced every 5 trading days.
* Costs: 2 bp one way (equity, bond, real-estate ETFs), 5 bp (commodity, currency ETFs);
  short borrow 0.5% a year; returns over T-bills, with T-bill + 0.5% on gross above 1x.

Reported: the sleeve's Sharpe, return, vol, drawdown by period and by class; correlation
with the saved book (core plus overlays), overall and in the book's worst 10% of months; the
2008 Q4, 2020 and 2022 episodes; KMLM (a 22-market trend index) over its years as a check.
Then the book plus the sleeve at fixed risk shares, ``book + a * sleeve`` with the sleeve at
10% vol, ``a`` in 0.1 / 0.2 / 0.3.

Rule: the sleeve's Sharpe above 0.3 since 2008-09, *and* the book with ``a = 0.2`` up at
least 0.02 Sharpe over the window and in both halves (split 2017-07-26).

**Result (2026-11-01): fails; trend across these markets has not paid since 2017.** The
sleeve's Sharpe is +0.43 (2005-08), +0.37 (2008-16), -0.06 (2017-26): +0.13 since 2008-09,
under the 0.3 bar. It runs net long (+1.24x, gross 1.9x), so it is +0.32 correlated with the
book and lost 14.3% in Feb-Apr 2020 (book -2.8%); it paid in late 2008 (+6.1%) and 2022
(+10.4%, book -2.7%). Gross by class since 2008-09: commodities 0.45, bonds 0.35, equity
0.21, currencies -0.03, real estate -0.07. The book falls at every share: 1.334 / 1.289 /
1.239 at 0.1 / 0.2 / 0.3, against 1.371. Since 2020-12 it trails KMLM (Sharpe 0.02 against
0.32; correlation 0.39). Diagnostics run after (not candidates): costs, borrow and financing
take ~1.5% a year (0.51% / 0.25% / 0.75%; turnover 17x), most of which futures would
remove, but even gross it is ~0.3; without the equity class, as KMLM, +0.07 since 2008-09
(+0.50 / -0.28), the book 1.289 at 0.2. Breadth does what the law says -- the sleeve beats
the 17-ETF trend tilt's own IC -- but the IC it multiplies has been ~0 since 2017.

Usage:
    uv run python research/research_breadth_sleeve.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, DataBundle, YFinanceReader  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.signals import EwmacTrend, Identity, TrendStrength  # noqa: E402
from quantzy.signals.indicators import realized_vol  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

CLASSES = {
    "equity": ["SPY", "QQQ", "IWM", "EFA", "EEM", "EWJ", "EWG", "EWU", "EWC", "EWA", "EWZ",
               "EWH", "EWT", "EWY", "FXI", "EWW", "EZA"],
    "bonds": ["SHY", "IEF", "TLT", "TIP", "LQD", "HYG", "EMB", "BWX", "MUB"],
    "commodities": ["GLD", "SLV", "USO", "UNG", "DBA", "DBB", "DBC"],
    "currencies": ["UUP", "FXE", "FXY", "FXB", "FXA", "FXC", "FXF"],
    "real estate": ["VNQ", "RWX"],
}
TICKERS = [t for ts in CLASSES.values() for t in ts]
CLASS_OF = {t: c for c, ts in CLASSES.items() for t in ts}
COST_BPS = {c: (5.0 if c in ("commodities", "currencies") else 2.0) for c in CLASSES}
TARGET_VOL, MAX_LEV, REBAL = 0.10, 4.0, 5
BORROW, FUND_SPREAD = 0.005, 0.005
SPLIT = pd.Timestamp("2017-07-26")
SHARES = (0.1, 0.2, 0.3)
CANDIDATE = 0.2
PERIODS = (("2005", "2008-08"), ("2008-09", "2016"), ("2017", "2026"))


def load() -> tuple[pd.DataFrame, pd.Series]:
    px = CachedReader(YFinanceReader()).history(TICKERS, start="1993-01-01")
    px = px[px.notna().any(axis=1)]
    b = core.load()
    rf = b.prices[core.CASH].pct_change(fill_method=None).reindex(px.index).fillna(0.0)
    return px, rf


def signals(px: pd.DataFrame) -> pd.DataFrame:
    """Mean of EMA-200 tanh score and EWMAC 64/256 forecast per market, lagged one day."""
    b = DataBundle.build(px)
    trend = TrendStrength(200, ma="ema", ewm_tol=core.EWM_TOL, normalizer=Identity())
    ewmac = EwmacTrend(64, ewm_tol=core.EWM_TOL, normalizer=Identity())
    vol = realized_vol(px, ewmac.vol_window, annualize=True)
    t_raw = trend.raw(b)
    e_unit = ewmac.raw(b) / vol.where(vol > 0)
    rows = px.notna().cumsum()
    t_raw = t_raw.where(rows > trend.min_history)
    e_unit = e_unit.where(rows > ewmac.min_history)
    return ((t_raw + e_unit) / 2).shift(1)


def sleeve(px: pd.DataFrame, rf: pd.Series, classes: dict | None = None,
           detail: bool = False):
    """Daily excess return of the sleeve after costs, and its weights."""
    global CLASSES
    saved, CLASSES = CLASSES, classes or CLASSES
    try:
        return _sleeve(px[[t for ts in CLASSES.values() for t in ts]], rf, detail)
    finally:
        CLASSES = saved


def _sleeve(px: pd.DataFrame, rf: pd.Series, detail: bool):
    tickers = list(px.columns)
    s = signals(px)
    vol = realized_vol(px, 60, annualize=True).shift(1)
    live = s.notna() & vol.notna() & (vol > 0)
    n_classes = pd.DataFrame({c: live[ts].any(axis=1) for c, ts in CLASSES.items()}).sum(axis=1)
    n_in = pd.DataFrame({t: live[CLASSES[CLASS_OF[t]]].sum(axis=1) for t in tickers})
    budget = (1.0 / n_classes.replace(0, np.nan)).to_frame().values / n_in.where(live)
    raw_w = (s * budget / vol).where(live).fillna(0.0)
    rets = px.pct_change(fill_method=None).fillna(0.0)
    raw_r = (raw_w.shift(1) * rets).sum(axis=1)  # unscaled, to measure the sleeve's vol
    scale = (TARGET_VOL / (raw_r.rolling(TRADING_DAYS, min_periods=TRADING_DAYS).std()
                           * np.sqrt(TRADING_DAYS))).clip(upper=MAX_LEV)
    target = raw_w.mul(scale.shift(1), axis=0)
    gross = target.abs().sum(axis=1)
    target = target.div((gross / MAX_LEV).clip(lower=1.0), axis=0)
    # rebalance every REBAL days; hold the weights in between (no drift: weights in NAV terms)
    keep = pd.Series(np.arange(len(target)) % REBAL == 0, index=target.index)
    w = target.where(keep).ffill().fillna(0.0)
    held = w.shift(1).fillna(0.0)
    ex = rets.sub(rf, axis=0)
    trades = w.diff().abs().fillna(w.abs())
    cost = sum(trades[ts].sum(axis=1) * COST_BPS[c] / 1e4 for c, ts in CLASSES.items())
    short = held.clip(upper=0).abs().sum(axis=1) * BORROW / TRADING_DAYS
    fund = (held.abs().sum(axis=1) - 1).clip(lower=0) * FUND_SPREAD / TRADING_DAYS
    r = (held * ex).sum(axis=1) - cost - short - fund
    first = scale.first_valid_index()
    if detail:
        drag = pd.DataFrame({"trading": cost, "short borrow": short, "financing": fund})
        return r.loc[first:], w.loc[first:], drag.loc[first:]
    return r.loc[first:], w.loc[first:]


def stats(r: pd.Series) -> dict[str, float]:
    ann = np.sqrt(TRADING_DAYS)
    c = (1 + r).cumprod()
    return {"ret": r.mean() * TRADING_DAYS, "vol": r.std() * ann,
            "sharpe": r.mean() / r.std() * ann, "max_dd": float((c / c.cummax() - 1).min())}


def main() -> None:
    pd.set_option("display.width", 220)
    px, rf = load()
    r, w, drag = sleeve(px, rf, detail=True)
    print(f"sleeve {r.index[0].date()} .. {r.index[-1].date()}; "
          f"avg gross {w.abs().sum(axis=1).mean():.2f}x, net {w.sum(axis=1).mean():+.2f}x")

    print("\n1. SLEEVE by period")
    print(pd.DataFrame({f"{a}..{z}": stats(r.loc[a:z]) for a, z in PERIODS}
                       | {"since 2008-09": stats(r.loc["2008-09":])}).T
          .to_string(float_format="{:+.3f}".format))
    rets = px.pct_change(fill_method=None).fillna(0.0).sub(rf, axis=0)
    by_class = {c: (w[ts].shift(1) * rets[ts]).sum(axis=1).loc["2008-09":]
                for c, ts in CLASSES.items()}
    print("\n   contribution by class since 2008-09 (gross of costs): return / Sharpe")
    print("   " + ", ".join(f"{c} {x.mean() * TRADING_DAYS:+.2%} / "
                            f"{x.mean() / x.std() * np.sqrt(TRADING_DAYS):+.2f}"
                            for c, x in by_class.items()))

    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, core.load())
    s = r.reindex(book.index).fillna(0.0)
    m = book.groupby(book.index.to_period("M")).sum()
    sm = s.groupby(s.index.to_period("M")).sum()
    worst = m <= m.quantile(0.10)
    print(f"\n2. CORRELATION with the book: daily {s.corr(book):+.2f}, monthly {sm.corr(m):+.2f}; "
          f"in the book's worst 10% of months the sleeve averaged {sm[worst].mean():+.2%} "
          f"(book {m[worst].mean():+.2%})")
    for e, (a, z) in {"2008-09..12": ("2008-09-10", "2008-12-31"),
                      "2020-02-19..04-30": ("2020-02-19", "2020-04-30"),
                      "2022": ("2022-01-01", "2022-12-31")}.items():
        print(f"   {e}: sleeve {(1 + s.loc[a:z]).prod() - 1:+.1%}, "
              f"book {(1 + book.loc[a:z]).prod() - 1:+.1%}")
    kmlm = CachedReader(YFinanceReader()).history(["KMLM"], start="2020-01-01")["KMLM"]
    k = kmlm.pct_change(fill_method=None).reindex(s.index).dropna()
    kk = k - rf.reindex(k.index).fillna(0.0)
    ks = stats(s.loc[k.index])["sharpe"]
    print(f"   KMLM check, {k.index[0].date()} on: sleeve Sharpe {ks:+.2f}, "
          f"KMLM {stats(kk)['sharpe']:+.2f}, correlation {s.loc[k.index].corr(kk):+.2f}")

    books = {"book (core + overlays)": book}
    books |= {f"book + {a:.1f} x sleeve": book + a * s for a in SHARES}
    t = compare(books, split_date=SPLIT)
    out = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                        "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    print(f"\n3. BOOK + SLEEVE, {book.index[0].date()} .. {book.index[-1].date()}, split "
          f"{SPLIT.date()}; candidate a = {CANDIDATE}")
    print(out.to_string(float_format="{:.3f}".format))

    # Diagnostics added after the first run (not candidates).
    d = drag.loc["2008-09":].mean() * TRADING_DAYS
    turn = w.diff().abs().sum(axis=1).loc["2008-09":].mean() * TRADING_DAYS
    print("\n4. DIAGNOSTICS (after the first run, not candidates). Drag since 2008-09: "
          + ", ".join(f"{k} {v:.2%}" for k, v in d.items()) + f"; one-way turnover {turn:.1f}x/yr")
    no_eq = {c: ts for c, ts in CLASSES.items() if c != "equity"}
    r2, w2 = sleeve(px, rf, no_eq)
    s2 = r2.reindex(book.index).fillna(0.0)
    print(f"   without the equity class (KMLM-like): Sharpe since 2008-09 "
          f"{stats(r2.loc['2008-09':])['sharpe']:+.2f} (2008-16 "
          f"{stats(r2.loc['2008-09':'2016'])['sharpe']:+.2f}, 2017-26 "
          f"{stats(r2.loc['2017':])['sharpe']:+.2f}); net {w2.sum(axis=1).mean():+.2f}x; "
          f"corr with book {s2.corr(book):+.2f}; with KMLM {s2.loc[k.index].corr(kk):+.2f}")
    t2 = compare({"book": book, "book + 0.2 x no-equity sleeve": book + 0.2 * s2},
                 split_date=SPLIT)
    print("   " + ", ".join(f"{n}: {t2['full']['sharpe'][n]:.3f} ({t2['train']['sharpe'][n]:.3f}"
                            f" / {t2['test']['sharpe'][n]:.3f})" for n in t2["full"].index))


if __name__ == "__main__":
    main()
