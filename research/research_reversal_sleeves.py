"""Two reversal sleeves next to the saved book: an equity bounce-back overlay and country reversal.

From ``research_short_reversal.py``: equity ETFs tend to recover the week after a bad week
(5-day time-series IC +0.05, t 2.3-2.8, also since 2010), and among country ETFs last month's
losers beat last month's winners (21-day cross-sectional IC +0.035, t 3.1; long/short net
Sharpe 0.34, correlation with the core 0.04). **Fixed before any test.**

1. *Equity bounce-back overlay* (long-only): ``z`` = SPY's 5-day return over daily vol times
   sqrt 5, known at the prior close; hold ``0.125 * clip(-z, 0, 2)`` of NAV in SPY (at most
   0.25x), funded from cash, updated daily, skipping changes under 1% of NAV, 1 bp a trade.
2. *Country 21-day reversal*, formed every 21 days on the 23 country ETFs and held 21 days:
   A, long the bottom third and short the top third (equal weight, dollar-neutral); B, long
   the bottom third and short a 50/50 EFA/EEM mix at the trailing one-year beta. Costs 5 bp a
   trade for ETFs above $50M a day, 15 bp below, 1 bp for EFA/EEM; shorts pay 1% a year.

Reported: each sleeve's excess return and Sharpe by period, correlation with the saved book
(core plus the Treasury month-end overlay), its returns in the 2008, 2020 and 2022 sell-offs,
and the saved book plus the sleeve (country sleeves scaled to 4% vol on their trailing
one-year vol, causal).

**Result (2026-10-18): neither added.** The bounce-back overlay (Sharpe 0.42 alone) is 0.44
correlated with the saved book and deepened the crashes it buys into (2020: -13.8% against
-8.9%); combined Sharpe 1.21 -> 1.17. Country reversal is independent (+0.04) but weak since
2017 (A: Sharpe 0.17; B: -0.01): A at 4% vol moves the combined book from 1.21 to 1.22 with a
lower test half (1.18 -> 1.17), and needs shorting about 8 country ETFs.

Usage:
    uv run python research/research_reversal_sleeves.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import research_broad_universe as broad
from research_short_reversal import COUNTRIES

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme

ANN = np.sqrt(TRADING_DAYS)
SELLOFFS = {"2008-09..2009-03": ("2008-09-10", "2009-03-09"),
            "2020-02..03": ("2020-02-19", "2020-03-23"), "2022": ("2022-01-01", "2022-12-31")}
PERIODS = [("2008", "2016"), ("2017", "2026")]
BORROW = 0.01


def saved_book(b) -> pd.Series:
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, core.build())
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    return r + tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None),
                                  cash).reindex(r.index).fillna(0)


def bounce_back(spy: pd.Series, rf: pd.Series) -> tuple[pd.Series, pd.Series]:
    r = spy.pct_change(fill_method=None)
    vol = r.rolling(60, min_periods=60).std()
    z = ((spy / spy.shift(5) - 1) / (vol * np.sqrt(5))).shift(1)  # through the prior close
    target = 0.125 * (-z).clip(0, 2)
    held, w, cost = [], 0.0, []
    for t in target.fillna(0).to_numpy():
        c = 0.0
        if abs(t - w) >= 0.01 or (t == 0 and w != 0):
            c, w = abs(t - w) * 1e-4, t
        held.append(w)
        cost.append(c)
    held = pd.Series(held, index=spy.index)
    ex = held.shift(1).fillna(0) * (r - rf.reindex(r.index).fillna(0)) - pd.Series(cost,
                                                                                  index=r.index)
    return ex.dropna(), held


def country_sleeves(px: pd.DataFrame, hedge: pd.DataFrame, costs: pd.Series,
                    rf: pd.Series) -> dict[str, pd.Series]:
    rets = px.pct_change(fill_method=None)
    h = hedge.pct_change(fill_method=None).mean(axis=1)  # 50/50 EFA/EEM
    past = px.shift(1) / px.shift(22) - 1
    dates = px.index[23::21]
    longs, shorts, betas = [], [], []
    for d in dates:
        p = past.loc[d].dropna()
        if len(p) < 9:
            longs.append(pd.Series(dtype=float, name=d))
            shorts.append(pd.Series(dtype=float, name=d))
            betas.append(np.nan)
            continue
        n = len(p) // 3
        lo, hi = p.nsmallest(n).index, p.nlargest(n).index
        longs.append(pd.Series(1.0 / n, index=lo, name=d))
        shorts.append(pd.Series(1.0 / n, index=hi, name=d))
        window = rets.loc[:d].iloc[-252:]
        lr = window[lo].mean(axis=1)
        hh = h.reindex(window.index)
        ok = lr.notna() & hh.notna()
        betas.append(float(np.cov(lr[ok], hh[ok])[0, 1] / hh[ok].var()) if ok.sum() > 120
                     else np.nan)
    L = pd.DataFrame(longs).reindex(columns=px.columns).fillna(0.0)
    S = pd.DataFrame(shorts).reindex(columns=px.columns).fillna(0.0)
    beta = pd.Series(betas, index=dates).ffill().fillna(1.0)

    def daily(w):
        return w.reindex(px.index).ffill().fillna(0.0).shift(1).fillna(0.0)

    Lh, Sh, bh = daily(L), daily(S), beta.reindex(px.index).ffill().shift(1).fillna(1.0)
    r0 = rets.fillna(0.0)
    turn_L = L.diff().abs().fillna(L).reindex(px.index).fillna(0.0)
    turn_S = S.diff().abs().fillna(S).reindex(px.index).fillna(0.0)
    cost_ls = (turn_L * costs).sum(axis=1) + (turn_S * costs).sum(axis=1)
    a = (Lh * r0).sum(axis=1) - (Sh * r0).sum(axis=1) - cost_ls - BORROW / TRADING_DAYS
    hedge_turn = beta.diff().abs().fillna(beta).reindex(px.index).fillna(0.0)
    cost_b = (turn_L * costs).sum(axis=1) + hedge_turn * 1e-4
    bb = ((Lh * r0).sum(axis=1) - rf.reindex(px.index).fillna(0) - bh * (h.fillna(0) - rf.reindex(
        px.index).fillna(0)) - cost_b - bh * BORROW / TRADING_DAYS)
    start = px.index[23 + 21]
    return {"A: long/short": a.loc[start:], "B: long losers, short EFA/EEM": bb.loc[start:]}


def sleeve_table(sleeves: dict[str, pd.Series], book: pd.Series) -> pd.DataFrame:
    rows = {}
    for n, x in sleeves.items():
        x = x.loc[book.index[0]:]
        row = {"excess a year": x.mean() * TRADING_DAYS, "vol": x.std() * ANN,
               "Sharpe": x.mean() / x.std() * ANN, "corr with book": x.corr(book)}
        for a, b in PERIODS:
            y = x.loc[a:b]
            row[f"Sharpe {a}-{b[2:]}"] = y.mean() / y.std() * ANN
        for lab, (a, z) in SELLOFFS.items():
            row[lab] = (1 + x.loc[a:z]).prod() - 1
        rows[n] = row
    return pd.DataFrame(rows).T


def combined(book: pd.Series, adds: dict[str, pd.Series]) -> pd.DataFrame:
    rows = {}
    for n, r in {"saved book (core + month-end)": book, **{
            f"+ {k}": book + v.reindex(book.index).fillna(0) for k, v in adds.items()}}.items():
        nav = (1 + r).cumprod()
        s0, s1 = r.loc[:"2017-07-25"], r.loc["2017-07-26":]
        rows[n] = {"Sharpe": r.mean() / r.std() * ANN, "train": s0.mean() / s0.std() * ANN,
                   "test": s1.mean() / s1.std() * ANN,
                   "return": nav.iloc[-1] ** (TRADING_DAYS / len(r)) - 1,
                   "vol": r.std() * ANN, "max DD": (nav / nav.cummax() - 1).min(),
                   **{lab: (1 + r.loc[a:z]).prod() - 1 for lab, (a, z) in SELLOFFS.items()}}
    return pd.DataFrame(rows).T


def main() -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    book = saved_book(b)
    rf = b.prices[core.CASH].pct_change(fill_method=None)
    spy = b["market"].iloc[:, 0]
    bounce, held = bounce_back(spy, rf)
    print(f"1. bounce-back overlay: in SPY on {(held > 0).mean():.0%} of days, average "
          f"{held.mean():.1%} of NAV, at the 0.25 cap on {(held >= 0.249).mean():.1%}")

    raw = CachedReader(YFinanceReader()).history([*COUNTRIES, "EFA", "EEM"], start="1998-12-22")
    raw = raw[raw.notna().any(axis=1)]
    tiers = broad.cost_tiers()
    costs = pd.Series({t: (5e-4 if tiers.get(t, 10) <= 2 else 15e-4) for t in COUNTRIES})
    cs = country_sleeves(raw[COUNTRIES], raw[["EFA", "EEM"]], costs, rf.reindex(raw.index))
    sleeves = {"bounce-back overlay": bounce, **{f"countries {k}": v for k, v in cs.items()}}
    print("\n   SLEEVES (excess over cash, net), from the book's start "
          f"{book.index[0].date()}")
    print(sleeve_table(sleeves, book).to_string(float_format=lambda v: f"{v:+.3f}"))

    adds = {"bounce-back overlay": bounce}
    for k, v in cs.items():
        vol = v.rolling(252, min_periods=126).std().shift(1) * ANN
        adds[f"countries {k} at 4% vol"] = (v * (0.04 / vol)).dropna()
    adds["bounce-back + countries A at 4% vol"] = (bounce.reindex(book.index).fillna(0)
                                                   + adds["countries A: long/short at 4% vol"]
                                                   .reindex(book.index).fillna(0))
    print("\n2. SAVED BOOK PLUS EACH SLEEVE")
    print(combined(book, adds).to_string(float_format=lambda v: f"{v:+.3f}"))


if __name__ == "__main__":
    main()
