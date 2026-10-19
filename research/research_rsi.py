"""RSI dip-buying on SPY with a trend filter, against the 5-day z bounce-back.

RSI (Wilder): ``100 - 100 / (1 + RS)``, RS the ratio of Wilder-smoothed average gains to losses
over ``n`` days. RSI(2) dip-buying (popularized by Connors around 2008) and RSI(14) 30/70
bands are short-horizon reversal triggers. ``research_bounce_filtered.py`` found the 5-day z
version, filtered by SPY above its EMA-200, made Sharpe 0.60 alone but added ~0.01 to the
book. **Fixed before any test.**

Overlays on SPY, on only while SPY is above its 200-day EMA, 0.25x NAV while in a trade,
1 bp a trade:

* RSI(2): enter when RSI(2) < 10; exit when RSI(2) > 70 or after 10 days.
* RSI(14): enter when RSI(14) crosses up through 30; exit when RSI(14) > 50 or after 21 days.
* Reference: the 5-day z overlay of ``research_bounce_filtered.py`` with the trend filter.

Signals use the prior close and trade at the next close; as a check, a "same close" version
computes the signal on the close it trades at (market-on-close with a near-close estimate).
Long history: ^GSPC (price only) from 1950, before and after RSI(2)'s publication.

**Result (2026-10-19): no change.** RSI(2) dip-buying is real and survived publication
(^GSPC Sharpe 0.48 since 1950, 0.39-0.42 since 2009; ~7 trades a year, ~4 days each, in the
market 12% of the time) but is no better than the 5-day z (0.46-0.55 against 0.60 since
2008), and same-close execution does not help over the long history (0.28 against 0.48).
RSI(14) with the trend filter almost never triggers (one trade every two years). Added to the
saved book, every version is neutral (1.231 -> 1.225-1.240, flat test half): part-time
equity beta again.

Usage:
    uv run python research/research_rsi.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from research_bounce_filtered import overlay, saved_book, stats
from research_reversal_sleeves import ANN, SELLOFFS

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core

SIZE = 0.25
PERIODS = [("1950", "1979"), ("1980", "1999"), ("2000", "2008"), ("2009", "2016"),
           ("2017", "2026")]


def rsi(px: pd.Series, n: int) -> pd.Series:
    d = px.diff()
    gain, loss = d.clip(lower=0), (-d).clip(lower=0)
    ag = gain.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    al = loss.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + ag / al.where(al > 0))


def rule_positions(px: pd.Series, rule: str, lag: int) -> pd.Series:
    """0 / SIZE held into each day; signals from the close ``lag`` days before it."""
    ema = px.ewm(span=200, adjust=False, min_periods=200).mean()
    up = (px > ema)
    n = 2 if rule == "RSI(2)" else 14
    r = rsi(px, n)
    rv, upv = r.to_numpy(), up.to_numpy()
    out = np.zeros(len(px))
    in_trade, held = False, 0
    for i in range(len(px)):
        j = i - lag  # the close the decision for day i+1 is made on
        if j < 1 or np.isnan(rv[j]):
            out[i] = SIZE if in_trade else 0.0
            continue
        if in_trade:
            held += 1
            limit = 10 if n == 2 else 21
            done = rv[j] > 70 if n == 2 else rv[j] > 50
            if done or held >= limit or not upv[j]:
                in_trade = False
        elif upv[j]:
            enter = rv[j] < 10 if n == 2 else (rv[j - 1] < 30 <= rv[j])
            if enter:
                in_trade, held = True, 0
        out[i] = SIZE if in_trade else 0.0
    return pd.Series(out, index=px.index)


def run_rule(px: pd.Series, rf: pd.Series, rule: str, lag: int) -> tuple[pd.Series, dict]:
    pos = rule_positions(px, rule, lag)
    r = px.pct_change(fill_method=None)
    ex = pos.shift(1).fillna(0) * (r - rf.reindex(r.index).fillna(0))
    ex = ex - pos.diff().abs().fillna(0) * 1e-4
    starts = (pos.diff() > 0)
    tid = starts.cumsum().where(pos.shift(1) > 0)
    per = ex.groupby(tid).sum()
    info = {"trades": int(starts.sum()), "per year": starts.sum() / (len(px) / TRADING_DAYS),
            "avg days": (pos > 0).sum() / max(starts.sum(), 1),
            "hit rate": (per > 0).mean(), "on (% of days)": (pos > 0).mean()}
    return ex.dropna(), info


def sharpe(x: pd.Series) -> float:
    x = x.dropna()
    return x.mean() / x.std() * ANN if len(x) > 250 and x.std() > 0 else np.nan


def main() -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    book = saved_book(b)
    rf = b.prices[core.CASH].pct_change(fill_method=None)
    spy = b["market"].iloc[:, 0]
    ema = spy.ewm(span=200, adjust=False, min_periods=200).mean()

    rows, adds = {}, {}
    ref, held = overlay(spy, rf, (spy > ema).shift(1).astype("boolean").fillna(False).astype(bool))
    adds["5-day z, trend filter (reference)"] = ref
    for rule in ("RSI(2)", "RSI(14)"):
        for lag, tag in ((1, "prior close"), (0, "same close")):
            ex, info = run_rule(spy, rf, rule, lag)
            adds[f"{rule}, {tag}"] = ex
            rows[f"{rule}, {tag}"] = info
    tab = {}
    for name, ex in adds.items():
        ex = ex.loc[book.index[0]:]
        row = {**rows.get(name, {}), "excess %/yr": ex.mean() * TRADING_DAYS * 100,
               "Sharpe": sharpe(ex), "corr with book": ex.corr(book),
               "Sharpe 2008-16": sharpe(ex.loc[:"2016"]), "Sharpe 2017-26": sharpe(ex.loc["2017":])}
        for lab, (a, z) in SELLOFFS.items():
            row[lab] = (1 + ex.loc[a:z]).prod() - 1
        tab[name] = row
    print(f"1. SPY OVERLAYS (excess over cash, net), {book.index[0].date()} on")
    print(pd.DataFrame(tab).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    comb = {"saved book": stats(book)}
    for name, ex in adds.items():
        comb[f"+ {name}"] = stats(book + ex.reindex(book.index).fillna(0))
    print("\n2. SAVED BOOK (core + overlays) PLUS EACH")
    print(pd.DataFrame(comb).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    raw = CachedReader(YFinanceReader()).history(["^GSPC", "^IRX"], start="1950-01-01")
    irx = ((1 + raw["^IRX"].ffill() / 100) ** (1 / TRADING_DAYS) - 1).shift(1)
    rf_long = irx.fillna(0.04 / TRADING_DAYS)
    spx = raw["^GSPC"].dropna()
    hist = {}
    for rule in ("RSI(2)", "RSI(14)"):
        for lag, tag in ((1, "prior close"), (0, "same close")):
            ex, info = run_rule(spx, rf_long, rule, lag)
            hist[f"{rule}, {tag}"] = {**info, **{f"Sharpe {a}-{z[2:]}": sharpe(ex.loc[a:z])
                                                 for a, z in PERIODS},
                                      "Sharpe all": sharpe(ex)}
    print("\n3. LONG HISTORY: ^GSPC (price only), 1950 on")
    print(pd.DataFrame(hist).T.to_string(float_format=lambda v: f"{v:+.2f}"))


if __name__ == "__main__":
    main()
