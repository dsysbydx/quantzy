"""Spread trades with an economic anchor: Treasury curve slopes and tied ETF pairs.

A single asset's moving-average spread reverts by construction (``research_ma_band_reversion
.py``). A spread *between* two assets tied by economics can revert for a reason: curve
slopes cycle with policy expectations and term premia; pairs share an exposure. **Fixed
before any test.**

A. Curve: the 5s10s (from 1962) and 10s30s (from 1977) Treasury yield spreads. ``z`` is the
   spread against its trailing year (a 3-year window as a check). A steep curve (``z > 0``) is
   expected to flatten: hold ``clip(z, +/-2) / 2`` of a DV01-neutral flattener (long the longer
   bond, short the shorter at the duration ratio), built from par bonds rolled daily from the
   yields, as excess over cash. The ETF version, TLT against IEF, from 2002.
B. Pairs: EWA/EWC, EWG/EWQ, EWH/EWS, XLP/XLU, GLD/SLV, XLK/QQQ. ``spread = log A - beta log B``,
   ``beta`` a rolling one-year regression of log prices (lagged); ``z`` against the trailing
   year; hold ``-clip(z, +/-2) / 2`` of (A - beta B), rebalanced weekly; 2-5 bp a trade by
   liquidity; the short leg pays 1% a year.

All signals from data through the prior close. Reported per spread: the implied half-life of
``z`` (from its 21-day autocorrelation), the rank IC of ``-z`` with the next 21 days' spread
return, Sharpe by period, correlation with the saved book; then each family as an equal-risk
sleeve, and the saved book plus it at 4% vol.

**Result (2026-10-19): negative.** With a one-year z, every spread's half-life (59-166 days)
is in the range of a random walk's (73): no faster reversion for having an anchor. Curve
slopes reverted in 1962-89 (10s30s Sharpe 0.47-0.73) but not since 1990; full-sample ICs are
about zero. Several pairs trend instead (EWH/EWS reversion IC -0.135, t -2.7; GLD/SLV,
EWG/EWQ negative). As 4% vol sleeves both families lower the saved book (curve 1.23 -> 1.05,
pairs -> 1.13).

Usage:
    uv run python research/research_spread_trades.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from research_bounce_filtered import saved_book
from research_calendar_flows import daily_par_bond

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import newey_west_t
from quantzy.strategies import core_multiasset as core

ANN = np.sqrt(TRADING_DAYS)
PAIRS = [("EWA", "EWC"), ("EWG", "EWQ"), ("EWH", "EWS"), ("XLP", "XLU"), ("GLD", "SLV"),
         ("XLK", "QQQ")]
COST = {"EWA": 2e-4, "EWC": 2e-4, "EWG": 2e-4, "EWQ": 5e-4, "EWH": 2e-4, "EWS": 5e-4,
        "XLP": 1e-4, "XLU": 1e-4, "GLD": 1e-4, "SLV": 1e-4, "XLK": 1e-4, "QQQ": 1e-4,
        "TLT": 1e-4, "IEF": 1e-4}
BORROW = 0.01
PERIODS = [("1962", "1989"), ("1990", "2007"), ("2008", "2016"), ("2017", "2026")]


def zscore(s: pd.Series, window: int) -> pd.Series:
    r = s.rolling(window, min_periods=window)
    return (s - r.mean()) / r.std()


def par_duration(y: pd.Series, maturity: float) -> pd.Series:
    """Modified duration of an annual-pay par bond at yield y (decimal)."""
    return (1 - (1 + y) ** -maturity) / y


def half_life(z: pd.Series) -> float:
    a = z.dropna().autocorr(21)
    return 21 * np.log(0.5) / np.log(a) if 0 < a < 1 else np.inf


def ic21(z: pd.Series, spread_ret: pd.Series) -> str:
    fwd = spread_ret.rolling(21).sum().shift(-21)
    d = pd.concat([-z, fwd], axis=1).dropna()
    a, b = d.iloc[:, 0].rank(), d.iloc[:, 1].rank()
    prod = (a - a.mean()) / a.std() * (b - b.mean()) / b.std()
    return f"{prod.mean():+.3f} ({newey_west_t(prod, 42):+.1f})"


def sharpe(x: pd.Series) -> float:
    x = x.dropna()
    return x.mean() / x.std() * ANN if len(x) > 250 and x.std() > 0 else np.nan


def curve(rf: pd.Series) -> dict[str, tuple[pd.Series, pd.Series, pd.Series]]:
    """For each slope: z (1y window), the position's daily excess return, the flattener's."""
    y = CachedReader(YFinanceReader()).history(["^FVX", "^TNX", "^TYX"], start="1962-01-01")
    out = {}
    for name, (short, s_mat), (long_, l_mat) in (("5s10s", ("^FVX", 5), ("^TNX", 10)),
                                                 ("10s30s", ("^TNX", 10), ("^TYX", 30))):
        ys, yl = y[short].dropna() / 100, y[long_].dropna() / 100
        idx = ys.index.intersection(yl.index)
        ys, yl = ys[idx], yl[idx]
        rs, rl = daily_par_bond(ys * 100, s_mat), daily_par_bond(yl * 100, l_mat)
        ratio = (par_duration(yl, l_mat) / par_duration(ys, s_mat)).shift(1)
        cash = rf.reindex(idx).fillna(0.0)
        flat = (rl - cash) - ratio * (rs - cash)  # DV01-neutral flattener, excess over cash
        for window, tag in ((252, "1y"), (756, "3y")):
            z = zscore((yl - ys), window).shift(1)
            pos = z.clip(-2, 2) / 2
            trades = pos.diff().abs().fillna(0) * 2e-4  # both legs, futures-like cost
            out[f"{name} ({tag})"] = (z, (pos * flat - trades).dropna(), flat)
    return out


def tlt_ief(rf: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    px = CachedReader(YFinanceReader()).history(["TLT", "IEF", "^TNX", "^TYX"],
                                                start="2002-01-01").dropna()
    r = px[["TLT", "IEF"]].pct_change(fill_method=None)
    cash = rf.reindex(px.index).fillna(0.0)
    ratio = (r["TLT"].rolling(252).cov(r["IEF"]) / r["IEF"].rolling(252).var()).shift(1)
    flat = (r["TLT"] - cash) - ratio * (r["IEF"] - cash)
    z = zscore(px["^TYX"] / 100 - px["^TNX"] / 100, 252).shift(1)
    pos = (z.clip(-2, 2) / 2).where(z.index.dayofweek == 4).ffill()  # weekly
    trades = pos.diff().abs().fillna(0) * (1 + ratio.fillna(1)) * 1e-4
    short_cost = pos.abs() * BORROW / TRADING_DAYS
    return z, (pos.shift(1) * flat - trades - short_cost).dropna(), flat


def pair(a: str, b: str, px: pd.DataFrame, rf: pd.Series):
    la, lb = np.log(px[a]), np.log(px[b])
    d = pd.concat([la, lb], axis=1).dropna()
    beta = (d.iloc[:, 0].rolling(252).cov(d.iloc[:, 1]) / d.iloc[:, 1].rolling(252).var()).shift(1)
    spread = d.iloc[:, 0] - beta * d.iloc[:, 1]
    z = zscore(spread, 252).shift(1)
    ra, rb = px[a].pct_change(fill_method=None), px[b].pct_change(fill_method=None)
    spread_ret = (ra - beta * rb).reindex(d.index)
    pos = (-z.clip(-2, 2) / 2).where(z.index.dayofweek == 4).ffill()
    trades = pos.diff().abs().fillna(0) * (COST[a] + beta.abs().fillna(1) * COST[b])
    short = (pos.clip(lower=0) * beta.abs() + (-pos).clip(lower=0)) * BORROW / TRADING_DAYS
    pnl = pos.shift(1) * spread_ret - trades - short
    return z, pnl.dropna(), spread_ret


def main() -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    book = saved_book(b)
    yf = CachedReader(YFinanceReader())
    irx = yf.history(["^IRX"], start="1960-01-01")["^IRX"].ffill() / 100
    rf = ((1 + irx) ** (1 / TRADING_DAYS) - 1).shift(1)
    rf = rf.reindex(pd.date_range("1960-01-01", rf.index[-1], freq="B")).ffill().fillna(
        0.03 / TRADING_DAYS)

    rows, sleeves = {}, {"curve": {}, "pairs": {}}
    for name, (z, pnl, base) in curve(rf).items():
        row = {"from": f"{pnl.index[0]:%Y}", "half-life (days)": half_life(z),
               "IC of -z vs next 21d": ic21(-z, base), "Sharpe": sharpe(pnl),
               "corr with book": pnl.reindex(book.index).corr(book)}
        for a, c in PERIODS:
            row[f"Sharpe {a}-{c[2:]}"] = sharpe(pnl.loc[a:c])
        rows[("curve", name)] = row
        if name.endswith("(1y)"):
            sleeves["curve"][name] = pnl
    z, pnl, base = tlt_ief(rf)
    rows[("curve", "TLT vs IEF (ETFs, 1y)")] = {
        "from": f"{pnl.index[0]:%Y}", "half-life (days)": half_life(z),
        "IC of -z vs next 21d": ic21(-z, base), "Sharpe": sharpe(pnl),
        "corr with book": pnl.reindex(book.index).corr(book),
        **{f"Sharpe {a}-{c[2:]}": sharpe(pnl.loc[a:c]) for a, c in PERIODS}}
    px = yf.history(sorted({t for p in PAIRS for t in p}), start="1998-12-22")
    px = px[px.notna().any(axis=1)]
    for a, c in PAIRS:
        z, pnl, sret = pair(a, c, px, rf)
        row = {"from": f"{pnl.index[0]:%Y}", "half-life (days)": half_life(z),
               "IC of -z vs next 21d": ic21(z, sret), "Sharpe": sharpe(pnl),
               "corr with book": pnl.reindex(book.index).corr(book)}
        for p0, p1 in PERIODS:
            row[f"Sharpe {p0}-{p1[2:]}"] = sharpe(pnl.loc[p0:p1])
        rows[("pairs", f"{a} / {c}")] = row
        sleeves["pairs"][f"{a}/{c}"] = pnl
    # a pure random walk spread, for the half-life benchmark
    rng = np.random.default_rng(1)
    rw = pd.Series(np.cumsum(rng.normal(0, 1, 6000)))
    print(f"benchmark: a spread with no anchor (random walk) has a z half-life of "
          f"{half_life(zscore(rw, 252)):.0f} days\n")
    print("1. EACH SPREAD (positions from data through the prior close, net of costs)")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.2f}"))

    print("\n2. EQUAL-RISK SLEEVES, and the saved book plus each at 4% vol "
          f"({book.index[0].date()} on)")
    out = {}
    for fam, parts in sleeves.items():
        df = pd.DataFrame(parts).reindex(book.index)
        vol = df.rolling(252, min_periods=126).std().shift(1)
        scaled = (df / vol).mean(axis=1, skipna=True)  # equal risk across spreads
        svol = scaled.rolling(252, min_periods=126).std().shift(1) * ANN
        sleeve = (scaled * 0.04 / svol).dropna()
        comb = book + sleeve.reindex(book.index).fillna(0)
        nav = (1 + comb).cumprod()
        out[fam] = {"sleeve Sharpe": sharpe(sleeve), "sleeve 2017-26": sharpe(sleeve.loc["2017":]),
                    "corr with book": sleeve.corr(book),
                    "book Sharpe": sharpe(book), "book + sleeve Sharpe": sharpe(comb),
                    "test half: book / + sleeve": f"{sharpe(book.loc['2017-07-26':]):.2f} / "
                                                 f"{sharpe(comb.loc['2017-07-26':]):.2f}",
                    "max DD book + sleeve": (nav / nav.cummax() - 1).min()}
    print(pd.DataFrame(out).T.to_string(float_format=lambda v: f"{v:+.3f}"))


if __name__ == "__main__":
    main()
