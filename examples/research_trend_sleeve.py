"""Trend as its own long/short sleeve, outside the long-only book.

In the long-only book a negative trend can only cut a position to zero, and a currency with a
zero prior is never shorted, so half of what the IC measures cannot be traded. This builds
the classic time-series-momentum portfolio on the 45 ETFs of ``research_trend_global.py``
and measures it directly:

    w_i = score_i / vol_i / (n_groups * n_assets_in_group) * scale

``score_i`` is the EMA-200 :class:`~quantzy.signals.TrendStrength` in [-1, 1] (or its sign),
``vol_i`` the trailing 60-day vol, and every group gets the same risk budget. ``scale``
targets ``TARGET_VOL`` from the sleeve's own trailing vol. No optimizer and no priors, so
nothing but the signal decides the positions.

The sleeve is split into its long and short legs to show how much depends on shorting, and
added as an overlay to the long-only no-trend book to see what it is worth there.

Returns include T-bill interest on the collateral and are net of 5 bp per trade and a borrow
fee on shorts. Sharpe is return over vol, with no cash deduction.

Usage:
    uv run python examples/research_trend_sleeve.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import research_trend_breadth as rb
import research_trend_global as g

from quantzy.config import TRADING_DAYS
from quantzy.research import newey_west_t

TARGET_VOL = 0.10
VOL_WINDOW = 60
SCALE_HALFLIFE = 60  # halflife of the sleeve-vol estimate used for the vol target
MAX_GROSS = 4.0  # cap on sum |w|, so a quiet stretch cannot lever the sleeve without limit
COST_BPS = 5.0
BORROW_BPS = 50.0  # annual fee on short notional
START = "2000-06-30"  # by then most groups have an asset with a trusted score


def sleeve(bundle, mode: str = "score", legs: str = "both", rebalance: int = 1) -> dict:
    """Daily sleeve returns, weights and turnover for one variant."""
    px = bundle.prices[g.RISKY]
    r = px.pct_change()
    rf = bundle.prices[g.CASH].pct_change().fillna(0.0)
    s = g.tradeable_scores(g.trend(*g.SPEC), bundle)
    s = np.sign(s) if mode == "sign" else s
    if legs == "long":
        s = s.clip(lower=0)
    elif legs == "short":
        s = s.clip(upper=0)
    vol = r.rolling(VOL_WINDOW, min_periods=VOL_WINDOW).std() * np.sqrt(TRADING_DAYS)
    raw = (s / vol).where(s.notna() & vol.notna())
    live = raw.notna()
    n_in_group = live.T.groupby(g.GROUP_OF).transform("sum").T
    n_groups = live.T.groupby(g.GROUP_OF).any().T.sum(axis=1)
    raw = (raw / n_in_group).div(n_groups, axis=0).fillna(0.0)

    # Scale to the target with the unscaled sleeve's own trailing vol, known at t.
    unscaled = (raw.shift(1) * r).sum(axis=1)
    est = unscaled.ewm(halflife=SCALE_HALFLIFE, min_periods=VOL_WINDOW).std() * np.sqrt(
        TRADING_DAYS)
    w = raw.mul(TARGET_VOL / est, axis=0)
    gross = w.abs().sum(axis=1)
    w = w.mul((MAX_GROSS / gross).clip(upper=1.0), axis=0)
    if rebalance > 1:  # hold each book for `rebalance` days
        keep = np.arange(len(w)) % rebalance == 0
        w = w.where(pd.Series(keep, index=w.index), axis=0).ffill()
    w = w.loc[START:]
    held = w.shift(1).fillna(0.0)  # the book set at t earns t+1
    pnl = (held * r.loc[START:].fillna(0.0)).sum(axis=1)
    trades = w.diff().abs().sum(axis=1).fillna(w.iloc[0].abs().sum())
    cost = trades.shift(1).fillna(0.0) * COST_BPS / 1e4
    borrow = held.clip(upper=0).abs().sum(axis=1) * BORROW_BPS / 1e4 / TRADING_DAYS
    excess = pnl - cost - borrow
    return {"excess": excess, "total": excess + rf.loc[START:], "w": w,
            "turnover": trades.sum() / (len(w) / TRADING_DAYS)}


def stats(x: pd.Series) -> dict:
    nav = (1 + x).cumprod()
    m, v = x.mean() * TRADING_DAYS, x.std() * np.sqrt(TRADING_DAYS)
    return {"ret": m, "vol": v, "sharpe": m / v, "dd": (nav / nav.cummax() - 1).min(),
            "t": newey_west_t(x, 21)}


def main() -> None:
    pd.set_option("display.width", 250)
    bundle = g.load()
    pct = "{:.1%}".format
    fmt = {"ret": pct, "vol": pct, "dd": pct, "sharpe": "{:.2f}".format, "t": "{:+.1f}".format,
           "turnover": "{:.1f}".format, "avg gross": "{:.2f}".format,
           "avg net": "{:+.2f}".format, "corr book": "{:+.2f}".format}

    variants = {
        "long/short, score": ("score", "both", 1),
        "long/short, sign": ("sign", "both", 1),
        "long/short, score, weekly": ("score", "both", 5),
        "long leg only": ("score", "long", 1),
        "short leg only": ("score", "short", 1),
    }
    out = {n: sleeve(bundle, *v) for n, v in variants.items()}
    rows = {}
    for n, o in out.items():
        rows[n] = {**stats(o["total"]), "turnover": o["turnover"],
                   "avg gross": o["w"].abs().sum(axis=1).mean(),
                   "avg net": o["w"].sum(axis=1).mean()}
    print(f"\nTREND SLEEVE, 45 ETFs, {START} .. {bundle.index[-1].date()} | EMA-200 trend, "
          f"group risk budgets, {TARGET_VOL:.0%} vol target, cost {COST_BPS:g}bp, borrow "
          f"{BORROW_BPS:g}bp | total return incl. T-bill on collateral; t = Newey-West")
    print(pd.DataFrame(rows).T.to_string(formatters=fmt))
    print("   (each leg alone keeps its own 10% vol target, so the legs do not add up to the "
          "sleeve)")

    main_ = out["long/short, score"]
    print("\n   long/short score sleeve by period (excess of T-bills, Sharpe and t)")
    for a, z in (("2000", "2004"), ("2005", "2009"), ("2010", "2014"), ("2015", "2019"),
                 ("2020", "2026")):
        st = stats(main_["excess"].loc[a:z])
        print(f"     {a}-{z}: ret {st['ret']:+.1%}  Sharpe {st['sharpe']:+.2f}  t {st['t']:+.1f}")

    print("\n   by group: each group's own slice of the sleeve, scaled to 10% vol (excess)")
    r = bundle.prices[g.RISKY].pct_change().loc[START:]
    held = main_["w"].shift(1).fillna(0.0)
    grp = {}
    for c, ts in g.GROUPS.items():
        x = (held[ts] * r[ts].fillna(0.0)).sum(axis=1)
        x = x.loc[x.ne(0).idxmax():]
        x = x * TARGET_VOL / (x.std() * np.sqrt(TRADING_DAYS))
        grp[c] = stats(x)
    print(pd.DataFrame(grp).T.to_string(formatters=fmt))

    # --- overlay on the long-only, no-trend 17-ETF book at the production settings ---
    name, book, _, _ = g.run_one({**rb.PORTFOLIO, "universe": "17 ETF", "k": 0.0})
    idx = book.index.intersection(main_["excess"].index)
    b, ex = book.loc[idx], main_["excess"].loc[idx]
    print(f"\n   OVERLAY on the long-only no-trend 17-ETF book ({idx[0].date()} .. "
          f"{idx[-1].date()}); the sleeve adds its excess return, sized to the vol shown")
    bm, sm = (x.groupby(x.index.to_period("M")).sum() for x in (b, ex))
    worst = bm.rank(pct=True) <= 0.05
    print(f"   correlation, sleeve vs book: {b.corr(ex):+.2f}; in the book's worst 5% of "
          f"months the book averaged {bm[worst].mean():+.1%} and the sleeve "
          f"{sm[worst].mean():+.1%}")
    ov = {"book alone": stats(b)}
    for sv in (0.025, 0.05, 0.075, 0.10):
        ov[f"book + sleeve at {sv:.1%} vol"] = stats(b + ex * sv / TARGET_VOL)
    long_ex = out["long leg only"]["excess"].loc[idx]
    ov["book + long leg only at 5% vol"] = stats(b + long_ex * 0.05 / TARGET_VOL)
    print(pd.DataFrame(ov).T.to_string(formatters=fmt))
    yr = pd.DataFrame({"book": b, "sleeve (10% vol)": ex, "book + sleeve 5%": b + ex * 0.5})
    print("\n   calendar years")
    print(yr.groupby(yr.index.year).apply(lambda d: (1 + d).prod() - 1).to_string(
        float_format="{:+.1%}".format))


if __name__ == "__main__":
    main()
