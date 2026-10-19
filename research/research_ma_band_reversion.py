"""Mean reversion of a normalized moving-average spread, traded on band re-entry.

The spread between a fast and the 200-day EMA, normalized, swings around zero like an
Ornstein-Uhlenbeck process. The idea: buy when it comes back up from an extreme low, sell
(or short) when it comes back down from an extreme high. The question is whether *prices*
pay for it, or whether the spread only reverts because the averages catch up. **Fixed before
any test.**

* ``spread = (EMA(x) - EMA(200)) / EMA(200)``; ``z = spread / std(spread, trailing 252 days)``,
  centered at 0 (the averages equal).
* Long: enter when ``z`` crosses up through ``-B`` (below it the day before, at or above it
  now); exit when ``z >= exit`` or after 126 days. Short: the mirror image through ``+B``,
  exiting at ``z <= -exit``. Decisions use ``z`` through the prior close and trade at the next
  close.
* Primary: ``x = 20``, ``B = 2``, exit at 0. Sensitivity: ``x`` in 10, 20, 50; ``B`` in 1.5, 2;
  exit at 0 or at the opposite band (``+B`` for longs).

Tests: (1) excess return a day while in a trade against always holding, by class, with trade
counts, holding periods and hit rates, on the 17 ETFs; (2) sleeves, long-only and long/short,
each open trade at 2% annual vol (``0.02 / vol``), 2 bp a trade: Sharpe by period and
correlation with the saved book; (3) ^GSPC (price only) from 1950 for the grid.

**Result (2026-10-19): negative.** The spread's OU look is mechanical: on a simulated random
walk its z has autocorrelation 0.87 / 0.56 / 0.32 at 21 / 63 / 126 days (S&P 0.88 / 0.63 /
0.44) and sits beyond +/-2 on 27% of days. In-trade returns are no better than always holding
(primary x=20, B=2: t -0.5 to +1.2 by class; high long hit rates are drift); shorts lose. Over
12 settings, long-only sleeves made Sharpe 0.11-0.35 at 0.21-0.39 correlation with the book
(part-time beta) and long/short -0.17 to -0.47; on ^GSPC since 1950 no setting shows an edge
and all lose in 2000-09. The spread is momentum; the trend alphas already use it the
profitable way round.

Usage:
    uv run python research/research_ma_band_reversion.py
"""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme

ANN = np.sqrt(TRADING_DAYS)
MAX_HOLD = 126
COST = 2e-4
PERIODS = [("1950", "1979"), ("1980", "1999"), ("2000", "2009"), ("2010", "2026")]


def zscore(px: pd.Series, fast: int) -> pd.Series:
    slow = px.ewm(span=200, adjust=False, min_periods=200).mean()
    spread = (px.ewm(span=fast, adjust=False, min_periods=fast).mean() - slow) / slow
    return spread / spread.rolling(252, min_periods=252).std()


def positions(z: pd.Series, band: float, exit_at: float, side: int) -> pd.Series:
    """+1 / -1 while in a trade, 0 otherwise; decided on z through the prior close."""
    zz = (z * side).to_numpy()  # mirror the short side onto the long logic
    out = np.zeros(len(zz))
    in_trade, held = False, 0
    for i in range(2, len(zz)):
        prev, now = zz[i - 2], zz[i - 1]  # known at the prior close
        if in_trade:
            held += 1
            if now >= exit_at or held >= MAX_HOLD or np.isnan(now):
                in_trade = False
        elif not np.isnan(prev) and prev < -band <= now:
            in_trade, held = True, 0
        out[i] = side if in_trade else 0.0
    return pd.Series(out, index=z.index)


def trade_stats(pos: pd.Series, ex: pd.Series) -> dict:
    p = pos.abs()
    starts = (p.diff() > 0).sum()
    held = p.sum()
    trade_id = (p.diff() > 0).cumsum().where(p > 0)
    per_trade = (pos.fillna(0) * ex).groupby(trade_id).sum()
    return {"trades": int(starts), "avg days": held / starts if starts else np.nan,
            "hit rate": (per_trade > 0).mean() if len(per_trade) else np.nan}


def evaluate(px: pd.DataFrame, rf: pd.Series, fast: int, band: float, exit_rule: str,
             classes: dict[str, list[str]]) -> tuple[dict, pd.Series, pd.Series]:
    rets = px.pct_change(fill_method=None)
    ex = rets.sub(rf.reindex(px.index).fillna(0), axis=0)
    vol = rets.rolling(60, min_periods=60).std().shift(1) * ANN
    long_p, short_p = {}, {}
    for t in px.columns:
        z = zscore(px[t].dropna(), fast).reindex(px.index)
        exit_at = 0.0 if exit_rule == "0" else band
        long_p[t] = positions(z, band, exit_at, +1)
        short_p[t] = positions(z, band, exit_at, -1)
    L, S = pd.DataFrame(long_p), pd.DataFrame(short_p)
    size = (0.02 / vol).clip(upper=1.0)
    rows = {}
    for c, ts in classes.items():
        for lab, P in (("long", L.shift(1)), ("short", S.shift(1))):  # the day's return earned
            held = ex[ts].where(P[ts] != 0).stack().dropna() * (1 if lab == "long" else -1)
            allx = ex[ts].stack().dropna() * (1 if lab == "long" else -1)
            t = ((held.mean() - allx.mean())
                 / np.sqrt(held.var() / max(len(held), 1) + allx.var() / len(allx)))
            st = [trade_stats(P[x].fillna(0), ex[x]) for x in ts]
            rows[(c, lab)] = {"trades": sum(s["trades"] for s in st),
                              "avg days": np.nanmean([s["avg days"] for s in st]),
                              "hit rate": np.nanmean([s["hit rate"] for s in st]),
                              "excess %/day in trade": held.mean() * 100,
                              "always %/day": allx.mean() * 100, "t (in - always)": t}
    sleeves = {}
    for lab, P in (("long-only", L), ("long/short", L + S)):
        w = (P * size).fillna(0.0)
        cost = w.diff().abs().sum(axis=1) * COST
        sleeves[lab] = (w.shift(1).fillna(0) * ex.fillna(0)).sum(axis=1) - cost
    return rows, sleeves["long-only"], sleeves["long/short"]


def sharpe(x: pd.Series) -> float:
    x = x.loc[x.ne(0).idxmax():]
    return x.mean() / x.std() * ANN if x.std() > 0 else np.nan


def main() -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    px = b.prices[core.RISKY]
    rf = b.prices[core.CASH].pct_change(fill_method=None)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, core.build())
    book = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    book = book + tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None),
                                     rf).reindex(book.index).fillna(0)

    rows, lo, ls = evaluate(px, rf, 20, 2.0, "0", core.CLASSES)
    print("1. PRIMARY (x=20, B=2, exit at 0) on the 17 ETFs: trades and excess return in trade")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    print("\n2. SLEEVES (each open trade at 2% vol, 2 bp a trade), grid on the 17 ETFs")
    grid = {}
    for fast, band, ex_rule in itertools.product((10, 20, 50), (1.5, 2.0), ("0", "band")):
        _, lo_g, ls_g = evaluate(px, rf, fast, band, ex_rule, core.CLASSES)
        for lab, s in (("long-only", lo_g), ("long/short", ls_g)):
            row = {"Sharpe": sharpe(s), "excess %/yr": s.mean() * TRADING_DAYS * 100,
                   "corr with book": s.reindex(book.index).corr(book)}
            for a, z in (("2000", "2009"), ("2010", "2017"), ("2018", "2026")):
                row[f"Sharpe {a}-{z[2:]}"] = sharpe(s.loc[a:z])
            grid[(f"x={fast}", f"B={band}", f"exit {ex_rule}", lab)] = row
    print(pd.DataFrame(grid).T.to_string(float_format=lambda v: f"{v:+.2f}"))

    print("\n3. LONG HISTORY: ^GSPC (price only) from 1950, long side; excess over T-bills")
    raw = CachedReader(YFinanceReader()).history(["^GSPC", "^IRX"], start="1950-01-01")
    irx = ((1 + raw["^IRX"].ffill() / 100) ** (1 / TRADING_DAYS) - 1).shift(1)
    rf_long = irx.fillna(0.04 / TRADING_DAYS)  # ^IRX starts 1960; 4% a year before it
    spx = raw[["^GSPC"]].dropna()
    hrows = {}
    for fast, band, ex_rule in itertools.product((10, 20, 50), (1.5, 2.0), ("0", "band")):
        r, lo_h, _ = evaluate(spx, rf_long, fast, band, ex_rule, {"S&P 500": ["^GSPC"]})
        row = {**r[("S&P 500", "long")]}
        for a, z in PERIODS:
            row[f"Sharpe {a}-{z[2:]}"] = sharpe(lo_h.loc[a:z])
        hrows[(f"x={fast}", f"B={band}", f"exit {ex_rule}")] = row
    print(pd.DataFrame(hrows).T.to_string(float_format=lambda v: f"{v:+.3f}"))


if __name__ == "__main__":
    main()
