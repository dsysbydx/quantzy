"""Dollar value on DXY history it was never fitted on: is a weight above 0.10 justified?

The question (2026-11-03): in ``research_signal_weights`` only the dollar value tilt did
better above its saved weight in both halves (x2 1.43 / 1.51, x3 1.43 / 1.53, against
1.41 / 1.47), but that was seen in test. ``research_value`` chose the signal on UUP's
years (2007 on); DXY prints from 1971, so 1981-2007 (once the score has 10 years of
history) is data the signal and its weight never saw. **Fixed before any run.**

Score as saved: ``core.dollar_value_score`` (minus log DXY over its 5-year mean, z-scored
against the trailing 10 years, capped at +/-2), known a day late. Target: DXY's return over
its 60-day vol (DXY is UUP without carry; carry is not in free daily data).

1. IC of the score with the next 1, 21, 63 and 252 days' return over vol (Pearson,
   Newey-West t, lags max(21, 2h)), on 1981-01 .. 2007-02 (untouched) in three parts
   (1981-89, 1990-99, 2000-07) and on 2007-03 .. 2026-10 (seen).
2. The weight the IC implies: ``k = IC_h * sqrt(252 / h)`` Sharpe per unit of score
   (Grinold; the score has unit spread), against the saved 0.10, x2 0.20, x3 0.30.
3. A diagnostic sleeve: hold ``score`` units of DXY at 60-day vol scaling; Sharpe by period,
   and without 1985-87 (the Plaza Accord reversal).
4. Reported, not in the rule: the book on proxy data (history 1995-10 .. 2008-09) with the
   score from DXY's full history and dollar value x1, x2, x3.

Rule: raise the weight to x2 only if the 21-day implied k on 1981-2007 is 0.15 or more with
t >= 2, and the 21-day IC is positive in each of the three parts. Otherwise keep 0.10.

**Result (2026-11-03): fails, and the sign is wrong; keep 0.10, and the signal itself is
in doubt.** On 1981-2007 the score predicts DXY the *other* way: 21-day IC -0.125 (t -2.4),
implied k -0.43; 1981-89 -0.18 (t -2.1), 1990-99 -0.01, 2000-07 -0.10 (t -0.9). The sleeve:
Sharpe -0.49 (t -2.5), -0.40 without 1985-87. Five-year reversal lost to multi-year dollar
trends (the 1981-84 rise, the 1987-95 fall). On DXY it is ~0 in the years it was chosen too
(2007-26: 21-day IC +0.001, 252-day +0.08); on UUP it is +0.07 / +0.22 (rank +0.10 / +0.32,
t 2.4 at 252 days, close to ``research_value``'s +0.43). The difference between UUP and DXY
is carry, so the measured edge is partly the interest-rate gap lining up with the score, not
the price reversal. In the book the weight barely matters (UUP 5-8% of capital): on proxy
history x1 / x2 / x3 1.201 / 1.209 / 1.214, max drawdown -21.1% / -20.6% / -20.6%.

Usage:
    uv run python research/research_dollar_value_long.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import newey_west_t
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

HORIZONS = (1, 21, 63, 252)
PERIODS = (("1981-01-01", "2007-02-28", "1981-2007 (untouched)"),
           ("1981-01-01", "1989-12-31", "  1981-89"), ("1990-01-01", "1999-12-31", "  1990-99"),
           ("2000-01-01", "2007-02-28", "  2000-07"), ("2007-03-01", "2026-10-30", "2007-26 (seen)"))
SAVED_K = 0.10
MULTS = (1.0, 2.0, 3.0)
HISTORY = ("1995-10-01", "2008-09-09")


def dxy() -> pd.Series:
    return CachedReader(YFinanceReader()).history([core.DOLLAR_INDEX], start="1971-01-01")[
        core.DOLLAR_INDEX].dropna()


def ic(score: pd.Series, r: pd.Series, vol: pd.Series, h: int, a: str, z: str) -> tuple:
    fwd = r.rolling(h).sum().shift(-h) / (vol * np.sqrt(h))
    d = pd.concat([score, fwd], axis=1, keys=["s", "f"]).loc[a:z].dropna()
    zs, zf = [(c - c.mean()) / c.std() for c in (d["s"], d["f"])]
    prod = zs * zf
    return prod.mean(), newey_west_t(prod, max(21, 2 * h)), len(d)


def book(mult: float):
    b = core_proxies.load()
    full = core.dollar_value_score(dxy()).reindex(b.index, method="ffill")
    dv = b["dollar_value"].copy()
    dv["UUP"] = full
    b = DataBundle({**b, "dollar_value": dv})
    alphas = {**core.ALPHAS, "dollar_value": (core.ALPHAS["dollar_value"][0],
                                              core.ALPHAS["dollar_value"][1] * mult)}
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(
        b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return mult, r, res.weights["UUP"].reindex(r.index)


def main() -> None:
    pd.set_option("display.width", 180)
    px = dxy()
    r = np.log(px).diff()
    vol = r.rolling(60).std()
    score = core.dollar_value_score(px).shift(1)
    print(f"DXY {px.index[0].date()} .. {px.index[-1].date()}; score from "
          f"{score.first_valid_index().date()}")

    print("\n1-2. IC (t) and implied k = IC * sqrt(252 / h); saved k 0.10, x2 0.20, x3 0.30")
    rows = {}
    for a, z, lab in PERIODS:
        row = {}
        for h in HORIZONS:
            c, t, n = ic(score, r, vol, h, a, z)
            row[f"{h}d IC (t)"] = f"{c:+.3f} ({t:+.1f})"
            row[f"{h}d k"] = f"{c * np.sqrt(TRADING_DAYS / h):+.2f}"
        rows[lab] = row
    print(pd.DataFrame(rows).T.to_string())

    print("\n3. SLEEVE: score units of DXY, vol-scaled (Sharpe, t)")
    pnl = score * r / vol.shift(1)
    for a, z, lab in PERIODS + (("1981-01-01", "2007-02-28", "1981-2007 ex 1985-87"),):
        x = pnl.loc[a:z].dropna()
        if "ex" in lab:
            x = x[(x.index.year < 1985) | (x.index.year > 1987)]
        sr = x.mean() / x.std()
        print(f"   {lab:22s} Sharpe {sr * np.sqrt(TRADING_DAYS):+.2f}  t {sr * np.sqrt(len(x)):+.2f}")

    print("\n4. BOOK on proxy data, history 1995-10 .. 2008-09 (score from DXY's full history)")
    with ProcessPoolExecutor(3) as pool:
        out = list(pool.map(book, MULTS))
    for m, rr, w in out:
        x = rr.loc[slice(*HISTORY)]
        nav = (1 + x).cumprod()
        print(f"   x{m:g}: Sharpe {x.mean() / x.std() * np.sqrt(TRADING_DAYS):.3f}, "
              f"max DD {(nav / nav.cummax() - 1).min():.1%}, "
              f"UUP weight {w.loc[slice(*HISTORY)].mean():.1%}")


if __name__ == "__main__":
    main()
