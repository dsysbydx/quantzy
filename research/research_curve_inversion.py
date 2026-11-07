"""Does an inverted yield curve predict equity drawdowns, and should the book act on it?

The user's idea (2026-11-05). The book already uses the curve for Treasuries (the carry prior,
``research_term_spread_prior``), not for equities. An inverted curve (10-year yield below the
3-month bill) has preceded every US recession since the 1960s, with a lead of 6-24 months;
the question here is equity drawdowns, and whether the warning is early enough, and rare
enough in false alarms, to be worth cutting equities for. **Fixed before any test.**

Spread: ``^TNX - ^IRX`` (10-year minus 3-month, Yahoo, daily from 1962). States, each known at
the date it is used:

* ``inverted``  -- the 21-day average spread is below 0 (monthly: the month's average).
* ``inv 24m``   -- ``inverted`` at any time in the past 24 months (the classic lead window).
* ``re-steep``  -- not inverted now, but inverted within the past 12 months (the curve
  re-steepening as the Fed cuts, often near the equity peak).

1. **Event study, S&P 500 (price), month-ends 1962-2026.** For each state against the rest:
   the next 12 (and 24) months' maximum drawdown (path from the month-end), the chance of a
   drawdown of 15% or more, and the 12-month return over bills. Inference: regression of the
   outcome on the state with Newey-West t (lags = horizon), full sample and halves
   (1962-93 / 1994-2026). Episodes are few, so also a table per inversion episode: the date it
   began, months to the S&P peak, the return from the start to the peak (the cost of leaving
   early) and the drawdown that followed.
2. **Book.** While a state is on, the 12 equity ETFs get a score that cancels their 0.30
   Sharpe prior (-0.30, and -0.15 for half), through ``PanelScore``; everything else as saved,
   with overlays and ``PORTFOLIO`` fixed. On the ETFs' own prices (2008-09 on) and with proxy
   history (1995-10 on), which adds the 1998, 2000 and 2006-07 inversions.

Adoption rule: (1) for the state used, the 12-month drawdown is worse when on, NW t <= -2 on
the full sample and the same sign in both halves; and (2) in the book, a Sharpe gain of at
least +0.02 on the actual run with neither train nor test lower, and the proxy run's full
Sharpe not lower. Three states and two sizes are tried; a pass by only one is weak evidence.

**Result (2026-11-05, data to 2026-11-04): it predicts drawdowns, but too early to trade;
not adopted.** Part (1) passes, part (2) fails.

* Event study, 1962-2026: after an inverted month the next 12 months' max drawdown averages
  -19.1% against -13.4% (t -2.2; halves -2.5 / -0.7), and a 15%+ drawdown came in 64% of
  cases against 29%. ``inv 24m`` -18.2% against -11.7% (t -3.2; halves -1.6 / -3.1);
  ``re-steep`` -18.8% against -13.4% (t -2.2), all from after 1994. But 12-month excess
  return is not reliably lower (t -1.5, -1.8, -0.5): the drawdowns come after a rally. Only 8
  episodes, so the t-stats overstate the evidence.
* Per episode, the S&P rose from the first inverted month to its peak by a median ~27%
  (+34%, +3%, +7%, +46%, +0%, +20%, +46%, +48%), with the peak 13-24 months later in five of
  eight. Measured from the inversion's start, the 36-month low was only -4% (1966), 0%
  (1978) and -7% (2022): selling at the inversion and waiting would have missed the rally
  and gained little. 2019's -24% low was COVID, not the cycle the curve was forecasting.
* Book, actual prices 2008-2026 (saved 1.433, train 1.41 / test 1.46): ``inverted`` 1.434 at
  -0.30 and 1.443 at -0.15; ``inv 24m`` 1.397 / 1.445 (test 1.31 / 1.43); ``re-steep``
  1.395 / 1.426. None gains +0.02 with both halves held. It cost in 2019-21 and 2024, and
  ``inv 24m`` was on through 2026 (to about Dec 2026): -16 points this year.
* Book on proxy history (1995 on): ``inv 24m`` -0.30 lifts Sharpe 1.32 to 1.46 (before
  2008-09: 1.20 to 1.59), from 2001-02 and 2008. The gain is all in the two inversions that
  ended in deep bear markets; the two since (2019, 2022-24) were early or false alarms for
  equities. With 2 good and 2 bad episodes in the book's history, the rule is a coin we
  cannot weigh. A diversified book already cuts equities through trend once the fall starts.

Usage:
    uv run python research/research_curve_inversion.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import compare, newey_west_t
from quantzy.signals import Identity, PanelScore
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

SPLIT = pd.Timestamp("2017-07-26")
HALF = pd.Timestamp("1994-01-01")
STATES = ("inverted", "inv 24m", "re-steep")


def states_daily(spread: pd.Series) -> pd.DataFrame:
    """The three states on a daily spread (rows = days with a 10-year print)."""
    inv = spread.rolling(21, min_periods=15).mean() < 0
    any24 = inv.astype(float).rolling(504, min_periods=1).max() > 0
    any12 = inv.astype(float).rolling(252, min_periods=1).max() > 0
    return pd.DataFrame({"inverted": inv, "inv 24m": any24, "re-steep": ~inv & any12})


def states_monthly(spread: pd.Series) -> pd.DataFrame:
    inv = spread.resample("ME").mean() < 0
    any24 = inv.astype(float).rolling(24, min_periods=1).max() > 0
    any12 = inv.astype(float).rolling(12, min_periods=1).max() > 0
    return pd.DataFrame({"inverted": inv, "inv 24m": any24, "re-steep": ~inv & any12})


# --- 1. event study --------------------------------------------------------------------------

def forward_dd(px: pd.Series, months: int) -> pd.Series:
    """Max drawdown over the next ``months`` month-ends' worth of daily closes, from each
    month-end close (the starting level counts as a peak)."""
    me = px.resample("ME").last().index
    out = {}
    for d in me:
        end = d + pd.DateOffset(months=months)
        if end > px.index[-1]:
            break
        path = px.loc[px.index[px.index.get_indexer([d], method="ffill")[0]]:end]
        out[d] = float((path / path.cummax() - 1).min())
    return pd.Series(out)


def nw_reg(y: pd.Series, x: pd.Series, lags: int) -> tuple[float, float, float, int]:
    """Mean of y when x is on, when off, their difference and the months on."""
    d = pd.concat([y, x.astype(float)], axis=1).dropna()
    yy, xx = d.iloc[:, 0], d.iloc[:, 1]
    on, off = yy[xx == 1].mean(), yy[xx == 0].mean()
    return on, off, on - off, int(xx.sum())


def event_study() -> None:
    raw = CachedReader(YFinanceReader()).history(["^GSPC", "^TNX", "^IRX"], start="1950-01-01")
    spx = raw["^GSPC"].dropna()
    spread = (raw["^TNX"] - raw["^IRX"]).dropna()
    st = states_monthly(spread).loc["1962-01":]
    bill = raw["^IRX"].resample("ME").mean() / 100
    me = spx.resample("ME").last()
    rows = []
    for h in (12, 24):
        dd = forward_dd(spx.loc["1961-12":], h)
        ret = (me.shift(-h) / me - 1) - bill * h / 12
        for name in STATES:
            x = st[name].reindex(dd.index)
            for lab, y in ((f"maxDD {h}m", dd), (f"P(DD>=15%) {h}m", (dd <= -0.15).astype(float)),
                           (f"excess ret {h}m", ret.reindex(dd.index))):
                for per, sl in (("all", slice(None)), ("1962-93", slice(None, HALF)),
                                ("1994-", slice(HALF, None))):
                    on, off, diff, n = nw_reg(y.loc[sl], x.loc[sl], h)
                    tt = _nw_t(y.loc[sl], x.loc[sl], h)
                    rows.append({"state": name, "outcome": lab, "period": per, "on": on,
                                 "off": off, "diff": diff, "t": tt, "months on": n})
    t = pd.DataFrame(rows).set_index(["outcome", "state", "period"])
    print("1a. S&P 500 after each state, month-ends: mean when on / off, difference, NW t "
          "(lags = horizon)")
    print(t.to_string(float_format="{:+.3f}".format))
    print(f"\n   share of months on (1962-): " + ", ".join(
        f"{s} {st[s].mean():.0%}" for s in STATES) + f"; latest month {st.index[-1]:%Y-%m}: "
        + ", ".join(f"{s} {'ON' if st[s].iloc[-1] else 'off'}" for s in STATES))
    episodes(spx, spread)


def _nw_t(y: pd.Series, x: pd.Series, lags: int) -> float:
    """HAC t of the on-minus-off difference: regress y on [1, x]."""
    d = pd.concat([y, x.astype(float)], axis=1).dropna()
    yy, xx = d.iloc[:, 0].to_numpy(), d.iloc[:, 1].to_numpy()
    if xx.min() == xx.max():
        return np.nan
    X = np.column_stack([np.ones_like(xx), xx])
    beta, *_ = np.linalg.lstsq(X, yy, rcond=None)
    u = yy - X @ beta
    xtx_inv = np.linalg.inv(X.T @ X)
    g = X * u[:, None]
    S = g.T @ g
    for k in range(1, lags + 1):
        w = 1 - k / (lags + 1)
        c = g[k:].T @ g[:-k]
        S += w * (c + c.T)
    V = xtx_inv @ S @ xtx_inv
    return float(beta[1] / np.sqrt(V[1, 1]))


def episodes(spx: pd.Series, spread: pd.Series) -> None:
    inv = states_monthly(spread)["inverted"].loc["1962-01":]
    starts = [d for i, d in enumerate(inv.index)
              if inv.iloc[i] and not inv.iloc[max(0, i - 12):i].any()]
    rows = []
    for s in starts:
        run = inv.loc[s:]
        end = run.index[(~run).argmax()] if (~run).any() else run.index[-1]
        p0 = spx.loc[:s].iloc[-1]
        win = spx.loc[s:s + pd.DateOffset(months=36)]
        peak_d = win.loc[:s + pd.DateOffset(months=24)].idxmax()
        trough = win.loc[peak_d:]
        rows.append({"inversion began": f"{s:%Y-%m}", "ended": f"{end:%Y-%m}",
                     "months to S&P peak": round((peak_d - s).days / 30.4),
                     "start->peak": win.loc[peak_d] / p0 - 1,
                     "peak->low (36m)": trough.min() / win.loc[peak_d] - 1,
                     "start->36m low": win.min() / p0 - 1,
                     "12m return": spx.loc[:s + pd.DateOffset(months=12)].iloc[-1] / p0 - 1})
    t = pd.DataFrame(rows)
    print("\n1b. each inversion episode (first inverted month after 12 months without): the S&P "
          "peak within 24 months, the low within 36")
    print(t.to_string(index=False, float_format="{:+.1%}".format))


# --- 2. book ---------------------------------------------------------------------------------

def with_curve(b: DataBundle, state: str | None, size: float) -> DataBundle:
    if state is None:
        return b
    spread = b["carry"]["IEF"].astype(float)
    on = states_daily(spread.dropna()).reindex(spread.index).ffill()[state].fillna(False)
    panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
    for t in core.EQUITY:
        panel[t] = np.where(on, -size, 0.0)
    return DataBundle({**b, "curve": panel})


def run_book(args: tuple[str, str, str | None, float]) -> tuple[str, str, pd.Series, float]:
    data, name, state, size = args
    b = (core.load if data == "actual" else core_proxies.load)()
    warm = core.warmup(b, core.build())
    b = with_curve(b, state, size)
    alphas = dict(core.ALPHAS)
    if state is not None:
        alphas["curve"] = (PanelScore("curve", normalizer=Identity()), 1.0)
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    eq = res.weights[core.EQUITY].sum(axis=1).mean()
    return data, name, r, float(eq)


def book_report() -> None:
    variants = {"saved": (None, 0.0)}
    variants |= {f"{s} -0.30": (s, 0.30) for s in STATES}
    variants |= {f"{s} -0.15": (s, 0.15) for s in STATES}
    jobs = [(d, n, s, k) for d in ("actual", "proxy") for n, (s, k) in variants.items()]
    with ProcessPoolExecutor(7) as pool:
        out = list(pool.map(run_book, jobs))
    for data in ("actual", "proxy"):
        runs = {n: r for d, n, r, _ in out if d == data}
        eq = {n: e for d, n, _, e in out if d == data}
        t = compare(runs, split_date=SPLIT)
        m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                          "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                          "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
        m = m.loc[list(variants)]
        m["equity wt"] = [eq[n] for n in m.index]
        r0 = runs["saved"]
        if data == "proxy":
            m["pre-2008"] = [compare({n: runs[n].loc[:"2008-09-09"]})["sharpe"].iloc[0]
                             for n in m.index]
        print(f"\n2. book with overlays, {data}, {r0.index[0].date()} .. {r0.index[-1].date()}, "
              f"split {SPLIT.date()}; PORTFOLIO fixed")
        print(m.to_string(float_format="{:.3f}".format))
        yrs = pd.DataFrame({n: (1 + runs[n]).groupby(runs[n].index.year).prod() - 1
                            for n in ("saved", "inverted -0.30", "inv 24m -0.30",
                                      "re-steep -0.30")})
        diff = yrs.sub(yrs["saved"], axis=0).drop(columns="saved")
        diff = diff[(diff.abs() > 0.002).any(axis=1)]
        print("   calendar-year return minus saved, years that moved by more than 0.2 points")
        print(diff.to_string(float_format="{:+.1%}".format))


def main() -> None:
    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 200)
    event_study()
    book_report()


if __name__ == "__main__":
    main()
