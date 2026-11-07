"""Rebound detectors: once an equity drawdown is under way, which signal says it has subsided?

The user's idea (2026-11-05): a signal that is on only in a rebound and exactly zero
otherwise, such as a higher high after a drawdown. The book re-enters slowly after crashes
(``research_reentry``: ~25% equities at the March 2020 low, 50% three months later) and the
VRP alpha is at its most bearish at the low, so a rebound trigger would add what neither
has. **Fixed before any test.**

Episodes, on the S&P 500 (SPY in the book; ``^GSPC`` in the event study), all causal:

* An episode starts the day the close is 10% or more below its 252-day high (the reference
  high) and ends the day the close regains it. The trough is the running lowest close; a new
  trough re-arms every trigger and switches any that are on off (a failed rebound).
* Triggers, each firing at most once per trough:
  1. ``higher high`` -- close above the last rally high before the trough (highest close of
     the 21 days before it), at least 5 days after the trough.
  2. ``HL + HH`` -- after the trough, a pullback of at least 3% from the bounce high that
     holds above the trough (a higher low), then a close above that bounce high.
  3. ``thrust`` -- close at least 10% above the trough.
  4. ``VIX cooling`` -- the VIX 30% or more below its peak since the episode began, at least 5
     days after the trough (VIX from 1990).
* Once fired, a trigger is on for 63 trading days, or until a new trough or the episode's
  end, whichever is first. Otherwise it is off: zero, no view.

1. **Event study, 1950-2026** (VIX cooling from 1990): forward 21 / 63 / 126-day S&P return
   over bills on days the trigger is on, against other in-episode days and against all days;
   Newey-West t (lags = horizon) on on-vs-rest inside episodes. Episodes are few, so also per
   firing: the date, days after the trough, return to the trough's next 63 days, and whether
   a new low followed (a failure).
2. **Book**: while on, the 12 equity ETFs get +0.15 or +0.30 of Sharpe (``PanelScore``);
   zero otherwise. Saved book and overlays, ``PORTFOLIO`` fixed, on actual prices (2008-09
   on) and with proxy history (1995-10 on). Also the equity weight 21 and 63 days after the
   2009, 2020 and 2022 lows.

Adoption rule: (1) the trigger's 63-day return when on beats other in-episode days with NW t
>= 2, and the same sign before and after 1990; and (2) in the book, a Sharpe gain of at
least +0.02 on actual prices with neither train nor test lower, and the proxy run's full
Sharpe not lower. Four triggers and two sizes are tried; one pass alone is weak evidence.

**Result (2026-11-05, data to 2026-11-04): fails; not adopted. Confirmation comes too late
and fails too often.** 26 episodes since 1950, in drawdown on 50% of days.

* Event study: no trigger beats the other in-episode days at 63 days (t +0.35, -0.68, +0.09,
  -1.65). Since 1990 every trigger is *worse* than the other in-episode days (63-day +0.4 to
  +0.8% when on against +1.9 to +2.4% off): buying into the drawdown before any sign of a
  turn paid better than waiting for one, the same lesson VRP gave. Before 1990 ``higher
  high`` and ``thrust`` look better (63-day +2.4% and +2.2% against +0.7%, t 1.1 and 0.8),
  which does not hold up after.
* Per firing: 37-59% were followed by a new low (``VIX cooling`` worst, 59%). Median firing
  came 9-23 days after the trough and 7-11% above it, so most of the snap-back was gone
  already. The bear rallies of 2000-02, 2008 and 2022 fired every trigger, several times.
* Book, actual prices (saved 1.433): every variant lower, 1.38-1.39 at +0.15 and 1.31-1.34 at
  +0.30, both halves, max drawdown -17% to -20% against -15%. Re-entry is faster (2020
  equity weight 63 days after the low 46% to 57-66%, 2022 61% to 84-105%), and 2020 and 2025
  gain 2-6 points, but bear-market rallies cost more (2018, 2019, 2022, and on proxy data
  2001-02 and 2008 -3 to -11 points). Proxy run 1.18-1.26 against 1.32.

Usage:
    uv run python research/research_rebound.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import compare
from quantzy.signals import Identity, PanelScore
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

SPLIT = pd.Timestamp("2017-07-26")
HALF = pd.Timestamp("1990-01-01")
TRIGGERS = ("higher high", "HL + HH", "thrust", "VIX cooling")
DD, HOLD, MIN_AGE, PULLBACK, THRUST, VIX_DROP = 0.10, 63, 5, 0.03, 0.10, 0.30
LOWS = {"2009": "2009-03-09", "2020": "2020-03-23", "2022": "2022-10-12"}


def rebound_states(px: pd.Series, vix: pd.Series | None = None) -> tuple[pd.DataFrame, list]:
    """Daily on/off for each trigger, and a log of firings. Causal: row t uses data to t."""
    p = px.to_numpy(float)
    v = (vix.reindex(px.index).ffill().to_numpy(float) if vix is not None
         else np.full(len(p), np.nan))
    hi = px.rolling(252, min_periods=1).max().to_numpy()
    n = len(p)
    on = {k: np.zeros(n, bool) for k in TRIGGERS}
    fired_at = {k: -1 for k in TRIGGERS}
    armed = {k: True for k in TRIGGERS}
    log = []
    in_ep = False
    for t in range(n):
        if not np.isfinite(p[t]):
            continue
        if not in_ep:
            if p[t] <= (1 - DD) * hi[t]:
                in_ep, ref, trough, t_tr = True, hi[t], p[t], t
                vpk = v[t]
                bounce, hl_low, hl_set = p[t], np.inf, False
                armed = {k: True for k in TRIGGERS}
                fired_at = {k: -1 for k in TRIGGERS}
            else:
                continue
        if p[t] >= ref:  # episode over
            in_ep = False
            continue
        if np.isfinite(v[t]):
            vpk = v[t] if not np.isfinite(vpk) else max(vpk, v[t])
        if p[t] < trough:  # new low: failed rebounds switch off, everything re-arms
            for k in TRIGGERS:
                if fired_at[k] >= 0:
                    log[-1 - [e["trigger"] for e in reversed(log)].index(k)]["failed"] = True
            trough, t_tr = p[t], t
            bounce, hl_low, hl_set = p[t], np.inf, False
            armed = {k: True for k in TRIGGERS}
            fired_at = {k: -1 for k in TRIGGERS}
            continue
        # track bounce high and higher low for HL + HH
        if not hl_set:
            if p[t] > bounce:
                bounce, hl_low = p[t], np.inf
            else:
                hl_low = min(hl_low, p[t])
                if hl_low <= (1 - PULLBACK) * bounce and hl_low > trough:
                    hl_set = True
        age = t - t_tr
        rally_high = np.nanmax(p[max(0, t_tr - 21):t_tr]) if t_tr > 0 else np.inf
        cond = {
            "higher high": age >= MIN_AGE and p[t] > rally_high,
            "HL + HH": hl_set and p[t] > bounce,
            "thrust": p[t] >= (1 + THRUST) * trough,
            "VIX cooling": age >= MIN_AGE and np.isfinite(vpk) and v[t] <= (1 - VIX_DROP) * vpk,
        }
        for k in TRIGGERS:
            if armed[k] and cond[k]:
                armed[k], fired_at[k] = False, t
                log.append({"trigger": k, "date": px.index[t], "trough": px.index[t_tr],
                            "days after trough": age, "above trough": p[t] / trough - 1,
                            "failed": False})
            if fired_at[k] >= 0 and t - fired_at[k] < HOLD:
                on[k][t] = True
    # episode membership, for the in-episode baseline
    return pd.DataFrame(on, index=px.index), log


def episode_mask(px: pd.Series) -> pd.Series:
    hi = px.rolling(252, min_periods=1).max()
    m, in_ep, ref = [], False, np.nan
    for p, h in zip(px.to_numpy(), hi.to_numpy()):
        if not in_ep and p <= (1 - DD) * h:
            in_ep, ref = True, h
        elif in_ep and p >= ref:
            in_ep = False
        m.append(in_ep)
    return pd.Series(m, index=px.index)


# --- 1. event study --------------------------------------------------------------------------

def _nw_t(y: np.ndarray, x: np.ndarray, lags: int) -> float:
    X = np.column_stack([np.ones_like(x), x])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    g = X * (y - X @ beta)[:, None]
    S = g.T @ g
    for k in range(1, lags + 1):
        c = g[k:].T @ g[:-k]
        S += (1 - k / (lags + 1)) * (c + c.T)
    inv = np.linalg.inv(X.T @ X)
    return float(beta[1] / np.sqrt((inv @ S @ inv)[1, 1]))


def event_study() -> None:
    raw = CachedReader(YFinanceReader()).history(["^GSPC", "^VIX", "^IRX"], start="1950-01-01")
    spx = raw["^GSPC"].dropna()
    vix = raw["^VIX"]
    bill = (raw["^IRX"].ffill() / 100).reindex(spx.index).ffill().fillna(0.04)
    st, log = rebound_states(spx, vix)
    ep = episode_mask(spx)
    print(f"episodes (>= {DD:.0%} below the 252-day high) 1950-2026: "
          f"{int((ep & ~ep.shift(fill_value=False)).sum())}, {ep.mean():.0%} of days")
    rows = []
    for h in (21, 63, 126):
        fwd = spx.shift(-h) / spx - 1 - bill * h / 252
        for k in TRIGGERS:
            for per, sl in (("all", slice(None)), ("1950-89", slice(None, HALF)),
                            ("1990-", slice(HALF, None))):
                if k == "VIX cooling" and per == "1950-89":
                    continue
                d = pd.DataFrame({"y": fwd, "on": st[k], "ep": ep}).loc[sl].dropna()
                if k == "VIX cooling":
                    d = d.loc["1990":]
                inn = d[d.ep | d.on]
                t = _nw_t(inn.y.to_numpy(), inn.on.astype(float).to_numpy(), h)
                rows.append({"h": h, "trigger": k, "period": per, "on": d.y[d.on].mean(),
                             "in-episode off": inn.y[~inn.on].mean(), "all days": d.y.mean(),
                             "t (on vs off)": t, "days on": int(d.on.sum())})
    print("\n1a. S&P 500 forward return over bills, mean: trigger on, other in-episode days, "
          "all days; NW t of on vs in-episode off")
    print(pd.DataFrame(rows).set_index(["h", "trigger", "period"])
          .to_string(float_format="{:+.3f}".format))

    lg = pd.DataFrame(log)
    fwd63 = (spx.shift(-63) / spx - 1).reindex(lg["date"]).to_numpy()
    lg["next 63d"] = fwd63
    print("\n1b. firings per trigger: count, share later failed (new low), median days after "
          "trough, median gain off the trough at firing, mean / median next 63 days")
    print(lg.groupby("trigger").agg(
        firings=("date", "size"), failed=("failed", "mean"),
        days_after=("days after trough", "median"), above_trough=("above trough", "median"),
        next63_mean=("next 63d", "mean"), next63_median=("next 63d", "median"))
        .loc[list(TRIGGERS)].to_string(float_format="{:+.3f}".format))
    print("\n1c. firings since 2000")
    show = lg[lg["date"] >= "2000"].copy()
    show["date"] = show["date"].dt.date
    show["trough"] = show["trough"].dt.date
    print(show.to_string(index=False, float_format="{:+.1%}".format))


# --- 2. book ---------------------------------------------------------------------------------

def with_rebound(b: DataBundle, trig: str | None, size: float) -> DataBundle:
    if trig is None:
        return b
    spy = b["market"].iloc[:, 0].astype(float)
    vix = b["vix"].iloc[:, 0].astype(float)
    st, _ = rebound_states(spy.dropna(), vix)
    on = st[trig].reindex(b.index).fillna(False)
    panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
    for t in core.EQUITY:
        panel[t] = np.where(on, size, 0.0)
    return DataBundle({**b, "rebound": panel})


def run_book(args):
    data, name, trig, size = args
    b = (core.load if data == "actual" else core_proxies.load)()
    warm = core.warmup(b, core.build())
    b = with_rebound(b, trig, size)
    alphas = dict(core.ALPHAS)
    if trig is not None:
        alphas["rebound"] = (PanelScore("rebound", normalizer=Identity()), 1.0)
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    eqw = res.weights[core.EQUITY].sum(axis=1)
    after = {}
    for lab, d in LOWS.items():
        i = eqw.index.searchsorted(pd.Timestamp(d))
        if i < len(eqw):
            after[lab] = (eqw.iloc[min(i + 21, len(eqw) - 1)], eqw.iloc[min(i + 63, len(eqw) - 1)])
    return data, name, r, after


def book_report() -> None:
    variants = {"saved": (None, 0.0)}
    variants |= {f"{k} +{s:.2f}": (k, s) for s in (0.15, 0.30) for k in TRIGGERS}
    jobs = [(d, n, k, s) for d in ("actual", "proxy") for n, (k, s) in variants.items()]
    with ProcessPoolExecutor(7) as pool:
        out = list(pool.map(run_book, jobs))
    for data in ("actual", "proxy"):
        runs = {n: r for d, n, r, _ in out if d == data}
        after = {n: a for d, n, _, a in out if d == data}
        t = compare(runs, split_date=SPLIT)
        m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                          "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                          "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
        m = m.loc[list(variants)]
        for lab in LOWS:
            m[f"eq {lab} +21/+63"] = [
                "{:.0%} / {:.0%}".format(*after[n][lab]) if lab in after[n] else ""
                for n in m.index]
        r0 = runs["saved"]
        if data == "proxy":
            m["pre-2008"] = [compare({n: runs[n].loc[:"2008-09-09"]})["sharpe"].iloc[0]
                             for n in m.index]
        print(f"\n2. book with overlays, {data}, {r0.index[0].date()} .. {r0.index[-1].date()}, "
              f"split {SPLIT.date()}; PORTFOLIO fixed. Equity weight 21 / 63 days after each low")
        print(m.to_string(float_format="{:.3f}".format))
        yrs = pd.DataFrame({n: (1 + runs[n]).groupby(runs[n].index.year).prod() - 1
                            for n in runs})
        diff = yrs.sub(yrs["saved"], axis=0).drop(columns="saved")
        diff = diff[[c for c in diff.columns if c.endswith("0.30")]]
        diff = diff[(diff.abs() > 0.002).any(axis=1)]
        print("   calendar-year return minus saved at +0.30, years moved by more than 0.2 points")
        print(diff.to_string(float_format="{:+.1%}".format))


def main() -> None:
    pd.set_option("display.width", 240)
    pd.set_option("display.max_rows", 300)
    event_study()
    book_report()


if __name__ == "__main__":
    main()
