"""Cross-asset time-series momentum: past bond returns for equities, past equity returns for bonds.

Pitkajarvi, Suominen and Vaittinen (2020): past bond returns predict equity returns
*positively* (falling yields signal easier conditions that equities price with a lag), and
past equity returns predict bond returns *negatively* (rising risk appetite pulls money out
of bonds). That is a different bet from each asset's own trend. **Fixed before any test.**

Signals, from the prior close: ``z(12-month IEF return)`` for the 12 equity ETFs, and
``-z(12-month SPY return)`` for IEF and TLT, each z-scored against the trailing 5 years (from
2 years of history) and capped at +/-2. In ``mu`` like VRP: score times vol times ``k``, with
``k`` matched to the trend budget's train-window dispersion (0.1 on EMA-200).

Tests: time-series rank IC on SPY (from 1994) and on a 10-year par bond rolled daily from
^TNX, with ^GSPC (price only) and the par bond from 1963 as a long check: 1-day held-out
47-day folds, 21 and 63 days, by period. Then the book with both legs, the equity leg only
and the bond leg only, ``PORTFOLIO`` unchanged; and correlation with the trend scores.

**Result (2026-10-18): negative.** Bond momentum for equities has no IC on SPY since 1994
(1-day held out +0.002, t 0.2); on ^GSPC from 1965 it worked only before 1994. Equity
momentum for bonds (negative) has a 1-day IC of +0.018-0.027 (t 1.9-2.4) but under t 1 at
21 days. Uncorrelated with trend (-0.11) yet no book gain: Sharpe 1.11-1.14 against 1.13
(difference t -0.4 to +0.3), with deeper drawdowns (-18% against -16%).

Usage:
    uv run python research/research_cross_asset_mom.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from research_calendar_flows import daily_par_bond
from research_reentry import MarketStateView

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
LOOKBACK, Z_WINDOW, Z_MIN, CAP = 252, 1260, 504, 2.0
BONDS = ["IEF", "TLT"]
EQUITY = core.EQUITY


def zscore(x: pd.Series) -> pd.Series:
    roll = x.rolling(Z_WINDOW, min_periods=Z_MIN)
    return ((x - roll.mean()) / roll.std()).clip(-CAP, CAP)


def momentum(price: pd.Series) -> pd.Series:
    return price / price.shift(LOOKBACK) - 1


# --- 1. IC on long histories -------------------------------------------------------------------

def ic_report() -> None:
    yf = CachedReader(YFinanceReader())
    px = yf.history(["SPY", "^GSPC", "^TNX"], start="1950-01-01")
    bond_r = daily_par_bond(px["^TNX"], 10)
    bond_px = (1 + bond_r).cumprod()
    pairs = {
        "equity <- bond (SPY, 1994+)": (zscore(momentum(bond_px)), px["SPY"]),
        "equity <- bond (^GSPC, 1963+)": (zscore(momentum(bond_px)), px["^GSPC"]),
        "bond <- equity (10y bond, SPY signal)": (-zscore(momentum(px["SPY"])), bond_px),
        "bond <- equity (10y bond, ^GSPC signal, 1963+)": (-zscore(momentum(px["^GSPC"])),
                                                           bond_px),
        "own trend, SPY (12m, for reference)": (zscore(momentum(px["SPY"])), px["SPY"]),
    }
    rows = {}
    for name, (sig, target) in pairs.items():
        df = pd.concat([sig.rename("s"), target.rename("p")], axis=1).dropna()
        if "1994" in name:
            df = df.loc["1994":]
        s, p = df[["s"]].shift(1), df[["p"]]
        s.columns = p.columns = ["x"]
        folds = chunk_folds(df.index, 47)
        a, b = folds == 0, folds == 1

        def ic(h, fit=None, s=s, p=p):
            zx, zy = _ts_pairs(s, p, h, "spearman", 60, fit=fit)
            return (zx * zy).mean(axis=1)

        held = pd.concat([ic(1, a)[b], ic(1, b)[a]]).dropna()
        row = {"from": f"{df.index[0]:%Y}",
               "1d held out": f"{held.mean():+.4f} ({newey_west_t(held, 21):+.1f})"}
        for h in (21, 63):
            x = ic(h).dropna()
            row[f"{h}d"] = f"{x.mean():+.3f} ({newey_west_t(x, 2 * h):+.1f})"
        x = ic(21).dropna()
        for lo, hi in (("1963", "1993"), ("1994", "2007"), ("2008", "2026")):
            y = x.loc[lo:hi]
            row[f"21d {lo}-{hi[2:]}"] = (f"{y.mean():+.3f} ({newey_west_t(y, 42):+.1f})"
                                         if len(y) > 500 else "")
        rows[name] = row
    print("1. TIME-SERIES RANK IC vs forward return / 60d vol (Newey-West t)")
    print(pd.DataFrame(rows).T.to_string())


# --- 2. book -----------------------------------------------------------------------------------

def bundle_with(legs: str) -> DataBundle:
    b = latest_complete(core.load(), core.RISKY)
    px = b.prices
    eq_sig = zscore(momentum(px["IEF"]))
    bd_sig = -zscore(momentum(b["market"].iloc[:, 0]))  # SPY
    p = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
    if legs in ("both", "equity"):
        for t in EQUITY:
            p[t] = eq_sig
    if legs in ("both", "bond"):
        for t in BONDS:
            p[t] = bd_sig
    return DataBundle.build(px, vix=b["vix"], market=b["market"], carry=b["carry"], xmom=p)


def matched_k(b: DataBundle) -> float:
    vol = realized_vol(b.prices, 60, annualize=True)
    cols = [*EQUITY, *BONDS]
    trend_sd = (core.ALPHAS["trend"][0].raw(b) / vol)[cols].loc[:SPLIT].stack().std()
    return 0.1 * trend_sd / b["xmom"][cols].loc[:SPLIT].stack().std()


def run_one(args: tuple[str, str, float]):
    name, legs, k = args
    b = bundle_with(legs)
    alphas = dict(core.ALPHAS)
    if legs != "none":
        alphas["xmom"] = (MarketStateView("xmom", normalizer=Identity()), k)
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def book_report(workers: int) -> None:
    b = bundle_with("both")
    k = matched_k(b)
    vol = realized_vol(b.prices, 60, annualize=True)
    sc = pd.DataFrame({"xmom": b["xmom"][[*EQUITY, *BONDS]].stack(),
                       "EMA-200": (core.ALPHAS["trend"][0].raw(b) / vol)[
                           [*EQUITY, *BONDS]].stack(),
                       "VRP": (core.ALPHAS["vrp"][0].raw(b) / vol)[[*EQUITY, *BONDS]].stack()})
    print(f"\n   matched k = {k:.3f}; correlation of scores (pooled):")
    print(sc.corr().round(2).to_string())
    jobs = [("saved book", "none", 0.0), ("+ both legs", "both", k),
            ("+ equity leg (bond momentum)", "equity", k), ("+ bond leg (-equity momentum)",
                                                             "bond", k)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, t) for n, r, t in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][1] for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    print(f"\n2. BOOK, {runs[names[0]].index[0].date()} .. {runs[names[0]].index[-1].date()}")
    print(m.to_string(formatters={**{c: "{:.1%}".format for c in ("ret", "vol", "max_dd")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe", "diff t")},
                                  "turnover": "{:.1f}".format}))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved book (percentage points)")
    print(yr[names[1:]].sub(yr[names[0]], axis=0).to_string(
        float_format=lambda v: f"{v * 100:+.1f}"))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    ic_report()
    book_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
