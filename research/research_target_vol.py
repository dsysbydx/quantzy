"""Target vol 15% instead of ~10%: what it costs in Sharpe, drawdown and margin.

The saved book runs at ~9.8% vol (risk aversion 2, leverage cap 1.3x) and ~14% return with
the overlays. The question (2026-11-02): raise the target to 15% for more return. Two ways:

* **Bolder optimizer**: lower risk aversion and raise the cap. The grid below. The
  optimizer's own book changes shape (``research_size_grid``: the extra room goes to sectors
  and Treasuries, and a bolder book had a lower Sharpe at every cap).
* **Lever the saved book**: hold the same weights x L, overlays included, the extra notional
  financed at T-bill + 50 bp. Same Sharpe before financing by construction; at 15% vol it
  needs ~1.5x the gross, ~2-3x on month-end days, so it is over Reg T's 2x as ETFs and
  needs futures or portfolio margin. Shown as the benchmark the grid should beat.

**Fixed before any test.** Grid: risk aversion 2 (saved), 1.5, 1.2, 1.0 x cap 1.3x (saved),
1.6x, 2.0x, everything else in ``PORTFOLIO`` unchanged. Each cell: core plus the five
overlays at their saved sizes, with the daily job's margin scale-back applied (overlays
shrunk so gross stays under 1.95x; their returns shrink with them). Reported: Sharpe (split
2017-07-26), return, vol, max drawdown, Calmar, worst month, average core gross, days the
overlays were scaled back, median and minimum distance to a margin call (the long book's
fall that would exhaust the Reg T cushion), and the worst 5-day loss.

This is a sizing decision, not an alpha test, so there is no adoption rule: pick on return,
drawdown and margin. Default if nothing is clearly better: keep 10%.

**Result (2026-11-02): keep ~10% in the optimizer; 15% only by levering the saved book.**
2008-09-10 .. 2026-10-30, core plus overlays, scale-back applied. Saved (lambda 2, 1.3x):
Sharpe 1.44, 14.5% return, 9.8% vol, -15.1% drawdown. A bolder optimizer buys little return
at a large Sharpe cost, the same in both halves: Sharpe falls with every step down in lambda
and up in cap. Only lambda 1.2 / 2x (15.6% vol) and lambda 1 / 2x (17.0%) reach the target,
at Sharpe 1.17 / 1.10, return 18.6% / 18.7%, drawdown -21.5% / -22.8%, and the overlays
scaled back on 55-60% of days (the core alone uses 1.9x). At 1.3x more boldness adds about
one point of return and no vol past 12.8%: the cap binds. Levering the saved book 1.54x
reaches 15.0% vol with 21.5% return, Sharpe 1.37 (the cash return is not levered, so this
Sharpe falls a little), drawdown -22.5%, worst month -14.4%, worst 5 days -13.3%: three
points more return than the best grid cell at the same vol. It needs ~1.9x gross on average
and ~3x on month-end days, so it is not tradeable under Reg T as ETFs: it needs portfolio
margin (IBKR, risk-based, for accounts above $110k) or futures for part of the book.

Usage:
    uv run python research/research_target_vol.py [--workers 6]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs import margin as mg
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays
from quantzy.strategies import pension_rebalance as pr
from quantzy.strategies import treasury_auction as ta
from quantzy.strategies import treasury_month_end as tme
from quantzy.strategies import vix_hedge as vh

CASH = core.CASH
SPLIT = pd.Timestamp("2017-07-26")
RISK_AVERSION = (2.0, 1.5, 1.2, 1.0)
LEVERAGE = (1.3, 1.6, 2.0)
TARGET = 0.15
BORROW = 0.005  # over T-bill, as in PORTFOLIO
LIMIT = mg.REG_T.max_gross - mg.GROSS_BUFFER


def name(ra: float, lev: float) -> str:
    return f"lambda {ra:g}, cap {lev:g}x"


def run_one(args):
    ra, lev = args
    b = latest_complete(core.load(), core.RISKY)
    p = replace(core.PORTFOLIO, risk_aversion=ra, max_leverage=lev)
    s = core.build(portfolio=p)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    r = p.net_of_borrowing(res.returns, res.weights, CASH)
    return name(ra, lev), r, res.weights.reindex(r.index).fillna(0.0)


def overlay_weights(b, idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Each overlay's weight held into each day, by asset (as ``scaleback_impact``)."""
    o = pd.DataFrame(0.0, index=idx, columns=[*core.RISKY, "SPY", "VIXY"])
    win = tme.in_window(idx)
    o["TLT"] += win.astype(float) * tme.SIZE
    z = pr.z_after_close(pr.asset_prices(b), b.prices[pr.BOND]).reindex(idx)
    entry = win.shift(-1, fill_value=False) & ~win
    o["SPY"] = z.shift(1).where(entry).ffill().where(win).map(pr.weight).fillna(0.0)
    o["IEF"] -= ta.SIZE * ta.in_pre_window(idx, ta.auction_dates("10-Year")).astype(float)
    o["TLT"] -= ta.SIZE * ta.in_pre_window(idx, ta.auction_dates("30-Year")).astype(float)
    on = vh.regime(vh._vix(b)).shift(1, fill_value=False).reindex(idx).fillna(False)
    o["VIXY"] = on.astype(float) * vh.SIZE
    return o


def book(r_core: pd.Series, w: pd.DataFrame, ov_r: pd.Series, ov_w: pd.DataFrame):
    """Core plus scaled-back overlays: returns, combined weights held, the scale."""
    idx = r_core.index
    wc = w.shift(1).reindex(columns=ov_w.columns).fillna(0.0)  # weights held into the day
    scale = pd.Series([mg.scale_overlays(wc.loc[d], ov_w.loc[d], LIMIT) for d in idx],
                      index=idx)
    total = wc + ov_w.mul(scale, axis=0)
    return r_core + ov_r * scale, total, scale


def margin_stats(total: pd.DataFrame) -> dict[str, float]:
    drop = pd.Series([mg.margin(total.loc[d])["drop_to_call"] for d in total.index])
    drop = drop[np.isfinite(drop)]
    return {"call: median fall": drop.median(), "call: min fall": drop.min()}


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    grid = list(itertools.product(RISK_AVERSION, LEVERAGE))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, grid)}
    names = [name(ra, lev) for ra, lev in grid]
    idx = out[names[0]][0].index
    ov_w = overlay_weights(b, idx)
    ov_r = sum(o.backtest_returns(b).reindex(idx).fillna(0.0) for o in overlays(core))

    runs, extra = {}, {}
    for n in names:
        r, w = out[n]
        r, total, scale = book(r.reindex(idx).fillna(0.0), w.reindex(idx).fillna(0.0),
                               ov_r, ov_w)
        runs[n] = r
        core_gross = w.drop(columns=CASH).abs().sum(axis=1)
        extra[n] = {"core gross": core_gross.mean(), "days scaled": (scale < 1).mean(),
                    **margin_stats(total)}

    # the saved book levered to TARGET vol: weights x L, extra (L - 1) x NAV borrowed
    saved = runs[name(2.0, 1.3)]
    cash_r = b.prices[CASH].pct_change(fill_method=None).reindex(idx).fillna(0.0)
    lev = TARGET / (saved.std() * np.sqrt(TRADING_DAYS))
    # the saved book's excess over cash scales by L; the extra notional pays BORROW
    levered = cash_r + lev * (saved - cash_r) - (lev - 1) * BORROW / TRADING_DAYS
    lname = f"saved book x {lev:.2f} (futures / PM)"
    runs[lname] = levered
    _, total_saved, _ = book(out[name(2.0, 1.3)][0].reindex(idx).fillna(0.0),
                             out[name(2.0, 1.3)][1].reindex(idx).fillna(0.0), ov_r, ov_w)
    extra[lname] = {"core gross": lev * extra[name(2.0, 1.3)]["core gross"],
                    "days scaled": np.nan, **margin_stats(total_saved * lev)}

    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]}).loc[list(runs)]
    m["calmar"] = m["ret"] / -m["max_dd"]
    m["worst month"] = [(1 + r).resample("ME").prod().min() - 1 for r in runs.values()]
    m["worst 5d"] = [(1 + r).rolling(5).apply(np.prod, raw=True).min() - 1
                     for r in runs.values()]
    m = m.join(pd.DataFrame(extra).T)
    pct = "{:.1%}".format
    print(f"\nCORE + OVERLAYS (job's scale-back applied), {idx[0].date()} .. {idx[-1].date()}, "
          f"split {SPLIT.date()}")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "worst month",
                                                   "worst 5d", "days scaled",
                                                   "call: median fall", "call: min fall")}
                      | {c: "{:.2f}".format for c in ("sharpe", "train", "test", "calmar",
                                                      "core gross")}))
    for col in ("sharpe", "ret", "vol", "max_dd"):
        piv = m.loc[names, col].copy()
        piv.index = pd.MultiIndex.from_tuples(grid, names=["lambda", "cap"])
        f = "{:.2f}".format if col == "sharpe" else "{:.1%}".format
        print(f"\n   {col}")
        print(piv.unstack().to_string(float_format=f))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
