"""Target vol by scaling: a high-risk-aversion book levered back up to 10-12% vol.

``research_size_grid.py`` found Sharpe rising with risk aversion (1.33 at 2, 1.40 at 3, 1.46
at 4, with the 1.3x cap) as vol fell (9.8%, 8.1%, 6.8%), and that a higher leverage cap does
not bring the vol back: at risk aversion 3 the book reaches only 8.9% with a 2x cap, because
it does not want more. To run a high-risk-aversion book at a target vol, every risky position
has to be scaled up by a factor ``k`` after the optimizer. This asks whether that beats
risk aversion 2 at the same vol.

* Core books at risk aversion 2 (saved), 3 and 4, saved ``PORTFOLIO`` otherwise; then every
  risky weight times ``k``, the rest in cash (borrowed at T-bill + 50 bp when negative).
* ``k`` constant, solved for 10%, 11% and 12% vol over the whole window: a look-ahead that
  flatters scaling (a live rule would use trailing vol).
* Both overlays as traded, not scaled. Gross includes them; Reg T allows 2x overnight.

**Result (2026-10-27): at the same vol, risk aversion barely matters; stay at 2.**
2008-09-10 .. 2026-10-16, split 2017-07-26. At ~10% vol: risk aversion 2 unscaled 1.33
(1.31 / 1.35), 9.8% vol, gross 1.33x average / 1.85x peak, no day above 2x; 3 scaled by
1.24, 1.34 (1.35 / 1.34), drawdown -16.9% against -15.5%, gross 1.53x / 2.16x, 6% of days
above 2x; 4 scaled by 1.50, 1.36, gross 1.64x / 2.49x, 6% of days above 2x. Most of the
higher-risk-aversion Sharpe is holding and borrowing less (it answers the open question from
the size grid). Scaled to 11-12% the return rises (14.9-16.3%) but Sharpe falls (1.30-1.34),
drawdowns reach -18.5% to -20.1%, and gross passes 2x on 7.5-8% of days (56% for risk
aversion 4 at 12%), mostly in the month-end overlay's window. Risk aversion 2 at the 1.3x
cap is already the ~10% vol target without a scaling step or Reg T breaches; 11-12% would
need risk aversion ~1.5-1.75 and a smaller or rescheduled month-end overlay.

Usage:
    uv run python research/research_vol_scaling.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme
from quantzy.strategies import vix_hedge

CASH = core.CASH
SPLIT = "2017-07-26"
SPREAD = core.PORTFOLIO.borrow_spread_bps / 1e4 / TRADING_DAYS
RISK_AVERSION = (2.0, 3.0, 4.0)
TARGETS = (0.10, 0.11, 0.12)
REG_T = 2.0


def run(ra: float):
    b = latest_complete(core.load(), core.RISKY)
    s = core.build(portfolio=replace(core.PORTFOLIO, risk_aversion=ra))
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    return ra, res.returns, res.weights


def overlay_weights(b) -> pd.Series:
    """The overlays' combined weight held into each day."""
    days = b.prices["TLT"].dropna().index
    tme_w = (tme.in_window(days).astype(float) * tme.SIZE).reindex(b.index).fillna(0.0)
    vix = b["vix"].iloc[:, 0].ffill()
    vix_w = vix_hedge.regime(vix).shift(1, fill_value=False).astype(float) * vix_hedge.SIZE
    return tme_w + vix_w.where(b["vixy"].iloc[:, 0].notna(), 0.0)


def stats(r: pd.Series, gross: pd.Series) -> dict:
    nav = (1 + r).cumprod()

    def sharpe(x):
        return x.mean() / x.std() * np.sqrt(TRADING_DAYS)

    return {"sharpe": sharpe(r), "tr_sharpe": sharpe(r.loc[:SPLIT]),
            "te_sharpe": sharpe(r.loc[SPLIT:]), "cagr": nav.iloc[-1] ** (TRADING_DAYS / len(r)) - 1,
            "vol": r.std() * np.sqrt(TRADING_DAYS), "max_dd": (nav / nav.cummax() - 1).min(),
            "avg gross": gross.mean(), "peak gross": gross.max(),
            "days > 2x": (gross > REG_T + 1e-9).mean()}


def main() -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), core.RISKY)
    cash = b.prices[CASH].pct_change(fill_method=None)
    overlays = tme.backtest_returns(b) + vix_hedge.backtest_returns(b)
    over_w = overlay_weights(b)
    with ProcessPoolExecutor(len(RISK_AVERSION)) as pool:
        out = {ra: (r, w) for ra, r, w in pool.map(run, RISK_AVERSION)}

    rows = {}
    for ra, (r, w) in out.items():
        held = w.shift(1).dropna()  # the weights earning each day
        rr, c = r.loc[held.index], cash.reindex(held.index).fillna(0.0)
        risky = held.drop(columns=CASH).sum(axis=1)

        def book(k, rr=rr, c=c, risky=risky):
            x = k * (rr - c) + c - (k * risky - 1).clip(lower=0) * SPREAD
            return (x + overlays.reindex(x.index).fillna(0.0),
                    k * risky + over_w.reindex(x.index).fillna(0.0))

        rows[f"lambda {ra:g}, unscaled"] = stats(*book(1.0))
        if ra == RISK_AVERSION[0]:
            continue
        for tv in TARGETS:
            k = 1.0
            for _ in range(20):  # solve for the k that gives the target vol
                k *= tv / (book(k)[0].std() * np.sqrt(TRADING_DAYS))
            rows[f"lambda {ra:g}, scaled to {tv:.0%} (k {k:.2f})"] = stats(*book(k))
    m = pd.DataFrame(rows).T
    pct = "{:.1%}".format
    print("\nSAVED BOOK (CORE + OVERLAYS), HIGHER RISK AVERSION SCALED TO A TARGET VOL")
    print(m.to_string(formatters={c: pct for c in ("cagr", "vol", "max_dd", "days > 2x")}
                      | {c: "{:.2f}".format for c in ("sharpe", "tr_sharpe", "te_sharpe",
                                                      "avg gross", "peak gross")}))


if __name__ == "__main__":
    main()
