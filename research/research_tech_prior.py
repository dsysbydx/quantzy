"""Tech Sharpe prior: nudge XLK (and QQQ) above the 0.30 equity prior; re-check every prior.

The question (2026-11-03): the user expects higher earnings growth from tech and asks whether
to nudge its Sharpe prior up, and whether any prior is off from the data. The 100-year check
(``research_prior_check``) found tech average among sectors (Fama-French BusEq since 1926)
and every sector at 0.38-0.51; ``research_equity_prior`` found raising all equities to
0.35-0.40 added risk, not Sharpe; ``research_prior_sensitivity`` found random prior errors of
0.10 cost a median 0.21 Sharpe. A tech nudge is a conviction view, so the backtest can only
say what it costs if wrong and how concentrated it makes the book.

**Fixed before any run.** Variants: XLK prior 0.30 (saved), 0.35, 0.40, 0.50; XLK and QQQ
both at 0.35, 0.40. Everything else as saved, overlays included. Periods (the four used for
this book): history (proxy data, 1995-10 .. 2008-09, a sanity check only), train
2008-09-10 .. 2017-07-25, test 2017-07-26 .. 2026-10-30, production from 2026-11-01 (too
short to read). Train, test and production on the ETFs' own prices; history from the
proxy-extended run. Reported: Sharpe, return, vol, max drawdown; average XLK and QQQ weight.
Prior re-check: each ETF's excess-over-T-bill Sharpe per period against its prior, with the
standard error ``sqrt((1 + SR^2 / 2) / T)``; a gap beyond 2 SE is flagged.

Rule: a nudge is acceptable if test Sharpe is within 0.05 of the saved book's and the history
drawdown is no more than 3 points deeper; the largest acceptable nudge is the candidate.

**Result (2026-11-03): no prior is off beyond noise; a tech nudge is a bet, not a fix.
Candidate if wanted: XLK and QQQ at 0.35.** Prior re-check: no ETF's realized Sharpe is
more than 2 SE from its prior in train or test. Tech is the highest (XLK 0.71 / 0.83 in
train / test, +1.1 / +1.4 SE; QQQ 0.80 / 0.76, +1.3 / +1.2 SE), but XLK earned exactly 0.30
in history (1995-2008, the dot-com cycle) and is average over 100 years. Others within
2 SE: Treasuries -0.2 in test (2022; the carry half of their prior already cuts them), DBC
-0.39 in train, UUP -0.51 in history (DXY without carry, the 2002-08 dollar fall).
The book is very sensitive to the nudge: XLK at 0.35 takes its weight from 1% / 13% (train /
test) to 21% / 21%. Every nudge raises train and test Sharpe (saved 1.41 / 1.47; XLK 0.35
1.47 / 1.51; XLK+QQQ 0.35 1.50 / 1.52; XLK+QQQ 0.40 1.54 / 1.58), but that is the years
tech beat, so partly hindsight. The history check is the case of being wrong: every nudge
loses Sharpe there (1.20 -> 1.12-1.14) and deepens the 2000-01 drawdown (-21.2% -> -22.9% to
-25.6%). By the rule, XLK 0.35 (-23.2%) and XLK+QQQ 0.35 (-22.9%, XLK 15% + QQQ 14% in test)
pass; 0.40 and 0.50 fail (3.3-4.4 points deeper). Not adopted here: a portfolio decision.
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.signals import CarrySharpePrior, Identity
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

PERIODS = {"history": (None, "2008-09-09"), "train": ("2008-09-10", "2017-07-25"),
           "test": ("2017-07-26", "2026-10-30"), "production": ("2026-11-01", None)}
VARIANTS = {"saved": {}, "XLK 0.35": {"XLK": 0.35}, "XLK 0.40": {"XLK": 0.40},
            "XLK 0.50": {"XLK": 0.50}, "XLK+QQQ 0.35": {"XLK": 0.35, "QQQ": 0.35},
            "XLK+QQQ 0.40": {"XLK": 0.40, "QQQ": 0.40}}
LOADERS = {"actual": core.load, "proxy": core_proxies.load}


def alphas(nudge: dict[str, float]) -> dict:
    prior = CarrySharpePrior({**core.PRIOR_SR, **nudge}, carry_vol=core.CARRY_VOL,
                             carry_weight=0.5, normalizer=Identity())
    return {**core.ALPHAS, "prior": (prior, 1.0)}


def run(variant: str, data: str):
    b = LOADERS[data]()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(
        b, core.build(alphas=alphas(VARIANTS[variant])))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return variant, data, r, res.weights.reindex(r.index).fillna(0.0)


def stats(r: pd.Series) -> dict:
    nav = (1 + r).cumprod()
    ann = np.sqrt(TRADING_DAYS)
    return {"sharpe": r.mean() / r.std() * ann, "ret": r.mean() * TRADING_DAYS,
            "vol": r.std() * ann, "dd": (nav / nav.cummax() - 1).min()}


def book_table() -> pd.DataFrame:
    with ProcessPoolExecutor(4) as pool:
        out = list(pool.map(run, *zip(*[(v, d) for v in VARIANTS for d in LOADERS])))
    rows = []
    for variant, data, r, w in out:
        for p, (a, z) in PERIODS.items():
            if (p == "history") != (data == "proxy") or len(r.loc[a:z]) < 21:
                continue
            ww = w.loc[a:z]
            rows.append({"variant": variant, "period": p, **stats(r.loc[a:z]),
                         "XLK": ww["XLK"].mean(), "QQQ": ww["QQQ"].mean()})
    return pd.DataFrame(rows).set_index(["period", "variant"]).sort_index()


def prior_table() -> pd.DataFrame:
    """Realized excess Sharpe per ETF and period against its prior, in standard errors."""
    rows = []
    for data, periods in (("proxy", ["history"]), ("actual", ["train", "test"])):
        px = LOADERS[data]().prices
        ex = px[core.RISKY].pct_change(fill_method=None).sub(
            px[core.CASH].pct_change(fill_method=None), axis=0)
        for p in periods:
            a, z = PERIODS[p]
            e = ex.loc[a:z]
            for t in core.RISKY:
                s = e[t].dropna()
                sr = s.mean() / s.std() * np.sqrt(TRADING_DAYS)
                se = np.sqrt((1 + sr**2 / 2) / (len(s) / TRADING_DAYS))
                rows.append({"etf": t, "period": p, "prior": core.PRIOR_SR[t], "realized": sr,
                             "gap_se": (sr - core.PRIOR_SR[t]) / se})
    return pd.DataFrame(rows).pivot(index="etf", columns="period",
                                    values=["realized", "gap_se"]).reindex(core.RISKY)


def main() -> None:
    pd.set_option("display.width", 160)
    pr = prior_table()
    pr.insert(0, "prior", pd.Series(core.PRIOR_SR))
    print("Realized excess Sharpe and gap to the prior in SEs (IEF/TLT prior is half carry):")
    print(pr.round(2).to_string())
    print("\nBook (core + overlays) by period and variant:")
    bt = book_table()
    fmt = {"sharpe": "{:.2f}", "ret": "{:.1%}", "vol": "{:.1%}", "dd": "{:.1%}",
           "XLK": "{:.1%}", "QQQ": "{:.1%}"}
    print(bt.apply(lambda c: c.map(fmt[c.name].format)).to_string())


if __name__ == "__main__":
    main()
