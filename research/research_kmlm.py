"""Managed futures as a tradeable asset: KMLM in the saved book at a fixed Sharpe prior.

The managed-futures ETFs were checked against the book first (weekly returns): KMLM +0.03
with the book, +0.38 with our long-only trend sleeve, +0.31 with the 45-ETF long/short one;
DBMF 0.54-0.62 with our trend; CTA ~0 but only from 2022. In Jan-Oct 2022 KMLM made +51%
while the book lost 9%. KMLM (the KFA MLM Index: trend on ~22 commodity, currency and bond
futures, no equities) is the cleanest diversifier, so it goes into the optimizer like any
other ETF rather than as another overlay. **Fixed before any test.**

* Asset: KMLM (from 2020-12-03), its own class "managed futures", prior Sharpe 0.2 on its
  excess return (its total return already includes T-bill interest on the collateral). Long
  run, trend earned ~0.7 gross (Hurst, Ooi and Pedersen) and the CTA industry ~0.3-0.4 net
  since 2000; less the 0.9% fee and decay, and below the equity prior (0.30).
* Excluded from ``trend`` and ``ewmac`` (trend on a trend fund counts the signal twice) and
  from VRP (equities only). Charged 5 bp a trade, like DBC and UUP. Saved ``PORTFOLIO``
  unchanged: 20% cap, 1.3x leverage, lambda 1.5. Both overlays on in every run.
* Scored from the first day KMLM has the risk model's history (252 rows, ~2021-12), against
  the saved book on the same dates; split at the midpoint. Priors 0.1 / 0.15 / 0.25 as a
  sensitivity check only.

Adopt if the 0.2 prior lifts Sharpe by at least 0.02 over the window and in both halves
without a deeper drawdown. Five years, one of them 2022, cannot show much more than that.

**Result (2026-10-25): not adopted.** Over 2021-12-06 .. 2026-10-16 the 0.2 prior lowers
Sharpe 1.07 -> 0.94 (train 0.52 -> 0.34, test 1.69 -> 1.58), CAGR 13.6% -> 11.3%, max
drawdown -16.0% -> -16.9%; every prior from 0.1 to 0.25 is worse than the saved book, more so
the larger. KMLM averages 15% of NAV (it is uncorrelated, so a small prior buys a large
weight, near the 20%-of-book cap) and displaces mostly UUP, then sectors. The timing was
wrong at both ends: the optimizer built up only to 6% in Q1 2022, missing most of KMLM's
2022 run, held ~25% through its losing 2023-25 (-1.7% of NAV over cash), then cut it to ~0
from Q3 2025, before its +19% in 2026. With a fixed prior the weight follows the 5/20/60-day
covariance, so for a low-Sharpe diversifier it swings between zero and the cap. The earlier
fixed-share blend (book plus 20% KMLM from cash, Dec 2020 on) had looked better, 1.18 ->
1.22, largely from holding KMLM in early 2022.

**Fixed share, checked after (2026-10-25), not adopted either.** The book plus KMLM at a
constant share of NAV, borrowed at T-bill + 50 bp (the book is at its 1.3x cap), 2020-12 ..
2026-10. At 20%: Sharpe 1.185 -> 1.223 and max drawdown -17.2% -> -14.7%, but without 2022
1.614 -> 1.566, and from 2023 1.512 -> 1.431 with a slightly deeper drawdown; lower at every
size from 10% to 30% outside 2022. KMLM's excess over cash was +21% in 2022 and averaged
about +0.5% a year in the other years: +5 to +16% in 2020-21 and 2026, -7 to -11% a year in
2023-25. Correlation with the book by year drifted from -0.19 to +0.20. Insurance for a
2022-style year, near-zero return otherwise; only a longer history (the KFA MLM Index from
1988) could tell a usual trend drawdown from decay. Idea closed.

Usage:
    uv run python research/research_kmlm.py
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.signals import CarrySharpePrior, EwmacTrend, Identity, VolScaledTrend
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme
from quantzy.strategies import vix_hedge
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

MF = "KMLM"
PRIOR = 0.2
SENSITIVITY = (0.1, 0.15, 0.25)
COST_BPS = 5.0
PANELS = ("vix", "market", "carry", "dollar_value", "vixy")


def load() -> DataBundle:
    b = latest_complete(core.load(), core.RISKY)
    raw = CachedReader(YFinanceReader()).history([MF], start="2020-01-01")
    px = b.prices.copy()
    px.insert(len(core.RISKY), MF, raw[MF].reindex(b.index))
    panels = {k: b[k].reindex(columns=px.columns) for k in PANELS}
    for k in ("vix", "market", "vixy"):  # market-wide panels: the same in every column
        panels[k][MF] = panels[k].iloc[:, 0]
    return DataBundle.build(px, **panels)


def strategy(prior: float | None) -> Strategy:
    """The saved strategy, plus KMLM at ``prior`` (``None`` leaves it out)."""
    tickers = [*core.RISKY, *([MF] if prior is not None else [])]
    sr = {**core.PRIOR_SR, MF: prior}
    excl = (core.CASH, MF)
    alphas = {
        **core.ALPHAS,
        "prior": (CarrySharpePrior({t: sr[t] for t in tickers}, carry_vol=core.CARRY_VOL,
                                   carry_weight=0.5, normalizer=Identity()), 1.0),
        "trend": (VolScaledTrend(200, exclude=excl, ma="ema", ewm_tol=core.EWM_TOL,
                                 normalizer=Identity()), core.ALPHAS["trend"][1]),
        "ewmac": (EwmacTrend(64, exclude=excl, ewm_tol=core.EWM_TOL, normalizer=Identity()),
                  core.ALPHAS["ewmac"][1]),
    }
    p = replace(core.PORTFOLIO, execution_cost_bps={**core.EXECUTION_COST_BPS, MF: COST_BPS})
    uni = Universe(name="core_kmlm", assets=[Asset(t, t, "etf") for t in tickers]
                   + [Asset(core.CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    return Strategy(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=p.optimizer(core.CASH), name=uni.name)


def main() -> None:
    pd.set_option("display.width", 300)
    b = load()
    first = b.index.get_loc(b.prices[MF].first_valid_index())
    start = max(first + strategy(PRIOR).risk_model.min_history + 2, core.warmup(b))
    base_bundle = DataBundle.build(b.prices.drop(columns=MF),
                                   **{k: b[k].drop(columns=MF) for k in PANELS})
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    overlays = (tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None), cash)
                + vix_hedge.backtest_returns(b))

    out = {}
    for prior in (None, PRIOR, *SENSITIVITY):
        name = "saved book" if prior is None else f"+ {MF} at prior {prior:g}"
        res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(
            base_bundle if prior is None else b, strategy(prior))
        r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
        r = r + overlays.reindex(r.index).fillna(0)
        out[name] = (r, res.weights,
                     sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS))

    runs = {n: v[0] for n, v in out.items()}
    idx = runs["saved book"].index
    split = idx[len(idx) // 2]
    t = compare(runs, split_date=split)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[list(runs)]
    m["calmar"] = m["ret"] / -m["max_dd"]
    m["turnover"] = [v[2] for v in out.values()]
    m[f"{MF} wt"] = [v[1][MF].mean() if MF in v[1] else 0.0 for v in out.values()]
    m["at cap %"] = [(v[1][MF] >= core.PORTFOLIO.max_position * v[1].drop(
        columns=core.CASH).sum(axis=1) - 1e-3).mean() if MF in v[1] else 0.0
        for v in out.values()]
    m["gross"] = [v[1].drop(columns=core.CASH).sum(axis=1).mean() for v in out.values()]
    pct = "{:.1%}".format
    print(f"\n{MF} IN THE SAVED BOOK, {idx[0].date()} .. {idx[-1].date()}, split {split.date()}"
          f" | saved PORTFOLIO, both overlays")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", f"{MF} wt",
                                                   "at cap %")}
                      | {c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe",
                                                      "calmar", "gross")}
                      | {"turnover": "{:.1f}".format}))

    main_ = f"+ {MF} at prior {PRIOR:g}"
    yr = pd.DataFrame({n: (1 + runs[n]).groupby(runs[n].index.year).prod() - 1
                       for n in ("saved book", main_)})
    print("\n   calendar years")
    print(yr.to_string(float_format="{:+.1%}".format))

    w0, w1 = out["saved book"][1], out[main_][1]
    cols = [*core.RISKY, MF]
    by_class = {c: ts for c, ts in core.CLASSES.items()} | {"managed futures": [MF]}
    shift = pd.DataFrame({
        "saved": {c: w0.reindex(columns=cols, fill_value=0)[ts].sum(axis=1).mean()
                  for c, ts in by_class.items()},
        main_: {c: w1[ts].sum(axis=1).mean() for c, ts in by_class.items()},
    })
    print("\n   average weight by class (% of NAV): what KMLM displaces")
    print(shift.to_string(float_format="{:.1%}".format))
    diff = runs[main_] - runs["saved book"]
    print(f"\n   difference ({main_} minus saved): {diff.mean() * TRADING_DAYS:+.2%} a year, "
          f"corr of KMLM with the saved book {b.prices[MF].pct_change(fill_method=None).reindex(idx).corr(runs['saved book']):+.2f}, "
          f"tracking error {diff.std() * np.sqrt(TRADING_DAYS):.2%}")


if __name__ == "__main__":
    main()
