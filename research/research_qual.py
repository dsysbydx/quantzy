"""A quality tilt: QUAL (iShares MSCI USA Quality Factor) as an equity ETF in the saved book.

QUAL holds ~125 large and mid-cap US stocks ranked on return on equity, earnings variability
and debt to equity. The quality premium (Asness, Frazzini and Pedersen, quality minus junk)
is documented long/short; long-only, QUAL is mostly US equity beta. **Fixed before any test.**

* Asset: QUAL (from 2013-07-18), its own class, prior Sharpe 0.30 (the equity prior); 0.35
  as a sensitivity check for a small quality premium on top.
* Treated like the other equity ETFs: trend, EWMAC and VRP on it. Charged 2 bp a trade, like
  the sector SPDRs. Saved ``PORTFOLIO`` unchanged; both overlays on in every run.
* Scored from the first day QUAL has history for every signal, against the saved book on
  the same dates; split at the midpoint.

Adopt if the 0.30 prior lifts Sharpe by at least 0.02 over the window and in both halves
without a deeper drawdown.

**Result (2026-10-26): not adopted.** Over 2015-01-29 .. 2026-10-16 (split 2020-12-03) QUAL at
the equity prior 0.30 never enters: weight 0 on every day, so the book is unchanged (Sharpe
1.12, train 1.05 / test 1.18). QUAL is 0.68 correlated with the book and is spanned by the
sector SPDRs it already holds (77% of NAV); at the same prior, its trend and VRP are no
different from theirs. At 0.35 it takes 13.5% of NAV (at the cap 4% of days), mostly in
place of sectors, and Sharpe slips to 1.11 (1.04 / 1.17). A quality tilt would need a
quality signal, not another equity ETF.

Usage:
    uv run python research/research_qual.py
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
from quantzy.signals import CarrySharpePrior, Identity, VarianceRiskPremium
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme
from quantzy.strategies import vix_hedge
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

MF = "QUAL"
PRIOR = 0.30
SENSITIVITY = (0.35,)
COST_BPS = 2.0
CLASS = "US quality"
PANELS = ("vix", "market", "carry", "dollar_value", "vixy")


def load() -> DataBundle:
    b = latest_complete(core.load(), core.RISKY)
    raw = CachedReader(YFinanceReader()).history([MF], start="2013-01-01")
    px = b.prices.copy()
    px.insert(len(core.RISKY), MF, raw[MF].reindex(b.index))
    panels = {k: b[k].reindex(columns=px.columns) for k in PANELS}
    for k in ("vix", "market", "vixy"):  # market-wide panels: the same in every column
        panels[k][MF] = panels[k].iloc[:, 0]
    return DataBundle.build(px, **panels)


def strategy(prior: float | None) -> Strategy:
    """The saved strategy, plus QUAL at ``prior`` (``None`` leaves it out)."""
    tickers = [*core.RISKY, *([MF] if prior is not None else [])]
    sr = {**core.PRIOR_SR, MF: prior}
    equity = (*core.EQUITY, *([MF] if prior is not None else []))
    alphas = {
        **core.ALPHAS,
        "prior": (CarrySharpePrior({t: sr[t] for t in tickers}, carry_vol=core.CARRY_VOL,
                                   carry_weight=0.5, normalizer=Identity()), 1.0),
        "vrp": (VarianceRiskPremium(equity, normalizer=Identity()), core.ALPHAS["vrp"][1]),
    }
    p = replace(core.PORTFOLIO, execution_cost_bps={**core.EXECUTION_COST_BPS, MF: COST_BPS})
    uni = Universe(name="core_qual", assets=[Asset(t, t, "etf") for t in tickers]
                   + [Asset(core.CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    return Strategy(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=p.optimizer(core.CASH), name=uni.name)


def per_asset(s: Strategy) -> int:
    """Rows a new ETF needs before every signal and the risk model trust it (``core.warmup``)."""
    return max([s.risk_model.min_history] + [getattr(g, "asset_history", g.min_history) + g.lag
                                             + 1 for g in s.signals.values()])


def main() -> None:
    pd.set_option("display.width", 300)
    b = load()
    first = b.index.get_loc(b.prices[MF].first_valid_index())
    start = max(first + per_asset(strategy(PRIOR)), core.warmup(b))
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
    by_class = {c: ts for c, ts in core.CLASSES.items()} | {CLASS: [MF]}
    shift = pd.DataFrame({
        "saved": {c: w0.reindex(columns=cols, fill_value=0)[ts].sum(axis=1).mean()
                  for c, ts in by_class.items()},
        main_: {c: w1[ts].sum(axis=1).mean() for c, ts in by_class.items()},
    })
    print("\n   average weight by class (% of NAV): what QUAL displaces")
    print(shift.to_string(float_format="{:.1%}".format))
    diff = runs[main_] - runs["saved book"]
    corr = b.prices[MF].pct_change(fill_method=None).reindex(idx).corr(runs["saved book"])
    print(f"\n   difference ({main_} minus saved): {diff.mean() * TRADING_DAYS:+.2%} a year, "
          f"corr of QUAL with the saved book {corr:+.2f}, "
          f"tracking error {diff.std() * np.sqrt(TRADING_DAYS):.2%}")


if __name__ == "__main__":
    main()
