"""Bitcoin as a trend-only asset: IBIT at a zero Sharpe prior in the saved book.

Bitcoin pays no cash flow, so under the book's priors it has no expected return of its own
(like the dollar). At a zero prior it can only enter through the trend alphas. Unlike the
zero-prior ETFs of ``research_trend_sleeve_broad.py`` it can clear the no-trade band: at
about 63% vol a full trend score is worth up to ~7-8% a year of ``mu``, against a ~2.4%
hurdle. **Fixed before any test.**

* Asset: IBIT (spot ETF, from 2024-01-11), with BTC-USD before it (its close on each ETF
  trading day, so weekend moves land on Monday) less a 0.25% a year fee, chain-linked.
* Prior 0; trend and EWMAC as on every asset; no VRP; charged 2 bp a trade; the saved caps,
  settings and Treasury month-end overlay.
* Yahoo's BTC-USD close is 00:00 UTC, about four hours after the US close: treated as noise.

Tests: time-series IC of the trend scores on the Bitcoin series (1-day held-out 47-day
folds, 21 days); then the saved book with and without IBIT from the first date both can
trade (about 2016-03): Sharpe in both halves, drawdown, 2018 and 2022 (crypto drawdowns),
IBIT's weight and P&L.

**Result (2026-10-18): not added.** EMA-200 trend on Bitcoin has a 1-day IC of +0.051
(t 2.3), but at 21 days it was +0.20 to 2020 and +0.02 since 2021 (EWMAC -0.11). In the book
(2016-03 .. 2026-10) IBIT at a zero prior lifted Sharpe from 1.16 to 1.24 (+1.1% a year,
t 3.0) at an average 0.7% weight (max 6.3%) and no drawdown cost, but about 70% of the gain
is 2017 (Bitcoin +1,421%) and the test half barely moves (1.03 -> 1.05).

Usage:
    uv run python research/research_bitcoin.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import CarrySharpePrior, Identity
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

BTC = "IBIT"
FEE = 0.0025
SPLIT = pd.Timestamp("2021-06-01")  # the midpoint of 2016-03 .. 2026-10


def load() -> DataBundle:
    b = latest_complete(core.load(), core.RISKY)
    raw = CachedReader(YFinanceReader()).history(["BTC-USD", "IBIT"], start="2014-01-01")
    btc = raw["BTC-USD"].reindex(b.index).ffill()
    r = btc.pct_change(fill_method=None) - FEE / TRADING_DAYS
    ibit = raw["IBIT"].reindex(b.index).pct_change(fill_method=None)
    live = ibit.notna()
    r = r.where(~live, ibit)
    first = btc.first_valid_index()
    price = (1 + r.loc[first:].fillna(0)).cumprod().reindex(b.index)
    px = b.prices.copy()
    px.insert(len(core.RISKY), BTC, price)
    panels = {k: b[k].reindex(columns=px.columns) for k in ("vix", "market", "carry")}
    for k in ("vix", "market"):
        panels[k][BTC] = panels[k].iloc[:, 0]
    return DataBundle.build(px, **panels)


def strategy(with_btc: bool) -> Strategy:
    tickers = [*core.RISKY, *([BTC] if with_btc else [])]
    prior = {**core.PRIOR_SR, BTC: 0.0}
    alphas = {**core.ALPHAS, "prior": (CarrySharpePrior({t: prior[t] for t in tickers},
                                                        carry_vol=core.CARRY_VOL,
                                                        carry_weight=0.5,
                                                        normalizer=Identity()), 1.0)}
    p = core.PORTFOLIO
    if with_btc:
        from dataclasses import replace
        p = replace(p, execution_cost_bps={**core.EXECUTION_COST_BPS, BTC: 2.0})
    uni = Universe(name="core_btc", assets=[Asset(t, t, "etf") for t in tickers]
                   + [Asset(core.CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    return Strategy(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=p.optimizer(core.CASH), name="core_btc")


def ic_report(b: DataBundle) -> None:
    px = b.prices[[BTC]]
    vol = realized_vol(px, 60, annualize=True)
    folds = chunk_folds(b.index, 47)
    a, bb = folds == 0, folds == 1
    rows = {}
    for name in ("trend", "ewmac"):
        sig = core.ALPHAS[name][0]
        s = (sig.raw(DataBundle.build(px)) / vol).shift(1)

        def ic(h, fit=None, s=s):
            zx, zy = _ts_pairs(s, px, h, "spearman", 60, fit=fit)
            return (zx * zy).mean(axis=1)

        held = pd.concat([ic(1, a)[bb], ic(1, bb)[a]]).dropna()
        x21 = ic(21).dropna()
        rows[name] = {"1d held out": f"{held.mean():+.4f} ({newey_west_t(held, 21):+.1f})",
                      "21d": f"{x21.mean():+.3f} ({newey_west_t(x21, 42):+.1f})",
                      "21d to 2020": f"{x21.loc[:'2020'].mean():+.3f}",
                      "21d 2021+": f"{x21.loc['2021':].mean():+.3f}"}
    print("1. TREND IC ON BITCOIN (time-series rank IC vs forward return / 60d vol, Newey-West t)")
    print(pd.DataFrame(rows).T.to_string())


def main() -> None:
    pd.set_option("display.width", 300)
    b = load()
    ic_report(b)
    s_btc = strategy(True)
    first = b.prices[BTC].first_valid_index()
    per_asset = max([s_btc.risk_model.min_history]
                    + [getattr(g, "asset_history", g.min_history) + g.lag + 1
                       for g in s_btc.signals.values()])
    start = max(b.index.get_loc(first) + per_asset, core.warmup(b))
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    overlay = tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None), cash)
    out = {}
    for name, with_btc in (("saved book (+ overlay)", False), ("+ IBIT at prior 0", True)):
        s = strategy(with_btc)
        bundle = b if with_btc else DataBundle.build(
            b.prices.drop(columns=BTC), **{k: b[k].drop(columns=BTC)
                                           for k in ("vix", "market", "carry")})
        res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(bundle, s)
        r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
        r = r + overlay.reindex(r.index).fillna(0)
        out[name] = (r, res.weights, sum(d.turnover for d in res.decisions)
                     / (len(r) / TRADING_DAYS))
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[list(out)]
    m["turnover"] = [v[2] for v in out.values()]
    for y in ("2018", "2021", "2022"):
        m[y] = [(1 + runs[n].loc[y]).prod() - 1 for n in out]
    d = runs["+ IBIT at prior 0"] - runs["saved book (+ overlay)"]
    print(f"\n2. BOOK, {b.index[start].date()} .. {b.index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={**{c: "{:.1%}".format for c in ("ret", "vol", "max_dd",
                                                                  "2018", "2021", "2022")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe")},
                                  "turnover": "{:.1f}".format}))
    print(f"   difference: {d.mean() * TRADING_DAYS:+.2%} a year, t "
          f"{d.mean() / d.std() * np.sqrt(len(d)):+.2f}")
    w = out["+ IBIT at prior 0"][1][BTC]
    ret_btc = b.prices[BTC].pct_change(fill_method=None).reindex(w.index).fillna(0)
    pnl = (w.shift(1).fillna(0) * ret_btc)
    print(f"   IBIT weight: average {w.mean():.1%}, max {w.max():.1%}, held (>0.5%) on "
          f"{(w > 0.005).mean():.0%} of days; P&L {pnl.mean() * TRADING_DAYS:+.2%} a year")
    by_year = pd.DataFrame({"avg weight": w.groupby(w.index.year).mean(),
                            "P&L (pp)": pnl.groupby(pnl.index.year).sum() * 100,
                            "Bitcoin return": (1 + ret_btc).groupby(ret_btc.index.year).prod() - 1})
    print(by_year.to_string(formatters={"avg weight": "{:.1%}".format,
                                        "P&L (pp)": "{:+.1f}".format,
                                        "Bitcoin return": "{:+.0%}".format}))


if __name__ == "__main__":
    main()
