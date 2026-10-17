"""A broader ETF universe: 65 ETFs across nine classes against the saved 17.

The saved book's 17 ETFs carry about 5 independent bets (principal components for 80% of
weekly variance, 2012-2026); about 80 retail-tradable instruments carry about 15. The
45-ETF test in ``research_trend_global.py`` was worse than 17, because many correlated
equity markets with the same prior crowded the book. This tries breadth again with the
saved alphas and class risk limits. **Fixed before any test.**

Universe (65 ETFs plus T-bills): the 17, plus IWM and MDY; 13 developed-country ETFs; 10
emerging-country ETFs; SHY, IEI, TIP, MBB, BWX, IGOV; LQD, HYG, EMB, PCY, MUB; VNQ; SLV;
DBB, DBA, USO, UNG, UGA; FXE, FXY, FXB, FXC, FXF. Dropped: median dollar volume under $1M a
day (RWX, FXA) and 2010 launches (PPLT, PALL, EIDO, CORN, EPHE), which would push the start
to 2012.

Priors (judgment): equities 0.30 in every region; Treasuries 0.25 (IEF and TLT half way to
carry, as saved); unhedged foreign bonds (BWX, IGOV) 0.15; credit and real estate 0.25;
gold 0.20, silver 0.15; DBC 0.15, metals and agriculture 0.10, single energy ETFs 0
(contango); currencies 0.

Alphas as saved: the prior, EMA-200 trend 0.05, EWMAC 64/256 0.037 on everything; VRP 0.091
on every equity ETF. Portfolio settings as saved except the per-ETF cap, 10% of the invested
book (20% with 17). Costs charged by median dollar volume: 1 bp above $500M a day, 2 bp
above $50M, 5 bp above $10M, 10 bp below (an estimate, from the last year's volume).

Variants: the saved 17 on the same dates (its own settings: 20% cap, its costs); the 65
with no class limits; the 65 with class limits on the share of the invested book (US equity
50%, developed 25%, emerging 20%, rates 50%, credit 25%, real estate 10%, precious metals
20%, commodities 20%, currencies 20%); and that again without the currency ETFs.

**Result (2026-10-17): negative.** On 2010-08 .. 2026-10 the saved 17 made Sharpe 1.17; the
65 made 0.78 without class limits and 0.88 with them (0.87 without currency ETFs). With equal
priors the optimizer read 11 partly independent EM ETFs as a diversified sleeve and moved
21-34% of capital there (31-47% of book variance); international equity lagged the US by
the widest margin in 2014, 2020 and 2021. The currency ETFs added nothing (+0.0% a year at
a zero prior). Breadth multiplied prior bets, not alpha bets; a fair breadth test would fix
the class mix and let the alphas choose within each class, with priors set per class.

Usage:
    uv run python research/research_broad_universe.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import compare
from quantzy.signals import CarrySharpePrior, Identity, VarianceRiskPremium
from quantzy.strategies import core_multiasset as core
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

SPLIT = pd.Timestamp("2017-07-26")
CASH = core.CASH
CLASSES = {
    "US equity": [*core.CLASSES["US sectors"], "QQQ", "IWM", "MDY"],
    "developed equity": ["EFA", "EWJ", "EWG", "EWU", "EWC", "EWA", "EWL", "EWQ", "EWI", "EWP",
                         "EWN", "EWD", "EWH", "EWS"],
    "emerging equity": ["EEM", "EWT", "EWY", "EWZ", "EWW", "FXI", "EWM", "THD", "TUR", "EZA",
                        "ECH"],
    "rates": ["SHY", "IEI", "IEF", "TLT", "TIP", "MBB", "BWX", "IGOV"],
    "credit": ["LQD", "HYG", "EMB", "PCY", "MUB"],
    "real estate": ["VNQ"],
    "precious metals": ["GLD", "SLV"],
    "commodities": ["DBC", "DBB", "DBA", "USO", "UNG", "UGA"],
    "currencies": ["UUP", "FXE", "FXY", "FXB", "FXC", "FXF"],
}
CLASS_CAP = {"US equity": 0.50, "developed equity": 0.25, "emerging equity": 0.20,
             "rates": 0.50, "credit": 0.25, "real estate": 0.10, "precious metals": 0.20,
             "commodities": 0.20, "currencies": 0.20}
PRIOR = {**{t: 0.30 for c in ("US equity", "developed equity", "emerging equity")
            for t in CLASSES[c]},
         **{t: 0.25 for t in CLASSES["rates"]}, "BWX": 0.15, "IGOV": 0.15,
         **{t: 0.25 for t in CLASSES["credit"]}, "VNQ": 0.25, "GLD": 0.20, "SLV": 0.15,
         "DBC": 0.15, "DBB": 0.10, "DBA": 0.10, "USO": 0.0, "UNG": 0.0, "UGA": 0.0,
         **{t: 0.0 for t in CLASSES["currencies"]}}
EQUITY = [t for c in ("US equity", "developed equity", "emerging equity") for t in CLASSES[c]]
ALL = [t for ts in CLASSES.values() for t in ts]
assert len(ALL) == 65 and set(core.RISKY) <= set(ALL)


def load() -> DataBundle:
    yields = list(core.CARRY_YIELDS.values())
    raw = CachedReader(YFinanceReader()).history([*ALL, "^IRX", "^VIX", "SPY", *yields],
                                                 start=core.START)
    yld = raw["^IRX"].ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    prices = raw[ALL].copy()
    prices[CASH] = (1.0 + cash_ret).cumprod()

    def wide(s: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({t: s for t in prices.columns}, index=prices.index)

    carry = pd.DataFrame(index=prices.index, columns=prices.columns, dtype=float)
    for t, y in core.CARRY_YIELDS.items():
        carry[t] = (raw[y].ffill() - raw["^IRX"].ffill()) / 100.0
    return DataBundle.build(prices, vix=wide(raw["^VIX"]), market=wide(raw["SPY"]), carry=carry)


def cost_tiers() -> dict[str, float]:
    import yfinance as yf

    d = yf.download(ALL, period="1y", auto_adjust=True, progress=False)
    adv = (d["Close"] * d["Volume"]).median()
    return {t: (1.0 if adv[t] >= 5e8 else 2.0 if adv[t] >= 5e7 else 5.0 if adv[t] >= 1e7
                else 10.0) for t in ALL}


def class_caps(caps: dict[str, float], classes: dict[str, list[str]]):
    def _build(w, ctx):
        risky = [i for i, t in enumerate(ctx.tickers) if t != CASH]
        book = ctx.cp.sum(w[risky])
        out = []
        for c, lim in caps.items():
            idx = [i for i, t in enumerate(ctx.tickers) if t in classes[c]]
            if idx:
                out.append(ctx.cp.sum(w[idx]) <= lim * book)
        return out
    return _build


def strategy(tickers: list[str], caps: bool, costs: dict[str, float]) -> Strategy:
    prior = {t: PRIOR[t] for t in tickers}
    alphas = {
        "prior": (CarrySharpePrior(prior, carry_vol=core.CARRY_VOL, carry_weight=0.5,
                                   normalizer=Identity()), 1.0),
        "trend": core.ALPHAS["trend"],
        "ewmac": core.ALPHAS["ewmac"],
        "vrp": (VarianceRiskPremium(tuple(t for t in EQUITY if t in tickers),
                                    normalizer=Identity()), core.ALPHAS["vrp"][1]),
    }
    p = replace(core.PORTFOLIO, max_position=0.10,
                execution_cost_bps={t: costs[t] for t in tickers})
    opt = p.optimizer(CASH)
    if caps:
        opt = type(opt)(risk_aversion=opt.risk_aversion,
                        constraints=[*opt._constraints, class_caps(CLASS_CAP, CLASSES)],
                        cost_model=opt.cost_model, cost_aversion=opt.cost_aversion,
                        execution_cost=opt.execution_cost)
    uni = Universe(name="broad", assets=[Asset(t, t, "etf") for t in tickers]
                   + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    return Strategy(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=opt, name="broad")


def warmup(bundle: DataBundle, s: Strategy, tickers: list[str]) -> int:
    first = bundle.prices[tickers].apply(lambda x: bundle.index.get_loc(x.first_valid_index()))
    per_asset = max([s.risk_model.min_history]
                    + [getattr(g, "asset_history", g.min_history) + g.lag + 1
                       for g in s.signals.values()])
    return int(max(first.max() + per_asset, s.min_history))


def run_one(args: tuple[str, list[str], bool, dict[str, float], int]):
    name, tickers, caps, costs, start = args
    full = load()
    px = full.prices[[*tickers, CASH]]
    bundle = DataBundle.build(px, **{k: full[k][px.columns] for k in ("vix", "market", "carry")})
    s = core.build() if name == "saved 17" else strategy(tickers, caps, costs)
    res = WalkForwardBacktester(rebalance_every=1, warmup=max(start, warmup(bundle, s, tickers))
                                ).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    cost = sum(d.trades.cost or 0.0 for d in res.decisions) / (len(r) / TRADING_DAYS)
    years = len(r) / TRADING_DAYS
    return name, r, res.weights, cost, sum(d.turnover for d in res.decisions) / years


def main(workers: int) -> None:
    pd.set_option("display.width", 320)
    costs = cost_tiers()
    print("cost tiers (bp): " + ", ".join(f"{b:g}: {sorted(t for t in ALL if costs[t] == b)}"
                                          for b in (1, 2, 5, 10)))
    full = load()
    start = warmup(full, strategy(ALL, False, costs), ALL)
    print(f"common start: {full.index[start].date()}")
    no_fx = [t for t in ALL if t not in CLASSES["currencies"]]
    jobs = [("saved 17", list(core.RISKY), False, costs, start),
            ("65, no class limits", ALL, False, costs, start),
            ("65, class limits", ALL, True, costs, start),
            ("59 (no currency ETFs), class limits", no_fx, True, costs, start)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    s0 = max(out[n][0].index[0] for n in names)
    runs = {n: out[n][0].loc[s0:] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][3] for n in names]
    m["cost/yr"] = [out[n][2] for n in names]
    m["gross"] = [out[n][1].drop(columns=CASH).loc[s0:].sum(axis=1).mean() for n in names]
    m["held >1%"] = [(out[n][1].drop(columns=CASH).loc[s0:] > 0.01).sum(axis=1).mean()
                     for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    pct = "{:.1%}".format
    print(f"\nBOOK, {s0.date()} .. {runs[names[0]].index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd")}, "cost/yr": "{:.2%}".format,
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "diff t", "gross")},
        "turnover": "{:.1f}".format, "held >1%": "{:.0f}".format}))

    cls = {t: c for c, ts in CLASSES.items() for t in ts}
    px = full.prices
    for label, fn in (("average weight, % of capital", "w"),
                      ("P&L contribution, % a year", "pnl"),
                      ("share of book variance", "risk")):
        rows = {}
        for n in names:
            w = out[n][1].drop(columns=CASH).loc[s0:]
            if fn == "w":
                rows[n] = w.T.groupby(cls).sum().T.mean()
                continue
            ar = px[w.columns].pct_change(fill_method=None).reindex(w.index).fillna(0)
            contrib = (w.shift(1).fillna(0) * ar).T.groupby(cls).sum().T
            if fn == "pnl":
                rows[n] = contrib.mean() * TRADING_DAYS
            else:
                tot = contrib.sum(axis=1)
                rows[n] = contrib.apply(lambda c, tot=tot: c.cov(tot)) / tot.var()
        print(f"\n   {label}, by class")
        print(pd.DataFrame(rows).T[list(CLASSES)].to_string(
            float_format=(lambda v: f"{v:+.1%}") if fn != "w" else "{:.0%}".format))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved 17 (percentage points)")
    print(yr[names[1:]].sub(yr[names[0]], axis=0).to_string(
        float_format=lambda v: f"{v * 100:+.1f}"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
