"""More commodities in the book, and momentum around scheduled commodity announcements.

The saved book holds two commodity ETFs, GLD (prior 0.20) and DBC (0.15). Single-commodity
futures have no reliable premium after roll costs, so candidates enter at a zero prior: only
the trend alphas can bring them in. **Fixed before any test.**

1. *Candidates* (miner equities excluded): SLV, PPLT, PALL (precious); CPER, DBB (industrial
   metals); USO, BNO, UNG, UGA (energy); DBA, CORN, WEAT, SOYB (agriculture). Each added to
   the saved 17 alone, then the five metals together, then all 13; prior 0, trend and EWMAC
   as on every asset, no VRP; charged 2, 5 or 10 bp a trade by dollar volume (above $50M,
   $10-50M, below $10M); ``PORTFOLIO`` unchanged; each compared with the saved book over the
   same dates (from the candidate's start plus warmup).
2. *Announcement days* (dates approximate, from weekday rules): USO on the EIA weekly
   petroleum report (Wednesday; Thursday after a Monday holiday); UNG on the EIA weekly
   storage report (Thursday; the trading day before if Thursday is a holiday); GLD and SLV
   on scheduled FOMC decisions (the Federal Reserve's calendars) and on payrolls (the first
   Friday of the month). Two hypotheses, each against all other days: (H1) the EMA-200 trend
   score predicts the event-day return better; (H2) the event-day return, over daily vol,
   predicts the next five days (continuation). WASDE (agriculture) is left out: no dates.

**Result (2026-10-18): negative.** At a zero prior none of the 13 enters for long (average
weights 0-1%); every incremental change is within noise (|t| < 1.7; best USO +0.11% a year,
SLV +0.04%), and all 13 together lower Sharpe (1.095 -> 1.083) and deepen the drawdown.
Announcement days move 10-23% more than other days but are not more predictable: trend's IC
on event days is no higher (for gold and silver on payroll days it is lower) and event-day
moves do not continue (no |t| above 1.2).

Usage:
    uv run python research/research_commodities.py [--workers 5]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd
from research_calendar_flows import fomc_dates

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare, newey_west_t
from quantzy.signals import CarrySharpePrior, Identity
from quantzy.strategies import core_multiasset as core
from quantzy.strategies.treasury_month_end import NYSEHolidays
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

GROUPS = {"precious": ["SLV", "PPLT", "PALL"], "industrial metals": ["CPER", "DBB"],
          "energy": ["USO", "BNO", "UNG", "UGA"], "agriculture": ["DBA", "CORN", "WEAT", "SOYB"]}
CANDIDATES = [t for ts in GROUPS.values() for t in ts]
METALS = [*GROUPS["precious"], *GROUPS["industrial metals"]]
SPLIT = pd.Timestamp("2017-07-26")
ANN = np.sqrt(TRADING_DAYS)


def load() -> DataBundle:
    b = latest_complete(core.load(), core.RISKY)
    raw = CachedReader(YFinanceReader()).history(CANDIDATES, start=core.START)
    px = b.prices.join(raw.reindex(b.index))
    px = px[[*core.RISKY, *CANDIDATES, core.CASH]]
    panels = {k: b[k].reindex(columns=px.columns) for k in ("vix", "market", "carry")}
    for k in ("vix", "market"):
        panels[k] = panels[k].apply(lambda c, k=k: b[k].iloc[:, 0])
    return DataBundle.build(px, **panels)


def costs_bps() -> dict[str, float]:
    import yfinance as yf

    d = yf.download(CANDIDATES, period="1y", auto_adjust=True, progress=False)
    adv = (d["Close"] * d["Volume"]).median()
    return {t: 2.0 if adv[t] >= 5e7 else 5.0 if adv[t] >= 1e7 else 10.0 for t in CANDIDATES}


def strategy(extra: list[str], costs: dict[str, float]) -> Strategy:
    tickers = [*core.RISKY, *extra]
    prior = {**core.PRIOR_SR, **{t: 0.0 for t in extra}}
    alphas = {**core.ALPHAS, "prior": (CarrySharpePrior({t: prior[t] for t in tickers},
                                                        carry_vol=core.CARRY_VOL,
                                                        carry_weight=0.5,
                                                        normalizer=Identity()), 1.0)}
    p = replace(core.PORTFOLIO, execution_cost_bps={**core.EXECUTION_COST_BPS,
                                                     **{t: costs[t] for t in extra}})
    uni = Universe(name="core_plus", assets=[Asset(t, t, "etf") for t in tickers]
                   + [Asset(core.CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")])
    return Strategy(universe=uni, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: w for n, (_, w) in alphas.items()},
                    risk_model=p.risk_model(), optimizer=p.optimizer(core.CASH), name="core_plus")


def run_one(args: tuple[str, list[str], dict[str, float]]):
    name, extra, costs = args
    b = load()
    cols = [*core.RISKY, *extra, core.CASH]
    bundle = DataBundle.build(b.prices[cols], **{k: b[k][cols] for k in ("vix", "market",
                                                                           "carry")})
    s_plus = strategy(extra, costs)
    first = bundle.prices[[*core.RISKY, *extra]].apply(
        lambda x: bundle.index.get_loc(x.first_valid_index()))
    per_asset = max([s_plus.risk_model.min_history]
                    + [getattr(g, "asset_history", g.min_history) + g.lag + 1
                       for g in s_plus.signals.values()])
    start = int(max(first.max() + per_asset, core.warmup(bundle)))
    out = {}
    for label, s, bnd in (("plus", s_plus, bundle),
                          ("saved", core.build(), DataBundle.build(
                              b.prices[[*core.RISKY, core.CASH]],
                              **{k: b[k][[*core.RISKY, core.CASH]] for k in
                                 ("vix", "market", "carry")}))):
        res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(bnd, s)
        out[label] = (core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH),
                      res.weights)
    return name, out


def book_report(workers: int) -> None:
    costs = costs_bps()
    jobs = [(t, [t], costs) for t in CANDIDATES]
    jobs += [("all five metals", METALS, costs), ("all 13", CANDIDATES, costs)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = dict(pool.map(run_one, jobs))
    rows = {}
    for name, extra, _ in jobs:
        rp, wp = out[name]["plus"]
        rs = out[name]["saved"][0]
        t = compare({"plus": rp, "saved": rs}, split_date=SPLIT)
        d = rp - rs
        w = wp[extra].sum(axis=1)
        rows[name] = {"from": f"{rp.index[0]:%Y-%m}",
                      "Sharpe saved": t["full"]["sharpe"]["saved"],
                      "Sharpe plus": t["full"]["sharpe"]["plus"],
                      "test saved": t["test"]["sharpe"]["saved"],
                      "test plus": t["test"]["sharpe"]["plus"],
                      "diff % a year": d.mean() * TRADING_DAYS * 100,
                      "diff t": d.mean() / d.std() * np.sqrt(len(d)),
                      "avg weight": w.mean(), "max weight": w.max(),
                      "held >0.5%": (w > 0.005).mean(),
                      "max DD saved": t["full"]["max_dd"]["saved"],
                      "max DD plus": t["full"]["max_dd"]["plus"]}
    print("1. EACH CANDIDATE ADDED TO THE SAVED BOOK AT A ZERO PRIOR (core only, same dates)")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))


# --- 2. announcement days ---------------------------------------------------------------------

def event_days(index: pd.DatetimeIndex, kind: str) -> pd.Series:
    hol = set(NYSEHolidays().holidays(index[0], index[-1] + pd.Timedelta(days=7)))
    s = pd.Series(False, index=index)
    if kind == "EIA petroleum":
        for d in index[index.weekday == 2]:
            monday = d - pd.Timedelta(days=2)
            target = d + pd.Timedelta(days=1) if monday in hol else d
            if target in s.index:
                s[target] = True
    elif kind == "EIA gas storage":
        for d in pd.date_range(index[0], index[-1], freq="W-THU"):
            target = d
            while target in hol or target not in s.index:
                target -= pd.Timedelta(days=1)
                if target < index[0]:
                    break
            if target in s.index:
                s[target] = True
    elif kind == "FOMC":
        s[s.index.isin(fomc_dates())] = True
    elif kind == "payrolls":
        for m in pd.period_range(index[0], index[-1], freq="M"):
            first = pd.Timestamp(m.start_time)
            fri = first + pd.Timedelta(days=(4 - first.weekday()) % 7)
            if fri in s.index:
                s[fri] = True
    return s


def event_report(b: DataBundle) -> None:
    raw = CachedReader(YFinanceReader()).history(["USO", "UNG", "GLD", "SLV"],
                                                 start="2004-01-01")
    raw = raw[raw.notna().any(axis=1)]
    tests = [("USO", "EIA petroleum"), ("UNG", "EIA gas storage"), ("GLD", "FOMC"),
             ("GLD", "payrolls"), ("SLV", "FOMC"), ("SLV", "payrolls")]
    rows = {}
    for asset, kind in tests:
        p = raw[asset].dropna()
        r = p.pct_change(fill_method=None)
        vol = r.rolling(60, min_periods=60).std()
        ev = event_days(p.index, kind)
        trend = core.ALPHAS["trend"][0].raw(DataBundle.build(p.to_frame()))[asset].shift(1)
        trend = trend / (vol * ANN)  # the tanh score
        z_r = r / vol.shift(1)  # day's return in daily vols
        fwd5 = (p.shift(-5) / p - 1) / (vol * np.sqrt(5))  # next five days, from that close

        def corr_t(x, y, mask):
            z = pd.concat([x[mask], y[mask]], axis=1).dropna()
            a, c = z.iloc[:, 0].rank(), z.iloc[:, 1].rank()
            prod = (a - a.mean()) / a.std() * (c - c.mean()) / c.std()
            return prod.mean(), newey_west_t(prod, 5), len(prod)

        h1e, h1et, ne = corr_t(trend, z_r, ev)
        h1o, h1ot, _ = corr_t(trend, z_r, ~ev)
        h2e, h2et, _ = corr_t(z_r, fwd5, ev)
        h2o, h2ot, _ = corr_t(z_r, fwd5, ~ev)
        absr = z_r.abs()
        rows[(asset, kind)] = {
            "event days": ne, "|move| event / other": absr[ev].mean() / absr[~ev].mean(),
            "H1 trend->day IC, event": f"{h1e:+.3f} ({h1et:+.1f})",
            "H1 other days": f"{h1o:+.3f} ({h1ot:+.1f})",
            "H2 day->next 5d IC, event": f"{h2e:+.3f} ({h2et:+.1f})",
            "H2 other days": f"{h2o:+.3f} ({h2ot:+.1f})"}
    print("\n2. ANNOUNCEMENT DAYS: rank ICs (Newey-West t). H1: trend score vs the day's return; "
          "H2: the day's return vs the next 5 days")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:.2f}"))


def main(workers: int) -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    event_report(b)
    book_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
