"""A value tilt for every class: priors that move with how cheap or expensive each asset is.

The saved priors are fixed Sharpe ratios per class (equities 0.30, Treasuries 0.25 moved
toward term-spread carry, gold 0.20, DBC 0.15, the dollar 0). Assets go through cheap and
expensive spells, so a fixed prior treats them as equally attractive throughout. This adds a
value score per asset, in the time-series spirit of Asness, Moskowitz and Pedersen (2013),
from free data. **Fixed before any test.**

Each asset's Sharpe prior becomes ``SR_prior + 0.10 * z``, where ``z`` is its value score
standardized against its own trailing 10 years (from 5 years of history), capped at +/-2,
and known a day late (CPI a month late, for its release). Positive means cheap.

* US equity (all 10 US ETFs): the equity premium over T-bills, 1/CAPE (multpl.com, monthly
  from 1871) plus trailing 10-year CPI inflation minus the T-bill rate.
* US sectors, relative: minus each sector's 5-year log return relative to the US group
  average (long-term reversal), standardized across the group each day, added to the above.
* EFA and EEM: minus their 5-year log return relative to SPY.
* IEF and TLT: the real yield, 10-year yield minus trailing 12-month CPI inflation (BLS).
* GLD: minus the log real gold price (GLD over CPI) relative to its trailing 10-year mean.
* DBC: minus log(price / its 5-year mean). UUP: the same on DXY (a purchasing-power proxy).

Tests: rank IC of each measure with the next 21, 63 and 252 days' return over vol (folds at
21 days); then the book with every tilt, and with each component alone, against the saved
book (``PORTFOLIO`` unchanged, core only).

**Result (2026-10-18): dollar value adopted; the rest rejected.** All tilts together cut
Sharpe from 1.13 to 0.80 (drawdown -24%): US equity value (CAPE-based ERP) said "expensive"
through the 2010s rally (book 0.87) and regional value bought EFA/EEM into a value trap
(1.01); sector-relative value is noise. Gold's real price has the wrong sign. The Treasury
real yield forecasts at a year (IC +0.30, t 2.2) but overlaps bond carry (book 1.14). The
dollar's 5-year reversal is the strongest at every horizon (21d +0.15, t 2.3; 252d +0.43,
t 2.9) and lifted the book to 1.15 (t 1.6); added to ``core_multiasset`` as
``dollar_value`` at 0.10.

Usage:
    uv run python research/research_value.py [--workers 4]
"""

from __future__ import annotations

import argparse
import json
import re
import urllib.request
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from research_reentry import MarketStateView

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.strategies import core_multiasset as core

CACHE = Path.home() / ".cache" / "quantzy" / "value"
UA = {"User-Agent": "Mozilla/5.0 (research; quantzy)"}
SPLIT = pd.Timestamp("2017-07-26")
US = [*core.CLASSES["US sectors"], "QQQ"]
SECTORS = core.CLASSES["US sectors"]
TILT = 0.10
Z_DAYS, Z_MIN = 2520, 1260
COMPONENTS = {"US equity (ERP)": US, "sectors (relative)": SECTORS,
              "EFA, EEM vs US": ["EFA", "EEM"],
              "Treasuries (real yield)": ["IEF", "TLT"], "gold (real price)": ["GLD"],
              "DBC (5y reversal)": ["DBC"], "dollar (DXY 5y)": ["UUP"]}


def _fetch(url: str, name: str, data: bytes | None = None, headers=None) -> str:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / name
    if not path.exists():
        req = urllib.request.Request(url, data=data, headers={**UA, **(headers or {})})
        with urllib.request.urlopen(req, timeout=60) as r:
            path.write_bytes(r.read())
    return path.read_text(errors="ignore")


def cape() -> pd.Series:
    html = _fetch("https://www.multpl.com/shiller-pe/table/by-month", "cape.html")
    rows = re.findall(r"<td>([A-Z][a-z]{2} \d{1,2}, \d{4})</td>\s*<td>\s*(?:&#x2002;)?\s*"
                      r"([0-9.]+)", html)
    s = pd.Series({pd.Timestamp(d): float(v) for d, v in rows}).sort_index()
    s = s[s.index.day == 1]  # month starts; drop the intra-month print
    s.index = s.index + pd.offsets.MonthEnd(0)  # the month's value, known at its end
    return s


def cpi() -> pd.Series:
    out = {}
    for start in range(1950, 2027, 10):
        body = json.dumps({"seriesid": ["CUUR0000SA0"], "startyear": str(start),
                           "endyear": str(min(start + 9, 2026))}).encode()
        txt = _fetch("https://api.bls.gov/publicAPI/v1/timeseries/data/", f"cpi_{start}.json",
                     body, {"Content-Type": "application/json"})
        for row in json.loads(txt)["Results"]["series"][0]["data"]:
            if (row["period"].startswith("M") and row["period"] != "M13"
                    and re.match(r"^[0-9.]+$", row["value"])):  # '-' = not published
                d = (pd.Timestamp(int(row["year"]), int(row["period"][1:]), 1)
                     + pd.offsets.MonthEnd(0))
                out[d] = float(row["value"])
    s = pd.Series(out).sort_index()
    s = s.reindex(pd.date_range(s.index[0], s.index[-1], freq="ME")).ffill()
    return s.shift(1, freq="ME")  # released about two weeks into the next month


def zscore(x: pd.Series) -> pd.Series:
    r = x.rolling(Z_DAYS, min_periods=Z_MIN)
    return ((x - r.mean()) / r.std()).clip(-2, 2)


def value_scores(b: DataBundle, parts: dict | None = None) -> pd.DataFrame:
    """Daily value z per asset (cheap > 0), known a day late; NaN where not defined. Pass
    ``parts`` to also get the sector-relative component alone."""
    idx = b.index
    px = b.prices
    yf = CachedReader(YFinanceReader())
    extra = yf.history(["^TNX", "^IRX", "DX-Y.NYB", "SPY"], start="1960-01-01")
    infl12 = (cpi() / cpi().shift(12) - 1)
    infl10 = (cpi() / cpi().shift(120)) ** 0.1 - 1
    daily = lambda s: s.reindex(idx.union(s.index)).ffill().reindex(idx)  # noqa: E731
    tbill = daily(extra["^IRX"].ffill() / 100)
    erp = 1 / daily(cape()) + daily(infl10) - tbill
    z = pd.DataFrame(np.nan, index=idx, columns=px.columns)
    z_erp = zscore(erp)
    for t in US:
        z[t] = z_erp
    logp = np.log(px)
    rel5 = logp[SECTORS] - logp[SECTORS].shift(1260)
    cs = -(rel5.sub(rel5.mean(axis=1), axis=0)).div(rel5.std(axis=1), axis=0).clip(-2, 2)
    z[SECTORS] = (z[SECTORS] + cs).clip(-2, 2)
    if parts is not None:
        parts["sectors (relative)"] = cs.shift(1)
    spy = np.log(daily(extra["SPY"]))
    for t in ("EFA", "EEM"):
        z[t] = zscore(-((logp[t] - logp[t].shift(1260)) - (spy - spy.shift(1260))))
    real = daily(extra["^TNX"].ffill() / 100) - daily(infl12)
    z["IEF"] = z["TLT"] = zscore(real)
    real_gold = np.log(px["GLD"] / daily(cpi()))
    z["GLD"] = zscore(-(real_gold - real_gold.rolling(Z_DAYS, min_periods=Z_MIN).mean()))
    z["DBC"] = zscore(-np.log(px["DBC"] / px["DBC"].rolling(1260, min_periods=1260).mean()))
    dxy = daily(extra["DX-Y.NYB"])
    z["UUP"] = zscore(-np.log(dxy / dxy.rolling(1260, min_periods=1260).mean()))
    return z.shift(1)


# --- 1. IC ---------------------------------------------------------------------------------------

def ic_report(b: DataBundle, z: pd.DataFrame, parts: dict) -> None:
    px = b.prices[core.RISKY]
    folds = chunk_folds(b.index, 47)
    a, bb = folds == 0, folds == 1
    rows = {}
    for name, ts in COMPONENTS.items():
        sc = parts[name] if name in parts else z[ts]
        row = {}
        for h in (21, 63, 252):
            zx, zy = _ts_pairs(sc, px[ts], h, "spearman", 60, fit=a if h == 21 else None)
            prod = (zx * zy).mean(axis=1)
            lags = max(21, 2 * h)
            if h == 21:
                for lab, f in (("21d fold A", a), ("21d fold B", bb)):
                    x = prod[f].dropna()
                    row[lab] = f"{x.mean():+.3f} ({newey_west_t(x, lags):+.1f})"
            x = prod.dropna()
            row[f"{h}d"] = f"{x.mean():+.3f} ({newey_west_t(x, lags):+.1f})"
            if h == 252:
                row["252d 2010+"] = f"{x.loc['2010':].mean():+.3f}"
        row["z now"] = ", ".join(f"{t} {sc[t].dropna().iloc[-1]:+.1f}" for t in ts[:3])
        rows[name] = row
    print("1. TIME-SERIES RANK IC of each value measure vs forward return / vol (Newey-West t)")
    print(pd.DataFrame(rows).T.to_string())


# --- 2. book -------------------------------------------------------------------------------------

def run_one(args: tuple[str, list[str] | None]):
    name, assets = args
    b = latest_complete(core.load(), core.RISKY)
    alphas = dict(core.ALPHAS)
    if assets is not None:
        z = value_scores(b)
        panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
        panel[assets] = z[assets]
        b = DataBundle.build(b.prices, vix=b["vix"], market=b["market"], carry=b["carry"],
                             value=panel)
        alphas["value"] = (MarketStateView("value", normalizer=Identity()), TILT)
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights, sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)


def book_report(workers: int) -> None:
    jobs = [("saved book", None), ("+ all value tilts", list(dict.fromkeys(
        t for ts in COMPONENTS.values() for t in ts)))]
    jobs += [(f"+ {n} only", ts) for n, ts in COMPONENTS.items() if n != "sectors (relative)"]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    m["turnover"] = [out[n][2] for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    print(f"\n2. BOOK (core only), {runs[names[0]].index[0].date()} .. "
          f"{runs[names[0]].index[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(formatters={**{c: "{:.1%}".format for c in ("ret", "vol", "max_dd")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe", "diff t")},
                                  "turnover": "{:.1f}".format}))
    cls = {t: c for c, ts in core.CLASSES.items() for t in ts}
    w = pd.DataFrame({n: out[n][1][core.RISKY].T.groupby(cls).sum().T.mean() for n in
                      names[:2]}).T
    print("\n   average weight by class, saved vs all value tilts")
    print(w.to_string(float_format="{:.0%}".format))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved book, all value tilts (pp)")
    print(yr[names[1]].sub(yr[names[0]]).mul(100).round(1).to_frame().T.to_string())


def main(workers: int) -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    parts: dict = {}
    z = value_scores(b, parts)
    ic_report(b, z, parts)
    book_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
