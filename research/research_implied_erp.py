"""US equity value from Damodaran's implied equity risk premium, instead of CAPE.

``research_value.py`` found a CAPE-based premium lowered Sharpe (0.87 against 1.13): CAPE
has no fixed anchor (it moved with rates, margins, buybacks and index mix). Damodaran's
implied ERP is forward-looking: the discount rate that equates the S&P 500's level with its
expected cash yield (dividends plus buybacks) growing at analysts' rates, minus the T-bond
yield. **Fixed before any test.**

Data (free, from pages.stern.nyu.edu/~adamodar): the annual implied premium (FCFE) at each
year-end 1960-2023, used from the January it was published; and the monthly trailing-12-month
ERP from 2008-09, at the start of each month. Two scores, each capped at +/-2 and known a day
late: ``z`` against the trailing 10 years, as the dollar tilt; and ``z`` against the expanding
history since 1961 (the "absolute level" version). Tilt: 0.10 Sharpe per ``z`` on the 10 US
equity ETFs.

Tests: daily rank IC at 21, 63 and 252 days on SPY (1994 on) and on the US ETFs; year-end ERP
against the next year's S&P 500 return over T-bills, 1961-2025; the book against the saved
one (``PORTFOLIO`` unchanged, core only).

**Result (2026-10-19): not added.** Annually the implied ERP forecasts the next year's S&P
excess return (rank corr +0.28, t ~2.3, 1961-2024; +0.29 / +0.25 in each half), unlike CAPE.
In the ETF era the daily ICs are positive but not significant, and in the book both tilts
lower Sharpe (1.05 trailing-10y, 1.06 since-1961, against 1.15) with deeper drawdowns
(-21.4%): the since-1961 version mostly adds equity (US weight 75% -> 87%), the trailing one
called 2021 expensive (-18.7 points). Now: 3.70%, z -1.9 (10y) / -0.6 (since 1961).

Usage:
    uv run --with xlrd --with openpyxl python research/research_implied_erp.py [--workers 3]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from research_reentry import MarketStateView

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.strategies import core_multiasset as core

CACHE = Path.home() / ".cache" / "quantzy" / "value"
US = [*core.CLASSES["US sectors"], "QQQ"]
SPLIT = pd.Timestamp("2017-07-26")
TILT = 0.10


def annual() -> pd.DataFrame:
    """Year, implied premium (FCFE), T-bill, dividend yield, S&P level, from histimpl.xls."""
    raw = pd.read_excel(CACHE / "histimpl.xls", header=None)
    head = raw.index[raw.iloc[:, 0].astype(str).str.strip() == "Year"][0]
    df = raw.iloc[head + 1:].copy()
    df.columns = [str(c).strip() for c in raw.iloc[head]]
    df = df[pd.to_numeric(df["Year"], errors="coerce").notna()]
    prem = "Implied ERP (FCFE)"  # from 1961; the "sustainable payout" variant starts 2001
    out = pd.DataFrame({"erp": pd.to_numeric(df[prem], errors="coerce"),
                        "tbill": pd.to_numeric(df["T.Bill Rate"], errors="coerce"),
                        "dy": pd.to_numeric(df["Dividend Yield"], errors="coerce"),
                        "spx": pd.to_numeric(df["S&P 500"], errors="coerce")})
    out.index = df["Year"].astype(int)
    return out


def monthly() -> pd.Series:
    raw = pd.read_excel(CACHE / "ERPbymonth.xlsx", sheet_name="Historical ERP", header=None)
    df = raw.iloc[1:, [0, 9]].dropna()

    def num(v) -> float:  # a few cells are text such as '4.06%'
        if isinstance(v, str):
            v = v.strip()
            return float(v.rstrip("%")) / 100 if v.endswith("%") else float(v)
        return float(v)

    dates = pd.to_datetime(df[0], errors="coerce")
    s = pd.Series([num(v) for v in df[9]], index=dates)
    return s[s.index.notna()].sort_index()


def erp_series() -> pd.Series:
    """The implied ERP as known on each date: annual (from Jan of the next year) to 2008-08,
    then monthly (from the first of the month)."""
    a = annual()["erp"].dropna()
    s = pd.Series(a.to_numpy(), index=[pd.Timestamp(y + 1, 1, 1) for y in a.index])
    m = monthly()
    return pd.concat([s[s.index < m.index[0]], m]).sort_index()


def scores(index: pd.DatetimeIndex) -> dict[str, pd.Series]:
    e = erp_series()
    e = e.reindex(index.union(e.index)).ffill().reindex(index)
    roll = e.rolling(2520, min_periods=1260)
    exp = e.expanding(min_periods=1260)
    return {"z, trailing 10y": ((e - roll.mean()) / roll.std()).clip(-2, 2),
            "z, since 1961": ((e - exp.mean()) / exp.std()).clip(-2, 2)}


def ic_report(b: DataBundle) -> None:
    yf = CachedReader(YFinanceReader())
    spy = yf.history(["SPY"], start="1993-01-01")
    full_idx = spy.index.union(b.index)
    sc = scores(full_idx)
    print(f"implied ERP now: {erp_series().iloc[-1]:.2%}; scores now: "
          + ", ".join(f"{k} {v.dropna().iloc[-1]:+.2f}" for k, v in sc.items()))
    rows = {}
    for name, z in sc.items():
        for label, px in (("SPY", spy), ("10 US ETFs", b.prices[US])):
            s = pd.DataFrame({c: z.reindex(px.index) for c in px.columns}).shift(1)
            folds = chunk_folds(px.index, 47)
            row = {}
            for h in (21, 63, 252):
                zx, zy = _ts_pairs(s, px, h, "spearman", 60, fit=(folds == 0) if h == 21
                                   else None)
                prod = (zx * zy).mean(axis=1)
                lags = max(21, 2 * h)
                if h == 21:
                    for lab, f in (("21d fold A", folds == 0), ("21d fold B", folds == 1)):
                        x = prod[f].dropna()
                        row[lab] = f"{x.mean():+.3f} ({newey_west_t(x, lags):+.1f})"
                x = prod.dropna()
                row[f"{h}d"] = f"{x.mean():+.3f} ({newey_west_t(x, lags):+.1f})"
                if h == 252:
                    row["252d 2010+"] = f"{x.loc['2010':].mean():+.3f}"
            rows[(name, label)] = row
    print("\n1. DAILY TIME-SERIES RANK IC vs forward return / vol (Newey-West t)")
    print(pd.DataFrame(rows).T.to_string())

    a = annual().dropna(subset=["erp"])
    spx = yf.history(["^GSPC"], start="1959-01-01")["^GSPC"]
    ye = spx.groupby(spx.index.year).last()
    ret = (ye / ye.shift(1) - 1).reindex(a.index + 1).to_numpy() + a["dy"].to_numpy()
    nxt = pd.Series(ret, index=a.index) - a["tbill"].shift(-1)  # next year's excess
    df = pd.DataFrame({"erp": a["erp"], "next": nxt}).dropna()
    rho = df.rank().corr().iloc[0, 1]
    n = len(df)
    print(f"\n2. ANNUAL, {df.index[0]}-{df.index[-1]}: year-end ERP vs next year's S&P excess "
          f"return, rank corr {rho:+.2f} (n {n}, t ~{rho * np.sqrt((n - 2) / (1 - rho ** 2)):+.1f})"
          f"; halves {df.loc[:1992].rank().corr().iloc[0, 1]:+.2f} / "
          f"{df.loc[1993:].rank().corr().iloc[0, 1]:+.2f}")


def run_one(args: tuple[str, str | None]):
    name, which = args
    b = latest_complete(core.load(), core.RISKY)
    alphas = dict(core.ALPHAS)
    if which is not None:
        z = scores(b.index)[which]
        panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
        for t in US:
            panel[t] = z
        b = DataBundle.build(b.prices, vix=b["vix"], market=b["market"], carry=b["carry"],
                             dollar_value=b["dollar_value"], erp=panel)
        alphas["erp"] = (MarketStateView("erp", normalizer=Identity()), TILT)
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights


def book_report(workers: int) -> None:
    jobs = [("saved book", None), ("+ ERP tilt, z trailing 10y", "z, trailing 10y"),
            ("+ ERP tilt, z since 1961", "z, since 1961")]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, jobs)}
    names = [j[0] for j in jobs]
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "sharpe": t["full"]["sharpe"],
        "max_dd": t["full"]["max_dd"]}).loc[names]
    m["US equity wt"] = [out[n][1][US].sum(axis=1).mean() for n in names]
    ex = pd.DataFrame({n: runs[n] - runs[names[0]] for n in names[1:]})
    m["diff t"] = [np.nan] + list(ex.mean() / ex.std() * np.sqrt(len(ex)))
    print(f"\n3. BOOK (core only), {runs[names[0]].index[0].date()} .. "
          f"{runs[names[0]].index[-1].date()}")
    print(m.to_string(formatters={**{c: "{:.1%}".format for c in ("ret", "max_dd",
                                                                  "US equity wt")},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe", "diff t")}}))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar-year return minus the saved book (pp)")
    print(yr[names[1:]].sub(yr[names[0]], axis=0).mul(100).round(1).T.to_string())


def main(workers: int) -> None:
    pd.set_option("display.width", 320)
    b = latest_complete(core.load(), core.RISKY)
    ic_report(b)
    book_report(workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    main(ap.parse_args().workers)
