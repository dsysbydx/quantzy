"""Two Fear & Greed components as equity timing: credit risk appetite and sector breadth.

CNN's Fear & Greed index averages seven indicators; momentum, VIX and safe-haven demand are
already covered (trend, VRP, cross-asset and short-term reversal tests). Two are new:
junk-bond demand and breadth. The question is whether either adds to trend and VRP. **Fixed
before any test.**

* Credit excess return: HYG minus beta times IEF (beta a rolling one-year regression,
  lagged), from 2007; for a long check VWEHX minus beta times VFITX, from 1991.
  - C1 momentum: the 21-day credit excess return over (daily vol times sqrt 21), capped +/-2.
  - C2 contrarian: minus the same over 252 days (wide spreads after a year of credit losses).
* B1 breadth: the share of the 9 sector SPDRs above their 200-day EMA, scored
  ``(share - 0.5) * 4`` capped +/-2; for a long check the 12 Fama-French industries, 1927 on.

Tests, all signals from the prior close: rank IC with SPY's next 5, 21 and 63 days over vol
(47-day folds at 21 days), by period; correlation with the saved trend (EMA-200 on SPY) and
VRP scores, and the IC of the residual after regressing each signal on them. A signal goes to
the book only if its 21- or 63-day IC has t > 2 and its residual IC t > 1.5: then a tilt on
the 12 equity ETFs at 0.10 Sharpe per unit, ``PORTFOLIO`` unchanged.

**Result (2026-10-19): none passed; one reversed.** C1 (21-day credit momentum) has no IC at
any horizon. B1 breadth is 0.91 correlated with trend and nothing is left after it (residual
t -0.8 / -1.4); on the Fama-French industries it worked in 1927-69 (t 3.7-3.9) and not since
(t 0.4-1.0 in every later period). C2 has the *opposite* sign to the contrarian hypothesis: a
strong year of credit excess return predicts higher SPY returns (VWEHX 1993 on: 21 / 63-day IC
-0.15 / -0.27, t -3.4 / -3.3, both folds, residual after trend and VRP t -3.5 / -3.8; still
-2.8 / -2.9 with 252 Newey-West lags), weaker on HYG since 2009 (t -1.3 / -1.2). Credit
momentum at a year as a sign flip is post-hoc; ``research_credit_momentum.py`` tests it fresh.

Usage:
    uv run python research/research_credit_breadth.py
"""

from __future__ import annotations

import urllib.request
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from research_bounce_filtered import saved_book, stats
from research_reentry import MarketStateView

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.research import chunk_folds, newey_west_t
from quantzy.signals import Identity
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import treasury_month_end as tme

FRENCH = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
CACHE = Path.home() / ".cache" / "quantzy" / "french"
PERIODS = [("1927", "1969"), ("1970", "1993"), ("1994", "2007"), ("2008", "2026")]


def french_daily(name: str) -> pd.DataFrame:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / f"{name}.zip"
    if not path.exists():
        with urllib.request.urlopen(f"{FRENCH}{name}.zip", timeout=60) as r:
            path.write_bytes(r.read())
    z = zipfile.ZipFile(path)
    lines = z.read(z.namelist()[0]).decode("latin-1").splitlines()
    head = next(i for i, ln in enumerate(lines) if ln.strip().startswith(","))
    cols = [c.strip() for c in lines[head].split(",")][1:]
    rows = []
    for ln in lines[head + 1:]:
        p = [x.strip() for x in ln.split(",")]
        if len(p[0]) != 8 or not p[0].isdigit():
            break
        rows.append(p)
    df = pd.DataFrame([r[1:] for r in rows], columns=cols,
                      index=pd.to_datetime([r[0] for r in rows], format="%Y%m%d"))
    return df.astype(float) / 100


def ic_series(sig: pd.Series, target_px: pd.Series, h: int) -> pd.Series:
    """Daily product of the rank-standardized signal and forward return over vol."""
    r = target_px.pct_change(fill_method=None)
    vol = r.rolling(60, min_periods=60).std()
    fwd = (target_px.shift(-h) / target_px - 1) / (vol * np.sqrt(h))
    d = pd.concat([sig.rename("s"), fwd.rename("y")], axis=1, sort=True).dropna()
    a, b = d["s"].rank(), d["y"].rank()
    return (a - a.mean()) / a.std() * (b - b.mean()) / b.std()


def fmt(x: pd.Series, lags: int) -> str:
    x = x.dropna()
    return f"{x.mean():+.3f} ({newey_west_t(x, lags):+.1f})" if len(x) > 250 else ""


def credit_signals(hy: pd.Series, gov: pd.Series) -> dict[str, pd.Series]:
    rh, rg = hy.pct_change(fill_method=None), gov.pct_change(fill_method=None)
    beta = (rh.rolling(252).cov(rg) / rg.rolling(252).var()).shift(1)
    ex = rh - beta * rg
    vol = ex.rolling(60, min_periods=60).std()
    out = {}
    for name, n, sign in (("C1 credit momentum 21d", 21, 1),
                          ("C2 credit contrarian 252d", 252, -1)):
        out[name] = (sign * ex.rolling(n).sum() / (vol * np.sqrt(n))).clip(-2, 2)
    return out


def breadth(px: pd.DataFrame) -> pd.Series:
    ema = px.ewm(span=200, adjust=False, min_periods=200).mean()
    above = (px > ema).where(ema.notna())
    share = above.mean(axis=1, skipna=True).where(above.notna().sum(axis=1) >= 5)
    return ((share - 0.5) * 4).clip(-2, 2)


def main() -> None:
    pd.set_option("display.width", 320)
    yf = CachedReader(YFinanceReader())
    px = yf.history(["SPY", "HYG", "IEF", "VWEHX", "VFITX", *core.CLASSES["US sectors"]],
                    start="1990-01-01")
    px = px[px.notna().any(axis=1)]
    spy = px["SPY"]
    b = latest_complete(core.load(), core.RISKY)
    sigs = {**{f"{k} (HYG/IEF)": v for k, v in credit_signals(px["HYG"], px["IEF"]).items()},
            **{f"{k} (VWEHX/VFITX)": v for k, v in credit_signals(px["VWEHX"],
                                                                   px["VFITX"]).items()},
            "B1 sector breadth": breadth(px[core.CLASSES["US sectors"]])}
    raw_sigs = sigs
    sigs = {k: v.shift(1) for k, v in sigs.items()}  # known at the prior close

    # the saved alphas' views on SPY, for overlap
    trend = core.ALPHAS["trend"][0].raw(DataBundle.build(spy.to_frame()))["SPY"]
    vol60 = spy.pct_change(fill_method=None).rolling(60).std() * np.sqrt(252)
    trend = (trend / vol60).shift(1)
    vrp = core.ALPHAS["vrp"][0].premium(b).reindex(spy.index).shift(1)

    folds = chunk_folds(spy.index, 47)
    rows = {}
    for name, s in sigs.items():
        row = {"from": f"{s.first_valid_index():%Y}"}
        for h in (5, 21, 63):
            ic = ic_series(s, spy, h)
            row[f"{h}d"] = fmt(ic, max(21, 2 * h))
            if h == 21:
                f = folds.reindex(ic.index)
                row["21d fold A"], row["21d fold B"] = fmt(ic[f == 0], 42), fmt(ic[f == 1], 42)
                for a, z in PERIODS[2:]:
                    row[f"21d {a}-{z[2:]}"] = fmt(ic.loc[a:z], 42)
        d = pd.concat([s.rename("s"), trend.rename("t"), vrp.rename("v")], axis=1,
                      sort=True).dropna().astype(float)
        row["corr trend"], row["corr VRP"] = d["s"].corr(d["t"]), d["s"].corr(d["v"])
        X = np.column_stack([np.ones(len(d)), d["t"], d["v"]])
        resid = pd.Series(d["s"] - X @ np.linalg.lstsq(X, d["s"], rcond=None)[0], index=d.index)
        for h in (21, 63):
            row[f"residual {h}d"] = fmt(ic_series(resid, spy, h), 2 * h)
        rows[name] = row
    print("1. RANK IC vs SPY's forward return / vol (Newey-West t); overlap with trend and VRP")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.2f}"))

    ind = french_daily("12_Industry_Portfolios_daily_CSV")
    mkt = french_daily("F-F_Research_Data_Factors_daily_CSV")
    ind_px = (1 + ind).cumprod()
    mkt_px = (1 + mkt["Mkt-RF"] + mkt["RF"]).cumprod()
    bl = breadth(ind_px).shift(1)
    hist = {}
    for h in (21, 63):
        ic = ic_series(bl, mkt_px, h)
        hist[f"{h}d"] = {"all": fmt(ic, 2 * h), **{f"{a}-{z[2:]}": fmt(ic.loc[a:z], 2 * h)
                                                   for a, z in PERIODS}}
    print("\n2. LONG CHECK: breadth across the 12 Fama-French industries vs the market, 1927 on")
    print(pd.DataFrame(hist).T.to_string())

    keep = []
    for name, row in rows.items():
        def t_of(cell: str) -> float:
            return float(cell.split("(")[1].rstrip(")")) if cell else 0.0
        if max(t_of(row["21d"]), t_of(row["63d"])) > 2 and max(t_of(row["residual 21d"]),
                                                                 t_of(row["residual 63d"])) > 1.5:
            keep.append(name)
    print(f"\n3. PASSED THE PRE-SET BAR: {keep or 'none'}")
    if not keep:
        return
    book = saved_book(b)
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    ovl = tme.excess_returns(b.prices["TLT"].pct_change(fill_method=None), cash)
    out = {"saved book": stats(book)}
    for name in keep:
        s = raw_sigs[name].reindex(b.index).ffill()  # the signal lags it a day
        panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
        for t in core.EQUITY:
            panel[t] = s
        bb = DataBundle.build(b.prices, vix=b["vix"], market=b["market"], carry=b["carry"],
                              dollar_value=b["dollar_value"], tilt=panel)
        s_ = core.build(alphas={**core.ALPHAS,
                                "tilt": (MarketStateView("tilt", normalizer=Identity()), 0.10)})
        res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bb)).run(bb, s_)
        r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
        out[f"+ {name}"] = stats(r + ovl.reindex(r.index).fillna(0))
    print(pd.DataFrame(out).T.to_string(float_format=lambda v: f"{v:+.3f}"))


if __name__ == "__main__":
    main()
