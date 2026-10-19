"""A G10 currency sleeve: carry, value and momentum, long/short, alongside the book.

The core book is long-only and holds one currency (UUP). Currency carry, value and momentum
are among the most replicated return sources in macro (Lustig-Verdelhan; Asness, Moskowitz and
Pedersen 2013; Koijen et al. 2018), nearly uncorrelated with each other and with equities
outside crises. This tests them as an independent sleeve, run like an overlay with its own
risk budget. **Fixed before any test.**

Data (free): FRED H.10 daily noon rates against USD for EUR (1999 on), JPY, GBP, CHF, AUD,
NZD, CAD, NOK, SEK, from 1971; monthly 3-month interbank rates (OECD via FRED; call money
where the 3-month series is missing: JPY before 2002, CHF before 1999); monthly CPI (OECD;
Australia and New Zealand quarterly, interpolated; after a series ends, World Bank annual
inflation, then its last 12-month rate). A currency enters once it has all three.

* Excess return of long currency ``c`` a day: its spot change against USD plus
  ``(i_c - i_USD) / 252`` (covered interest parity: a rolled forward or future).
* Signals, at each month-end, from data known then (a month's rate from the next month on;
  CPI three months late):
  - carry: ``i_c - i_USD``;
  - value: minus the 5-year change in the log real exchange rate ``log(S * P_c / P_USD)``;
  - momentum: the 12-month excess return, skipping the latest month.
* Each leg ranks the available currencies plus USD (signal 0), weights rank minus mean (USD's
  weight is the funding leg), scaled to 5% ex-ante vol on the trailing 252-day covariance.
  The sleeve is the average of the three legs' weights, rescaled to 5% ex-ante vol. Held
  for the month.
* Costs: futures, 1 bp of turnover; IBKR spot, the same plus financing of 0.5% a year on
  each long currency and 0.8% on each short (USD leg included).

Adopted only if the sleeve's net Sharpe (futures costs) is above 0.3 in both 1980-2007 and
2008-2026, and adding it at 5% vol lifts the saved book's (core plus overlays) full-period
and test-half (2017-07-26 on) Sharpe by at least 0.02 each, with the spot-cost version still
positive.

**Result (2026-10-19): rejected; the premia were real before 2008 and are gone since.** Net
of futures costs the combined sleeve made Sharpe 0.67 over 1980-2026 (carry 0.56, value 0.28,
momentum 0.38; leg correlations -0.42 to +0.23, so the mix beats each leg), but 1.07 in
1980-2007 against 0.08 in 2008-2026 (2008-16 0.08, 2017-26 0.07). Carry fell from 0.89 to
0.06, value 0.40 -> 0.09, momentum 0.63 -> 0.00, consistent with the published post-2008
decay of G10 currency premia (near-zero rate dispersion, then synchronized hiking). Since
2008 it is +0.15 correlated with the book, and carry lost in 2008 (-7.8%) and 2020 (-10.3%).
At 5% vol it lowers the saved book from 1.267 to 1.127 (test 1.293 -> 1.146), max drawdown
-17.2% -> -21.0%; IBKR spot financing takes another 0.2 of the sleeve's Sharpe.

Usage:
    uv run python research/research_fx_sleeve.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays

FRED = Path.home() / ".cache" / "quantzy" / "fred"
OECD = Path.home() / ".cache" / "quantzy" / "oecd"
ANN = np.sqrt(TRADING_DAYS)
TARGET_VOL = 0.05
SPLIT = "2017-07-26"
#: currency: (FRED daily series, quoted as USD per unit?, OECD area, rate country code)
CCY = {"EUR": ("DEXUSEU", True, "EA20", "EZ"), "JPY": ("DEXJPUS", False, "JPN", "JP"),
       "GBP": ("DEXUSUK", True, "GBR", "GB"), "CHF": ("DEXSZUS", False, "CHE", "CH"),
       "AUD": ("DEXUSAL", True, "AUS", "AU"), "NZD": ("DEXUSNZ", True, "NZL", "NZ"),
       "CAD": ("DEXCAUS", False, "CAN", "CA"), "NOK": ("DEXNOUS", False, "NOR", "NO"),
       "SEK": ("DEXSDUS", False, "SWE", "SE")}
WB = {"JPN": "JPN", "CHE": "CHE", "NOR": "NOR", "SWE": "SWE", "EA20": "EMU"}
EVENTS = {"2008-09..2009-03": ("2008-09-01", "2009-03-09"),
          "2020-02..03": ("2020-02-19", "2020-03-23"), "2022": ("2022-01-03", "2022-12-30")}


def fred(series: str) -> pd.Series:
    d = pd.read_csv(FRED / f"{series}.csv", index_col=0, parse_dates=True)
    return pd.to_numeric(d.iloc[:, 0], errors="coerce").dropna()


def spot() -> pd.DataFrame:
    out = {}
    for c, (sid, usd_per, *_) in CCY.items():
        s = fred(sid)
        out[c] = s if usd_per else 1 / s
    return pd.DataFrame(out).ffill(limit=5)


def rates() -> pd.DataFrame:
    """Monthly 3-month rates in decimals, indexed by the month they describe."""
    out = {}
    for c, (*_, cc) in CCY.items():
        r = fred(f"IR3TIB01{cc}M156N")
        call = {"JP": "IRSTCI01JPM156N", "CH": "IRSTCI01CHM156N"}.get(cc)
        if call:
            r = r.combine_first(fred(call))
        out[c] = r
    out["USD"] = fred("IR3TIB01USM156N")
    return pd.DataFrame(out).resample("MS").last().ffill() / 100


def cpi() -> pd.DataFrame:
    """Monthly CPI by currency (USD included), extended past each series' end."""
    raw = pd.concat([pd.read_csv(OECD / "cpi.csv"), pd.read_csv(OECD / "cpi_extra.csv")])
    raw = raw[raw["METHODOLOGY"] == "N"].drop_duplicates(["REF_AREA", "FREQ", "TIME_PERIOD"])
    areas = {c: v[2] for c, v in CCY.items()} | {"USD": "USA"}
    idx = pd.date_range("1970-01-01", pd.Timestamp.today().normalize(), freq="MS")
    out = {}
    for c, area in areas.items():
        d = raw[raw["REF_AREA"] == area]
        m = d[d["FREQ"] == "M"]
        if len(m) > 100:
            s = pd.Series(m["OBS_VALUE"].values, index=pd.PeriodIndex(m["TIME_PERIOD"], freq="M")
                          .to_timestamp()).sort_index()
        else:
            q = d[d["FREQ"] == "Q"]
            s = pd.Series(q["OBS_VALUE"].values, index=pd.PeriodIndex(q["TIME_PERIOD"], freq="Q")
                          .to_timestamp(how="end").normalize().to_period("M").to_timestamp())
            s = np.exp(np.log(s.sort_index()).reindex(idx).interpolate(limit_area="inside"))
        s = s.reindex(idx)
        last = s.last_valid_index()
        infl = pd.Series(dtype=float)
        if area in WB:
            infl = fred(f"FPCPITOTLZG{WB[area]}") / 100
        trailing = s.loc[last] / s.loc[last - pd.DateOffset(years=1)] - 1
        for t in idx[idx > last]:
            a = infl.get(pd.Timestamp(year=t.year, month=1, day=1), trailing)
            s.loc[t] = s.loc[t - pd.DateOffset(months=1)] * (1 + a) ** (1 / 12)
        out[c] = s
    return pd.DataFrame(out)


def signals(sp: pd.DataFrame, rt: pd.DataFrame, px: pd.DataFrame, ex: pd.DataFrame,
            ends: pd.DatetimeIndex) -> dict[str, pd.DataFrame]:
    """Each leg's signal at each month-end, from data known then (NaN = not available)."""
    rk = rt.shift(1).reindex(ends, method="ffill")  # a month's rate, known the next month
    pk = px.shift(3).reindex(ends, method="ffill")  # CPI three months late
    s = sp.reindex(ends, method="ffill")
    carry = rk[list(CCY)].sub(rk["USD"], axis=0)
    q = np.log(s * pk[list(CCY)].div(pk["USD"], axis=0))
    value = -(q - q.shift(60))
    cum = (1 + ex.fillna(0)).cumprod().where(ex.notna().cummax())
    c = cum.reindex(ends, method="ffill")
    mom = c.shift(1) / c.shift(12) - 1
    ok = carry.notna() & value.notna() & mom.notna()
    return {"carry": carry.where(ok), "value": value.where(ok), "momentum": mom.where(ok)}


def rank_weights(sig: pd.Series) -> pd.Series:
    x = pd.concat([sig.dropna(), pd.Series({"USD": 0.0})])
    w = x.rank() - x.rank().mean()
    return (w / w.abs().sum()).drop("USD")


def scale(w: pd.Series, cov: pd.DataFrame) -> pd.Series:
    v = float(np.sqrt(w @ cov.loc[w.index, w.index] @ w))
    return w * (TARGET_VOL / v) if v > 0 else w * 0


def sleeve(ex: pd.DataFrame, sigs: dict[str, pd.DataFrame], ends: pd.DatetimeIndex
           ) -> dict[str, tuple[pd.Series, pd.Series, pd.Series]]:
    """Daily net returns (futures costs, spot costs) and gross notional per leg and combined."""
    legs = [*sigs, "combined"]
    W = {k: pd.DataFrame(0.0, index=ends, columns=list(CCY)) for k in legs}
    for t in ends:
        hist = ex.loc[:t].iloc[-252:]
        if len(hist) < 252:
            continue
        cov = hist.cov() * TRADING_DAYS
        ws = {}
        for k, s in sigs.items():
            if s.loc[t].notna().sum() < 6:
                break
            ok = s.loc[t].dropna().index.intersection(hist.dropna(axis=1).columns)
            ws[k] = scale(rank_weights(s.loc[t, ok]), cov)
        if len(ws) < 3:
            continue
        ws["combined"] = scale(pd.concat(ws.values(), axis=1).fillna(0).mean(axis=1), cov)
        for k, w in ws.items():
            W[k].loc[t, w.index] = w
    out = {}
    days = ex.index
    for k in legs:
        w = W[k].reindex(days, method="ffill").shift(1).fillna(0)  # set at month-end, held after
        gross = (ex.fillna(0) * w).sum(axis=1)
        turn = W[k].diff().abs().sum(axis=1).reindex(days).fillna(0)
        net = gross - turn * 1e-4
        usd = -w.sum(axis=1)
        long_n = w.clip(lower=0).sum(axis=1) + usd.clip(lower=0)
        short_n = (-w).clip(lower=0).sum(axis=1) + (-usd).clip(lower=0)
        spot_net = net - (0.005 * long_n + 0.008 * short_n) / TRADING_DAYS
        live = W[k].abs().sum(axis=1).gt(0).reindex(days, method="ffill").shift(1)
        first = live[live.fillna(False)].index[0]
        out[k] = (net.loc[first:], spot_net.loc[first:], (long_n + short_n).loc[first:] / 2)
    return out


def sharpe(x: pd.Series) -> float:
    x = x.dropna()
    return x.mean() / x.std() * ANN if len(x) > 60 and x.std() > 0 else np.nan


def book_returns() -> pd.Series:
    b = latest_complete(core.load(), core.RISKY)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, core.build())
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return r


def on_dates(x: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    """Compound a daily return series onto another calendar."""
    nav = (1 + x).cumprod().reindex(x.index.union(idx)).ffill().reindex(idx)
    return nav.pct_change().fillna(0.0)


def book_stats(r: pd.Series) -> dict:
    nav = (1 + r).cumprod()
    dd = (nav / nav.cummax() - 1).min()
    cagr = nav.iloc[-1] ** (TRADING_DAYS / len(r)) - 1
    return {"Sharpe": sharpe(r), "train": sharpe(r.loc[:SPLIT]), "test": sharpe(r.loc[SPLIT:]),
            "CAGR": cagr, "vol": r.std() * ANN, "max DD": dd, "Calmar": cagr / -dd,
            **{k: (1 + r.loc[a:z]).prod() - 1 for k, (a, z) in EVENTS.items()}}


def main() -> None:
    pd.set_option("display.width", 320)
    sp, rt, px = spot(), rates(), cpi()
    sp = sp.loc["1975":]
    diff = rt[list(CCY)].sub(rt["USD"], axis=0).shift(1)  # this month's carry: last month's rate
    carry_d = diff.reindex(sp.index, method="ffill") / TRADING_DAYS
    ex = sp.pct_change(fill_method=None) + carry_d
    ex = ex.where(ex.abs() < 0.15)  # no bad prints in H.10, but guard anyway
    ends = sp.groupby(sp.index.to_period("M")).tail(1).index
    sigs = signals(sp, rt, px, ex, ends)
    res = sleeve(ex, sigs, ends)

    print("0. CURRENCIES AVAILABLE AT EACH MONTH-END (all three signals): "
          + ", ".join(f"{y}: {int(sigs['carry'].loc[str(y)].notna().sum(axis=1).median())}"
                      for y in (1980, 1986, 1995, 2000, 2010, 2026)))
    periods = [("1980", "1994"), ("1995", "2007"), ("2008", "2016"), ("2017", "2026")]
    rows = {}
    for k, (net, spot_net, gross) in res.items():
        yrs = net.index.year
        nav = (1 + net).cumprod()
        rows[k] = {"from": f"{net.index[0]:%Y-%m}", "Sharpe (futures)": sharpe(net),
                   "Sharpe (spot)": sharpe(spot_net), "%/yr": net.mean() * TRADING_DAYS * 100,
                   "vol": net.std() * ANN, "max DD": (nav / nav.cummax() - 1).min(),
                   "gross": gross.mean(), "odd years": sharpe(net[yrs % 2 == 1]),
                   "even years": sharpe(net[yrs % 2 == 0]),
                   "1980-2007": sharpe(net.loc[:"2007"]), "2008-2026": sharpe(net.loc["2008":]),
                   **{f"{a}-{z[2:]}": sharpe(net.loc[a:z]) for a, z in periods}}
    print("\n1. THE LEGS AND THE SLEEVE ALONE (each scaled to 5% ex-ante vol), net")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.2f}"))
    legs = pd.DataFrame({k: v[0] for k, v in res.items() if k != "combined"}).dropna()
    print("\nleg correlations (daily): " + ", ".join(
        f"{a}/{b} {legs[a].corr(legs[b]):+.2f}" for a, b in
        (("carry", "value"), ("carry", "momentum"), ("value", "momentum"))))

    book = book_returns()
    idx = book.index
    on = {k: on_dates(v[0], idx) for k, v in res.items()}
    on_spot = on_dates(res["combined"][1], idx)
    m = book.resample("ME").apply(lambda x: (1 + x).prod() - 1)
    worst = m[m <= m.quantile(0.1)].index
    rows = {}
    for k, x in on.items():
        xm = x.resample("ME").apply(lambda y: (1 + y).prod() - 1)
        rows[k] = {"corr book (daily)": x.corr(book), "corr book (monthly)": xm.corr(m),
                   "avg in book's worst 10% months": xm.loc[worst].mean(),
                   **{e: (1 + x.loc[a:z]).prod() - 1 for e, (a, z) in EVENTS.items()}}
    print(f"\n2. AGAINST THE SAVED BOOK (core + overlays), {idx[0].date()} on")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    t = {"saved book": book_stats(book)}
    for k, x in on.items():
        t[f"+ {k} at 5% vol"] = book_stats(book + x)
    t["+ combined at 5% vol, spot costs"] = book_stats(book + on_spot)
    print("\n3. SAVED BOOK PLUS THE SLEEVE")
    print(pd.DataFrame(t).T.to_string(float_format=lambda v: f"{v:+.3f}"))
    last = {k: v.iloc[-1] for k, v in sigs.items()}
    print("\nlatest signals: " + "; ".join(
        f"{k}: " + " ".join(f"{c} {s[c]:+.3f}" for c in s.dropna().sort_values().index)
        for k, s in last.items()))


if __name__ == "__main__":
    main()
