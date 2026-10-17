"""Fast, market-implied valuation and carry for bonds, gold, commodities and the dollar.

CAPE needs ten years of earnings and moves slowly. These measures are read straight off
market prices and yields, so they update daily. They follow the time-series version of
Asness, Moskowitz and Pedersen's "value" and "carry" (2013). Sampled at month-ends, all
known at the close of month ``t``. **Fixed before any test.**

Treasuries (target: a 10- or 30-year par bond rolled monthly, excess over T-bills)
    * ``term spread``: 10-year yield minus the 3-month bill (carry plus roll-down).
    * ``real yield``: 10-year yield minus trailing 12-month CPI inflation (CPI lagged one
      month; Shiller's CPI, so this one stops in 2024-09).
    * ``yield - EMA(N)``: the 10-year yield against its own N-month EMA, N in 12, 36, 120.
      Yields above trend mean cheap bonds.
Gold (target: gold futures)
    * ``-real yield``: gold has no coupon, so a low real yield makes it cheap to hold.
    * ``value``: minus log(price / its 60-month EMA), a five-year reversal.
Commodities (target: S&P GSCI) and the dollar (target: DXY spot change)
    * ``value``: the same five-year reversal; for the dollar, a purchasing-power proxy.
Dollar carry (target: DXY)
    * ``EUR carry``: the US minus euro short rate, read off the euro futures basis:
      ``(F / S - 1) / tau`` with ``F`` the front 6E future, ``S`` spot EURUSD and ``tau`` the
      years to the next IMM date. A long-dollar position earns it.

Statistics: rank IC of each measure with the excess return over the next 1, 3 and 12
months (starting the next month), Newey-West t with ``max(h, 3)`` lags, for all months and
each half; and, where there are 25+ years, the out-of-sample R^2 of an expanding regression
against the historical mean, forecasts from year 15.

**Result (2026-10-17).** Only the Treasury term spread qualifies: 1-month rank IC +0.15
(t 3.9) on the 10-year since 1962 and +0.13 (t 3.2) on the 30-year since 1977, positive in
both halves, out-of-sample R^2 +2.0% (1 month) and +10.9% (12 months). Real yield is close
but needs CPI. Yield minus its EMA does not work (12/36/120 months), and the 30-year EMA120
fails out of sample (-45%). Gold's real yield has the wrong sign since 2000 (12-month IC
-0.41); commodity and dollar value and euro carry are noise.

Usage:
    uv run --with xlrd python research/research_fast_valuation.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from research_prior_check import french, par_bond_returns
from research_shiller_valuation import load as shiller

from quantzy.data import CachedReader, YFinanceReader
from quantzy.research import newey_west_t

HORIZONS = (1, 3, 12)


def month_end(s: pd.Series) -> pd.Series:
    out = s.dropna().resample("ME").last()
    out.index = out.index.to_period("M").to_timestamp("M")
    return out


def next_imm(t: pd.Timestamp) -> pd.Timestamp:
    """The third Wednesday of the next March, June, September or December after ``t``."""
    for k in range(5):
        mm = t.month + k
        yy, mm = t.year + (mm - 1) // 12, (mm - 1) % 12 + 1
        if mm % 3:
            continue
        first = pd.Timestamp(yy, mm, 1)
        third = first + pd.Timedelta(days=(2 - first.weekday()) % 7 + 14)
        if third > t:
            return third
    raise AssertionError("unreachable")


def load() -> tuple[dict[str, pd.Series], dict[str, dict[str, pd.Series]], pd.Series]:
    yf = CachedReader(YFinanceReader())
    px = yf.history(["^TNX", "^TYX", "^IRX", "GC=F", "^SPGSCI", "DX-Y.NYB", "6E=F",
                     "EURUSD=X"], start="1900-01-01")
    m = {k: month_end(px[k]) for k in px.columns}
    rf = french("F-F_Research_Data_Factors_CSV")["RF"]
    rf_y = (m["^IRX"] / 100 / 12).shift(1)
    rf = rf.combine_first(rf_y)
    cpi = shiller()["CPI"]
    infl = (cpi / cpi.shift(12) - 1).shift(1)  # known a month later

    def excess(r: pd.Series) -> pd.Series:
        r.index = r.index.to_period("M").to_timestamp("M")
        return (np.log1p(r) - np.log1p(rf.reindex(r.index))).dropna()

    targets = {
        "10y Treasury": excess(par_bond_returns(px["^TNX"], 10)),
        "30y Treasury": excess(par_bond_returns(px["^TYX"], 30)),
        "gold": excess(m["GC=F"].pct_change()),
        "commodities (GSCI)": excess(m["^SPGSCI"].pct_change()),
        "dollar (DXY spot)": np.log1p(m["DX-Y.NYB"].pct_change()).dropna(),
    }
    y10, y3m = m["^TNX"] / 100, m["^IRX"] / 100
    real = (y10 - infl.reindex(y10.index)).dropna()

    def value(p: pd.Series) -> pd.Series:
        return -np.log(p / p.ewm(span=60, adjust=False, min_periods=60).mean())

    def dev(x: pd.Series, n: int) -> pd.Series:
        return x - x.ewm(span=n, adjust=False, min_periods=n).mean()

    daily = px[["6E=F", "EURUSD=X"]].dropna()
    tau = pd.Series([(next_imm(t) - t).days / 365 for t in daily.index], index=daily.index)
    basis = ((daily["6E=F"] / daily["EURUSD=X"] - 1) / tau).where(tau > 10 / 365)
    eur_carry = month_end(basis.rolling(5, min_periods=1).median())

    signals = {
        "10y Treasury": {"term spread": y10 - y3m, "real yield": real,
                         **{f"yield - EMA{n}": dev(y10, n) for n in (12, 36, 120)}},
        "30y Treasury": {"term spread (30y - 3m)": m["^TYX"] / 100 - y3m, "real yield": real,
                         **{f"yield - EMA{n}": dev(m["^TYX"] / 100, n) for n in (12, 36, 120)}},
        "gold": {"-real yield": -real, "value (5y)": value(m["GC=F"])},
        "commodities (GSCI)": {"value (5y)": value(m["^SPGSCI"])},
        "dollar (DXY spot)": {"value (5y)": value(m["DX-Y.NYB"]), "EUR carry": eur_carry},
    }
    return targets, signals, rf


def forward(r: pd.Series, h: int) -> pd.Series:
    return r.rolling(h).sum().shift(-h) * 12 / h


def ic(x: pd.Series, y: pd.Series, h: int) -> tuple[float, float, int]:
    z = pd.concat([x, y], axis=1).dropna()
    if len(z) < 36:
        return np.nan, np.nan, len(z)
    a, b = z.iloc[:, 0].rank(), z.iloc[:, 1].rank()
    prod = (a - a.mean()) / a.std() * (b - b.mean()) / b.std()
    return prod.mean(), newey_west_t(prod, max(h, 3)), len(z)


def r2_oos(x: pd.Series, y: pd.Series, h: int, burn_years: int = 15) -> float:
    z = pd.concat([x.rename("x"), y.rename("y")], axis=1).dropna()
    if len(z) < 25 * 12:
        return np.nan
    em, eb = 0.0, 0.0
    for i, t in enumerate(z.index):
        if i < burn_years * 12:
            continue
        train = z.loc[: t - pd.offsets.MonthEnd(h)]
        b, a = np.polyfit(train["x"], train["y"], 1)
        em += (z.at[t, "y"] - (a + b * z.at[t, "x"])) ** 2
        eb += (z.at[t, "y"] - train["y"].mean()) ** 2
    return 1 - em / eb


def main() -> None:
    pd.set_option("display.width", 300)
    targets, signals, _ = load()
    rows = {}
    for asset, sigs in signals.items():
        r = targets[asset]
        for name, x in sigs.items():
            x = x.dropna()
            row = {"from": f"{max(x.index[0], r.index[0]):%Y-%m}"}
            for h in HORIZONS:
                y = forward(r, h)
                m, t, n = ic(x, y, h)
                row[f"h={h}"] = f"{m:+.2f} ({t:+.1f})"
                if h in (1, 12):
                    z = pd.concat([x, y], axis=1).dropna()
                    half = z.index[len(z) // 2]
                    m1, t1, _ = ic(x.loc[:half], y.loc[:half], h)
                    m2, t2, _ = ic(x.loc[half:], y.loc[half:], h)
                    row[f"h={h} 1st half"] = f"{m1:+.2f} ({t1:+.1f})"
                    row[f"h={h} 2nd half"] = f"{m2:+.2f} ({t2:+.1f})"
                    row[f"R2 oos h={h}"] = r2_oos(x, y, h)
            row["months"] = n
            rows[(asset, name)] = row
    t = pd.DataFrame(rows).T
    print("RANK IC with the forward excess return (Newey-West t); halves split at each "
          "series' midpoint; R2 oos vs the historical mean, forecasts from year 15")
    print(t.to_string(formatters={"R2 oos h=1": lambda v: "" if pd.isna(v) else f"{v:+.1%}",
                                  "R2 oos h=12": lambda v: "" if pd.isna(v) else f"{v:+.1%}"}))
    print("\n   latest readings")
    for asset, sigs in signals.items():
        print("   " + asset + ": " + ", ".join(
            f"{n} {s.dropna().iloc[-1]:+.3f} ({s.dropna().index[-1]:%Y-%m})"
            for n, s in sigs.items()))


if __name__ == "__main__":
    main()
