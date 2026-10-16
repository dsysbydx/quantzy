"""Does US equity valuation forecast excess returns? Shiller data, 1871-2024.

The saved book gives US equities a constant Sharpe prior of 0.30. A forward-looking prior
would move with valuation. Before building one, this asks the prior question on the longest
data there is: does a valuation measure known at month ``t`` forecast the S&P's return over
T-bills in the following 1, 12, 60 and 120 months better than a constant does?

**Fixed before any test.**

Measures (annual units), with earnings and dividends lagged 3 months for publication:

* ``CAEY``: 1 / CAPE, the cyclically adjusted earnings yield (10-year average real earnings
  over price). A real expected return.
* ``ECY``: CAEY minus the real 10-year yield (GS10 minus trailing 10-year CPI inflation),
  Shiller's excess CAPE yield: equities against bonds.
* ``ERP``: CAEY plus trailing 10-year inflation minus the T-bill rate: an expected nominal
  equity return over cash, the units the book's prior is in.
* ``DY``: trailing dividend yield, for comparison.

Forms: the level, and the deviation from its own EMA over 60, 120 and 200 months.

Target: the S&P total return minus T-bills, compounded over ``h`` months, starting the month
*after* the next one: Shiller's prices are monthly averages, so the next month's return
partly overlaps the month the signal is measured in.

Statistics, from 1926 (Ken French T-bills):

* In sample: rank correlation of the measure with the forward return, with a Newey-West t
  (``h`` lags) on the slope; for 1926-1974 and 1975-2024 as well as all.
* Out of sample (Campbell and Thompson, 2008): each month, regress forward returns on the
  measure using only pairs that had fully happened by then, forecast, and compare the
  squared error with that of the historical mean. ``R2_OOS > 0`` means the measure beat the
  mean. Forecasts start in 1950 (24 years of training) and are also scored from 1990.

**Result (2026-10-17): informative but slow, not used in the book yet.** In sample every
measure points the right way, strongest ERP and ECY (12-month rank IC +0.34, t 4.2; 10-year
+0.73), but mostly before 1975 (12-month +0.09 since). Out of sample, levels drift: CAEY loses
to the mean everywhere (-29% at 12 months); ERP has +49% at 10 years from 1950 but -9% at 12
months from 1990. Deviations from a 200-month EMA hold up best since 1990 at 5-10 years
(ERP +34%, DY +22% at 10 years). At 1-12 months every measure is near zero (R^2 <= 3%). In
2024-09 ERP was 0.9%, near its 1926+ low.

Usage:
    uv run --with xlrd python examples/research_shiller_valuation.py
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd
from research_prior_check import french

from quantzy.research import newey_west_t

SHILLER = ("https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab25-4763ac982e53/downloads/"
           "ie_data.xls")
CACHE = Path.home() / ".cache" / "quantzy" / "shiller" / "ie_data.xls"
LAG = 3  # months of publication delay for earnings and dividends
HORIZONS = (1, 12, 60, 120)
EMAS = (60, 120, 200)
OOS_START, OOS_LATE = "1950-01", "1990-01"


def load() -> pd.DataFrame:
    """Monthly Shiller data with the measures, total returns and T-bills (annual units)."""
    if not CACHE.exists():
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(SHILLER, timeout=60) as r:
            CACHE.write_bytes(r.read())
    raw = pd.read_excel(CACHE, sheet_name="Data", header=7)
    raw = raw[pd.to_numeric(raw["Date"], errors="coerce").notna()]
    yr = raw["Date"].astype(float)
    year, month = yr.astype(int), ((yr - yr.astype(int)) * 100).round().astype(int)
    idx = pd.to_datetime(dict(year=year, month=month, day=1)) + pd.offsets.MonthEnd(0)
    d = pd.DataFrame({"P": raw["P"].to_numpy(float), "D": raw["D"].to_numpy(float),
                      "E": raw["E"].to_numpy(float), "CPI": raw["CPI"].to_numpy(float),
                      "GS10": raw["Rate GS10"].to_numpy(float) / 100,
                      "real_tr": raw.iloc[:, 9].to_numpy(float)}, index=idx)
    real_e = (d["E"] / d["CPI"]).shift(LAG)
    real_p = d["P"] / d["CPI"]
    d["CAEY"] = real_e.rolling(120, min_periods=120).mean() / real_p
    infl10 = (d["CPI"] / d["CPI"].shift(120)) ** (1 / 10) - 1
    rf = french("F-F_Research_Data_Factors_CSV")["RF"].reindex(d.index)
    d["rf"] = rf
    d["ECY"] = d["CAEY"] - (d["GS10"] - infl10)
    d["ERP"] = d["CAEY"] + infl10 - rf * 12
    d["DY"] = d["D"].shift(LAG) / d["P"]
    nominal = d["real_tr"] / d["real_tr"].shift(1) * d["CPI"] / d["CPI"].shift(1) - 1
    d["excess"] = np.log1p(nominal) - np.log1p(rf)
    return d


def forward(excess: pd.Series, h: int) -> pd.Series:
    """Log excess return over months t+2 .. t+1+h, annualized, aligned at t."""
    total = excess.rolling(h).sum()
    return total.shift(-(h + 1)) * 12 / h


def measures(d: pd.DataFrame) -> dict[str, pd.Series]:
    out = {}
    for m in ("CAEY", "ECY", "ERP", "DY"):
        x = d[m]
        out[f"{m} level"] = x
        for n in EMAS:
            out[f"{m} - EMA{n}"] = x - x.ewm(span=n, adjust=False, min_periods=n).mean()
    return out


def in_sample(x: pd.Series, y: pd.Series, h: int, period: slice) -> str:
    z = pd.concat([x, y], axis=1).loc[period].dropna()
    if len(z) < 60:
        return ""
    rho = z.iloc[:, 0].rank().corr(z.iloc[:, 1].rank())
    xs = (z.iloc[:, 0] - z.iloc[:, 0].mean()) / z.iloc[:, 0].std()
    prod = xs * (z.iloc[:, 1] - z.iloc[:, 1].mean())
    return f"{rho:+.2f} ({newey_west_t(prod, max(h, 12)):+.1f})"


def r2_oos(x: pd.Series, y: pd.Series, h: int) -> tuple[float, float]:
    """Campbell-Thompson out-of-sample R^2 from OOS_START and from OOS_LATE."""
    z = pd.concat([x.rename("x"), y.rename("y")], axis=1).dropna()
    dates = z.index[z.index >= pd.Timestamp(OOS_START)]
    err_m, err_b, when = [], [], []
    for t in dates:
        # pairs whose forward window (t' + 2 .. t' + 1 + h) ended by month t
        train = z.loc[: t - pd.offsets.MonthEnd(h + 1)]
        if len(train) < 120:
            continue
        b, a = np.polyfit(train["x"], train["y"], 1)
        f, m = a + b * z.at[t, "x"], train["y"].mean()
        err_m.append((z.at[t, "y"] - f) ** 2)
        err_b.append((z.at[t, "y"] - m) ** 2)
        when.append(t)
    e = pd.DataFrame({"m": err_m, "b": err_b}, index=when)
    late = e.loc[OOS_LATE:]
    return 1 - e["m"].sum() / e["b"].sum(), 1 - late["m"].sum() / late["b"].sum()


def main() -> None:
    pd.set_option("display.width", 300)
    d = load()
    last = d["excess"].last_valid_index()
    print(f"Shiller data {d.index[0]:%Y-%m} .. {d.index[-1]:%Y-%m}; tests from 1926 "
          f"(T-bills); returns to {last:%Y-%m}")
    print("\n   the measures now and in history (annual units)")
    snap = d[["CAEY", "ECY", "ERP", "DY"]]
    print(pd.concat([snap.loc["1926":].describe().loc[["mean", "min", "max"]],
                     snap.dropna().iloc[[-1]].rename(lambda t: f"last {t:%Y-%m}")])
          .to_string(float_format="{:.3f}".format))
    xs = measures(d)
    ys = {h: forward(d["excess"], h) for h in HORIZONS}

    print("\n1. IN SAMPLE: rank correlation with the forward excess return (Newey-West t on "
          "the slope, h lags)")
    rows = {}
    for name, x in xs.items():
        row = {}
        for h in HORIZONS:
            row[f"h={h} all"] = in_sample(x, ys[h], h, slice("1926", None))
            if h in (12, 60):
                row[f"h={h} 1926-74"] = in_sample(x, ys[h], h, slice("1926", "1974"))
                row[f"h={h} 1975+"] = in_sample(x, ys[h], h, slice("1975", None))
        rows[name] = row
    print(pd.DataFrame(rows).T.to_string())

    print(f"\n2. OUT OF SAMPLE R^2 against the historical mean (Campbell-Thompson), forecasts "
          f"from {OOS_START} | from {OOS_LATE}")
    rows = {}
    for name, x in xs.items():
        row = {}
        for h in HORIZONS:
            a, b = r2_oos(x, ys[h], h)
            row[f"h={h}"] = f"{a:+.1%} | {b:+.1%}"
        rows[name] = row
    print(pd.DataFrame(rows).T.to_string())


if __name__ == "__main__":
    main()
