"""How far is each judgment Sharpe prior from what the asset class actually earned?

``core_multiasset`` sets a constant Sharpe prior per asset class by judgment. In the optimizer
the T-bill sleeve has no view (``mu = 0``), so a prior acts as a Sharpe ratio *over cash*.
This compares each prior with the realized excess-over-T-bill Sharpe on the longest history
that is a fair stand-in for the ETF, and asks whether the gap is bigger than sampling error.

Long proxies (monthly total returns unless noted):

* US sectors: Fama-French 12 industries, value-weighted, 1926+ (Ken French data library).
  XLB Chems, XLE Enrgy, XLF Money, XLI Manuf, XLK BusEq, XLP NoDur, XLU Utils, XLV Hlth,
  XLY Shops. The industry definitions are broader than the sector SPDRs.
* QQQ: ^NDX price only, 1985+ (misses roughly 1% a year of dividends).
* EFA, EEM: Fama-French developed ex US and emerging market factors, 1990+ and 1989+.
* IEF, TLT: a par bond rolled monthly at the 10- and 30-year Treasury yield, 1962+ and 1977+.
  Checked against the overlap with VFITX, VUSTX and the ETFs.
* DBC: S&P GSCI (^SPGSCI), 1984+. GLD: gold futures (GC=F), 2000+. UUP: DXY spot change
  as the excess return, 1971+ (misses the interest-rate carry UUP earns).

The standard error of an annual Sharpe estimate over ``T`` years of roughly independent
returns is ``sqrt((1 + SR^2 / 2) / T)`` (Lo, 2002). A gap of more than about two standard
errors is one the data can see; anything less is noise at that length of history.

**Result (2026-10-16).** Only the equity level stands out: all nine US sectors earned an
excess Sharpe of 0.38-0.51 over 100 years (consumer staples +2.0 SE, health +1.9 SE above
0.30), QQQ 0.57 since 1985, EEM 0.40. Sectors do not differ by more than noise beyond a mild
defensive (low-vol) edge; tech is average over the century. Treasuries (0.20-0.24 since 1962
and 1977) match 0.25 but swing by era (-0.21 in 1960-79, +0.38 in 1980-99). Gold earned 0.60
since 2000 (+1.9 SE) on only 26 years. Commodities (0.09) and the dollar (0.01, before carry)
match their priors.

Usage:
    uv run python examples/research_prior_check.py
"""

from __future__ import annotations

import urllib.request
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.data import CachedReader, YFinanceReader
from quantzy.strategies import core_multiasset as core

FRENCH = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
CACHE = Path.home() / ".cache" / "quantzy" / "french"
FF12 = {"XLB": "Chems", "XLE": "Enrgy", "XLF": "Money", "XLI": "Manuf", "XLK": "BusEq",
        "XLP": "NoDur", "XLU": "Utils", "XLV": "Hlth", "XLY": "Shops"}
ERAS = [("1926", "1959"), ("1960", "1979"), ("1980", "1999"), ("2000", "2026")]


# --- data --------------------------------------------------------------------------------------

def french(name: str) -> pd.DataFrame:
    """A Ken French monthly table in decimal returns, first block of the CSV only."""
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / f"{name}.zip"
    if not path.exists():
        with urllib.request.urlopen(f"{FRENCH}{name}.zip", timeout=60) as r:
            path.write_bytes(r.read())
    text = zipfile.ZipFile(path).read(zipfile.ZipFile(path).namelist()[0]).decode("latin-1")
    lines = text.splitlines()
    head = next(i for i, ln in enumerate(lines) if ln.strip().startswith(","))
    rows = []
    for ln in lines[head + 1:]:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts[0]) != 6 or not parts[0].isdigit():
            break
        rows.append(parts)
    cols = [c.strip() for c in lines[head].split(",")][1:]
    idx = pd.to_datetime([r[0] for r in rows], format="%Y%m").to_period("M").to_timestamp("M")
    df = pd.DataFrame([r[1:] for r in rows], columns=cols, index=idx)
    return df.astype(float) / 100.0


def monthly_returns(px: pd.Series) -> pd.Series:
    return px.dropna().resample("ME").last().pct_change().dropna()


def par_bond_returns(yld: pd.Series, maturity: float) -> pd.Series:
    """Monthly return of a par bond bought at last month's yield and repriced at this month's."""
    y = yld.dropna().resample("ME").last() / 100.0
    y0, y1 = y.shift(1), y
    n = maturity - 1 / 12
    price = y0 / y1 * (1 - (1 + y1) ** -n) + (1 + y1) ** -n
    return (price - 1 + y0 / 12).dropna()


def load() -> tuple[dict[str, dict[str, pd.Series]], pd.Series]:
    """Per prior ticker, named monthly total-return series; and the monthly T-bill return."""
    yf = CachedReader(YFinanceReader())
    tick = [*core.RISKY, "^NDX", "^TNX", "^TYX", "^IRX", "VFITX", "VUSTX", "^SPGSCI",
            "GC=F", "DX-Y.NYB"]
    px = yf.history(tick, start="1900-01-01")
    irx = px["^IRX"].dropna().resample("ME").last() / 100.0
    rf_yahoo = (irx.shift(1) / 12).dropna()
    rf_yahoo.index = rf_yahoo.index.to_period("M").to_timestamp("M")
    ff = french("F-F_Research_Data_Factors_CSV")
    rf = ff["RF"].combine_first(rf_yahoo)  # French's T-bill to its last month, then ^IRX

    ind = french("12_Industry_Portfolios_CSV")
    dev = french("Developed_ex_US_3_Factors_CSV")
    emg = french("Emerging_5_Factors_CSV")
    s: dict[str, dict[str, pd.Series]] = {t: {} for t in core.RISKY}
    for t, col in FF12.items():
        s[t]["FF industry " + col] = ind[col]
    s["QQQ"]["^NDX price only"] = monthly_returns(px["^NDX"])
    s["EFA"]["FF developed ex US"] = dev["Mkt-RF"] + dev["RF"]
    s["EEM"]["FF emerging"] = emg["Mkt-RF"] + emg["RF"]
    s["IEF"]["10y par bond"] = par_bond_returns(px["^TNX"], 10)
    s["IEF"]["VFITX fund"] = monthly_returns(px["VFITX"])
    s["TLT"]["30y par bond"] = par_bond_returns(px["^TYX"], 30)
    s["TLT"]["VUSTX fund"] = monthly_returns(px["VUSTX"])
    s["DBC"]["S&P GSCI"] = monthly_returns(px["^SPGSCI"])
    s["GLD"]["gold futures"] = monthly_returns(px["GC=F"])
    # A spot FX change is already an excess return (a futures position earns T-bills on the
    # cash on top), so add T-bills back: "excess" then equals the spot change, before carry.
    dxy = monthly_returns(px["DX-Y.NYB"])
    dxy.index = dxy.index.to_period("M").to_timestamp("M")
    s["UUP"]["DXY spot, no carry"] = (dxy + rf.reindex(dxy.index)).dropna()
    for t in core.RISKY:
        s[t][f"{t} ETF"] = monthly_returns(px[t])
    for d in s.values():  # month-end index everywhere
        for k in d:
            d[k].index = d[k].index.to_period("M").to_timestamp("M")
    return s, rf


# --- statistics --------------------------------------------------------------------------------

def sharpe(r: pd.Series, rf: pd.Series) -> dict[str, float]:
    x = pd.concat([r, rf.rename("rf")], axis=1, join="inner").dropna()
    ex = x.iloc[:, 0] - x["rf"]
    years = len(x) / 12
    sr_ex = ex.mean() / ex.std() * np.sqrt(12)
    return {"start": x.index[0].strftime("%Y-%m"), "years": years,
            "ret": (1 + x.iloc[:, 0]).prod() ** (1 / years) - 1,
            "vol": x.iloc[:, 0].std() * np.sqrt(12),
            "SR total": x.iloc[:, 0].mean() / x.iloc[:, 0].std() * np.sqrt(12),
            "SR excess": sr_ex, "SE": np.sqrt((1 + sr_ex ** 2 / 2) / years)}


def main() -> None:
    pd.set_option("display.width", 250)
    series, rf = load()
    cls = {t: c for c, ts in core.CLASSES.items() for t in ts}
    rows = {}
    for t in core.RISKY:
        for name, r in series[t].items():
            st = sharpe(r, rf)
            gap = st["SR excess"] - core.PRIOR_SR[t]
            rows[(t, name)] = {"class": cls[t], "prior": core.PRIOR_SR[t], **st,
                               "gap": gap, "gap / SE": gap / st["SE"]}
    t = pd.DataFrame(rows).T
    fmt = {"prior": "{:.2f}", "years": "{:.0f}", "ret": "{:.1%}", "vol": "{:.1%}",
           "SR total": "{:+.2f}", "SR excess": "{:+.2f}", "SE": "{:.2f}", "gap": "{:+.2f}",
           "gap / SE": "{:+.1f}"}
    print("1. PRIOR vs REALIZED SHARPE (excess over T-bills; 'SR total' uses rf = 0 for "
          "reference). gap = realized excess SR - prior; |gap / SE| > 2 is a clear gap")
    print(t.to_string(formatters={k: v.format for k, v in fmt.items()}))

    print("\n2. REALIZED EXCESS SHARPE BY ERA, longest series per prior")
    era = {}
    for tk in core.RISKY:
        name, r = max(series[tk].items(), key=lambda kv: len(kv[1].dropna()))
        row = {"series": name, "prior": core.PRIOR_SR[tk]}
        for a, b in ERAS:
            x = r.loc[a:b]
            row[f"{a}-{b[2:]}"] = (sharpe(x, rf)["SR excess"] if len(x.dropna()) >= 60
                                   else np.nan)
        era[tk] = row
    print(pd.DataFrame(era).T.to_string(float_format="{:+.2f}".format))

    print("\n3. PROXY CHECK: correlation and annual return gap with the ETF on the overlap")
    chk = {}
    for tk in core.RISKY:
        etf = series[tk][f"{tk} ETF"]
        for name, r in series[tk].items():
            if name.endswith("ETF"):
                continue
            x = pd.concat([etf, r], axis=1, join="inner").dropna()
            chk[(tk, name)] = {"months": len(x), "corr": x.corr().iloc[0, 1],
                               "ret gap (proxy - ETF)": (x.iloc[:, 1] - x.iloc[:, 0]).mean() * 12}
    print(pd.DataFrame(chk).T.to_string(formatters={"months": "{:.0f}".format,
                                                    "corr": "{:.2f}".format,
                                                    "ret gap (proxy - ETF)": "{:+.1%}".format}))


if __name__ == "__main__":
    main()
