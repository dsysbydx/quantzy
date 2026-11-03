"""Pre-inception history for the core_multiasset ETFs, from index funds, indexes and futures.

The book's own data starts on 2008-09-10, when the youngest ETF (UUP, 2007-03) has warmed
up. :func:`load` extends each ETF back with the daily returns of what tracked the same
exposure before it listed, all free (Yahoo, Ken French), so the saved book can be run from
late 1995; EEM's proxy (VEIEX, 1994-05) is the binding one. The strategy itself is
unchanged: only its data reader is.

    ETF      proxy, chained in order
    XL*      Ken French 12 industries, daily value-weighted (before 1998-12)
    QQQ      ^NDX, price only (the index yielded well under 1%)
    EFA      60% VEURX + 40% VPACX, Vanguard's Europe and Pacific index funds
    EEM      VEIEX, Vanguard's emerging markets index fund
    IEF/TLT  VFITX / VUSTX, Vanguard's intermediate / long Treasury funds, their return over
             T-bills scaled 1.33x / 1.11x to the ETFs' beta (the funds run shorter duration)
    GLD      GC=F gold futures (from 2000-08); before them, gold miners (VGPMX) hedged of
             the S&P 500, scaled to gold's vol and demeaned, fitted on 2000-2010: the
             weakest proxy (correlation 0.57 with GLD), which assumes gold had no drift
             before 2000 (it fell ~28% from late 1995; the miners alone imply ~63%)
    DBC      ^SPGSCI plus T-bills (the index has no collateral return; DBC holds bills)
    UUP      DX-Y.NYB, the dollar index itself (no rate differential)
    SPY      VFINX, for the VRP and the pension overlay

Fund proxies are net of their fees (0.2-0.4%), indexes gross. ``research/
research_proxy_history.py`` measures each one against its ETF where both trade.
"""

from __future__ import annotations

import io
import urllib.request
import zipfile
from functools import cache
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.data.splice import ReturnSource, SplicedReader
from quantzy.strategies import core_multiasset as core

START = "1990-01-02"  # the VIX's first day; nothing else binds before VEIEX
FRENCH = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
FRENCH_FILE = "12_Industry_Portfolios_daily_CSV"
FRENCH_CACHE = Path(".cache/french")

#: Sector SPDR -> Ken French 12-industry weights.
SECTORS = {"XLB": {"Chems": 1.0}, "XLE": {"Enrgy": 1.0}, "XLF": {"Money": 1.0},
           "XLI": {"Manuf": 1.0}, "XLK": {"BusEq": 1.0}, "XLP": {"NoDur": 1.0},
           "XLU": {"Utils": 1.0}, "XLV": {"Hlth": 1.0}, "XLY": {"Shops": 0.75, "Durbl": 0.25}}
#: The years over which the gold-miner fallback is fitted to gold futures.
GOLD_FIT = ("2000-09-01", "2010-12-31")


@cache
def _yahoo(tickers: tuple[str, ...]) -> pd.DataFrame:
    return CachedReader(YFinanceReader()).history(list(tickers), start=START)


def yahoo(weights: dict[str, float], plus_cash: bool = False,
          excess_scale: float = 1.0) -> ReturnSource:
    """Daily returns of a daily-rebalanced mix of Yahoo series.

    ``plus_cash`` adds T-bills (for an index with no collateral return); ``excess_scale``
    scales the return over T-bills (for a fund with a different duration from the ETF's).
    """
    def source(start, end) -> pd.Series:
        px = _yahoo(tuple(sorted({*weights, "^IRX"})))
        r = px[list(weights)].pct_change(fill_method=None)
        out = sum(r[t] * w for t, w in weights.items())
        out = out[r.notna().all(axis=1)]
        cash = _cash(px["^IRX"]).reindex(out.index).fillna(0.0)
        if excess_scale != 1.0:
            out = cash + excess_scale * (out - cash)
        return out + cash if plus_cash else out
    return source


def _cash(irx: pd.Series) -> pd.Series:
    """The T-bill sleeve's daily return, as :func:`core.load` compounds it."""
    return ((1.0 + irx.ffill() / 100.0) ** (1.0 / TRADING_DAYS) - 1.0).shift(1)


@cache
def french_industries() -> pd.DataFrame:
    """Ken French's 12 industries, daily value-weighted returns in decimals."""
    FRENCH_CACHE.mkdir(parents=True, exist_ok=True)
    path = FRENCH_CACHE / f"{FRENCH_FILE}.zip"
    if not path.exists():
        with urllib.request.urlopen(f"{FRENCH}{FRENCH_FILE}.zip", timeout=60) as r:
            path.write_bytes(r.read())
    z = zipfile.ZipFile(path)
    lines = z.read(z.namelist()[0]).decode("latin-1").splitlines()
    head = next(i for i, ln in enumerate(lines) if ln.strip().startswith(","))
    block = [lines[head]]
    for ln in lines[head + 1:]:
        if not ln[:8].strip().isdigit():
            break  # the value-weighted block ends where the equal-weighted one begins
        block.append(ln)
    df = pd.read_csv(io.StringIO("\n".join(block)), index_col=0, skipinitialspace=True)
    df.index = pd.to_datetime(df.index.astype(str), format="%Y%m%d")
    df.columns = df.columns.str.strip()
    return df.replace([-99.99, -999], np.nan) / 100.0


def french(weights: dict[str, float]) -> ReturnSource:
    def source(start, end) -> pd.Series:
        ind = french_industries()
        return sum(ind[k] * w for k, w in weights.items())
    return source


def gold_miners(start, end) -> pd.Series:
    """Gold before futures: VGPMX hedged of VFINX, fitted to GC=F and scaled to its vol.

    Demeaned over the years it fills, before GC=F: miners lost far more than gold did, and
    a flat drift is the assumption that uses no hindsight.
    """
    px = _yahoo(("GC=F", "VFINX", "VGPMX"))
    r = px.pct_change(fill_method=None)
    fit = r.loc[GOLD_FIT[0]:GOLD_FIT[1]].dropna()
    x = fit[["VGPMX", "VFINX"]].to_numpy()
    beta, *_ = np.linalg.lstsq(x, fit["GC=F"].to_numpy(), rcond=None)
    est = (r["VGPMX"] * beta[0] + r["VFINX"] * beta[1]).dropna()
    est = est * (fit["GC=F"].std() / est.loc[fit.index].std())
    before = est.index < px["GC=F"].first_valid_index()
    return est - est[before].mean()


PROXIES: dict[str, list[ReturnSource]] = {
    **{t: [french(w)] for t, w in SECTORS.items()},
    "QQQ": [yahoo({"^NDX": 1.0})],
    "EFA": [yahoo({"VEURX": 0.6, "VPACX": 0.4})],
    "EEM": [yahoo({"VEIEX": 1.0})],
    "IEF": [yahoo({"VFITX": 1.0}, excess_scale=1.33)],
    "TLT": [yahoo({"VUSTX": 1.0}, excess_scale=1.11)],
    "GLD": [yahoo({"GC=F": 1.0}), gold_miners],
    "DBC": [yahoo({"^SPGSCI": 1.0}, plus_cash=True)],
    "UUP": [yahoo({"DX-Y.NYB": 1.0})],
    "SPY": [yahoo({"VFINX": 1.0})],
}

#: What each chain is, for display, in chain order.
LABELS: dict[str, list[str]] = {
    "XLB": ["French industry: chemicals"], "XLE": ["French industry: energy"],
    "XLF": ["French industry: money"], "XLI": ["French industry: manufacturing"],
    "XLK": ["French industry: business equipment"], "XLP": ["French industry: non-durables"],
    "XLU": ["French industry: utilities"], "XLV": ["French industry: health"],
    "XLY": ["French industries: 75% shops, 25% durables"], "QQQ": ["^NDX, price index"],
    "EFA": ["60% VEURX + 40% VPACX"], "EEM": ["VEIEX"],
    "IEF": ["VFITX, excess return x 1.33"], "TLT": ["VUSTX, excess return x 1.11"],
    "GLD": ["GC=F gold futures", "Gold miners (VGPMX) hedged, vol-scaled, demeaned"],
    "DBC": ["^SPGSCI + T-bills"], "UUP": ["DX-Y.NYB dollar index"], "SPY": ["VFINX"],
}

#: The first day every ETF in the book trades on its own prices (UUP's first).
ACTUAL_FROM = "2007-03-01"


def load(start: str = START) -> DataBundle:
    """The core book's data with every ETF extended back by :data:`PROXIES`."""
    return core.load(start=start, reader=SplicedReader(CachedReader(YFinanceReader()), PROXIES))
