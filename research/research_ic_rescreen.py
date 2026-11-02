"""Re-screen the cross-sectional signals with the factor return, not only the IC.

``research_fast_sleeves.py`` found relative sector momentum's daily IC (t 3.6 since 2008) sits
on calm days: its factor return -- what the score's portfolio earns -- had t 0.6. The
cross-sectional IC divides each day by that day's realized dispersion; the factor return
(``quantzy.research.factor_return``, a per-date Fama-MacBeth slope) does not. The pooled
time-series IC used for trend, EWMAC, VRP, dollar value and carry is not affected (it
standardizes by constant per-asset statistics). So this re-checks the cross-sectional
signals SYNTHESIS.md leaned on, with the definitions of the studies that found them:
12-1 month momentum, 5-day and 21-day reversal, on the 9 sector SPDRs plus QQQ and on the 28
country markets. Signals through t-1. Rank IC and rank-score factor return, Newey-West t
(lags = max(5, 2 x horizon)), all dates and since 2008-09 and 2017. An exploratory check of
earlier claims: no adoption follows from it directly.

**Result (2026-11-02): the strong cross-sectional ICs do not survive as payoffs.** 12-1
momentum: rank IC t 4.8 (sectors) and 4.0 (countries) at 1 day, still 3.0 / 2.7 since 2017,
but its factor return is t 0.7 / 0.5 overall, -0.2 to 1.0 in every split -- an artifact of
equal-weighting days in both universes. 5-day reversal is real by both measures but has
decayed: countries factor return t 6.7 overall, 3.3 since 2008-09, 0.9 since 2017; sectors
3.0 / 1.2 / 0.4. 21-day reversal has neither (|t| <= 1.6), unlike the 23-country universe of
``research_short_reversal`` (net Sharpe 0.34 there). So idea 20 has no live inputs, and idea
19 would carry nothing for now; the dividend calendar and Halloween results were already
portfolio returns, not ICs.

Usage:
    uv run python research/research_ic_rescreen.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from research_country_seasonality import MARKETS  # noqa: E402

from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.research import factor_return, information_coefficient, newey_west_t  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

SECTORS = [*core.CLASSES["US sectors"], "QQQ"]
SIGNALS = {"12-1 momentum": (None, (1, 5, 21)), "5-day reversal": (5, (1, 5)),
           "21-day reversal": (21, (1, 5, 21))}


def score(px: pd.DataFrame, name: str) -> pd.DataFrame:
    lag = px.shift(1)  # closes through the day before
    if name == "12-1 momentum":
        return lag.shift(21) / lag.shift(252) - 1
    n = SIGNALS[name][0]
    return -(lag / lag.shift(n) - 1)


def fmt(x: pd.Series, h: int) -> str:
    x = x.dropna()
    return f"{x.mean():+.4f} ({newey_west_t(x, max(5, 2 * h)):+.1f})"


def main() -> None:
    pd.set_option("display.width", 240)
    r = CachedReader(YFinanceReader())
    universes = {"sectors": r.history(SECTORS, start="1998-01-01"),
                 "countries": r.history(MARKETS, start="1995-01-01")}
    rows = {}
    for uni, px in universes.items():
        px = px[px.notna().sum(axis=1) >= 5]
        for name, (_, horizons) in SIGNALS.items():
            s = score(px, name).where(px.notna())
            for h in horizons:
                ic = information_coefficient(s, px, horizon=h, method="spearman", min_names=5)
                fr = factor_return(s, px, horizon=h, min_names=5) * 1e4 / h  # bp a day
                rows[(uni, name, f"{h}d")] = {
                    "rank IC all": fmt(ic, h), "IC since 2008-09": fmt(ic.loc["2008-09":], h),
                    "IC since 2017": fmt(ic.loc["2017":], h),
                    "factor ret bp/day all": fmt(fr, h),
                    "factor since 2008-09": fmt(fr.loc["2008-09":], h),
                    "factor since 2017": fmt(fr.loc["2017":], h)}
    out = pd.DataFrame(rows).T
    out.index.names = ["universe", "signal", "horizon"]
    print("RANK IC and FACTOR RETURN (bp a day per unit rank exposure), mean (Newey-West t)")
    print(out.to_string())


if __name__ == "__main__":
    main()
