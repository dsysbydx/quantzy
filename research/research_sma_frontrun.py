"""Front-running 200-day SMA crosses with the known drop-off prices.

Idea (the user's, 2026-10-31): ``SMA_200(t+1) = SMA_200(t) + (P(t+1) - P(t-199)) / 200``,
and ``P(t-199)`` is known today, so tomorrow's SMA is known up to tomorrow's price; with a
vol estimate, so is the chance price crosses it. If enough money trades the 200-day cross,
its flow could be front-run. Only an SMA has this property; an EMA has no drop-off. An
exploratory check, not a pre-registered candidate:

1. *Free crosses*: days within one daily vol of the SMA where price crosses tomorrow even
   if it does not move, because of the drop-off alone; next-day and next-5-day return
   against other days on the same side and as near.
2. *Do crosses move prices?* Returns around actual up- and down-crosses: the 5 days before,
   the next day (Welch t against all days), the next 10.
3. *Flat-price cross within 10 days*: with price held flat, the SMA's path is known (every
   price that leaves the window is in the data), so "the SMA crosses a flat price within 10
   days" is a forecast; its 10-day return against days as near the SMA, overall and within
   distance buckets (the same-distance difference).

On ^GSPC (1950 on, and 1990 on) and the 17 ETFs pooled (1999 on).

**Result (2026-10-31): no alpha; not taken further.** (1) The drop-off moves the SMA by about
1/200 of a 200-day return, ~0.1% against ~1% daily vol, so free crosses almost never happen:
5 days on ^GSPC in 76 years, 1 on SPY. (2) Crosses follow big moves (+103 to +121 bp over the
5 days before an up-cross, -156 to -158 bp before a down-cross) and then *reverse*: 17 ETFs
next day -3.4 bp after an up-cross (t -2.5), +9.1 bp after a down-cross (t +1.7); ^GSPC since
1990 -1.0 / +16.8 bp. That is the known short-term reversal (``research_short_reversal``),
not trend followers pushing through the line. (3) The 10-day flat-price cross has no
consistent sign: same-distance excess +24 bp before predicted up-crosses and +18.5 bp before
predicted down-crosses on the 17 ETFs (^GSPC since 1990: +96 bp on 22 events, t 1.8; +16 bp).
Trend money is spread over many signals and days, and the most-watched line is the easiest
to arbitrage.

Usage:
    uv run python research/research_sma_frontrun.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

from quantzy.data import CachedReader, YFinanceReader
from quantzy.strategies import core_multiasset as core

N, K = 200, 10  # SMA length; horizon of the flat-price forecast


def welch(a: pd.Series, b: pd.Series) -> float:
    return float(stats.ttest_ind(a, b, equal_var=False).statistic) if len(a) > 1 else np.nan


def panel(px: pd.Series) -> pd.DataFrame:
    """Per day: actual crosses, free crosses, flat-price crosses within K days, and returns."""
    px = px.dropna()
    sma = px.rolling(N).mean()
    vol = px.pct_change().rolling(60).std()
    tomorrow_flat = sma + (px - px.shift(N - 1)) / N  # tomorrow's SMA if price does not move
    drops = sum(px.shift(N - j) for j in range(1, K + 1))  # the K prices that leave next
    in_k_flat = sma + (K * px - drops) / N  # the SMA in K days if price does not move
    return pd.DataFrame({
        "z": (px / sma - 1) / vol,
        "cross_up": (px > sma) & (px.shift(1) <= sma.shift(1)),
        "cross_dn": (px < sma) & (px.shift(1) >= sma.shift(1)),
        "free_up": (px < sma) & (px > tomorrow_flat),
        "free_dn": (px > sma) & (px < tomorrow_flat),
        "flat_up": (px < sma) & (px > in_k_flat),
        "flat_dn": (px > sma) & (px < in_k_flat),
        "r1": px.shift(-1) / px - 1,
        "r5": px.shift(-6) / px.shift(-1) - 1,
        "rK": px.shift(-K) / px - 1,
        "pre5": px / px.shift(5) - 1,
    }).dropna()


def report(df: pd.DataFrame, name: str) -> None:
    print(f"\n{name}: {len(df)} days, unconditional next-day {df.r1.mean() * 1e4:+.2f} bp")
    for flag, side in (("free_up", df.z < 0), ("free_dn", df.z > 0)):
        near = df[side & (df.z.abs() < 1)]
        a, b = near[near[flag]], near[~near[flag]]
        print(f"   1. {flag}: {len(a)} days vs {len(b)} as near; next day {a.r1.mean() * 1e4:+.1f}"
              f" vs {b.r1.mean() * 1e4:+.1f} bp (t {welch(a.r1, b.r1):+.1f})")
    for flag in ("cross_up", "cross_dn"):
        e = df[df[flag]]
        print(f"   2. {flag}: n {len(e)}; prior 5d {e.pre5.mean() * 1e4:+.1f} bp; next day "
              f"{e.r1.mean() * 1e4:+.1f} bp (t vs all {welch(e.r1, df.r1):+.1f}); next {K}d "
              f"{e.rK.mean() * 1e4:+.1f} bp")
    for flag, side in (("flat_up", df.z < 0), ("flat_dn", df.z > 0)):
        near = df[side & (df.z.abs() < 2)]
        a, b = near[near[flag]], near[~near[flag]]
        diffs = [(g[g[flag]].rK.mean() - g[~g[flag]].rK.mean(), int(g[flag].sum()))
                 for _, g in near.groupby(pd.cut(near.z, np.linspace(-2, 2, 9)), observed=True)
                 if g[flag].sum() > 10 and (~g[flag]).sum() > 10]
        same = sum(d * n for d, n in diffs) / max(sum(n for _, n in diffs), 1)
        print(f"   3. {flag} within {K}d: {len(a)} vs {len(b)} as near; next {K}d "
              f"{a.rK.mean() * 1e4:+.1f} vs {b.rK.mean() * 1e4:+.1f} bp "
              f"(t {welch(a.rK, b.rK):+.1f}); same-distance diff {same * 1e4:+.1f} bp")


def main() -> None:
    gspc = CachedReader(YFinanceReader()).history(["^GSPC"], start="1950-01-01")["^GSPC"]
    report(panel(gspc), "^GSPC 1950-2026")
    report(panel(gspc.loc["1990":]), "^GSPC 1990-2026")
    etf = core.load().prices[core.RISKY]
    report(pd.concat([panel(etf[t]) for t in etf.columns]), "17 ETFs pooled")


if __name__ == "__main__":
    main()
