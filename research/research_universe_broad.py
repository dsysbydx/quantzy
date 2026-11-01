"""Universe: add a broad index -- SPY, or MSCI ACWI -- to the 17?

The same question as ``research_universe_qqq.py`` from the other side. SPY is the sector
SPDRs combined (plus the communication and real-estate names), so it should be spanned
even more fully than QQQ; MSCI World is ~70% US plus developed ex-US, both held. ACWI (all
country, March 2008) stands in for MSCI World (URTH starts 2012). The lessons say new assets
rarely enter a long-only book. **Fixed before any test.**

* ``saved``: the 17.
* ``+ SPY``: 18; equity index class, Sharpe prior 0.30, trend, EWMAC and VRP; 1 bp to trade.
* ``+ ACWI``: 18; as SPY, 3 bp to trade.

``+ SPY`` against ``saved`` over the saved window (split 2017-07-26); ``+ ACWI`` against
``saved`` from the later start ACWI's history allows (split at the same date). Rule: +0.02
Sharpe over the window and in both halves.

**Result (2026-11-01): no change; the optimizer never holds either (weight 0.000).** + SPY,
2008-09 on: 1.211 / 1.371 with overlays, identical to saved; + ACWI, 2009-10 on: 1.269 /
1.444, identical. Not a bug: SPY is in the universe with ``mu`` 4.3% and vol 12.2% on
2015-06-30. Per-asset Sharpe priors make any index lose to its constituents: SPY's ``mu`` is
0.30 x its vol, but a basket of sectors at SPY's weights expects 0.30 x their *average*
vol, which is higher than SPY's vol, so the optimizer rebuilds SPY from the sectors at more
expected return and tilts them besides. Hence QQQ's ~3%, and part of the book's 65% in
sectors: under equal priors, diversification looks like free alpha.

Usage:
    uv run python research/research_universe_broad.py
"""

from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_universe_qqq as uq  # noqa: E402

from quantzy.data import CachedReader, DataBundle, YFinanceReader  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

ADD = {"SPY": 1.0, "ACWI": 3.0}  # ticker -> one-way cost, bp
uq.NEW = list(ADD)
uq.NEW_COST_BPS = dict(ADD)
uq.UNIVERSES = {"saved": core.RISKY, **{f"+ {t}": [*core.RISKY, t] for t in ADD}}


def load() -> DataBundle:
    b = core.load()
    extra = CachedReader(YFinanceReader()).history(list(ADD), start="2007-01-01")
    extra = extra.reindex(b.index)
    px = pd.concat([b.prices, extra], axis=1)
    panels = {}
    for k in b:
        if k == "prices":
            continue
        p = b[k].reindex(columns=px.columns)
        if k in ("vix", "market", "vixy"):
            p[list(ADD)] = np.repeat(b[k].iloc[:, [0]].to_numpy(), len(ADD), axis=1)
        panels[k] = p
    return DataBundle.build(px, **panels)


uq.load = load


def main() -> None:
    pd.set_option("display.width", 220)
    b = load()
    s0 = max(uq.warmup(b, n) for n in ("saved", "+ SPY"))
    s1 = uq.warmup(b, "+ ACWI")
    jobs = [("saved", s0), ("+ SPY", s0), ("saved", s1), ("+ ACWI", s1)]
    with ProcessPoolExecutor(max_workers=4) as pool:
        out = list(pool.map(uq.run_one, jobs))
    for title, part in ((f"1. + SPY, {b.index[s0].date()} on", out[:2]),
                        (f"2. + ACWI, {b.index[s1].date()} on", out[2:])):
        runs = {n: r for n, r, _ in part}
        print(f"\n{title}, split {uq.SPLIT.date()}")
        print(uq.table(runs, uq.SPLIT, b).to_string(float_format="{:.3f}".format))
        w = part[1][2]
        new = [t for t in ADD if t in w.columns][0]
        print(f"   average weight in {new}: {w[new].mean():.3f}; max {w[new].max():.3f}")


if __name__ == "__main__":
    main()
