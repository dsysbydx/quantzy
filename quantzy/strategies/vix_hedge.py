"""VIX hedge overlay: hold VIXY while the VIX is calm, sell it into a spike.

What it is
    A regime rule, not an optimizer book. The regime switches on when the VIX closes at or
    below ``LOW`` (13) and off when it closes at or above ``HIGH`` (30). While it is on, hold
    ``SIZE`` (0.05x NAV) of VIXY (short-term VIX futures), funded from cash; the holding drifts
    with VIXY's price and is reset to ``SIZE`` when it is more than ``BAND`` (25%) away. It
    runs on top of :mod:`core_multiasset`, which lists it in ``OVERLAYS``.

Why
    Insurance, chosen deliberately: it did **not** pass the research bar (+0.02 on both the
    full-period and the test-half Sharpe; ``research/research_vixy_hedge.py``). Low VIX is not
    cheap (implied variance runs 1.3-1.5x realized at every level, ``research_tail_hedge.py``);
    what the rule buys is a hedge that costs little, because it sells into the spike. It is
    -0.4 correlated with the book and paid in 2015, 2018 and 2020, but does nothing in a slow
    sell-off like 2022 (the book's worst), and lost ~1.7% of NAV a year in 2011-17. The
    thresholds are the best of a 9-cell grid, so expect less than the backtest shows.

Review rule
    Revisit if over about three years it costs more than 1.5% of NAV a year without paying
    off in a spike. A judgement, not a test.

Trading
    The job runs after a close and its orders execute at the next close, so the position held
    from a close was decided on the VIX through the close before it. 5 bp a trade.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

NAME = "vix_hedge"
ASSET = "VIXY"
SIZE = 0.05
LOW, HIGH = 13.0, 30.0
BAND = 0.25
COST_BPS = 5.0
PANEL = "vixy"  # the bundle panel with VIXY's closes


def regime(vix: pd.Series) -> pd.Series:
    """True from a close at or below ``LOW`` until a close at or above ``HIGH``."""
    state, out = False, []
    for x in vix.to_numpy():
        if not np.isnan(x):
            state = True if x <= LOW else False if x >= HIGH else state
        out.append(state)
    return pd.Series(out, index=vix.index, dtype=bool)


def asset_prices(bundle) -> pd.Series:
    return bundle[PANEL].iloc[:, 0]


def _vix(bundle) -> pd.Series:
    return bundle["vix"].iloc[:, 0].ffill()


def rebalance(target: float, current: float) -> float:
    """The weight to hold given the rule's target and the drifted holding."""
    if target > 0 and current > 0 and abs(current - target) <= BAND * target:
        return current
    return target


def target_after_close(day, bundle, current: float = 0.0) -> float:
    """The weight in ``ASSET`` to hold from ``day``'s close: decided on the VIX through the
    close before ``day``; ``current`` is the holding now, kept if inside the band."""
    vix = _vix(bundle)
    vix = vix[vix.index < pd.Timestamp(day).normalize()]
    on = bool(regime(vix).iloc[-1]) if len(vix) else False
    return rebalance(SIZE if on else 0.0, current)


def backtest_returns(bundle) -> pd.Series:
    """The overlay's return over cash on the bundle's dates, after costs."""
    px = asset_prices(bundle)
    r = px.pct_change(fill_method=None).fillna(0.0).to_numpy()
    c = bundle.prices["TBILL"].pct_change(fill_method=None).fillna(0.0).to_numpy()
    # the target set at close i is decided on the VIX at close i-1; it earns day i+1
    target = np.where(regime(_vix(bundle)).shift(1, fill_value=False).to_numpy(), SIZE, 0.0)
    target = np.where(px.notna().to_numpy(), target, 0.0)
    w, out = 0.0, np.zeros(len(r))
    for i in range(len(r)):
        out[i] = w * (r[i] - c[i])
        w *= (1 + r[i]) / (1 + w * r[i] + (1 - w) * c[i])  # drift, as a fraction of NAV
        new = rebalance(target[i], w)
        out[i] -= abs(new - w) * COST_BPS / 1e4
        w = new
    return pd.Series(out, index=bundle.index)
