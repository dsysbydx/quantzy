"""VIX hedge overlay: hold VIXY while the VIX is calm or the book is heavy in equities.

What it is
    A rule, not an optimizer book. Two parts, the larger target wins:

    * *Regime* (2026-10-19). On when the VIX closes at or below ``LOW`` (13), off when it
      closes at or above ``HIGH`` (30). While on, ``SIZE`` (0.05x NAV) of VIXY.
    * *Exposure* (2026-11-07, ``research_exposure_controls.py``, "H2"). While the VIX's last
      close is below ``HIGH``, ``EXPOSURE_RATE x max(E - EXPOSURE_FLOOR, 0)`` of VIXY, where
      ``E`` is the core book's equity weight as held: zero at 75% of capital, 0.05x at 125%.

    VIXY (short-term VIX futures) is funded from cash; the holding drifts with its price and
    is reset to the target when it is more than ``BAND`` (25%) away. Nothing new is bought
    once the VIX closes at or above ``HIGH`` (it sells into the spike). It runs on top of
    :mod:`core_multiasset`, which lists it in ``OVERLAYS``.

Why
    Insurance, chosen deliberately: it did **not** pass the research bar (+0.02 on both the
    full-period and the test-half Sharpe; ``research/research_vixy_hedge.py``). Low VIX is not
    cheap (implied variance runs 1.3-1.5x realized at every level, ``research_tail_hedge.py``);
    what the rule buys is a hedge that costs little, because it sells into the spike. It is
    -0.4 correlated with the book and paid in 2015, 2018 and 2020, but does nothing in a slow
    sell-off like 2022 (the book's worst), and lost ~1.7% of NAV a year in 2011-17. The
    thresholds are the best of a 9-cell grid, so expect less than the backtest shows.

    The exposure part was added after 2026-11-05 (-4.41%, QQQ -6.5%, VIX 16.6 -> 28.2), when
    the book held 125% of capital in equities and the regime part was off: the VIX had not
    closed at 13 or below during the build-up. It arms on the book's exposure, not on calm.
    Over 2008-2026 it costs ~0.03% of NAV a year more than the regime part alone (0.37%
    against 0.34%), Sharpe 1.408 against 1.399 (train 1.412 both, test 1.409 against 1.394);
    it cut 2026-11-05 from -4.4% to -3.2% and 2026's drawdown from -6.4% to -5.3%. It did not
    change the backtest's worst drawdown or week: the book rarely held this much equity
    before 2026. Untested before 2011 (no VIXY).

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

from quantzy.returns import holding_returns

NAME = "vix_hedge"
ASSET = "VIXY"
SIZE = 0.05
LOW, HIGH = 13.0, 30.0
EXPOSURE_FLOOR, EXPOSURE_RATE = 0.75, 0.10  # VIXY = 0.10 x (equity weight - 0.75)
USES_EQUITY = True  # callers pass the core's equity weight (see target_after_close)
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


def target(regime_on: bool, last_vix: float, equity: float) -> float:
    """The rule's target: the regime's ``SIZE``, or the exposure part if larger; the exposure
    part buys nothing once the last VIX close is at or above ``HIGH``."""
    t = SIZE if regime_on else 0.0
    if np.isfinite(last_vix) and last_vix < HIGH:
        t = max(t, EXPOSURE_RATE * max(equity - EXPOSURE_FLOOR, 0.0))
    return t


def target_after_close(day, bundle, current: float = 0.0, equity: float = 0.0) -> float:
    """The weight in ``ASSET`` to hold from ``day``'s close: decided on the VIX through the
    close before ``day`` and on ``equity``, the core's equity weight held until then (0 means
    the regime part only); ``current`` is the holding now, kept if inside the band."""
    vix = _vix(bundle)
    vix = vix[vix.index < pd.Timestamp(day).normalize()]
    on = bool(regime(vix).iloc[-1]) if len(vix) else False
    last = float(vix.iloc[-1]) if len(vix) else np.nan
    return rebalance(target(on, last, equity), current)


def backtest_returns(bundle, equity: pd.Series | None = None) -> pd.Series:
    """The overlay's return over cash on the bundle's dates, after costs.

    ``equity`` is the core's equity weight held on each day (a backtest's weights summed over
    the equity ETFs); ``None`` runs the regime part only."""
    px = asset_prices(bundle)
    r = holding_returns(px).fillna(0.0).to_numpy()
    c = holding_returns(bundle.prices["TBILL"]).fillna(0.0).to_numpy()
    # the target set at close i is decided on the VIX at close i-1 and the equity weight held
    # on day i (set at close i-1); it earns day i+1
    vix = _vix(bundle)
    on = regime(vix).shift(1, fill_value=False).to_numpy()
    last = vix.shift(1).to_numpy()
    e = (np.zeros(len(r)) if equity is None
         else equity.reindex(bundle.index).fillna(0.0).to_numpy())
    goal = np.array([target(bool(o), lv, ei) for o, lv, ei in zip(on, last, e, strict=True)])
    goal = np.where(px.notna().to_numpy(), goal, 0.0)
    w, out = 0.0, np.zeros(len(r))
    for i in range(len(r)):
        out[i] = w * (r[i] - c[i])
        w *= (1 + r[i]) / (1 + w * r[i] + (1 - w) * c[i])  # drift, as a fraction of NAV
        new = rebalance(goal[i], w)
        out[i] -= abs(new - w) * COST_BPS / 1e4
        w = new
    return pd.Series(out, index=bundle.index)
