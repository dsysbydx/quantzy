"""Vectorized, causal price indicators shared across signals.

Every function maps a ``(dates x tickers)`` panel to a panel of the same shape, using only
backward-looking data, so the value at row ``t`` never depends on a row after ``t``. That
property is what lets a signal be computed once over full history and sliced at any as-of
date (see :mod:`quantzy.signals.base`).
"""

from __future__ import annotations

import math

import pandas as pd


def ema(panel: pd.DataFrame, span: int) -> pd.DataFrame:
    """Exponential moving average with a ``span``-row burn-in of NaN.

    ``adjust=False`` gives the recursive form ``y_t = a*x_t + (1-a)*y_{t-1}`` with
    ``a = 2/(span+1)`` -- the standard trading definition, and the one
    :func:`ewm_min_history` solves against.
    """
    _check_window(span, "span")
    return panel.ewm(span=span, adjust=False, min_periods=span).mean()


def sma(panel: pd.DataFrame, window: int) -> pd.DataFrame:
    """Simple moving average over a trailing ``window``."""
    _check_window(window, "window")
    return panel.rolling(window, min_periods=window).mean()


def realized_vol(prices: pd.DataFrame, window: int, annualize: bool = False) -> pd.DataFrame:
    """Trailing standard deviation of simple returns.

    Args:
        prices: Price panel.
        window: Trailing window in rows.
        annualize: Multiply by ``sqrt(TRADING_DAYS)``. Off by default -- a signal that divides
            by vol only needs a consistent scale, not an annual one.
    """
    _check_window(window, "window")
    vol = prices.pct_change().rolling(window, min_periods=window).std(ddof=1)
    if annualize:
        from quantzy.config import TRADING_DAYS

        vol = vol * math.sqrt(TRADING_DAYS)
    return vol


def ewm_min_history(span: int, tol: float = 1e-9) -> int:
    """Rows of warmup an EWM needs before the seed value is forgotten to within ``tol``.

    An EWM is an infinite-impulse-response filter: truncating *old* history perturbs today's
    value rather than leaving it unchanged. The influence of the seed decays as
    ``(1-a)**n`` with ``a = 2/(span+1)``, so solving ``(1-a)**n < tol`` gives the warmup.

    This is why ``min_history = span`` is wrong. At ``span=60`` a 200-row window is off by
    ~6.5e-03 -- enough to reorder a tight cross-section -- while this returns 622.
    """
    _check_window(span, "span")
    alpha = 2.0 / (span + 1.0)
    if alpha >= 1.0:  # span == 1 -> no memory to forget
        return 1
    return int(math.ceil(math.log(tol) / math.log1p(-alpha)))


def _check_window(n: int, label: str) -> None:
    if not isinstance(n, int | float) or int(n) != n or n < 1:
        raise ValueError(f"{label} must be a positive integer, got {n!r}")
