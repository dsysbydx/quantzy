"""Helpers to turn a price panel into returns."""

from __future__ import annotations

import numpy as np
import pandas as pd


def to_returns(prices: pd.DataFrame, method: str = "simple") -> pd.DataFrame:
    """Convert a wide price panel (dates x tickers) to per-period returns.

    Args:
        prices: Adjusted prices, indexed by date with one column per ticker.
        method: ``"simple"`` for ``p_t / p_{t-1} - 1`` or ``"log"`` for ``ln(p_t / p_{t-1})``.

    Returns:
        Returns DataFrame with the first row (all-NaN) dropped.
    """
    if method == "simple":
        rets = prices.pct_change()
    elif method == "log":
        rets = np.log(prices).diff()
    else:  # pragma: no cover - guard
        raise ValueError(f"unknown method: {method!r}")
    return rets.iloc[1:]


def holding_returns(prices: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    """Per-period returns of *holding* each asset, for P&L: a missing price inside a series
    is marked at the last price, so the day earns 0 and the move lands on the next priced day.

    Without the fill, ``pct_change`` (pandas 3 does not pad) leaves both the gap day and the
    day after it NaN, and a book that counts NaN as 0 loses the move entirely. Only gaps
    between two prices are filled: before an asset's first price there is no return. The
    first row is NaN, as from ``pct_change``.
    """
    return prices.ffill(limit_area="inside").pct_change(fill_method=None)


def align(*frames: pd.DataFrame) -> list[pd.DataFrame]:
    """Inner-join several frames on their index (dates), dropping rows with any NaN."""
    common = None
    for f in frames:
        common = f.index if common is None else common.intersection(f.index)
    return [f.loc[common].dropna(how="any") for f in frames]


def forward_returns(prices: pd.DataFrame, horizon: int = 1) -> pd.DataFrame:
    """Return earned from date ``t`` to ``t + horizon``, aligned at ``t``.

    Deliberately forward-looking -- this is the *target* a signal is scored against, never an
    input to one. Row ``t`` holds ``p[t+horizon]/p[t] - 1``, so pairing it with a score at row
    ``t`` asks the honest question: does today's tradeable score predict the return that
    follows? The last ``horizon`` rows are NaN.
    """
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1, got {horizon}")
    return prices.shift(-horizon) / prices - 1.0
