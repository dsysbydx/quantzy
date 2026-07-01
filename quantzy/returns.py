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


def align(*frames: pd.DataFrame) -> list[pd.DataFrame]:
    """Inner-join several frames on their index (dates), dropping rows with any NaN."""
    common = None
    for f in frames:
        common = f.index if common is None else common.intersection(f.index)
    return [f.loc[common].dropna(how="any") for f in frames]
