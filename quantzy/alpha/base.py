"""The alpha-signal interface and shared helpers."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Alpha(Protocol):
    """A cross-sectional expected-return forecast.

    ``forecast`` consumes a price panel and returns one score per asset for a given
    as-of date. Scores are conventionally z-scored across assets so that signals are
    comparable and can be blended by :class:`~quantzy.alpha.combiner.AlphaCombiner`.
    """

    name: str

    def forecast(self, prices: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.Series:
        """Return a Series indexed by ticker of expected-return scores as of ``asof``."""
        ...


def zscore(s: pd.Series) -> pd.Series:
    """Cross-sectional z-score; returns zeros if the dispersion is degenerate."""
    s = s.astype(float)
    std = s.std(ddof=0)
    if not std or pd.isna(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std
