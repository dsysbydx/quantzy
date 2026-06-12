"""Cross-sectional price momentum."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quantzy.alpha.base import zscore


@dataclass(slots=True)
class Momentum:
    """Classic 12-1 momentum: total return over ``lookback`` days, skipping the most recent
    ``skip`` days to avoid short-term reversal. Higher past return -> higher forecast.

    Args:
        lookback: Trailing window length in trading days (default ~12 months).
        skip: Most-recent days to exclude (default ~1 month).
    """

    lookback: int = 252
    skip: int = 21
    name: str = "momentum"

    def forecast(self, prices: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.Series:
        px = prices if asof is None else prices.loc[:asof]
        if len(px) <= self.lookback:
            raise ValueError(
                f"need > {self.lookback} rows for momentum, got {len(px)}"
            )
        end = px.iloc[-1 - self.skip]
        start = px.iloc[-1 - self.lookback]
        total_return = end / start - 1.0
        return zscore(total_return)
