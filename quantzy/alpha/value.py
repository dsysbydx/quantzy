"""A long-horizon mean-reversion ("value") proxy.

For a handful of broad ETFs there is no per-name fundamental ratio to lean on, so this uses
a long-horizon reversal proxy: assets stretched far *above* their long moving average score
*low* (expensive), and vice versa. It is a deliberate placeholder -- when the universe grows
to single names, swap in a real fundamental signal (E/P, B/P, ...) behind the same interface.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quantzy.alpha.base import zscore


@dataclass(slots=True)
class Value:
    """Negative deviation from a long moving average, z-scored.

    Args:
        window: Length of the long moving average in trading days (default ~5y).
    """

    window: int = 252 * 5
    name: str = "value"

    def forecast(self, prices: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.Series:
        px = prices if asof is None else prices.loc[:asof]
        window = min(self.window, len(px))
        long_ma = px.iloc[-window:].mean()
        # Cheap (price below its long average) -> positive score.
        deviation = px.iloc[-1] / long_ma - 1.0
        return zscore(-deviation)
