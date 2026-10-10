"""A long-horizon mean-reversion ("value") proxy.

For a handful of broad ETFs there is no per-name fundamental ratio to lean on, so this uses
a long-horizon reversal proxy: assets stretched far *above* their long moving average score
*low* (expensive), and vice versa. It is a deliberate placeholder -- when the universe grows
to single names, swap in a real fundamental signal (E/P, B/P, ...) behind the same interface.

A real fundamental signal needs point-in-time vintages -- what the filing said when it was
published, not as later restated -- which is a :class:`~quantzy.data.bundle.DataBundle`
concern, not a signal one. Add the panel; this interface does not change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import sma
from quantzy.signals.normalize import EPS


@dataclass
class Value(Signal):
    """Negative deviation from a long moving average.

    Args:
        window: Length of the long moving average in rows (default ~5y).

    Note:
        The window is fixed, not ``min(window, len(history))``. A window that shrinks to fit
        the available history would make the score at row ``t`` depend on how much data the
        caller happened to pass, so computing the panel once and slicing it would no longer
        equal computing it as of that date -- the invariant the whole
        :class:`~quantzy.signals.base.Signal` design rests on. Early rows are NaN instead,
        which the normalizer renders as a neutral 0.0.
    """

    name: ClassVar[str] = "value"

    window: int = 252 * 5

    def __post_init__(self) -> None:
        if self.window < 2:
            raise ValueError(f"window must be >= 2, got {self.window}")

    def raw(self, data: DataBundle) -> pd.DataFrame:
        prices = data.prices
        long_ma = sma(prices, self.window)
        # Cheap (price below its long average) -> positive score.
        deviation = prices / long_ma.where(long_ma.abs() > EPS) - 1.0
        return -deviation

    @property
    def min_history(self) -> int:
        return self.window
