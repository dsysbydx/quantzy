"""Cross-sectional price momentum."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.signals.normalize import EPS


@dataclass
class Momentum(Signal):
    """Classic 12-1 momentum: total return over ``lookback`` rows, skipping the most recent
    ``skip`` rows to avoid short-term reversal. Higher past return -> higher raw score.

    The skip is the whole point of the construction. The most recent month of return is
    dominated by bid-ask bounce and liquidity-driven reversal, which is the opposite sign to
    the momentum effect, so including it dilutes the signal it is meant to measure.

    Args:
        lookback: Trailing window in rows (default ~12 months).
        skip: Most-recent rows to exclude (default ~1 month).
        vol_window: If set, divide the raw return by trailing volatility over this window,
            giving a risk-adjusted (Sharpe-like) momentum. ``None`` -- the default -- keeps
            the classic raw-return form.
    """

    name: ClassVar[str] = "momentum"

    lookback: int = 252
    skip: int = 21
    vol_window: int | None = None

    def __post_init__(self) -> None:
        if self.lookback < 2:
            raise ValueError(f"lookback must be >= 2, got {self.lookback}")
        if self.skip < 0:
            raise ValueError(f"skip must be >= 0, got {self.skip}")
        if self.skip >= self.lookback:
            raise ValueError(
                f"skip must be < lookback or the window has no width, "
                f"got skip={self.skip}, lookback={self.lookback}"
            )

    def raw(self, data: DataBundle) -> pd.DataFrame:
        prices = data.prices
        # Row t compares the price ``skip`` rows back against ``lookback`` rows back. Both are
        # backward shifts, so row t depends on nothing after t.
        end = prices.shift(self.skip)
        start = prices.shift(self.lookback)
        total_return = end / start.where(start.abs() > EPS) - 1.0
        if self.vol_window:
            vol = realized_vol(prices, self.vol_window)
            total_return = total_return / vol.where(vol > EPS)
        return total_return

    @property
    def min_history(self) -> int:
        """One row beyond the lookback, so the oldest leg of the window exists."""
        need = self.lookback + 1
        if self.vol_window:
            need = max(need, self.vol_window + 1)
        return need
