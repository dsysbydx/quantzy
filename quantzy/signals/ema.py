"""EMA-spread: the gap between a fast and a slow exponential moving average."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import ema, ewm_min_history, realized_vol
from quantzy.signals.normalize import EPS


@dataclass
class EmaSpread(Signal):
    """Fast-minus-slow EMA gap, made comparable across assets.

    The raw gap ``EMA(fast) - EMA(slow)`` is in price units, so a $400 stock dwarfs a $20 one
    for no reason. Two normalizations fix that before the cross-section is ranked:

    - divide by the slow EMA, giving a unitless *relative* gap;
    - divide by trailing volatility, so a 5% gap on a quiet asset is not equated with 5% on a
      violent one. A gap is only informative relative to how far the asset normally travels.

    Args:
        fast: Span of the fast EMA.
        slow: Span of the slow EMA. Must exceed ``fast``.
        sign: ``+1`` for ``fast - slow``, the MACD-style trend reading -- positive when price
            has recently been strong. ``-1`` for ``slow - fast``, the contrarian reading --
            positive after recent weakness. Sweep it rather than assuming.
        vol_window: Trailing window for the volatility scaling; ``None`` disables it.
    """

    name: ClassVar[str] = "ema_spread"

    fast: int = 10
    slow: int = 60
    sign: int = 1
    vol_window: int | None = 60

    def __post_init__(self) -> None:
        if self.fast >= self.slow:
            raise ValueError(
                f"fast span must be < slow span, got fast={self.fast}, slow={self.slow}"
            )
        if self.sign not in (1, -1):
            raise ValueError(f"sign must be +1 (trend) or -1 (reversion), got {self.sign}")

    def raw(self, data: DataBundle) -> pd.DataFrame:
        prices = data.prices
        fast_ema = ema(prices, self.fast)
        slow_ema = ema(prices, self.slow)
        spread = self.sign * (fast_ema - slow_ema) / slow_ema.where(slow_ema.abs() > EPS)
        if self.vol_window:
            vol = realized_vol(prices, self.vol_window)
            spread = spread / vol.where(vol > EPS)
        return spread

    @property
    def min_history(self) -> int:
        """Warmup is set by EWM convergence, not by the span.

        An EWM never fully forgets its seed, so ``slow`` rows of history is far too few --
        see :func:`~quantzy.signals.indicators.ewm_min_history`.
        """
        need = max(ewm_min_history(self.fast), ewm_min_history(self.slow))
        if self.vol_window:
            need = max(need, self.vol_window + 1)
        return need
