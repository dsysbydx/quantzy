"""Trend-pullback: long-term momentum, entered on short-term reversal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import ema, ewm_min_history, realized_vol
from quantzy.signals.normalize import EPS, ZScore


@dataclass
class TrendPullback(Signal):
    """Favour assets in a long-term uptrend that have recently dipped below trend.

    Two legs, each a vol-scaled relative gap:

    - **LT trend** ``(EMA(mid) - EMA(long)) / EMA(long)``. Positive in an uptrend. The fast
      leg is ``EMA(mid)`` rather than price so that a short-term dip -- the thing the ST leg
      is there to measure -- does not also read as a broken trend.
    - **ST deviation** ``(price - EMA(short)) / EMA(short)``. Negative after a pullback.

    Both are divided by trailing volatility, then z-scored across assets, and combined as
    ``z(LT) - beta * z(ST)``: trend earns credit, and stretch above the short EMA costs it.

    A single ``EMA(long) - EMA(short)`` cannot express this. It is one number, positive after
    any recent weakness, whether or not the long-term trend is up.

    **The gate.** With ``gate=True`` the reversal term applies only where the *raw* LT leg is
    positive -- the dip is bought only inside an uptrend; a downtrending asset keeps just its
    (low) trend score. The test is on the raw leg because a cross-sectional z-score is
    relative: in a falling market the least-bad asset has a positive z with a negative trend.
    ``gate=False`` applies the reversal everywhere and leaves the optimizer to arbitrate.

    Args:
        short: Span of the ST EMA that price is measured against.
        mid: Fast span of the LT trend leg. Must lie strictly between ``short`` and ``long``.
        long: Slow span of the LT trend leg.
        beta: Weight of the reversal leg relative to the trend leg; ``0`` is pure trend.
        gate: Apply the reversal only where the LT trend is positive.
        vol_window: Trailing window for the volatility scaling of both legs.
        exclude: Tickers given no view (NaN -> neutral). Use for a cash sleeve: dividing a
            T-bill fund's tiny drift by its tiny volatility would otherwise rank it first.
        ewm_tol: Seed-decay tolerance that sizes :attr:`min_history`. The default keeps a
            production warmup window exact to ~1e-9; a research run on finite history can
            loosen it to start trading years earlier.
    """

    name: ClassVar[str] = "trend_pullback"

    short: int = 10
    mid: int = 50
    long: int = 200
    beta: float = 1.0
    gate: bool = True
    vol_window: int = 60
    exclude: tuple[str, ...] = ()
    ewm_tol: float = 1e-9

    def __post_init__(self) -> None:
        if not self.short < self.mid < self.long:
            raise ValueError(
                f"spans must satisfy short < mid < long, got "
                f"short={self.short}, mid={self.mid}, long={self.long}"
            )
        if self.beta < 0:
            raise ValueError(f"beta must be >= 0 (it is the reversal weight), got {self.beta}")
        if self.vol_window < 2:
            raise ValueError(f"vol_window must be >= 2, got {self.vol_window}")
        if not 0.0 < self.ewm_tol < 1.0:
            raise ValueError(f"ewm_tol must be in (0, 1), got {self.ewm_tol}")
        self.exclude = tuple(self.exclude)

    def legs(self, data: DataBundle) -> tuple[pd.DataFrame, pd.DataFrame]:
        """The raw ``(lt, st)`` legs, vol-scaled but not yet z-scored -- for diagnostics."""
        prices = data.prices
        vol = realized_vol(prices, self.vol_window)
        vol = vol.where(vol > EPS)
        slow = ema(prices, self.long)
        fast = ema(prices, self.mid)
        anchor = ema(prices, self.short)
        lt = (fast - slow) / slow.where(slow.abs() > EPS) / vol
        st = (prices - anchor) / anchor.where(anchor.abs() > EPS) / vol
        if self.exclude:
            drop = [t for t in self.exclude if t in prices.columns]
            lt[drop] = float("nan")
            st[drop] = float("nan")
        return lt, st

    def raw(self, data: DataBundle) -> pd.DataFrame:
        lt, st = self.legs(data)
        z = ZScore(on_degenerate="zero")
        # ZScore emits 0.0 for missing entries; restore NaN so "no view" survives to the
        # outer normalizer instead of reading as an average score.
        z_lt = z(lt).where(lt.notna())
        z_st = z(st).where(st.notna())
        reversal = -self.beta * z_st
        if self.gate:
            reversal = reversal.where(lt > 0, 0.0)
        return z_lt + reversal

    @property
    def min_history(self) -> int:
        """Set by the slowest EMA's convergence; see :attr:`ewm_tol`."""
        return max(ewm_min_history(self.long, self.ewm_tol), self.vol_window + 1)
