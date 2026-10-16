"""Sharpe-ratio priors per asset, expressed as expected returns: constant, or moved toward
carry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol


@dataclass
class SharpePrior(Signal):
    """``SR_i * annualized trailing vol_i``: a long-run Sharpe view per asset, in return units.

    The view is held in risk units, so a calm asset and a volatile one with the same Sharpe
    prior get expected returns proportional to their risk. Use with ``normalizer=Identity()``
    so the output stays an annual return that a mean-variance optimizer can use as ``mu``.

    Args:
        sr: Sharpe prior per ticker. Tickers left out get NaN, i.e. no view (e.g. cash).
        vol_window: Trailing window for the realized vol.
    """

    name: ClassVar[str] = "prior"
    sr: dict[str, float] | None = None
    vol_window: int = 60

    def raw(self, data: DataBundle) -> pd.DataFrame:
        vol = realized_vol(data.prices, self.vol_window, annualize=True)
        sr = pd.Series(self.sr or {}, dtype=float).reindex(vol.columns)
        return vol * sr

    @property
    def min_history(self) -> int:
        return self.vol_window + 1


@dataclass
class CarrySharpePrior(SharpePrior):
    """A :class:`SharpePrior` that moves part of the way to each asset's carry, for some assets.

    For a ticker in ``carry_vol`` the Sharpe prior becomes::

        carry_weight * carry / carry_vol + (1 - carry_weight) * sr

    where ``carry`` is the bundle's ``carry_panel`` (an expected excess return in annual
    units, such as a long bond's yield over the T-bill) and ``carry_vol`` the asset's long-run
    vol. Where the carry is missing the judgment prior stands. Other tickers keep ``sr``.

    Args:
        carry_vol: Long-run annual vol per ticker that gets a carry view.
        carry_weight: How far to move from the judgment prior to carry / vol, in [0, 1].
        carry_panel: Name of the bundle panel holding the carry.
    """

    carry_vol: dict[str, float] | None = None
    carry_weight: float = 0.5
    carry_panel: str = "carry"

    def __post_init__(self) -> None:
        if not 0.0 <= self.carry_weight <= 1.0:
            raise ValueError(f"carry_weight must be in [0, 1], got {self.carry_weight}")
        if any(v <= 0 for v in (self.carry_vol or {}).values()):
            raise ValueError("carry_vol values must be positive")

    def sharpe(self, data: DataBundle) -> pd.DataFrame:
        """The Sharpe prior per date and ticker, before it is turned into a return."""
        px = data.prices
        base = pd.Series(self.sr or {}, dtype=float).reindex(px.columns)
        sr = pd.DataFrame(np.tile(base.to_numpy(), (len(px.index), 1)), index=px.index,
                          columns=px.columns)
        if self.carry_vol:
            carry = data[self.carry_panel]
            for t, vol in self.carry_vol.items():
                if t in sr.columns:
                    moved = self.carry_weight * carry[t] / vol + (1 - self.carry_weight) * sr[t]
                    sr[t] = moved.fillna(sr[t])
        return sr

    def raw(self, data: DataBundle) -> pd.DataFrame:
        return self.sharpe(data) * realized_vol(data.prices, self.vol_window, annualize=True)
