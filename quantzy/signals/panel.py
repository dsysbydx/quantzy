"""A view read from a precomputed bundle panel, in return units."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol


@dataclass
class PanelScore(Signal):
    """``data[panel] * annualized vol``: a score built elsewhere (in a strategy's ``load``)
    turned into return units, so that with ``normalizer=Identity()`` a weight ``k`` adds
    ``k * score`` to an asset's Sharpe ratio. Assets with no score (NaN) get no view.

    The panel must be causal (row ``t`` from data through ``t``); the signal's own ``lag``
    then makes it tradeable a day later.

    Args:
        panel: Name of the bundle panel holding the score.
        vol_window: Trailing window for the vol that turns the score into a return.
    """

    name: ClassVar[str] = "panel_score"
    panel: str = "score"
    vol_window: int = 60

    def raw(self, data: DataBundle) -> pd.DataFrame:
        return data[self.panel] * realized_vol(data.prices, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return self.vol_window + 1
