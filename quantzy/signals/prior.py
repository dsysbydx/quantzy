"""A constant Sharpe-ratio prior per asset, expressed as an expected return."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

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
