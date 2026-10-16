"""The variance risk premium: implied minus realized variance of the equity market."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol


@dataclass
class VarianceRiskPremium(Signal):
    """``VRP = (VIX / 100)^2 - realized variance of the market``, as a capped z-score.

    Implied variance above realized means investors are paying up for protection, and equity
    returns over the following weeks have tended to be higher (Bollerslev, Tauchen and Zhou,
    2009). Neither part predicts alone; the gap does. The score is one market-wide number,
    applied to every ticker in ``assets`` and absent elsewhere, multiplied by each asset's
    annualized vol so that, with ``normalizer=Identity()``, a weight ``k`` adds ``k * z`` to
    its Sharpe ratio.

    The bundle carries the two inputs as panels, the same value in every column: ``vix``
    (the VIX close) and ``market`` (an index price such as SPY).

    Args:
        assets: Tickers that get the view, e.g. the equity ETFs.
        rv_window: Rows of daily returns in the realized variance.
        z_window: Trailing rows the premium is standardized over.
        cap: Cap on ``|z|``.
        vol_window: Trailing window for the asset vol that turns ``z`` into a return.
        vix_panel, market_panel: Names of the input panels.
    """

    name: ClassVar[str] = "vrp"

    assets: tuple[str, ...] = ()
    rv_window: int = 21
    z_window: int = 1260
    cap: float = 2.0
    vol_window: int = 60
    vix_panel: str = "vix"
    market_panel: str = "market"

    def __post_init__(self) -> None:
        if self.rv_window < 2 or self.z_window < 2 or self.vol_window < 2 or self.cap <= 0:
            raise ValueError("need rv_window, z_window, vol_window >= 2 and cap > 0")

    def premium(self, data: DataBundle) -> pd.Series:
        """The capped z-score of the variance risk premium, one value per date."""
        vix = data[self.vix_panel].iloc[:, 0].ffill()
        mkt = data[self.market_panel].iloc[:, 0].ffill()
        rv = (mkt.pct_change() ** 2).rolling(self.rv_window, min_periods=self.rv_window).mean()
        vrp = (vix / 100.0) ** 2 - rv * TRADING_DAYS
        roll = vrp.rolling(self.z_window, min_periods=self.z_window)
        return ((vrp - roll.mean()) / roll.std()).clip(-self.cap, self.cap)

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        z = self.premium(data)
        out = pd.DataFrame({t: z for t in px.columns}, index=px.index)
        out[[c for c in px.columns if c not in self.assets]] = float("nan")
        return out * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return max(self.rv_window + self.z_window, self.vol_window + 1)

    @property
    def asset_history(self) -> int:
        """Rows an asset itself needs: the market inputs come from the bundle's first row."""
        return self.vol_window + 1
