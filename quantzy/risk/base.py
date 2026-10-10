"""The risk-model interface and shared plumbing.

Risk models follow the same shape as :class:`~quantzy.signals.base.Signal`: the author
writes the estimator, the base class provides the point-in-time plumbing. A model reads a
:class:`~quantzy.data.bundle.DataBundle`, derives returns from its price panel once, and
slices that panel at the requested ``asof``.

Deriving returns once and slicing is sound for the same reason it is sound for signals --
``pct_change`` is backward-looking, so row ``t`` of the returns panel never depends on a row
after ``t``. It matters because the previous design had the engine pass ``asof=`` to the
alpha but hand the risk model a pre-sliced frame: two conventions for one idea, one line
apart, and only one of them exercised by the tests.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.returns import to_returns


@runtime_checkable
class RiskModel(Protocol):
    """Estimates an asset covariance matrix (Sigma) as of a point in time.

    Implementations return a square DataFrame indexed and columned by ticker. The optimizer
    consumes Sigma directly; per-asset volatilities are its diagonal, via :func:`vol_from_cov`.
    """

    def covariance(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        """Return the covariance matrix as of ``asof`` (the last row when ``None``)."""
        ...

    @property
    def min_history(self) -> int:
        """Price rows needed before the estimate is trustworthy."""
        ...


@dataclass(kw_only=True)
class PanelRiskModel(ABC):
    """Base for covariance models estimated from a returns panel.

    Subclasses implement :meth:`estimate`, the bare math on a returns frame. Everything
    point-in-time lives here.

    Args:
        annualize: If True, scale the per-period covariance by ``trading_days``.
        trading_days: Periods per year used for annualization.
        lookback: Use only the most recent ``lookback`` return rows. ``None`` uses every row
            visible at ``asof``, which makes the estimate's effective window grow through a
            backtest -- fine for a stationarity assumption you actually believe, misleading
            otherwise. Set it if you want the live estimate to match the backtest's.
        lag: Return rows to drop from the end of the window, matching
            :attr:`~quantzy.signals.base.Signal.lag`. The default of ``1`` means Sigma at
            date ``t`` is estimated from returns through ``t-1``, so it rests on the same
            information the signals do. Leaving it at ``0`` would let the covariance see the
            very return the book is about to earn.
    """

    annualize: bool = True
    trading_days: int = TRADING_DAYS
    lookback: int | None = None
    lag: int = 1
    _cache: tuple[tuple, pd.DataFrame] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    @abstractmethod
    def estimate(self, returns: pd.DataFrame) -> pd.DataFrame:
        """Covariance of a returns frame, before annualization. The bare math."""

    @property
    def min_history(self) -> int:
        """Price rows needed before the estimate is trustworthy."""
        base = (self.lookback + 1) if self.lookback else 3
        return base + self.lag

    def covariance(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        """Estimate Sigma from the returns visible at ``asof``.

        Args:
            data: A bundle, or a bare price panel.
            asof: Point in time; ``None`` means the last row available.
        """
        rets = self._returns(as_bundle(data))
        if asof is not None:
            rets = rets.loc[:asof]
        if self.lag:
            rets = rets.iloc[: -self.lag]
        if self.lookback:
            rets = rets.iloc[-self.lookback:]
        rets = rets.dropna(how="any")
        if len(rets) < 2:
            raise ValueError(
                f"need at least 2 return observations to estimate covariance"
                f"{f' as of {pd.Timestamp(asof).date()}' if asof is not None else ''}, "
                f"got {len(rets)}"
            )
        cov = self.estimate(rets)
        if self.annualize:
            cov = cov * self.trading_days
        return cov

    def _returns(self, bundle: DataBundle) -> pd.DataFrame:
        """Returns panel for this bundle, derived once and memoized on its fingerprint."""
        key = bundle.fingerprint()
        if self._cache is not None and self._cache[0] == key:
            return self._cache[1]
        rets = to_returns(bundle.prices)
        self._cache = (key, rets)
        return rets


def vol_from_cov(cov: pd.DataFrame) -> pd.Series:
    """Per-asset volatility = sqrt of the covariance diagonal."""
    return pd.Series(np.sqrt(np.diag(cov.to_numpy())), index=cov.index, name="vol")


def symmetrize(cov: pd.DataFrame) -> pd.DataFrame:
    """Average a matrix with its transpose, killing floating-point asymmetry."""
    arr = cov.to_numpy()
    arr = 0.5 * (arr + arr.T)
    return pd.DataFrame(arr, index=cov.index, columns=cov.columns)
