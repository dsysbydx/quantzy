"""The risk-model interface and shared helpers."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd


@runtime_checkable
class RiskModel(Protocol):
    """Estimates an asset covariance matrix (Sigma) from a returns panel.

    Implementations return a square DataFrame indexed and columned by ticker. The optimizer
    consumes Sigma directly; the per-asset volatilities (the "vol/var estimate") are just its
    diagonal, available via :func:`vol_from_cov`.
    """

    def covariance(self, returns: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.DataFrame:
        """Return the (annualized) covariance matrix as of ``asof``."""
        ...


def vol_from_cov(cov: pd.DataFrame) -> pd.Series:
    """Per-asset volatility = sqrt of the covariance diagonal."""
    return pd.Series(np.sqrt(np.diag(cov.to_numpy())), index=cov.index, name="vol")
