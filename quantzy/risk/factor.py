"""Factor risk model -- stub conforming to the RiskModel protocol.

A factor model decomposes covariance as ``Sigma = B F B' + D`` where ``B`` are factor
loadings, ``F`` the factor covariance, and ``D`` idiosyncratic variances. This keeps Sigma
low-rank-plus-diagonal, which scales to large universes far better than a full sample
covariance. Not implemented yet -- left as a typed placeholder.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class FactorRiskModel:
    """Placeholder factor risk model. See module docstring for the intended math."""

    factors: list[str] | None = None

    def covariance(self, returns: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.DataFrame:
        raise NotImplementedError(
            "FactorRiskModel is a stub; use SampleCovariance or LedoitWolfCovariance for now."
        )
