"""Factor risk model -- stub conforming to the RiskModel interface.

A factor model decomposes covariance as ``Sigma = B F B' + D`` where ``B`` are factor
loadings, ``F`` the factor covariance, and ``D`` idiosyncratic variances. This keeps Sigma
low-rank-plus-diagonal, which scales to large universes far better than a full sample
covariance. Not implemented yet -- left as a typed placeholder.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quantzy.risk.base import PanelRiskModel


@dataclass(kw_only=True)
class FactorRiskModel(PanelRiskModel):
    """Placeholder factor risk model. See module docstring for the intended math."""

    factors: list[str] | None = None

    def estimate(self, returns: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError(
            "FactorRiskModel is a stub; use SampleCovariance or LedoitWolfCovariance for now."
        )
