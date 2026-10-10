"""Sample covariance, optionally exponentially weighted."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quantzy.risk.base import PanelRiskModel, symmetrize


@dataclass(kw_only=True)
class SampleCovariance(PanelRiskModel):
    """Empirical covariance of returns.

    Args:
        halflife: If set, use an exponentially-weighted covariance with this halflife (in
            periods), giving more weight to recent observations.
    """

    halflife: float | None = None

    def estimate(self, returns: pd.DataFrame) -> pd.DataFrame:
        if self.halflife is not None:
            cov = returns.ewm(halflife=self.halflife).cov(bias=True)
            # ewm.cov returns a stacked (date, asset) x asset frame; take the last block.
            cov = cov.loc[returns.index[-1]]
        else:
            cov = returns.cov()
        return symmetrize(cov)
