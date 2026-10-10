"""Ledoit-Wolf shrinkage covariance.

Shrinks the noisy sample covariance toward a structured target (a constant-correlation /
scaled-identity matrix), which is better conditioned and far more stable out-of-sample --
especially when the number of assets is comparable to the number of observations.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from quantzy.risk.base import PanelRiskModel


@dataclass(kw_only=True)
class LedoitWolfCovariance(PanelRiskModel):
    """Ledoit-Wolf shrinkage toward a scaled-identity target.

    Args:
        shrinkage: Optional manual intensity in [0, 1]. If None (default), the optimal
            intensity is estimated from the data (Ledoit-Wolf 2004).
    """

    shrinkage: float | None = None

    def estimate(self, returns: pd.DataFrame) -> pd.DataFrame:
        n, p = returns.shape
        x = returns.to_numpy()
        x = x - x.mean(axis=0, keepdims=True)
        sample = (x.T @ x) / n  # MLE sample covariance (bias=True)

        mu = np.trace(sample) / p
        target = mu * np.eye(p)

        if self.shrinkage is None:
            delta = self._estimate_shrinkage(x, sample, target, n, p)
        else:
            delta = float(np.clip(self.shrinkage, 0.0, 1.0))

        shrunk = delta * target + (1.0 - delta) * sample
        return pd.DataFrame(shrunk, index=returns.columns, columns=returns.columns)

    @staticmethod
    def _estimate_shrinkage(x, sample, target, n, p) -> float:
        # pi: sum of asymptotic variances of the sample covariance entries.
        x2 = x**2
        phi_mat = (x2.T @ x2) / n - sample**2
        pi = phi_mat.sum()
        # rho: shrinkage target is diagonal, so only the diagonal of phi contributes.
        rho = np.trace(phi_mat)
        # gamma: misfit between sample and target (squared Frobenius norm).
        gamma = np.linalg.norm(sample - target, "fro") ** 2
        if gamma <= 0:
            return 0.0
        kappa = (pi - rho) / gamma
        return float(np.clip(kappa / n, 0.0, 1.0))
