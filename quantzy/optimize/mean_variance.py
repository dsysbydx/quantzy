"""Mean-variance optimizer."""

from __future__ import annotations

from quantzy.optimize.base import BaseOptimizer


class MeanVariance(BaseOptimizer):
    """Maximize ``mu'w - lambda * w'Sigma w`` (minus transaction cost), s.t. constraints.

    ``risk_aversion`` (lambda) trades expected return against variance: higher = more
    risk-averse, pushing weight toward low-vol assets (e.g. the SGOV sleeve).
    """

    def __init__(self, risk_aversion: float = 5.0, **kwargs) -> None:
        super().__init__(**kwargs)
        self.risk_aversion = risk_aversion

    def objective(self, w, inputs):
        return inputs.mu @ w - self.risk_aversion * inputs.variance(w)
