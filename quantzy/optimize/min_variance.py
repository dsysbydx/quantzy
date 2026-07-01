"""Minimum-variance optimizer (analytic + cvxpy hybrid)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.optimize.base import BaseOptimizer, Context, OptimizeResult


class MinVariance(BaseOptimizer):
    """Minimize portfolio variance ``w'Sigma w``.

    Hybrid behavior: when the only constraint is the budget (fully invested, shorting
    allowed) and there is no transaction cost, the global minimum-variance portfolio has the
    closed form ``w = Sigma^{-1} 1 / (1' Sigma^{-1} 1)`` -- :meth:`solve` uses it directly.
    Otherwise it falls back to the cvxpy template (long-only, bounds, turnover, costs, ...).
    """

    def objective(self, w, mu, sigma):
        import cvxpy as cp

        return -cp.quad_form(w, cp.psd_wrap(sigma))

    def _can_use_analytic(self, ctx: Context) -> bool:
        # Valid only for a single (budget) constraint with no transaction cost. The default
        # constraint set adds long-only, so callers must pass constraints=[constraints.budget()]
        # to opt into the closed form.
        return (
            self._constraints is not None
            and len(self._constraints) == 1
            and self.cost_model is None
        )

    def solve(self, mu: np.ndarray, sigma: np.ndarray, ctx: Context) -> OptimizeResult:
        if self._can_use_analytic(ctx):
            inv1 = np.linalg.solve(sigma, np.ones(ctx.n))
            w = inv1 / inv1.sum()
            weights = pd.Series(w, index=ctx.tickers)
            return self._build_result(weights, ctx, status="optimal_analytic", obj=None)
        return super().solve(mu, sigma, ctx)
