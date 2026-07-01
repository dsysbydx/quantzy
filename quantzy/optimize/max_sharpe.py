"""Maximum-Sharpe (tangency) optimizer (analytic + cvxpy hybrid)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.optimize.base import BaseOptimizer, Context, OptimizeResult


class MaxSharpe(BaseOptimizer):
    """Maximize the Sharpe ratio ``mu'w / sqrt(w'Sigma w)``.

    The ratio is not concave, so under the plain budget constraint (no costs, shorting
    allowed) :meth:`solve` uses the tangency closed form ``w = Sigma^{-1} mu`` normalized to
    sum to one. With long-only/bounds/costs it falls back to the standard convex
    reformulation: maximize ``mu'y`` s.t. ``y'Sigma y <= 1, y >= 0``, then rescale ``y`` to
    weights summing to one.
    """

    def objective(self, w, mu, sigma):  # pragma: no cover - solve() is overridden
        raise NotImplementedError("MaxSharpe overrides solve(); objective() is unused.")

    def _can_use_analytic(self, ctx: Context) -> bool:
        return (
            self._constraints is not None
            and len(self._constraints) == 1
            and self.cost_model is None
        )

    def solve(self, mu: np.ndarray, sigma: np.ndarray, ctx: Context) -> OptimizeResult:
        if self._can_use_analytic(ctx):
            y = np.linalg.solve(sigma, mu)
            w = y / y.sum()
            weights = pd.Series(w, index=ctx.tickers)
            return self._build_result(weights, ctx, status="optimal_analytic", obj=None)

        import cvxpy as cp

        y = cp.Variable(ctx.n)
        constraints = [cp.quad_form(y, cp.psd_wrap(sigma)) <= 1.0, y >= 0]
        problem = cp.Problem(cp.Maximize(mu @ y), constraints)
        problem.solve(solver=self.solver)
        if y.value is None or y.value.sum() <= 0:
            raise RuntimeError(f"max-sharpe optimization failed with status {problem.status!r}")

        w = np.asarray(y.value).ravel()
        w = w / w.sum()
        weights = pd.Series(w, index=ctx.tickers)
        return self._build_result(weights, ctx, status=problem.status, obj=problem.value)
