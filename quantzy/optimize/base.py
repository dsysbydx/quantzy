"""The optimizer template-method ABC, built on cvxpy."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from quantzy.construction.portfolio import Portfolio
from quantzy.construction.trades import TradeList, diff
from quantzy.optimize import constraints as C
from quantzy.optimize.cost import CostModel

if TYPE_CHECKING:
    import cvxpy as cp

    from quantzy.optimize.constraints import Constraint


@dataclass(slots=True)
class Context:
    """State shared with objective/constraint hooks during a single solve."""

    tickers: list[str]
    w_current: np.ndarray | None
    nav: float | None
    cp: object  # the imported cvxpy module, handed to constraint builders

    @property
    def n(self) -> int:
        return len(self.tickers)


@dataclass(slots=True)
class OptimizeResult:
    """The optimizer's output: the target portfolio and the trade to reach it."""

    target: Portfolio
    trades: TradeList
    status: str = "optimal"
    objective_value: float | None = None
    diagnostics: dict = field(default_factory=dict)

    @property
    def weights(self) -> pd.Series:
        return self.target.weights


class BaseOptimizer(ABC):
    """Template-method optimizer.

    The base class owns problem assembly and the cvxpy solve; subclasses override only the
    pieces that differ. The flow, given model-weighted ``mu``, a covariance ``sigma`` and the
    ``current`` holdings, is:

        objective(w, mu, sigma)  --(maximize)-->  minus transaction cost  s.t.  constraints

    Override points:
        * :meth:`objective`   -- required; the per-strategy expression to maximize.
        * :meth:`constraints` -- optional; defaults to fully-invested + long-only.
        * :meth:`solve`       -- optional; e.g. an analytic closed form (see MinVariance).
    """

    def __init__(
        self,
        constraints: list[Constraint] | None = None,
        cost_model: CostModel | None = None,
        solver: str | None = None,
    ) -> None:
        self._constraints = constraints
        self.cost_model = cost_model
        self.solver = solver

    # --- hooks ---------------------------------------------------------------

    @abstractmethod
    def objective(self, w: cp.Variable, mu: np.ndarray, sigma: np.ndarray) -> cp.Expression:
        """The expression to maximize (transaction cost is subtracted by the base class)."""

    def constraints(self, w: cp.Variable, ctx: Context) -> list:
        """Default constraint set: fully invested and long-only. Override to customize."""
        builders = self._constraints
        if builders is None:
            builders = [C.budget(), C.long_only()]
        out: list = []
        for build in builders:
            out.extend(build(w, ctx))
        return out

    # --- driver --------------------------------------------------------------

    def optimize(
        self,
        mu: pd.Series,
        sigma: pd.DataFrame,
        current: Portfolio | None = None,
    ) -> OptimizeResult:
        """Run the optimization and return target weights plus the trade vs ``current``."""
        tickers = list(sigma.index)
        mu_vec = mu.reindex(tickers).fillna(0.0).to_numpy()
        sigma_mat = sigma.reindex(index=tickers, columns=tickers).to_numpy()

        if current is not None:
            w_current = current.align_to(tickers).weights.to_numpy()
        else:
            w_current = None
        nav = current.nav if current is not None else None

        ctx = self._make_context(tickers, w_current, nav)
        return self.solve(mu_vec, sigma_mat, ctx)

    def solve(self, mu: np.ndarray, sigma: np.ndarray, ctx: Context) -> OptimizeResult:
        """Default cvxpy path: assemble Maximize(objective - cost) s.t. constraints, solve."""
        cp = ctx.cp
        w = cp.Variable(ctx.n)

        obj_expr = self.objective(w, mu, sigma)
        if self.cost_model is not None and ctx.w_current is not None:
            obj_expr = obj_expr - self.cost_model.cvxpy_cost(w, ctx.w_current)

        problem = cp.Problem(cp.Maximize(obj_expr), self.constraints(w, ctx))
        problem.solve(solver=self.solver)

        if w.value is None:
            raise RuntimeError(f"optimization failed with status {problem.status!r}")

        weights = pd.Series(np.asarray(w.value).ravel(), index=ctx.tickers)
        return self._build_result(weights, ctx, status=problem.status, obj=problem.value)

    # --- shared helpers ------------------------------------------------------

    def _make_context(self, tickers, w_current, nav) -> Context:
        import cvxpy as cp

        return Context(tickers=tickers, w_current=w_current, nav=nav, cp=cp)

    def _build_result(self, weights: pd.Series, ctx: Context, status: str, obj) -> OptimizeResult:
        target = Portfolio(weights=weights, nav=ctx.nav)
        current = Portfolio(
            weights=pd.Series(
                ctx.w_current if ctx.w_current is not None else np.zeros(ctx.n),
                index=ctx.tickers,
            ),
            nav=ctx.nav,
        )
        cost = None
        if self.cost_model is not None:
            cost = self.cost_model.estimate((target.weights - current.weights).to_numpy())
        trades = diff(current, target, cost=cost).nonzero()
        return OptimizeResult(target=target, trades=trades, status=status, objective_value=obj)
