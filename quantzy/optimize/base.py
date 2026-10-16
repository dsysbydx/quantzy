"""The optimizer template-method ABC, built on cvxpy.

**The problem is built once and re-solved.** A daily strategy asks for thousands of solves
whose structure never changes: same assets, same constraints, new numbers. Rebuilding the
cvxpy problem each time made cvxpy re-derive the solver's matrix form from scratch, which
cost several times the solve itself. Instead the per-solve numbers (``mu``, a covariance
factor, the current book) are ``cp.Parameter``s, and the built problem is cached per asset
list. cvxpy then caches its own conversion (it is "DPP", disciplined parametrized
programming), so a re-solve only refills the numbers.

That puts one rule on the hooks: **an objective or constraint must take every per-solve number
from its parameters**, never as a constant read at build time. A constraint that baked in
``ctx.w_current`` as an array would silently keep the first day's book forever.
"""

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
    """State shared with objective/constraint hooks.

    When the hooks build the problem, ``w_current`` is a ``cp.Parameter`` (or ``None`` when
    there is no current book) and ``nav`` is ``None``: the problem is reused across solves, so
    it may only depend on numbers through parameters. When a result is assembled,
    ``w_current`` is the actual array and ``nav`` the account value.

    ``inputs`` holds the per-solve parameters while the problem is built, so a constraint can
    use them too (a volatility cap needs the covariance factor); it is ``None`` otherwise.
    """

    tickers: list[str]
    w_current: np.ndarray | cp.Parameter | None
    nav: float | None
    cp: object  # the imported cvxpy module, handed to constraint builders
    inputs: Inputs | None = None

    @property
    def n(self) -> int:
        return len(self.tickers)


@dataclass(slots=True)
class Inputs:
    """The per-solve numbers, as parameters of a problem that is built once.

    Attributes:
        mu: Expected returns, shape ``(n,)``.
        factor: A matrix ``F`` with ``F @ F.T == Sigma``, shape ``(n, n)``. The variance is
            written as ``||F' w||^2`` rather than ``w' Sigma w`` because cvxpy can only cache
            a problem whose parameters enter this way.
    """

    mu: cp.Parameter
    factor: cp.Parameter

    def variance(self, w: cp.Variable) -> cp.Expression:
        """Portfolio variance ``w' Sigma w``, in a form cvxpy can re-solve cheaply."""
        import cvxpy as cp

        return cp.sum_squares(self.factor.T @ w)


@dataclass(slots=True)
class _Built:
    problem: cp.Problem
    w: cp.Variable
    inputs: Inputs
    w_current: cp.Parameter | None


def covariance_factor(sigma: np.ndarray) -> np.ndarray:
    """``F`` with ``F @ F.T == sigma``, tolerant of a singular or slightly indefinite matrix.

    Uses the eigendecomposition rather than Cholesky, which fails on a singular matrix such
    as one with a near-constant cash asset. Negative eigenvalues (estimation noise) are
    clipped to zero, the same matrix ``cp.psd_wrap`` used to assume.
    """
    sym = 0.5 * (sigma + sigma.T)
    vals, vecs = np.linalg.eigh(sym)
    return vecs * np.sqrt(np.clip(vals, 0.0, None))


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
        * :meth:`objective`   -- required; the per-strategy expression to maximize, built from
          the :class:`Inputs` parameters.
        * :meth:`constraints` -- optional; defaults to fully-invested + long-only.
        * :meth:`solve`       -- optional; e.g. an analytic closed form (see MinVariance).

    Args:
        constraints: Constraint builders; ``None`` means fully invested and long-only.
        cost_model: Transaction cost, subtracted from the objective (times ``cost_aversion``)
            and, unless ``execution_cost`` is given, reported on trades as the cost charged.
        cost_aversion: Multiplier on the cost *in the objective only*. ``mu`` is an annual
            return but a trade's cost is paid once, so at a cost aversion of 1 a trade only
            has to earn its cost back over a year. When the strategy decides daily, that makes
            it trade on almost every change. Set this to ``1 / expected holding period in years``
            (12 means a trade must pay for itself within about a month). The larger it is,
            the wider the band in which the optimizer holds the current book rather than trade.
            The cost reported on the trades is unaffected.
        execution_cost: The cost actually charged for a trade, reported on the trades (a
            backtest subtracts it from returns). ``None`` uses ``cost_model``. Separate them
            when the objective's cost is a smoothing penalty rather than an estimate of what a
            trade costs.
        solver: cvxpy solver name. ``None`` uses Clarabel, an interior-point solver. cvxpy's
            own choice for these problems is OSQP, which warm-starts from the previous
            solution and on a badly scaled book (a cash sleeve with a thousandth of the
            equities' variance) can stop with weights a few 1e-6 outside their bounds.
            Clarabel solves each problem to high accuracy, no slower.
    """

    def __init__(
        self,
        constraints: list[Constraint] | None = None,
        cost_model: CostModel | None = None,
        solver: str | None = None,
        cost_aversion: float = 1.0,
        execution_cost: CostModel | None = None,
    ) -> None:
        if cost_aversion < 0:
            raise ValueError(f"cost_aversion must be >= 0, got {cost_aversion}")
        self._constraints = constraints
        self.cost_model = cost_model
        self.cost_aversion = cost_aversion
        self.execution_cost = execution_cost
        self.solver = solver
        self._built: dict[tuple, _Built] = {}

    def __getstate__(self) -> dict:
        # A built cvxpy problem does not need to travel; it is rebuilt on first use.
        state = self.__dict__.copy()
        state["_built"] = {}
        return state

    # --- hooks ---------------------------------------------------------------

    @abstractmethod
    def objective(self, w: cp.Variable, inputs: Inputs) -> cp.Expression:
        """The expression to maximize (transaction cost is subtracted by the base class).

        Build it from ``inputs`` -- ``inputs.mu`` and ``inputs.variance(w)`` -- never from
        arrays, since the same expression is re-solved with new numbers.
        """

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
        """Default cvxpy path: fill the cached problem's parameters and re-solve it."""
        built = self._problem(ctx)
        built.inputs.mu.value = mu
        built.inputs.factor.value = covariance_factor(sigma)
        if built.w_current is not None:
            built.w_current.value = ctx.w_current
        built.problem.solve(solver=self.solver or "CLARABEL")

        if built.w.value is None:
            raise RuntimeError(f"optimization failed with status {built.problem.status!r}")

        weights = pd.Series(np.asarray(built.w.value).ravel(), index=ctx.tickers)
        return self._build_result(weights, ctx, status=built.problem.status,
                                  obj=built.problem.value)

    def _problem(self, ctx: Context) -> _Built:
        """The problem for this asset list (and with or without a current book), built once.

        Maximize ``objective - cost_aversion * cost`` subject to the constraints.
        """
        has_book = ctx.w_current is not None
        key = (tuple(ctx.tickers), has_book)
        if key in self._built:
            return self._built[key]

        cp = ctx.cp
        n = ctx.n
        w = cp.Variable(n)
        inputs = Inputs(mu=cp.Parameter(n), factor=cp.Parameter((n, n)))
        w_current = cp.Parameter(n) if has_book else None
        build_ctx = Context(tickers=list(ctx.tickers), w_current=w_current, nav=None, cp=cp,
                            inputs=inputs)

        obj_expr = self.objective(w, inputs)
        if self.cost_model is not None and w_current is not None:
            obj_expr = obj_expr - self.cost_aversion * self.cost_model.cvxpy_cost(w, w_current)
        problem = cp.Problem(cp.Maximize(obj_expr), self.constraints(w, build_ctx))
        if not problem.is_dpp():
            raise TypeError(
                f"{type(self).__name__} built a problem cvxpy cannot re-solve with new numbers "
                f"(not DPP). Build the objective from Inputs (inputs.mu, inputs.variance(w)) "
                f"and take per-solve numbers in constraints from ctx parameters, not arrays."
            )
        self._built[key] = _Built(problem=problem, w=w, inputs=inputs, w_current=w_current)
        return self._built[key]

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
        charged = self.execution_cost if self.execution_cost is not None else self.cost_model
        if charged is not None:
            dw = (target.weights - current.weights).to_numpy()
            try:
                cost = charged.estimate(dw, tickers=list(ctx.tickers))
            except TypeError:  # a cost model that does not take tickers
                cost = charged.estimate(dw)
        trades = diff(current, target, cost=cost).nonzero()
        return OptimizeResult(target=target, trades=trades, status=status, objective_value=obj)
