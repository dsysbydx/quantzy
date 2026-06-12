"""Composable cvxpy constraint builders.

Each builder returns a ``Constraint`` -- a callable ``(w, ctx) -> list[cp.Constraint]`` --
so an optimizer's constraint set is just a list of these, easy to extend or override.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import cvxpy as cp

    from quantzy.optimize.base import Context

Constraint = Callable[["cp.Variable", "Context"], list]


def budget(target: float = 1.0) -> Constraint:
    """Fully-invested: weights sum to ``target`` (1.0 = no leverage, all capital deployed)."""

    def _build(w, ctx):
        return [ctx.cp.sum(w) == target]

    return _build


def long_only() -> Constraint:
    """No shorting: every weight >= 0."""

    def _build(w, ctx):
        return [w >= 0]

    return _build


def box(lower: float = 0.0, upper: float = 1.0) -> Constraint:
    """Per-asset weight bounds ``lower <= w_i <= upper``."""

    def _build(w, ctx):
        return [w >= lower, w <= upper]

    return _build


def max_gross(limit: float = 1.0) -> Constraint:
    """Cap gross leverage: ``sum |w_i| <= limit``."""

    def _build(w, ctx):
        return [ctx.cp.norm1(w) <= limit]

    return _build


def turnover_cap(limit: float) -> Constraint:
    """Cap two-way turnover: ``sum |w_i - w_current_i| <= limit``. No-op if no current book."""

    def _build(w, ctx):
        if ctx.w_current is None:
            return []
        return [ctx.cp.norm1(w - ctx.w_current) <= limit]

    return _build


def min_weight(ticker: str, floor: float) -> Constraint:
    """Floor a single asset's weight (e.g. a minimum SGOV cash-equivalent buffer)."""

    def _build(w, ctx):
        idx = ctx.tickers.index(ticker)
        return [w[idx] >= floor]

    return _build
