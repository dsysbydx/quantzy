"""Composable cvxpy constraint builders.

Each builder returns a ``Constraint`` -- a callable ``(w, ctx) -> list[cp.Constraint]`` --
so an optimizer's constraint set is just a list of these, easy to extend or override.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
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


def long_only(exclude: Sequence[str] = ()) -> Constraint:
    """No shorting: every weight >= 0, except the tickers in ``exclude``.

    Excluding the cash ticker lets it go negative, which is borrowing; pair that with
    :func:`max_leverage` to bound how much.
    """

    def _build(w, ctx):
        idx = [i for i, t in enumerate(ctx.tickers) if t not in set(exclude)]
        return [w[idx] >= 0] if idx else []

    return _build


def box(lower: float = 0.0, upper: float = 1.0) -> Constraint:
    """Per-asset weight bounds ``lower <= w_i <= upper``."""

    def _build(w, ctx):
        return [w >= lower, w <= upper]

    return _build


def max_weight(limit: float, exclude: Sequence[str] = ()) -> Constraint:
    """Cap every asset's weight at ``limit``, except the tickers in ``exclude`` (e.g. cash)."""

    def _build(w, ctx):
        idx = [i for i, t in enumerate(ctx.tickers) if t not in set(exclude)]
        return [w[idx] <= limit] if idx else []

    return _build


def max_book_share(limit: float, exclude: Sequence[str] = ()) -> Constraint:
    """Cap every asset at ``limit`` of the invested book, not of capital.

    ``w_i <= limit * sum(w_j)`` over the non-excluded assets (exclude cash). Without
    leverage the book is at most capital, so this matches :func:`max_weight`. With leverage
    the cap grows with the book: at 1.3x gross, a 20% share is 26% of capital. That lets
    leverage scale the whole book instead of only the positions not yet at a capital cap.
    Assumes a long-only book, where the sum of weights is the gross exposure.
    """
    if not 0 < limit <= 1:
        raise ValueError(f"limit must be in (0, 1], got {limit}")

    def _build(w, ctx):
        idx = [i for i, t in enumerate(ctx.tickers) if t not in set(exclude)]
        return [w[idx] <= limit * ctx.cp.sum(w[idx])] if idx else []

    return _build


def max_vol(target: float) -> Constraint:
    """Cap ex-ante portfolio volatility: ``sqrt(w' Sigma w) <= target``.

    In the covariance's units, so an annualized risk model takes an annual ``target``. A
    second-order cone constraint, built from the covariance-factor parameter so the problem
    is still re-solved rather than rebuilt.
    """
    if target <= 0:
        raise ValueError(f"target must be > 0, got {target}")

    def _build(w, ctx):
        if ctx.inputs is None:
            raise ValueError("max_vol needs the optimizer's inputs in its context")
        return [ctx.cp.norm(ctx.inputs.factor.T @ w, 2) <= target]

    return _build


def max_gross(limit: float = 1.0) -> Constraint:
    """Cap gross leverage: ``sum |w_i| <= limit``."""

    def _build(w, ctx):
        return [ctx.cp.norm1(w) <= limit]

    return _build


def max_leverage(limit: float, cash: str) -> Constraint:
    """Cap the sum of non-cash weights at ``limit`` times capital.

    Under :func:`budget`, the rest is ``cash``: ``limit=1.2`` lets cash go to -20%, i.e.
    borrow 20% of capital, and ``limit=1.0`` keeps cash >= 0, no leverage. Cash must be in
    ``long_only(exclude=...)`` for the borrowing to be reachable.
    """
    if limit <= 0:
        raise ValueError(f"limit must be > 0, got {limit}")

    def _build(w, ctx):
        idx = [i for i, t in enumerate(ctx.tickers) if t != cash]
        if len(idx) == len(ctx.tickers):
            raise ValueError(f"max_leverage: cash ticker {cash!r} is not in the universe")
        return [ctx.cp.sum(w[idx]) <= limit]

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
