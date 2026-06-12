"""Transaction-cost models -- first-class inputs to the optimizer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import numpy as np

if TYPE_CHECKING:
    import cvxpy as cp


@runtime_checkable
class CostModel(Protocol):
    """Maps a trade vector ``dw = w - w_current`` to a cost the optimizer trades off."""

    def cvxpy_cost(self, w: cp.Variable, w_current: np.ndarray) -> cp.Expression:
        """Cost of trading to ``w`` from ``w_current`` as a (convex) cvxpy expression."""
        ...

    def estimate(self, delta_weights: np.ndarray) -> float:
        """Realized cost (in weight/NAV units) of an executed trade vector."""
        ...


@dataclass(slots=True)
class LinearCost:
    """Linear (proportional) cost: ``cost_bps`` of one-way traded notional.

    Args:
        cost_bps: Flat cost in basis points applied to ``|dw|`` for every asset.
        spread_bps: Optional per-asset extra bps (e.g. half-spread); keys are tickers.
    """

    cost_bps: float = 5.0
    spread_bps: dict[str, float] | None = None

    def _per_asset_rate(self, tickers) -> np.ndarray:
        base = np.full(len(tickers), self.cost_bps, dtype=float)
        if self.spread_bps:
            for i, tk in enumerate(tickers):
                base[i] += self.spread_bps.get(tk, 0.0)
        return base / 1e4

    def cvxpy_cost(self, w, w_current, tickers=None) -> cp.Expression:
        import cvxpy as cp

        if tickers is not None and self.spread_bps:
            rate = self._per_asset_rate(tickers)
            return cp.sum(cp.multiply(rate, cp.abs(w - w_current)))
        return (self.cost_bps / 1e4) * cp.norm1(w - w_current)

    def estimate(self, delta_weights: np.ndarray, tickers=None) -> float:
        if tickers is not None and self.spread_bps:
            rate = self._per_asset_rate(tickers)
            return float(np.sum(rate * np.abs(delta_weights)))
        return float((self.cost_bps / 1e4) * np.abs(delta_weights).sum())
