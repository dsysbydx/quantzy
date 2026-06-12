"""Global defaults and strategy configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

#: Number of trading days assumed per year (for annualizing returns/vol).
TRADING_DAYS = 252

#: Default cvxpy solver. ``None`` lets cvxpy pick; override for reproducibility.
DEFAULT_SOLVER: str | None = None

#: Default model weights for blending alpha signals into mu (keyed by ``Alpha.name``).
DEFAULT_ALPHA_WEIGHTS: dict[str, float] = {"momentum": 1.0, "value": 0.5}


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Top-level knobs shared across a strategy run.

    Attributes:
        trading_days: Periods per year used for annualization.
        risk_aversion: Lambda in the mean-variance objective ``mu'w - lambda w'Sigma w``.
        cost_bps: Linear transaction cost in basis points of traded notional.
        solver: cvxpy solver name, or ``None`` to let cvxpy choose.
        alpha_weights: Model weights for the alpha blend, keyed by signal name
            (``Alpha.name``); each z-scored signal is multiplied by its weight before
            being summed into mu. Signals absent from the mapping default to weight 1.0.
    """

    trading_days: int = TRADING_DAYS
    risk_aversion: float = 5.0
    cost_bps: float = 5.0
    solver: str | None = DEFAULT_SOLVER
    alpha_weights: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_ALPHA_WEIGHTS)
    )
