"""Portfolio-level settings: how big the book is and how it trades, shared by every alpha.

Alphas decide *what* to own, through ``mu``. These settings decide *how much* and *how
often*: risk aversion, leverage, concentration, trading costs and the risk model. They are
chosen once for the portfolio and changed only as a portfolio decision, never to make one
alpha look better. Keeping them in one object makes that separation hard to break: a
research script that tests a new alpha passes the saved strategy's ``PortfolioConfig``
unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.risk import BlendedRisk, ewma_blend


@dataclass(frozen=True, slots=True)
class PortfolioConfig:
    """Book sizing and trading, independent of the alphas.

    Attributes:
        risk_aversion: Lambda in ``mu'w - lambda w'Sigma w``. With ``mu`` in annual returns,
            a fully unconstrained book runs at roughly ``SR_book / (2 lambda)`` vol, so a
            stronger ``mu`` runs a bigger book at the same lambda; recalibrate with
            :func:`~quantzy.research.calibrate_risk_aversion` if realized vol drifts out of
            the intended band.
        max_leverage: Cap on the sum of non-cash weights, as a multiple of capital. Above 1
            the excess is borrowed against the cash sleeve.
        max_position: Per-asset cap.
        position_cap_of: ``"book"`` caps each asset as a share of the invested book, so the
            cap grows with leverage; ``"capital"`` caps it as a share of capital.
        cost_bps: Linear cost per unit traded, one way, in basis points, used in the
            optimizer's objective on every asset including cash.
        cost_aversion: Multiplier on that cost in the objective. Higher trades less. Together
            with ``cost_bps`` it sets the no-trade band; in practice it smooths the book
            against a noisy daily target more than it estimates cost
            (``research/research_execution_cost.py``).
        execution_cost_bps: The cost actually charged to returns, one way, per ticker; tickers
            left out (such as cash) are free. ``None`` charges ``cost_bps`` on every asset,
            as in the objective. It does not change any trade.
        borrow_spread_bps: Annual spread over the cash rate paid on borrowed cash. Not part of
            the optimization; backtests subtract it from returns.
        risk_halflives: Halflives of the EWMA covariances blended into the shared risk model.
        long_only: Forbid short positions in every asset except cash.
    """

    risk_aversion: float = 1.5
    max_leverage: float = 1.0
    max_position: float | None = 0.2
    position_cap_of: Literal["book", "capital"] = "book"
    cost_bps: float = 5.0
    cost_aversion: float = 48.0
    execution_cost_bps: Mapping[str, float] | None = None
    borrow_spread_bps: float = 50.0
    risk_halflives: tuple[float, ...] = field(default=(5, 20, 60))
    long_only: bool = True

    def risk_model(self) -> BlendedRisk:
        return ewma_blend(self.risk_halflives)

    def optimizer(self, cash: str) -> MeanVariance:
        """The mean-variance optimizer these settings describe, with ``cash`` as the funding
        sleeve."""
        cons = [C.budget(), C.max_leverage(self.max_leverage, cash)]
        if self.long_only:
            cons.append(C.long_only(exclude=(cash,)))
        if self.max_position is not None:
            cap = C.max_book_share if self.position_cap_of == "book" else C.max_weight
            cons.append(cap(self.max_position, exclude=(cash,)))
        return MeanVariance(
            risk_aversion=self.risk_aversion,
            constraints=cons,
            cost_model=LinearCost(self.cost_bps),
            cost_aversion=self.cost_aversion,
            execution_cost=(None if self.execution_cost_bps is None
                            else LinearCost(0.0, spread_bps=dict(self.execution_cost_bps))),
        )

    def net_of_borrowing(self, returns: pd.Series, weights: pd.DataFrame, cash: str,
                         periods: int = 252) -> pd.Series:
        """Backtest returns less the borrow spread on negative cash."""
        borrowed = (-weights[cash]).clip(lower=0).reindex(returns.index).fillna(0.0)
        return returns - borrowed * self.borrow_spread_bps / 1e4 / periods
