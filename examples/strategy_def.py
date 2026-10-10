"""The strategy definition, imported by every other example.

This file exists to make a point about the architecture: the strategy is defined *once*, and
the backtest, the research sweep and the daily production job all import this same object.
If the definition lived in each script -- as it did when the example applied a 5% SGOV floor
and the tuner did not -- then the thing being tuned would not be the thing being traded, and
nothing would catch it.
"""

from __future__ import annotations

from quantzy.config import StrategyConfig
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.risk import LedoitWolfCovariance
from quantzy.signals import Momentum, Value
from quantzy.strategy import Strategy
from quantzy.universe import default_etf_universe

CONFIG = StrategyConfig(
    risk_aversion=8.0,
    cost_bps=5.0,
    alpha_weights={"momentum": 1.0, "value": 0.5},
)


def build(lookback: int = 252, skip: int = 21) -> Strategy:
    """The live strategy. Sweepable parameters are arguments; everything else is fixed here."""
    universe = default_etf_universe()
    return Strategy(
        universe=universe,
        signals=[Momentum(lookback=lookback, skip=skip), Value(window=252 * 3)],
        weights=CONFIG.alpha_weights,
        risk_model=LedoitWolfCovariance(lookback=252),
        optimizer=MeanVariance(
            risk_aversion=CONFIG.risk_aversion,
            constraints=[C.budget(), C.long_only(), C.min_weight("SGOV", 0.05)],
            cost_model=LinearCost(cost_bps=CONFIG.cost_bps),
            solver=CONFIG.solver,
        ),
        name="momentum+value",
    )
