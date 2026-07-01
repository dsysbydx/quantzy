"""End-to-end pipeline on the starter ETF universe.

    prices -> (momentum + value) -> mu
    returns -> Ledoit-Wolf -> Sigma
    (mu, Sigma, current holdings) -> MeanVariance optimizer -> target weights + trades

Run:  python examples/end_to_end.py
First run hits Yahoo Finance; results are cached to .cache/ thereafter.
"""

from __future__ import annotations

import pandas as pd

from quantzy.alpha import AlphaCombiner, Momentum, Value
from quantzy.config import StrategyConfig
from quantzy.construction import Portfolio
from quantzy.data import CachedReader, YFinanceReader
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.returns import to_returns
from quantzy.risk import LedoitWolfCovariance, vol_from_cov
from quantzy.universe import default_etf_universe


def main() -> None:
    # Tune the strategy from one place: signal blend, risk aversion, trading cost.
    config = StrategyConfig(
        risk_aversion=8.0,
        cost_bps=5.0,
        alpha_weights={"momentum": 1.0, "value": 0.5},
    )

    universe = default_etf_universe()
    reader = CachedReader(YFinanceReader())
    prices = reader.history(universe.tickers, start="2018-01-01")
    prices = prices.dropna(how="any")
    print(f"Loaded {len(prices)} days for {universe.tickers}")
    print(f"Alpha weights: {config.alpha_weights}\n")

    # --- alpha: blend momentum + value into mu, weighted per config ---
    combiner = AlphaCombiner(
        [Momentum(lookback=252, skip=21), Value(window=252 * 3)],
        weights=config.alpha_weights,
    )
    mu = combiner.forecast(prices)
    print("Expected-return scores (mu):")
    print(mu.round(4), "\n")

    # --- risk: Ledoit-Wolf shrinkage covariance ---
    returns = to_returns(prices)
    sigma = LedoitWolfCovariance().covariance(returns)
    print("Annualized vol from Sigma:")
    print(vol_from_cov(sigma).round(4), "\n")

    # --- optimize: mean-variance, long-only, min 5% SGOV buffer, with trading cost ---
    current = Portfolio(
        weights=pd.Series({"QQQ": 0.40, "VOO": 0.40, "URTH": 0.10, "SGOV": 0.10}),
        nav=100_000.0,
    )
    optimizer = MeanVariance(
        risk_aversion=config.risk_aversion,
        constraints=[C.budget(), C.long_only(), C.min_weight("SGOV", 0.05)],
        cost_model=LinearCost(cost_bps=config.cost_bps),
        solver=config.solver,
    )
    result = optimizer.optimize(mu, sigma, current=current)

    print(f"Solver status: {result.status}")
    print("\nTarget weights:")
    print(result.weights.round(4))
    print(f"\nTurnover: {result.trades.turnover:.4f}   "
          f"Est. cost: {result.trades.cost:.6f} of NAV")
    print("\nTrades:")
    print(result.trades.to_frame().round(4))


if __name__ == "__main__":
    main()
