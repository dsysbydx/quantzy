"""Walk-forward backtest engine.

Steps through time on a rebalance schedule. At each rebalance date it uses only data available
up to that point (``asof``) to compute mu via an alpha and Sigma via a RiskModel, optimizes
target weights against the current book net of transaction cost, then holds those weights until
the next rebalance while accruing each period's portfolio return. The output is a return series
plus a weight history that feeds :mod:`quantzy.backtest.metrics`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from quantzy.construction.portfolio import Portfolio
from quantzy.returns import to_returns

if TYPE_CHECKING:
    from quantzy.alpha.base import Alpha
    from quantzy.optimize.base import BaseOptimizer
    from quantzy.risk.base import RiskModel


@dataclass(slots=True)
class BacktestResult:
    """Output of a walk-forward run.

    Attributes:
        returns: Per-period portfolio returns, net of estimated trading cost on rebalance days.
        weights: Held weights through time (dates x tickers); the row for date ``t`` is the book
            that earns ``returns[t]``.
        rebalance_dates: The dates on which the optimizer was re-run.
    """

    returns: pd.Series
    weights: pd.DataFrame
    rebalance_dates: pd.DatetimeIndex


@dataclass(slots=True)
class WalkForwardBacktester:
    """Periodic-rebalance simulator.

    Args:
        rebalance_every: Trading days between rebalances (~21 = monthly).
        warmup: Minimum number of price rows required before the first rebalance. Must exceed
            the alpha's longest lookback (e.g. ``Momentum.lookback``), or ``forecast`` will raise.
    """

    rebalance_every: int = 21
    warmup: int = 252

    def run(
        self,
        prices: pd.DataFrame,
        alpha: Alpha,
        risk_model: RiskModel,
        optimizer: BaseOptimizer,
        *,
        nav: float = 100_000.0,
    ) -> BacktestResult:
        """Simulate the strategy and return its return series + weight history.

        ``alpha`` is any object exposing ``forecast(prices, asof=...)`` -- a single signal or an
        :class:`~quantzy.alpha.combiner.AlphaCombiner`. The same ``optimizer`` (with its
        constraints and cost model) is reused at every rebalance.
        """
        if len(prices) <= self.warmup:
            raise ValueError(f"need > {self.warmup} price rows for warmup, got {len(prices)}")

        asset_returns = to_returns(prices)
        tickers = list(prices.columns)

        # Rebalance on the warmup row, then every ``rebalance_every`` rows thereafter.
        rebal_positions = range(self.warmup, len(prices), self.rebalance_every)
        rebal_dates = {prices.index[i] for i in rebal_positions}

        current = Portfolio.empty(tickers, nav=nav)
        held = pd.Series(0.0, index=tickers)  # weights earning today's return

        port_returns: dict[pd.Timestamp, float] = {}
        weight_rows: dict[pd.Timestamp, pd.Series] = {}
        used_rebal: list[pd.Timestamp] = []

        # Walk from the first day we have weights to deploy (the warmup date) to the end.
        first_date = prices.index[self.warmup]
        for date in asset_returns.index[asset_returns.index >= first_date]:
            # Accrue the day's return on the book we were holding coming into ``date``.
            day_ret = float((held * asset_returns.loc[date]).sum())

            if date in rebal_dates:
                mu = alpha.forecast(prices, asof=date)
                sigma = risk_model.covariance(asset_returns.loc[:date])
                result = optimizer.optimize(mu, sigma, current=current)
                if result.trades.cost is not None:
                    day_ret -= float(result.trades.cost)  # pay to trade into the new book
                held = result.weights.reindex(tickers).fillna(0.0)
                current = Portfolio(weights=held, nav=nav)
                used_rebal.append(date)

            port_returns[date] = day_ret
            weight_rows[date] = held

        returns = pd.Series(port_returns, name="return").sort_index()
        weights = pd.DataFrame(weight_rows).T.reindex(columns=tickers).sort_index()
        return BacktestResult(
            returns=returns,
            weights=weights,
            rebalance_dates=pd.DatetimeIndex(used_rebal),
        )
