"""Backtesting: a walk-forward engine and the simulated book it runs against.

Performance metrics live in :mod:`quantzy.metrics` (they score any return series, live or
simulated) and parameter search in :mod:`quantzy.research.tuning`.
"""

from quantzy.backtest.benchmark import FixedWeights
from quantzy.backtest.book import SimulatedBook
from quantzy.backtest.engine import BacktestResult, WalkForwardBacktester

__all__ = ["WalkForwardBacktester", "BacktestResult", "SimulatedBook", "FixedWeights"]
