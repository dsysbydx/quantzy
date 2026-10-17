"""quantzy -- a composable quant-investing library.

Pipeline: data -> signals (mu) -> risk (Sigma) -> optimization -> optimal trade.

The pipeline is composed in exactly one place, :meth:`quantzy.strategy.Strategy.decide`,
which the backtest engine and a production job both call. Research tooling
(:mod:`quantzy.research`) and the simulator (:mod:`quantzy.backtest`) sit around that call;
neither is on the path a live run takes.
"""

from quantzy.portfolio import PortfolioConfig
from quantzy.strategy import Decision, Strategy
from quantzy.universe import Asset, Universe, sector_etf_universe

__version__ = "0.1.0"

__all__ = [
    "Strategy",
    "Decision",
    "PortfolioConfig",
    "Asset",
    "Universe",
    "sector_etf_universe",
    "__version__",
]
