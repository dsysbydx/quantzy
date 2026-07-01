"""quantzy — a composable quant-investing library.

Pipeline: data -> alpha (mu) -> risk/variance (Sigma) -> optimization -> optimal trade.
"""

from quantzy.config import StrategyConfig
from quantzy.universe import Asset, Universe, default_etf_universe

__version__ = "0.1.0"

__all__ = ["StrategyConfig", "Asset", "Universe", "default_etf_universe", "__version__"]
