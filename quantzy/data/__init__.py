"""Data access: a source-agnostic reader protocol plus reference implementations."""

from quantzy.data.base import DataReader
from quantzy.data.cache import CachedReader
from quantzy.data.yfinance_reader import YFinanceReader

__all__ = ["DataReader", "CachedReader", "YFinanceReader"]
