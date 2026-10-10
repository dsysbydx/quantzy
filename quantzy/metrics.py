"""Performance and risk metrics for a return series."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.config import TRADING_DAYS


def annualized_return(returns: pd.Series, periods: int = TRADING_DAYS) -> float:
    """Geometric annualized return of a per-period return series."""
    returns = returns.dropna()
    if returns.empty:
        return float("nan")
    growth = (1.0 + returns).prod()
    return float(growth ** (periods / len(returns)) - 1.0)


def annualized_vol(returns: pd.Series, periods: int = TRADING_DAYS) -> float:
    """Annualized standard deviation of a per-period return series."""
    returns = returns.dropna()
    return float(returns.std(ddof=1) * np.sqrt(periods))


def sharpe_ratio(returns: pd.Series, rf: float = 0.0, periods: int = TRADING_DAYS) -> float:
    """Annualized Sharpe ratio; ``rf`` is the per-period risk-free rate."""
    returns = returns.dropna()
    excess = returns - rf
    sd = excess.std(ddof=1)
    if not np.isfinite(sd) or sd < 1e-12:  # constant series -> Sharpe undefined
        return float("nan")
    return float(excess.mean() / sd * np.sqrt(periods))


def max_drawdown(returns: pd.Series) -> float:
    """Worst peak-to-trough decline of the cumulative return path (negative number)."""
    returns = returns.dropna()
    if returns.empty:
        return float("nan")
    curve = (1.0 + returns).cumprod()
    running_max = curve.cummax()
    drawdown = curve / running_max - 1.0
    return float(drawdown.min())


def turnover(weights: pd.DataFrame) -> pd.Series:
    """Per-rebalance one-way turnover from a time series of weight vectors (dates x tickers)."""
    changes = weights.fillna(0.0).diff().abs().sum(axis=1) * 0.5
    return changes.iloc[1:]
