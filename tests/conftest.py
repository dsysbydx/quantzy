"""Shared synthetic fixtures -- no network access in any test."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def dates():
    return pd.bdate_range("2020-01-01", periods=300)


@pytest.fixture
def trend_prices(dates):
    """Three assets with clearly ordered drift: UP > FLAT > DOWN."""
    n = len(dates)
    up = 100.0 * (1.003) ** np.arange(n)
    flat = np.full(n, 100.0)
    down = 100.0 * (0.997) ** np.arange(n)
    return pd.DataFrame({"UP": up, "FLAT": flat, "DOWN": down}, index=dates)


@pytest.fixture
def random_returns(dates):
    """Reproducible correlated-ish returns for four assets."""
    rng = np.random.default_rng(42)
    cols = ["A", "B", "C", "D"]
    data = rng.normal(0.0005, 0.01, size=(len(dates), len(cols)))
    return pd.DataFrame(data, index=dates, columns=cols)


@pytest.fixture
def noisy_prices():
    """Long, reproducible price panel with noisy drift (so returns vary -> Sharpe defined).

    Three assets with clearly ordered drift (UP > MID > DOWN) plus idiosyncratic noise, over
    enough history to clear a momentum warmup and run several rebalances.
    """
    dates = pd.bdate_range("2018-01-01", periods=500)
    rng = np.random.default_rng(7)
    drifts = {"UP": 0.0015, "MID": 0.0003, "DOWN": -0.0010}
    out = {}
    for col, drift in drifts.items():
        rets = rng.normal(drift, 0.006, size=len(dates))
        out[col] = 100.0 * np.exp(np.cumsum(rets))
    return pd.DataFrame(out, index=dates)
