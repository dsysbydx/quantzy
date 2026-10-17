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


@pytest.fixture
def panel():
    """Four assets, ~5.5y of business days, reproducible, with ordered drift.

    Long enough to clear a 252-row momentum warmup and still leave several years to trade.
    """
    dates = pd.bdate_range("2015-01-01", periods=1400)
    rng = np.random.default_rng(7)
    spec = {"UP": (0.0012, 0.006), "MID": (0.0003, 0.006), "DOWN": (-0.0008, 0.006),
            "CASH": (0.00008, 0.0004)}
    return pd.DataFrame(
        {c: 100.0 * np.exp(np.cumsum(rng.normal(mu, sd, len(dates))))
         for c, (mu, sd) in spec.items()},
        index=dates,
    )


@pytest.fixture
def bundle(panel):
    from quantzy.data.bundle import DataBundle

    return DataBundle.build(panel)


@pytest.fixture
def universe(panel):
    from quantzy.universe import Universe

    return Universe.from_tickers(list(panel.columns))


@pytest.fixture
def make_strategy(universe):
    """Factory for a complete strategy; override any piece per test."""
    from quantzy.optimize import LinearCost, MeanVariance
    from quantzy.optimize import constraints as C
    from quantzy.risk import LedoitWolfCovariance
    from quantzy.signals import Momentum
    from quantzy.strategy import Strategy

    def _make(signals=None, risk_model=None, optimizer=None, **kwargs):
        return Strategy(
            universe=universe,
            signals=signals or [Momentum(lookback=252, skip=21)],
            risk_model=risk_model or LedoitWolfCovariance(lookback=252),
            optimizer=optimizer
            or MeanVariance(
                risk_aversion=8.0,
                constraints=[C.budget(), C.long_only()],
                cost_model=LinearCost(cost_bps=5.0),
            ),
            **kwargs,
        )

    return _make


@pytest.fixture(scope="module")
def core_bundle():
    """Synthetic data for the core universe: trending risky assets, a T-bill, the VIX and SPY
    panels the variance risk premium reads, and Treasury carry. Long enough for VRP's 5-year
    z-score."""
    from quantzy.data.bundle import DataBundle
    from quantzy.strategies import core_multiasset

    rng = np.random.default_rng(7)
    dates = pd.bdate_range("2015-01-01", periods=1500)
    drift = rng.normal(0.0002, 0.0004, len(core_multiasset.RISKY))
    rets = rng.normal(drift, 0.01, size=(len(dates), len(drift)))
    px = pd.DataFrame(100 * np.cumprod(1 + rets, axis=0), index=dates,
                      columns=core_multiasset.RISKY)
    px[core_multiasset.CASH] = 1.0001 ** np.arange(len(dates))
    spy = 100 * np.cumprod(1 + rng.normal(0.0003, 0.01, len(dates)))
    vix = 18 + 5 * np.sin(np.arange(len(dates)) / 50) + rng.normal(0, 1, len(dates))

    def wide(x):
        return pd.DataFrame({c: x for c in px.columns}, index=dates)

    carry = pd.DataFrame(np.nan, index=dates, columns=px.columns)
    carry["IEF"], carry["TLT"] = 0.01, 0.015

    return DataBundle.build(px, vix=wide(vix), market=wide(spy), carry=carry)
