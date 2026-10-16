"""Saved strategies: they build, decide within their limits, and stay reproducible."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.data.bundle import DataBundle
from quantzy.signals import Identity, Momentum
from quantzy.strategies import REGISTRY, core_multiasset, get


@pytest.fixture(scope="module")
def core_bundle():
    """Synthetic prices for the core universe: trending risky assets and a T-bill."""
    rng = np.random.default_rng(7)
    dates = pd.bdate_range("2015-01-01", periods=700)
    drift = rng.normal(0.0002, 0.0004, len(core_multiasset.RISKY))
    rets = rng.normal(drift, 0.01, size=(len(dates), len(drift)))
    px = pd.DataFrame(100 * np.cumprod(1 + rets, axis=0), index=dates,
                      columns=core_multiasset.RISKY)
    px[core_multiasset.CASH] = 1.0001 ** np.arange(len(dates))
    return DataBundle.build(px)


class TestRegistry:
    def test_core_is_registered(self):
        assert get("core_multiasset") is core_multiasset
        assert set(REGISTRY) == {"core_multiasset"}

    def test_unknown_name_lists_known(self):
        with pytest.raises(KeyError, match="core_multiasset"):
            get("nope")


class TestCoreMultiasset:
    def test_decision_respects_portfolio_limits(self, core_bundle):
        p = core_multiasset.PORTFOLIO
        w = core_multiasset.build().decide(core_bundle).weights
        risky = w.drop(core_multiasset.CASH)
        assert w.sum() == pytest.approx(1.0, abs=1e-6)
        assert (risky >= -1e-6).all()
        assert risky.sum() <= p.max_leverage + 1e-6
        assert (risky <= p.max_position * risky.sum() + 1e-6).all()

    def test_alphas_and_portfolio_are_separate(self):
        s = core_multiasset.build()
        assert set(s.weights) == set(core_multiasset.ALPHAS)
        assert s.optimizer.risk_aversion == core_multiasset.PORTFOLIO.risk_aversion

    def test_candidate_alpha_keeps_portfolio_settings(self, core_bundle):
        extra = Momentum(lookback=126, skip=5, normalizer=Identity())
        s = core_multiasset.build(alphas={**core_multiasset.ALPHAS, "mom": (extra, 0.05)})
        assert s.weights["mom"] == 0.05
        assert s.optimizer.risk_aversion == core_multiasset.PORTFOLIO.risk_aversion
        s.decide(core_bundle)  # still solves

    def test_zero_weight_alpha_is_dropped(self):
        alphas = {**core_multiasset.ALPHAS}
        alphas["trend"] = (alphas["trend"][0], 0.0)
        assert "trend" not in core_multiasset.build(alphas=alphas).weights
