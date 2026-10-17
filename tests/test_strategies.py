"""Saved strategies: they build, decide within their limits, and stay reproducible."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.signals import Identity, Momentum
from quantzy.strategies import REGISTRY, core_multiasset, get


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


class TestTradingDays:
    def test_holiday_rows_are_dropped_and_listings_kept(self):
        idx = pd.bdate_range("2024-01-01", periods=5)
        raw = pd.DataFrame({"A": [1.0, 2.0, np.nan, 3.0, 4.0],
                            "B": [np.nan, np.nan, np.nan, 5.0, 6.0], "^IRX": [5.0] * 5},
                           index=idx)
        out = core_multiasset.etf_trading_days(raw, ["A", "B"])
        assert list(out.index) == [idx[0], idx[1], idx[3], idx[4]]  # B not yet listed: kept
