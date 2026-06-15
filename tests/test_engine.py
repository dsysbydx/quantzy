import numpy as np
import pandas as pd
import pytest

from quantzy.alpha import Momentum
from quantzy.backtest import WalkForwardBacktester, sharpe_ratio
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.risk import LedoitWolfCovariance


def _backtester(warmup=100, rebalance_every=21):
    return WalkForwardBacktester(rebalance_every=rebalance_every, warmup=warmup)


class TestWalkForwardBacktester:
    def _run(self, prices, **kwargs):
        alpha = Momentum(lookback=60, skip=5)
        optimizer = MeanVariance(
            risk_aversion=5.0,
            constraints=[C.budget(), C.long_only()],
            cost_model=LinearCost(cost_bps=5.0),
        )
        return _backtester(**kwargs).run(prices, alpha, LedoitWolfCovariance(), optimizer)

    def test_produces_return_series_and_weight_history(self, noisy_prices):
        res = self._run(noisy_prices)

        # Non-empty, aligned return series with a usable Sharpe.
        assert len(res.returns) > 0
        assert res.returns.index.is_monotonic_increasing
        assert np.isfinite(sharpe_ratio(res.returns))

        # Weights: one row per return date, one column per asset.
        assert list(res.weights.columns) == list(noisy_prices.columns)
        assert res.weights.index.equals(res.returns.index)
        assert len(res.rebalance_dates) > 0

    def test_first_day_has_no_market_pnl(self, noisy_prices):
        res = self._run(noisy_prices)
        # The first dated row is the warmup/first rebalance: the book was empty entering the day,
        # so there is no market P&L -- only the (negative) cost of trading into the position.
        assert res.returns.iloc[0] <= 1e-12

    def test_long_only_budget_holds_after_first_rebalance(self, noisy_prices):
        res = self._run(noisy_prices)
        invested = res.weights.iloc[1:]  # after the initial empty book
        assert (invested.sum(axis=1) <= 1.0 + 1e-5).all()
        assert (invested.to_numpy() >= -1e-5).all()  # tiny negatives are solver slack

    def test_tilts_to_the_trending_winner(self, noisy_prices):
        res = self._run(noisy_prices)
        # Momentum + long-only mean-variance should, on average, favour the strong up-trender
        # over the down-trender.
        avg_w = res.weights.mean()
        assert avg_w["UP"] > avg_w["DOWN"]

    def test_rejects_history_shorter_than_warmup(self):
        prices = pd.DataFrame(
            {"A": np.linspace(100, 110, 50)},
            index=pd.bdate_range("2020-01-01", periods=50),
        )
        with pytest.raises(ValueError, match="warmup"):
            self._run(prices, warmup=100)
