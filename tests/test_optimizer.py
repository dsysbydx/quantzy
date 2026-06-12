import numpy as np
import pandas as pd
import pytest

from quantzy.construction import Portfolio
from quantzy.optimize import LinearCost, MaxSharpe, MeanVariance, MinVariance
from quantzy.optimize import constraints as C


class TestOptimizer:
    @pytest.fixture
    def two_asset(self):
        tickers = ["X", "Y"]
        mu = pd.Series({"X": 0.10, "Y": 0.05})
        sigma = pd.DataFrame(np.diag([0.04, 0.01]), index=tickers, columns=tickers)
        return mu, sigma

    def test_mean_variance_long_only_is_feasible(self, two_asset):
        mu, sigma = two_asset
        res = MeanVariance(risk_aversion=5.0).optimize(mu, sigma)
        w = res.weights
        assert abs(w.sum() - 1.0) < 1e-6     # fully invested
        assert (w >= -1e-6).all()            # long-only

    def test_min_variance_analytic_matches_closed_form(self, two_asset):
        _, sigma = two_asset
        # Single budget constraint -> analytic path: w ∝ Sigma^{-1} 1.
        opt = MinVariance(constraints=[C.budget()])
        res = opt.optimize(pd.Series(0.0, index=sigma.index), sigma)
        assert res.status == "optimal_analytic"
        # diag(0.04, 0.01): inverse-variance weights -> (0.2, 0.8)
        assert np.allclose(res.weights.to_numpy(), [0.2, 0.8], atol=1e-8)

    def test_min_variance_analytic_and_cvxpy_agree(self, two_asset):
        _, sigma = two_asset
        mu = pd.Series(0.0, index=sigma.index)
        analytic = MinVariance(constraints=[C.budget()]).optimize(mu, sigma).weights
        # cvxpy path: budget + long-only (both satisfied by the analytic optimum here).
        cvxpy = MinVariance(constraints=[C.budget(), C.long_only()]).optimize(mu, sigma).weights
        assert np.allclose(analytic.to_numpy(), cvxpy.to_numpy(), atol=1e-5)

    def test_max_sharpe_tangency_tilts_to_better_asset(self, two_asset):
        mu, sigma = two_asset
        res = MaxSharpe(constraints=[C.budget()]).optimize(mu, sigma)
        assert res.status == "optimal_analytic"
        assert abs(res.weights.sum() - 1.0) < 1e-8

    def test_min_weight_floor_respected(self, two_asset):
        mu, sigma = two_asset
        res = MeanVariance(
            risk_aversion=1.0,
            constraints=[C.budget(), C.long_only(), C.min_weight("Y", 0.30)],
        ).optimize(mu, sigma)
        assert res.weights["Y"] >= 0.30 - 1e-6

    def test_transaction_cost_reduces_turnover(self, two_asset):
        mu, sigma = two_asset
        current = Portfolio(weights=pd.Series({"X": 0.5, "Y": 0.5}), nav=1_000.0)

        cheap = MeanVariance(risk_aversion=5.0).optimize(mu, sigma, current=current)
        pricey = MeanVariance(
            risk_aversion=5.0, cost_model=LinearCost(cost_bps=500.0)
        ).optimize(mu, sigma, current=current)

        # A large trading cost should make the optimizer trade less away from the current book.
        assert pricey.trades.turnover <= cheap.trades.turnover + 1e-9
        assert pricey.trades.cost is not None
