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

    def test_cost_aversion_widens_the_no_trade_band(self, two_asset):
        mu, sigma = two_asset
        current = Portfolio(weights=pd.Series({"X": 0.5, "Y": 0.5}), nav=1_000.0)

        def solve(aversion):
            return MeanVariance(
                risk_aversion=5.0, cost_model=LinearCost(cost_bps=10.0), cost_aversion=aversion
            ).optimize(mu, sigma, current=current)

        annual, monthly = solve(1.0), solve(1_000.0)
        # Paying 10bp once is cheap against an annual edge, so it trades...
        assert annual.trades.turnover > 0.05
        # ...but demanding the trade pay for itself almost at once leaves the book alone.
        assert np.allclose(monthly.weights.to_numpy(), [0.5, 0.5], atol=1e-4)

    def test_cost_aversion_does_not_change_the_reported_cost(self, two_asset):
        mu, sigma = two_asset
        current = Portfolio(weights=pd.Series({"X": 0.0, "Y": 1.0}), nav=1_000.0)
        res = MeanVariance(
            risk_aversion=5.0, cost_model=LinearCost(cost_bps=10.0), cost_aversion=12.0
        ).optimize(mu, sigma, current=current)
        traded = (res.weights - current.weights).abs().sum()
        assert res.trades.cost == pytest.approx(10.0 / 1e4 * traded)

    def test_negative_cost_aversion_rejected(self):
        with pytest.raises(ValueError, match="cost_aversion"):
            MeanVariance(cost_aversion=-1.0)


class TestExecutionCost:
    """The cost charged on trades can differ from the cost the objective trades off."""

    @pytest.fixture
    def setup(self):
        tickers = ["X", "Y", "CASH"]
        mu = pd.Series({"X": 0.10, "Y": 0.05, "CASH": 0.0})
        sigma = pd.DataFrame(np.diag([0.04, 0.01, 1e-6]), index=tickers, columns=tickers)
        current = Portfolio(weights=pd.Series({"X": 0.2, "Y": 0.3, "CASH": 0.5}), nav=1_000.0)
        return mu, sigma, current

    def test_charged_cost_uses_execution_model_and_leaves_trades_alone(self, setup):
        mu, sigma, current = setup
        kw = dict(risk_aversion=2.0, cost_model=LinearCost(5.0), cost_aversion=10.0,
                  constraints=[C.budget(), C.long_only()])
        base = MeanVariance(**kw).optimize(mu, sigma, current=current)
        execution = LinearCost(0.0, spread_bps={"X": 2.0, "Y": 1.0})
        split = MeanVariance(**kw, execution_cost=execution).optimize(mu, sigma, current=current)
        pd.testing.assert_series_equal(base.weights, split.weights)
        dw = (split.weights - current.weights)
        expected = (2.0 * abs(dw["X"]) + 1.0 * abs(dw["Y"])) / 1e4  # cash is free
        assert split.trades.cost == pytest.approx(expected)
        assert base.trades.cost == pytest.approx(5.0 / 1e4 * dw.abs().sum())

    def test_portfolio_config_wires_execution_cost(self):
        from quantzy.portfolio import PortfolioConfig

        opt = PortfolioConfig(execution_cost_bps={"X": 2.0}).optimizer("CASH")
        assert opt.execution_cost.spread_bps == {"X": 2.0}
        assert opt.cost_model.cost_bps == 5.0
        assert PortfolioConfig().optimizer("CASH").execution_cost is None


class TestCachedProblem:
    @pytest.fixture
    def three(self):
        tickers = ["X", "Y", "Z"]
        sigma = pd.DataFrame(np.diag([0.04, 0.02, 0.01]), index=tickers, columns=tickers)
        return tickers, sigma

    def test_problem_is_built_once_and_resolved_with_new_numbers(self, three):
        tickers, sigma = three
        opt = MeanVariance(risk_aversion=2.0)
        a = opt.optimize(pd.Series([0.10, 0.0, 0.0], index=tickers), sigma).weights
        built = dict(opt._built)
        b = opt.optimize(pd.Series([0.0, 0.0, 0.10], index=tickers), sigma).weights
        assert opt._built == built  # no rebuild
        assert a.idxmax() == "X" and b.idxmax() == "Z"

    def test_matches_a_freshly_built_problem(self, three):
        tickers, sigma = three
        current = Portfolio(weights=pd.Series([0.2, 0.5, 0.3], index=tickers), nav=1.0)
        reused = MeanVariance(risk_aversion=3.0, cost_model=LinearCost(20.0))
        rng = np.random.default_rng(1)
        for _ in range(5):
            mu = pd.Series(rng.normal(0.05, 0.05, 3), index=tickers)
            fresh = MeanVariance(risk_aversion=3.0, cost_model=LinearCost(20.0))
            np.testing.assert_allclose(reused.optimize(mu, sigma, current).weights,
                                       fresh.optimize(mu, sigma, current).weights, atol=1e-6)

    def test_turnover_cap_tracks_the_current_book(self, three):
        tickers, sigma = three
        mu = pd.Series([0.20, 0.0, 0.0], index=tickers)
        opt = MeanVariance(risk_aversion=0.1,
                           constraints=[C.budget(), C.long_only(), C.turnover_cap(0.2)])
        for start in ([0.0, 0.0, 1.0], [0.0, 1.0, 0.0]):
            current = Portfolio(weights=pd.Series(start, index=tickers), nav=1.0)
            w = opt.optimize(mu, sigma, current).weights
            assert (w - current.weights).abs().sum() <= 0.2 + 1e-6
            assert w["X"] == pytest.approx(0.1, abs=1e-4)

    def test_objective_baking_in_arrays_is_rejected(self, three):
        tickers, sigma = three
        import cvxpy as cp

        class Baked(MeanVariance):
            def objective(self, w, inputs):
                # mu * mu is a product of two parameters: valid convex maths, but cvxpy
                # cannot re-solve it by refilling numbers.
                return cp.multiply(inputs.mu, inputs.mu) @ w - inputs.variance(w)

        with pytest.raises(TypeError, match="DPP"):
            Baked().optimize(pd.Series(0.05, index=tickers), sigma)

    def test_optimizer_pickles_without_its_built_problem(self, three):
        import pickle

        tickers, sigma = three
        opt = MeanVariance()
        opt.optimize(pd.Series(0.05, index=tickers), sigma)
        clone = pickle.loads(pickle.dumps(opt))
        assert clone._built == {}
        clone.optimize(pd.Series(0.05, index=tickers), sigma)


class TestRiskConstraints:
    @pytest.fixture
    def book(self):
        tickers = ["A", "B", "CASH"]
        mu = pd.Series({"A": 0.10, "B": 0.08, "CASH": 0.0})
        sigma = pd.DataFrame(np.diag([0.04, 0.0225, 1e-6]), index=tickers, columns=tickers)
        return mu, sigma

    def test_max_vol_binds_and_holds_across_solves(self, book):
        mu, sigma = book
        opt = MeanVariance(risk_aversion=0.1,
                           constraints=[C.budget(), C.long_only(), C.max_vol(0.10)])
        for scale in (1.0, 2.0):  # the second solve reuses the cached problem
            w = opt.optimize(mu, sigma * scale).weights.to_numpy()
            vol = float(np.sqrt(w @ (sigma.to_numpy() * scale) @ w))
            assert vol == pytest.approx(0.10, abs=1e-5)
            assert w[2] > 0  # the rest sits in cash

    def test_max_weight_exempts_cash(self, book):
        mu, sigma = book
        opt = MeanVariance(risk_aversion=0.01, constraints=[
            C.budget(), C.long_only(), C.max_weight(0.3, exclude=("CASH",))])
        w = opt.optimize(mu, sigma).weights
        assert w[["A", "B"]].max() <= 0.3 + 1e-6
        assert w["CASH"] == pytest.approx(0.4, abs=1e-5)

    def test_max_leverage_borrows_cash_up_to_limit(self, book):
        mu, sigma = book
        lev = [C.budget(), C.long_only(exclude=("CASH",)), C.max_leverage(1.2, cash="CASH")]
        w = MeanVariance(risk_aversion=0.01, constraints=lev).optimize(mu, sigma).weights
        assert w[["A", "B"]].sum() == pytest.approx(1.2, abs=1e-5)
        assert w["CASH"] == pytest.approx(-0.2, abs=1e-5)
        assert (w[["A", "B"]] >= -1e-8).all()

    def test_max_leverage_one_means_no_borrowing(self, book):
        mu, sigma = book
        cons = [C.budget(), C.long_only(exclude=("CASH",)), C.max_leverage(1.0, cash="CASH")]
        w = MeanVariance(risk_aversion=0.01, constraints=cons).optimize(mu, sigma).weights
        assert w["CASH"] >= -1e-6

    def test_max_leverage_rejects_unknown_cash(self, book):
        mu, sigma = book
        opt = MeanVariance(constraints=[C.budget(), C.max_leverage(1.2, cash="NOPE")])
        with pytest.raises(ValueError, match="NOPE"):
            opt.optimize(mu, sigma)

    def test_max_book_share_scales_with_leverage(self):
        tickers = ["A", "B", "C", "CASH"]
        mu = pd.Series({"A": 0.10, "B": 0.09, "C": 0.02, "CASH": 0.0})
        sigma = pd.DataFrame(np.diag([0.04, 0.04, 0.04, 1e-6]), index=tickers, columns=tickers)
        cons = [C.budget(), C.long_only(exclude=("CASH",)), C.max_leverage(1.3, cash="CASH"),
                C.max_book_share(0.4, exclude=("CASH",))]
        w = MeanVariance(risk_aversion=0.01, constraints=cons).optimize(mu, sigma).weights
        book = w[["A", "B", "C"]].sum()
        assert book == pytest.approx(1.3, abs=1e-4)
        assert w["A"] == pytest.approx(0.4 * 1.3, abs=1e-4)  # 52% of capital, above 40%
        assert (w[["A", "B", "C"]] <= 0.4 * book + 1e-6).all()

    def test_max_book_share_rejects_bad_limit(self):
        with pytest.raises(ValueError):
            C.max_book_share(0.0)

    def test_max_vol_rejects_nonpositive_target(self):
        with pytest.raises(ValueError):
            C.max_vol(0.0)
