import pytest

from quantzy.alpha import Momentum
from quantzy.backtest import TuneResult, holdout_tune
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.risk import LedoitWolfCovariance


def _tune(prices, param_grid, **kwargs):
    optimizer = MeanVariance(
        risk_aversion=5.0,
        constraints=[C.budget(), C.long_only()],
        cost_model=LinearCost(cost_bps=5.0),
    )
    return holdout_tune(
        prices,
        param_grid,
        build_alpha=lambda lookback, skip: Momentum(lookback=lookback, skip=skip),
        risk_model=LedoitWolfCovariance(),
        optimizer=optimizer,
        rebalance_every=21,
        warmup=80,
        **kwargs,
    )


class TestHoldoutTune:
    GRID = {"lookback": [40, 60], "skip": [5, 21]}

    def test_table_has_one_row_per_combo(self, noisy_prices):
        res = _tune(noisy_prices, self.GRID)
        assert isinstance(res, TuneResult)
        assert len(res.table) == 4  # 2 lookbacks x 2 skips
        assert set(res.table.columns) == {"lookback", "skip", "train_score"}

    def test_best_params_is_argmax_of_train_table(self, noisy_prices):
        res = _tune(noisy_prices, self.GRID)
        top = res.table.iloc[0]  # sorted best-first
        assert res.best_params == {"lookback": top["lookback"], "skip": top["skip"]}
        assert res.train_score == pytest.approx(top["train_score"])

    def test_reports_out_of_sample_test_score(self, noisy_prices):
        res = _tune(noisy_prices, self.GRID, train_frac=0.6)
        # The held-out backtest actually ran and produced returns.
        assert len(res.test_result.returns) > 0
        assert res.test_score == res.test_score  # finite (not NaN) for this data

    def test_raises_on_empty_grid(self, noisy_prices):
        with pytest.raises(ValueError, match="empty"):
            _tune(noisy_prices, {})

    def test_raises_when_slice_too_short_for_warmup(self, noisy_prices):
        # train_frac so small the train slice can't clear warmup.
        with pytest.raises(ValueError, match="warmup"):
            _tune(noisy_prices, self.GRID, train_frac=0.1)
