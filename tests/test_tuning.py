"""Holdout tuner tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.metrics import sharpe_ratio
from quantzy.research import holdout_tune, label
from quantzy.signals import Momentum


@pytest.fixture
def build(make_strategy):
    def _build(lookback, skip):
        return make_strategy(signals=[Momentum(lookback=lookback, skip=skip)])

    return _build


class TestHoldoutTune:
    def test_ranks_every_combo(self, bundle, build):
        grid = {"lookback": [60, 126], "skip": [5, 21]}
        res = holdout_tune(bundle, grid, build, rebalance_every=42)

        assert len(res.table) == 4
        assert list(res.table.columns) == [
            "lookback", "skip", "train_score", "test_score", "n_rebalances",
        ]
        # Sorted by train score, best first.
        assert res.table["train_score"].is_monotonic_decreasing
        assert res.best_params == {
            "lookback": res.table.loc[0, "lookback"],
            "skip": res.table.loc[0, "skip"],
        }

    def test_selection_uses_the_train_column_only(self, bundle, build):
        res = holdout_tune(bundle, {"lookback": [60, 126], "skip": [21]}, build,
                           rebalance_every=42)
        assert res.train_score == res.table["train_score"].max()
        # The winner need not lead on test -- that is the whole point of a holdout.
        assert res.test_score == res.table.loc[0, "test_score"]

    def test_split_is_continuous_not_a_cold_start(self, bundle, build):
        """The test slice continues the same run, so it never re-pays entry cost from cash."""
        res = holdout_tune(bundle, {"lookback": [126], "skip": [21]}, build, rebalance_every=42)
        returns = res.best_result.returns
        assert res.split_date in returns.index
        assert returns.index[0] < res.split_date < returns.index[-1]
        # One continuous run: every scored date belongs to exactly one slice.
        train = returns.loc[: res.split_date].iloc[:-1]
        test = returns.loc[res.split_date :]
        assert len(train) + len(test) == len(returns)

    def test_scores_match_the_metric_on_each_slice(self, bundle, build):
        res = holdout_tune(bundle, {"lookback": [126], "skip": [21]}, build, rebalance_every=42)
        test = res.best_result.returns.loc[res.split_date :]
        np.testing.assert_allclose(res.test_score, sharpe_ratio(test), rtol=1e-12)

    def test_combos_trade_the_same_dates_by_default(self, bundle, build):
        """A short and a long lookback are scored over one window, so they are comparable."""
        res = holdout_tune(bundle, {"lookback": [60, 504], "skip": [21]}, build,
                           rebalance_every=42)
        a, b = res.results.values()
        pd.testing.assert_index_equal(a.returns.index, b.returns.index)
        assert list(a.rebalance_dates) == list(b.rebalance_dates)

    def test_unaligned_warmup_is_derived_per_combo(self, bundle, build):
        """With align=False each combo starts as soon as its own warmup allows."""
        res = holdout_tune(bundle, {"lookback": [60, 504], "skip": [21]}, build,
                           rebalance_every=42, align=False)
        by_lookback = res.table.set_index("lookback")["n_rebalances"]
        assert by_lookback[60] > by_lookback[504]

    def test_keeps_every_result_in_ranked_order(self, bundle, build):
        res = holdout_tune(bundle, {"lookback": [60, 126], "skip": [5, 21]}, build,
                           rebalance_every=42)
        expected = [label({"lookback": r.lookback, "skip": r.skip})
                    for r in res.table.itertuples()]
        assert list(res.results) == expected
        assert res.results[expected[0]] is res.best_result

    def test_compare_splits_at_the_holdout_date(self, bundle, build):
        res = holdout_tune(bundle, {"lookback": [60, 126], "skip": [21]}, build,
                           rebalance_every=42)
        bench = res.best_result.returns * 0.5
        table = res.compare(top=1, half=bench)
        assert list(table.index) == [next(iter(res.results)), "half"]
        assert set(table.columns.get_level_values(0)) == {"full", "train", "test"}
        np.testing.assert_allclose(table.loc[table.index[0], ("test", "sharpe")],
                                   res.test_score, rtol=1e-12)

    def test_decay_reports_out_of_sample_survival(self, bundle, build):
        res = holdout_tune(bundle, {"lookback": [126], "skip": [21]}, build, rebalance_every=42)
        assert res.decay == pytest.approx(res.test_score / res.train_score)


class TestValidation:
    def test_rejects_an_empty_grid(self, bundle, build):
        with pytest.raises(ValueError, match="param_grid is empty"):
            holdout_tune(bundle, {}, build)

    @pytest.mark.parametrize("frac", [0.0, 1.0, -0.5, 2.0])
    def test_rejects_an_out_of_range_split(self, bundle, build, frac):
        with pytest.raises(ValueError, match="train_frac"):
            holdout_tune(bundle, {"lookback": [60], "skip": [5]}, build, train_frac=frac)


class TestCalibrateRiskAversion:
    @pytest.fixture
    def build_ra(self, make_strategy):
        from quantzy.optimize import LinearCost, MeanVariance
        from quantzy.optimize import constraints as C

        def _build(lam):  # no budget, so lambda alone sets the size
            opt = MeanVariance(risk_aversion=lam, constraints=[C.long_only()],
                               cost_model=LinearCost(cost_bps=5.0))
            return make_strategy(optimizer=opt)

        return _build

    def test_hits_target_within_tolerance(self, bundle, build_ra):
        from quantzy.backtest import WalkForwardBacktester
        from quantzy.research import calibrate_risk_aversion

        bt = WalkForwardBacktester(rebalance_every=21)
        lo = calibrate_risk_aversion(bundle, build_ra, 0.07, backtester=bt, tol=0.003,
                                     bounds=(1.0, 2000.0))
        hi = calibrate_risk_aversion(bundle, build_ra, 0.04, backtester=bt, tol=0.003,
                                     bounds=(1.0, 2000.0))
        for fit in (lo, hi):
            assert abs(fit.realized_vol - fit.target_vol) <= 0.003
        assert hi.risk_aversion > lo.risk_aversion  # a quieter book needs more aversion

    def test_unreachable_target_returns_boldest(self, bundle, build_ra):
        from quantzy.backtest import WalkForwardBacktester
        from quantzy.research import calibrate_risk_aversion

        fit = calibrate_risk_aversion(bundle, build_ra, 50.0, bounds=(1.0, 50.0),
                                      backtester=WalkForwardBacktester(rebalance_every=21))
        assert fit.risk_aversion == 1.0
        assert fit.realized_vol < 50.0

    def test_end_keeps_later_data_out(self, bundle, build_ra):
        from quantzy.backtest import WalkForwardBacktester
        from quantzy.research import calibrate_risk_aversion

        end = bundle.index[600]
        fit = calibrate_risk_aversion(bundle, build_ra, 0.12, end=end,
                                      backtester=WalkForwardBacktester(rebalance_every=21))
        assert fit.result.returns.index.max() <= end

    def test_rejects_bad_target(self, bundle, build_ra):
        from quantzy.research import calibrate_risk_aversion

        with pytest.raises(ValueError, match="target_vol"):
            calibrate_risk_aversion(bundle, build_ra, 0.0)
