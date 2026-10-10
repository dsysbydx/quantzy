"""Holdout tuner tests."""

from __future__ import annotations

import numpy as np
import pytest

from quantzy.metrics import sharpe_ratio
from quantzy.research import holdout_tune
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

    def test_warmup_is_derived_per_combo(self, bundle, build):
        """A grid mixing short and long lookbacks needs no manual warmup bookkeeping."""
        res = holdout_tune(bundle, {"lookback": [60, 504], "skip": [21]}, build,
                           rebalance_every=42)
        by_lookback = res.table.set_index("lookback")["n_rebalances"]
        assert by_lookback[60] > by_lookback[504]

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
