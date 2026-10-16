"""Run-comparison tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.backtest import WalkForwardBacktester
from quantzy.metrics import max_drawdown, sharpe_ratio
from quantzy.research import compare, summarize, yearly
from quantzy.signals import Momentum


@pytest.fixture
def runs(bundle, make_strategy):
    bt = WalkForwardBacktester(rebalance_every=42)
    fast = bt.run(bundle, make_strategy(signals=[Momentum(lookback=60, skip=5)]))
    slow = bt.run(bundle, make_strategy(signals=[Momentum(lookback=252, skip=21)]))
    return {"fast": fast, "slow": slow}


class TestCompare:
    def test_aligns_runs_to_their_common_dates(self, runs):
        """The fast run starts earlier; it is measured only where the slow one also trades."""
        assert runs["fast"].returns.index[0] < runs["slow"].returns.index[0]
        table = compare(runs)
        common = runs["slow"].returns
        assert table.loc["fast", "sharpe"] == pytest.approx(
            sharpe_ratio(runs["fast"].returns.loc[common.index])
        )
        assert table.loc["slow", "max_dd"] == pytest.approx(max_drawdown(common))

    def test_unaligned_measures_each_run_whole(self, runs):
        table = compare(runs, align=False)
        assert table.loc["fast", "sharpe"] == pytest.approx(sharpe_ratio(runs["fast"].returns))

    def test_accepts_a_bare_series_benchmark(self, runs, panel):
        bench = panel.pct_change().mean(axis=1).dropna()
        table = compare({**runs, "ew": bench})
        assert np.isnan(table.loc["ew", "turnover"])
        assert table.loc["fast", "turnover"] > 0

    def test_split_reports_full_train_and_test(self, runs):
        split = runs["slow"].returns.index[300]
        table = compare(runs, split)
        assert set(table.columns.get_level_values(0)) == {"full", "train", "test"}
        test = runs["slow"].returns.loc[split:]
        assert table.loc["slow", ("test", "sharpe")] == pytest.approx(sharpe_ratio(test))
        train = runs["slow"].returns.loc[: split].iloc[:-1]
        assert table.loc["slow", ("train", "sharpe")] == pytest.approx(sharpe_ratio(train))

    def test_rejects_disjoint_runs(self):
        a = pd.Series(0.01, index=pd.bdate_range("2020-01-01", periods=10))
        b = pd.Series(0.01, index=pd.bdate_range("2021-01-01", periods=10))
        with pytest.raises(ValueError, match="share no dates"):
            compare({"a": a, "b": b})


class TestSummarize:
    def test_turnover_counts_decisions_not_drift(self, runs):
        res = runs["slow"]
        out = summarize(res.returns, res)
        years = len(res.returns) / 252
        assert out["turnover"] == pytest.approx(sum(d.turnover for d in res.decisions) / years)

    def test_calmar_is_return_over_drawdown(self, runs):
        out = summarize(runs["slow"].returns)
        assert out["calmar"] == pytest.approx(out["ann_return"] / -out["max_dd"])


class TestYearly:
    def test_one_row_per_shared_calendar_year(self, runs):
        out = yearly(runs)
        common = runs["slow"].returns.index
        assert list(out.index) == sorted(set(common.year))
        assert list(out["return"].columns) == ["fast", "slow"]
        assert out["days"].sum().tolist() == [len(common), len(common)]

    def test_year_metrics_match_the_slice(self, runs):
        out = yearly(runs)
        year = out.index[1]
        r = runs["slow"].returns
        r = r[r.index.year == year]
        assert out.loc[year, ("return", "slow")] == pytest.approx((1 + r).prod() - 1)
        assert out.loc[year, ("sharpe", "slow")] == pytest.approx(sharpe_ratio(r))
        assert out.loc[year, ("max_dd", "slow")] == pytest.approx(max_drawdown(r))
