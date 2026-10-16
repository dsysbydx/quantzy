"""Walk-forward engine tests.

The engine's job is to supply a clock and a book, then ask the strategy the same question a
production job asks. So these tests check the *simulation* -- accounting, timing, warmup --
and leave the decision itself to ``test_strategy.py``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.backtest import BacktestResult, FixedWeights, WalkForwardBacktester
from quantzy.metrics import sharpe_ratio
from quantzy.optimize import LinearCost
from quantzy.signals import Momentum
from quantzy.universe import Universe


def _run(bundle, strategy, **kwargs):
    return WalkForwardBacktester(**kwargs).run(bundle, strategy)


class TestOutputShape:
    def test_produces_aligned_series(self, bundle, make_strategy):
        res = _run(bundle, make_strategy())
        assert isinstance(res, BacktestResult)
        assert res.returns.index.is_monotonic_increasing
        assert res.weights.index.equals(res.returns.index)
        assert res.nav.index.equals(res.returns.index)
        assert list(res.weights.columns) == ["UP", "MID", "DOWN", "CASH"]
        assert np.isfinite(sharpe_ratio(res.returns))

    def test_records_every_decision(self, bundle, make_strategy):
        res = _run(bundle, make_strategy(), rebalance_every=21)
        assert len(res.decisions) == len(res.rebalance_dates) > 0
        assert all(d.asof in res.returns.index for d in res.decisions)
        assert res.decision_at(res.rebalance_dates[0]) is res.decisions[0]

    def test_decision_at_rejects_a_non_rebalance_date(self, bundle, make_strategy):
        res = _run(bundle, make_strategy())
        with pytest.raises(KeyError, match="no rebalance on"):
            res.decision_at("1999-01-04")


class TestAccounting:
    def test_nav_compounds(self, bundle, make_strategy):
        """NAV must track realized returns, not reset to its starting value each rebalance."""
        res = _run(bundle, make_strategy(), rebalance_every=21)
        expected = 100_000.0 * (1.0 + res.returns).cumprod()
        np.testing.assert_allclose(res.nav.to_numpy(), expected.to_numpy(), rtol=1e-12)

    def test_notional_tracks_the_grown_book(self, bundle, make_strategy):
        """A later decision sizes trades against the book as it is, not as it started."""
        res = _run(bundle, make_strategy(), rebalance_every=21)
        last = res.decisions[-1]
        nav_before = res.nav.loc[: last.asof].iloc[-2]
        traded = last.trades.to_frame()["delta_notional"].abs().sum()
        weight_traded = last.trades.to_frame()["delta_weight"].abs().sum()
        np.testing.assert_allclose(traded, weight_traded * nav_before, rtol=1e-6)

    def test_cost_is_charged_on_rebalance_days(self, bundle, make_strategy):
        """A costlier book pays the difference on the days it trades, and only those days."""
        from quantzy.optimize import LinearCost, MeanVariance
        from quantzy.optimize import constraints as C

        def optimizer(bps):
            return MeanVariance(
                risk_aversion=8.0,
                constraints=[C.budget(), C.long_only()],
                cost_model=LinearCost(cost_bps=bps),
            )

        free = _run(bundle, make_strategy(optimizer=optimizer(0.0)), rebalance_every=21)
        dear = _run(bundle, make_strategy(optimizer=optimizer(50.0)), rebalance_every=21)

        assert free.decisions[0].trades.cost == pytest.approx(0.0, abs=1e-12)
        assert dear.decisions[0].trades.cost > 0
        # Non-rebalance days are pure market P&L in both runs, so they cannot differ by cost.
        off_days = free.returns.index.difference(free.rebalance_dates)
        assert (free.returns.loc[off_days] - dear.returns.loc[off_days]).abs().max() < 1.0

    def test_weights_row_is_the_book_that_earns_that_day(self, bundle, make_strategy):
        """weights[t] . asset_returns[t] - cost[t] == returns[t]."""
        from quantzy.returns import to_returns

        res = _run(bundle, make_strategy(), rebalance_every=21)
        asset_returns = to_returns(bundle.prices)
        costs = pd.Series(
            {d.asof: float(d.trades.cost or 0.0) for d in res.decisions}
        ).reindex(res.returns.index).fillna(0.0)
        gross = (res.weights * asset_returns.loc[res.weights.index]).sum(axis=1)
        np.testing.assert_allclose(
            (gross - costs).to_numpy(), res.returns.to_numpy(), atol=1e-12
        )


class TestConstraints:
    def test_long_only_budget_holds(self, bundle, make_strategy):
        res = _run(bundle, make_strategy())
        assert (res.weights.sum(axis=1) <= 1.0 + 1e-5).all()
        assert (res.weights.to_numpy() >= -1e-5).all()

    def test_tilts_to_the_trending_winner(self, bundle, make_strategy):
        res = _run(bundle, make_strategy())
        avg = res.weights.mean()
        assert avg["UP"] > avg["DOWN"]


class TestWarmup:
    def test_warmup_defaults_to_the_strategy_requirement(self, bundle, make_strategy):
        strategy = make_strategy()
        res = _run(bundle, strategy)
        assert res.returns.index[0] == bundle.index[strategy.min_history]

    def test_longer_lookback_delays_the_first_trade(self, bundle, make_strategy):
        short = _run(bundle, make_strategy(signals=[Momentum(lookback=60, skip=5)]))
        long = _run(bundle, make_strategy(signals=[Momentum(lookback=252, skip=21)]))
        assert short.returns.index[0] < long.returns.index[0]

    def test_rejects_history_shorter_than_warmup(self, bundle, make_strategy):
        strategy = make_strategy()
        with pytest.raises(ValueError, match="rows of history"):
            _run(bundle.tail(strategy.min_history - 5), strategy)

    def test_explicit_warmup_overrides_the_default(self, bundle, make_strategy):
        strategy = make_strategy(signals=[Momentum(lookback=60, skip=5)])
        res = _run(bundle, strategy, warmup=400)
        assert res.returns.index[0] == bundle.index[400]


class TestUniverseOrdering:
    def test_weights_follow_universe_order_not_panel_order(self, panel, make_strategy):
        """A reordered price panel must not reorder the book."""
        from quantzy.data.bundle import DataBundle
        from quantzy.strategy import Strategy

        reordered = panel[["CASH", "DOWN", "UP", "MID"]]
        strategy = Strategy(
            universe=Universe.from_tickers(["UP", "MID", "DOWN", "CASH"]),
            signals=make_strategy().signals,
            risk_model=make_strategy().risk_model,
            optimizer=make_strategy().optimizer,
        )
        res = _run(DataBundle.build(reordered), strategy)
        assert list(res.weights.columns) == ["UP", "MID", "DOWN", "CASH"]


class TestDrift:
    """Between rebalances the book drifts with prices instead of resetting for free."""

    def test_weights_drift_between_rebalances(self, bundle, universe):
        ew = FixedWeights(universe)
        res = _run(bundle, ew, rebalance_every=21)
        first = res.rebalance_dates[0]
        np.testing.assert_allclose(res.weights.loc[first].to_numpy(), 0.25)
        # UP rallies and DOWN falls, so by the day before the next rebalance they differ.
        before_next = res.weights.index[res.weights.index.get_loc(res.rebalance_dates[1]) - 1]
        drifted = res.weights.loc[before_next]
        assert drifted["UP"] != pytest.approx(0.25)
        assert drifted.sum() == pytest.approx(1.0)

    def test_drift_matches_buy_and_hold_arithmetic(self, bundle, universe, panel):
        res = _run(bundle, FixedWeights(universe), rebalance_every=21)
        d0 = res.rebalance_dates[0]
        i0 = panel.index.get_loc(d0)
        d5 = panel.index[i0 + 5]
        # Bought in equal dollars at the close before d0, then held. weights[d5] is the book
        # that earns d5, so it has drifted through d4's close: each weight is proportional
        # to that asset's price growth since purchase.
        held = panel.loc[panel.index[i0 + 4]] / panel.iloc[i0 - 1]
        np.testing.assert_allclose(res.weights.loc[d5].to_numpy(),
                                   (held / held.sum()).to_numpy(), rtol=1e-10)

    def test_rebalance_pays_to_undo_drift(self, bundle, universe):
        res = _run(bundle, FixedWeights(universe, cost_model=LinearCost(cost_bps=10.0)),
                   rebalance_every=21)
        later = res.decisions[1:]
        assert all(d.turnover > 0 for d in later)
        assert all(d.trades.cost == pytest.approx(d.turnover * 2 * 10.0 / 1e4) for d in later)


class TestFixedWeights:
    def test_daily_free_rebalance_is_the_mean_return(self, bundle, universe, panel):
        res = _run(bundle, FixedWeights(universe), rebalance_every=1)
        expected = panel.pct_change().mean(axis=1).loc[res.returns.index]
        np.testing.assert_allclose(res.returns.to_numpy(), expected.to_numpy(), atol=1e-12)

    def test_holdings_leaves_cash_out(self, universe):
        fw = FixedWeights(universe, holdings=["UP", "MID", "DOWN"])
        assert fw.decide(pd.DataFrame(1.0, index=pd.bdate_range("2020-01-01", periods=2),
                                      columns=universe.tickers)).weights["CASH"] == 0.0

    def test_rejects_unknown_tickers(self, universe):
        with pytest.raises(ValueError, match="outside the universe"):
            FixedWeights(universe, weights={"NOPE": 1.0})
        with pytest.raises(ValueError, match="subset of the universe"):
            FixedWeights(universe, holdings=["NOPE"])
