"""Strategy seam tests.

:class:`TestBacktestLiveParity` is the reason this module exists. A backtest asks
``decide(full_history, asof=T)``; a production job asks ``decide(window_ending_at_T)`` with
no ``asof`` at all. Those two calls must return the same decision, or every backtest number
is a statement about a strategy that is not the one being traded.

Nothing here runs the engine -- the seam is tested on its own, so a failure points at the
strategy rather than at the simulator.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.construction.portfolio import Portfolio
from quantzy.risk import LedoitWolfCovariance
from quantzy.signals import EmaSpread, Momentum, Value
from quantzy.strategy import Decision, Strategy


class TestBacktestLiveParity:
    """The same instant, asked two ways, must answer once."""

    def test_asof_equals_a_live_run_at_that_date(self, bundle, make_strategy):
        """decide(all history, asof=T) == decide(history truncated at T).

        The left side is the backtest's call; the right side is what a production job would
        compute if today were T. Exact equality, not approximate.
        """
        strategy = make_strategy()
        asof = bundle.index[1000]
        current = Portfolio(
            weights=pd.Series({"UP": 0.3, "MID": 0.3, "DOWN": 0.2, "CASH": 0.2}), nav=100_000.0
        )

        backtest = strategy.decide(bundle, asof=asof, current=current)
        live = make_strategy().decide(bundle.upto(asof), current=current)

        pd.testing.assert_series_equal(backtest.weights, live.weights)
        pd.testing.assert_series_equal(backtest.mu, live.mu)
        assert backtest.asof == live.asof == asof

    def test_parity_holds_from_a_warmup_window(self, bundle, make_strategy):
        """A live job pulls ``min_history`` rows, not a decade. It must still agree.

        Tolerance rather than equality: truncating history perturbs a recursive filter, and
        ``min_history`` exists to bound that perturbation rather than eliminate it.
        """
        strategy = make_strategy(
            signals=[Momentum(lookback=126, skip=21), EmaSpread(fast=10, slow=60)]
        )
        asof = bundle.index[1000]
        window = bundle.upto(asof).tail(strategy.min_history)

        backtest = strategy.decide(bundle, asof=asof)
        live = make_strategy(
            signals=[Momentum(lookback=126, skip=21), EmaSpread(fast=10, slow=60)]
        ).decide(window)

        np.testing.assert_allclose(
            backtest.weights.to_numpy(), live.weights.to_numpy(), atol=1e-6
        )

    def test_decision_ignores_data_after_asof(self, bundle, panel, make_strategy):
        """Corrupting every row after T must not move the decision at T."""
        from quantzy.data.bundle import DataBundle

        asof = panel.index[1000]
        corrupted = panel.copy()
        corrupted.loc[corrupted.index > asof] *= 1000.0

        base = make_strategy().decide(bundle, asof=asof)
        after = make_strategy().decide(DataBundle.build(corrupted), asof=asof)
        pd.testing.assert_series_equal(base.weights, after.weights)


class TestBlend:
    def test_weights_scale_each_signal(self, bundle, universe, make_strategy):
        mom_only = make_strategy(signals=[Momentum(lookback=252, skip=21)])
        blended = make_strategy(
            signals=[Momentum(lookback=252, skip=21), Value(window=252)],
            weights={"momentum": 1.0, "value": 0.0},
        )
        asof = bundle.index[1000]
        pd.testing.assert_series_equal(
            mom_only.mu(bundle, asof), blended.mu(bundle, asof), check_names=False
        )

    def test_mu_scale_is_linear(self, bundle, make_strategy):
        asof = bundle.index[1000]
        base = make_strategy().mu(bundle, asof)
        scaled = make_strategy(mu_scale=3.0).mu(bundle, asof)
        np.testing.assert_allclose(scaled.to_numpy(), base.to_numpy() * 3.0)

    def test_mu_panel_row_matches_mu(self, bundle, make_strategy):
        strategy = make_strategy()
        asof = bundle.index[1000]
        pd.testing.assert_series_equal(
            strategy.mu_panel(bundle).loc[asof], strategy.mu(bundle, asof)
        )

    def test_rejects_weights_for_an_unknown_signal(self, universe, make_strategy):
        with pytest.raises(ValueError, match="not in this strategy"):
            make_strategy(weights={"nonexistent": 1.0})

    def test_rejects_an_empty_signal_set(self, universe):
        from quantzy.optimize import MeanVariance

        with pytest.raises(ValueError, match="at least one signal"):
            Strategy(
                universe=universe,
                signals=[],
                risk_model=LedoitWolfCovariance(),
                optimizer=MeanVariance(),
            )


class TestContract:
    def test_min_history_covers_the_longest_signal(self, make_strategy):
        strategy = make_strategy(
            signals=[Momentum(lookback=60, skip=5), Momentum(lookback=252, skip=21)],
            risk_model=LedoitWolfCovariance(lookback=20),
        )
        longest = Momentum(lookback=252, skip=21)
        assert strategy.min_history >= longest.min_history + longest.lag + 1

    def test_min_history_covers_the_risk_model(self, make_strategy):
        strategy = make_strategy(
            signals=[Momentum(lookback=20, skip=2)],
            risk_model=LedoitWolfCovariance(lookback=500),
        )
        assert strategy.min_history >= 501

    def test_rejects_too_little_history(self, bundle, make_strategy):
        strategy = make_strategy()
        with pytest.raises(ValueError, match="to have converged"):
            strategy.decide(bundle.tail(strategy.min_history // 2))

    def test_rejects_data_missing_a_universe_asset(self, panel, make_strategy):
        from quantzy.data.bundle import DataBundle

        strategy = make_strategy()
        with pytest.raises(ValueError, match="missing 1 universe asset"):
            strategy.decide(DataBundle.build(panel.drop(columns=["CASH"])))

    def test_flat_book_when_current_is_none(self, bundle, make_strategy):
        decision = make_strategy().decide(bundle)
        # Starting flat, every trade is a buy into the target.
        assert decision.turnover == pytest.approx(decision.weights.abs().sum() * 0.5)


class TestDecision:
    def test_carries_its_inputs(self, bundle, make_strategy):
        decision = make_strategy().decide(bundle, asof=bundle.index[1000])
        assert isinstance(decision, Decision)
        assert decision.sigma.shape == (4, 4)
        assert list(decision.mu.index) == ["UP", "MID", "DOWN", "CASH"]
        assert decision.diagnostics["rows_visible"] == 1001

    def test_to_frame_covers_every_asset(self, bundle, make_strategy):
        frame = make_strategy().decide(bundle).to_frame()
        assert list(frame.index) == ["UP", "MID", "DOWN", "CASH"]
        assert not frame["side"].isna().any()

    def test_is_immutable(self, bundle, make_strategy):
        decision = make_strategy().decide(bundle)
        with pytest.raises((AttributeError, TypeError)):
            decision.asof = pd.Timestamp("2020-01-01")
