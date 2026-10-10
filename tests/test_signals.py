"""Signal contract tests.

The headline is :class:`TestCausality`. The whole ``Signal`` design rests on one claim --
that computing a panel over all history and slicing it at ``asof`` equals computing it from
truncated history -- and that claim is a property of each author's ``raw``, not something
the base class can enforce. So it is asserted here, for every signal, rather than trusted.

Any new signal should be added to ``ALL_SIGNALS``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.data.bundle import DataBundle
from quantzy.signals import EmaSpread, Momentum, Signal, Value, ewm_min_history

#: Every signal, at parameters small enough to converge inside the test panel.
ALL_SIGNALS = [
    Momentum(lookback=252, skip=21),
    Momentum(lookback=126, skip=5, vol_window=60),
    Value(window=252),
    EmaSpread(fast=10, slow=60),
    EmaSpread(fast=5, slow=20, sign=-1, vol_window=None),
]

IDS = [f"{type(s).__name__}{sorted(s.params().items())}" for s in ALL_SIGNALS]


def _fresh(signal: Signal) -> Signal:
    """A new instance of the same signal, so the panel cache cannot mask a difference."""
    return type(signal)(normalizer=signal.normalizer, lag=signal.lag, **signal.params())


class TestCausality:
    """Row ``t`` of a signal panel must not depend on any row after ``t``."""

    @pytest.mark.parametrize("signal", ALL_SIGNALS, ids=IDS)
    def test_slice_equals_truncated_history(self, signal, bundle):
        """scores(full).loc[T] == scores(history up to T).loc[T].

        This is the invariant that lets the backtest slice a precomputed panel and lets
        production read the last row of a short window -- and get the same number.
        """
        asof = bundle.index[900]
        full = signal.scores(bundle).loc[asof]
        truncated = _fresh(signal).scores(bundle.upto(asof)).loc[asof]
        pd.testing.assert_series_equal(full, truncated, check_names=False)

    @pytest.mark.parametrize("signal", ALL_SIGNALS, ids=IDS)
    def test_future_data_cannot_change_the_past(self, signal, bundle, panel):
        """Overwriting every row after T must leave the score at T untouched.

        A sharper probe than truncation: a centered rolling window or a full-sample mean
        survives the truncation test on the last row but fails this one.
        """
        asof = panel.index[900]
        corrupted = panel.copy()
        corrupted.loc[corrupted.index > asof] *= 1000.0

        base = signal.scores(bundle).loc[asof]
        after = _fresh(signal).scores(DataBundle.build(corrupted)).loc[asof]
        pd.testing.assert_series_equal(base, after, check_names=False)

    @pytest.mark.parametrize("signal", ALL_SIGNALS, ids=IDS)
    def test_min_history_window_reproduces_full_history(self, signal, bundle):
        """The last row of a ``min_history`` window matches the full-history panel.

        This is the production path: a daily job pulls a warmup window rather than a decade.
        For a recursive filter the match is close but not exact -- an EWM never entirely
        forgets its seed -- which is what ``min_history`` is sized to bound.
        """
        asof = bundle.index[900]
        window = signal.min_history + signal.lag + 1
        windowed = _fresh(signal).latest(bundle.upto(asof).tail(window))
        full = signal.scores(bundle).loc[asof]
        np.testing.assert_allclose(windowed.to_numpy(), full.to_numpy(), atol=1e-6)


class TestLag:
    """``lag`` is the gap between observing data and being able to trade on it."""

    def test_lag_shifts_the_panel(self, bundle):
        unlagged = Momentum(lookback=60, skip=5, lag=0).scores(bundle)
        lagged = Momentum(lookback=60, skip=5, lag=1).scores(bundle)
        # Row t of the lagged panel is row t-1 of the unlagged one.
        pd.testing.assert_frame_equal(
            lagged.iloc[1:].reset_index(drop=True),
            unlagged.iloc[:-1].reset_index(drop=True),
        )

    def test_default_lag_is_one(self):
        assert Momentum().lag == 1, "a signal tradeable on the close it measures is the exception"


class TestMinHistory:
    def test_latest_rejects_a_short_window(self, bundle):
        signal = Momentum(lookback=252, skip=21)
        too_short = bundle.tail(signal.min_history // 2)
        with pytest.raises(ValueError, match="rows to produce a trustworthy last row"):
            signal.latest(too_short)

    def test_ewm_min_history_exceeds_span(self):
        """An EWM's warmup is set by convergence, not by its span."""
        assert ewm_min_history(60) > 60 * 5

    def test_momentum_min_history_covers_the_lookback(self):
        assert Momentum(lookback=252, skip=21).min_history == 253


class TestScores:
    def test_scores_are_zero_mean_across_assets(self, bundle):
        scores = Momentum(lookback=252, skip=21).scores(bundle)
        live = scores.loc[bundle.index[900] :]
        np.testing.assert_allclose(live.mean(axis=1).to_numpy(), 0.0, atol=1e-9)

    def test_warmup_rows_are_neutral_not_nan(self, bundle):
        scores = Momentum(lookback=252, skip=21).scores(bundle)
        assert not scores.isna().any().any(), "NaN reaches the optimizer as a broken mu"
        assert (scores.iloc[:10] == 0.0).all().all()

    def test_panel_is_memoized_on_the_bundle(self, bundle):
        signal = Momentum(lookback=60, skip=5)
        assert signal.scores(bundle) is signal.scores(bundle)

    def test_cache_does_not_leak_across_bundles(self, bundle, panel):
        signal = Momentum(lookback=60, skip=5)
        first = signal.scores(bundle)
        other = DataBundle.build(panel.iloc[:-50])
        assert signal.scores(other).shape[0] == len(other.index)
        assert first.shape[0] == len(bundle.index)


class TestAuthorMistakes:
    """A signal is easy to write subtly wrong. Each way should fail loudly."""

    def test_non_dataclass_subclass_is_rejected(self):
        """Without @dataclass, config attributes never become fields and params() loses them.

        Silent otherwise -- and the causality suite rebuilds signals from ``params()``, so it
        would have tested a default-valued object while appearing to pass.
        """

        class Forgot(Signal):
            name = "forgot"
            window: int = 200

            def raw(self, data):
                return data.prices

        with pytest.raises(TypeError, match="is not a @dataclass"):
            Forgot().params()

    def test_classvar_name_is_not_mistaken_for_a_field(self):
        assert "name" not in Momentum().params()

    def test_raw_must_return_a_frame(self, bundle):
        from dataclasses import dataclass

        @dataclass
        class ReturnsASeries(Signal):
            def raw(self, data):
                return data.prices.iloc[-1]

        with pytest.raises(TypeError, match="must return a DataFrame"):
            ReturnsASeries().scores(bundle)

    def test_a_signal_flat_across_assets_is_rejected(self, bundle):
        """Cross-sectional demeaning annihilates a signal identical across names."""
        from dataclasses import dataclass

        from quantzy.signals import DegenerateCrossSection

        @dataclass
        class SameEverywhere(Signal):
            def raw(self, data):
                return data.prices * 0.0 + 1.0

        with pytest.raises(DegenerateCrossSection, match="no cross-sectional dispersion"):
            SameEverywhere().scores(bundle)
