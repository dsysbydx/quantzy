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
from quantzy.signals import (
    EmaSpread,
    Identity,
    Momentum,
    SharpePrior,
    Signal,
    TrendPullback,
    TrendStrength,
    Value,
    VolScaledTrend,
    ZScore,
    ewm_min_history,
)

#: Every signal, at parameters small enough to converge inside the test panel.
ALL_SIGNALS = [
    Momentum(lookback=252, skip=21),
    Momentum(lookback=126, skip=5, vol_window=60),
    Value(window=252),
    EmaSpread(fast=10, slow=60),
    EmaSpread(fast=5, slow=20, sign=-1, vol_window=None),
    TrendPullback(short=5, mid=20, long=60),
    TrendPullback(short=5, mid=20, long=60, beta=0.5, gate=False, exclude=("CASH",)),
    # Bounded scores: under the tests' extreme corruption every asset saturates at exactly 1.0,
    # which a strict cross-sectional z-score rejects. Score them as they are meant to be used.
    TrendStrength(window=60, vol_window=20, normalizer=Identity()),
    TrendStrength(window=100, vol_window=30, horizon=5, exclude=("CASH",),
                  normalizer=ZScore(on_degenerate="zero")),
    TrendStrength(window=40, vol_window=20, ma="ema", normalizer=Identity()),
    VolScaledTrend(window=40, vol_window=20, ma="ema", normalizer=Identity()),
    SharpePrior({"A": 0.3}, vol_window=20, normalizer=Identity()),
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


class TestTrendPullback:
    """The LT-trend / ST-reversal construction and its gate."""

    def _spiky(self, panel):
        """Clean trends with a three-day dip in UP and a pop in DOWN at the end.

        UP and DOWN are deterministic drifts plus a small wiggle (so vol is defined); MID and
        CASH come from ``panel``. The fixture's own random walks wander too much over a
        60-row EMA to pin the sign of a trend.
        """
        n = len(panel)
        t = np.arange(n)
        wiggle = 0.004 * np.sin(t * 1.7)
        out = panel.copy()
        out["UP"] = 100.0 * np.exp(0.001 * t + wiggle)
        out["DOWN"] = 100.0 * np.exp(-0.001 * t + wiggle)
        out.iloc[-3:, out.columns.get_loc("UP")] *= 0.92
        out.iloc[-3:, out.columns.get_loc("DOWN")] *= 1.08
        return DataBundle.build(out)

    def test_spans_must_be_ordered(self):
        with pytest.raises(ValueError, match="short < mid < long"):
            TrendPullback(short=20, mid=10, long=60)

    def test_beta_must_be_non_negative(self):
        with pytest.raises(ValueError, match="beta"):
            TrendPullback(beta=-1.0)

    def test_legs_read_trend_and_pullback(self, panel):
        """UP trends up, DOWN trends down; a dip in UP shows as a negative ST leg."""
        lt, st = TrendPullback(short=5, mid=20, long=60).legs(self._spiky(panel))
        last = lt.iloc[-1]
        assert last["UP"] > 0 > last["DOWN"]
        assert st.iloc[-1]["UP"] < 0 < st.iloc[-1]["DOWN"]

    def test_beta_zero_is_pure_trend(self, panel):
        bundle = self._spiky(panel)
        sig = TrendPullback(short=5, mid=20, long=60, beta=0.0)
        lt, _ = sig.legs(bundle)
        from quantzy.signals import ZScore

        expected = ZScore(on_degenerate="zero")(lt).where(lt.notna())
        pd.testing.assert_frame_equal(sig.raw(bundle), expected)

    def test_gate_withholds_reversal_credit_from_downtrends(self, panel):
        """DOWN popped above its short EMA in a downtrend: ungated, the reversal leg
        penalizes it further; gated, it keeps only its trend score."""
        bundle = self._spiky(panel)
        kw = dict(short=5, mid=20, long=60, beta=1.0)
        gated = TrendPullback(gate=True, **kw).raw(bundle).iloc[-1]
        ungated = TrendPullback(gate=False, **kw).raw(bundle).iloc[-1]
        assert ungated["DOWN"] < gated["DOWN"]
        # UP is in an uptrend, so the gate changes nothing for it.
        assert gated["UP"] == pytest.approx(ungated["UP"])

    def test_dip_in_an_uptrend_raises_the_score(self, panel):
        kw = dict(short=5, mid=20, long=60)
        bundle = self._spiky(panel)
        with_reversal = TrendPullback(beta=1.0, **kw).raw(bundle).iloc[-1]
        trend_only = TrendPullback(beta=0.0, **kw).raw(bundle).iloc[-1]
        assert with_reversal["UP"] > trend_only["UP"]

    def test_excluded_tickers_are_neutral(self, bundle):
        sig = TrendPullback(short=5, mid=20, long=60, exclude=("CASH",))
        assert sig.raw(bundle)["CASH"].isna().all()
        scores = sig.scores(bundle)
        assert (scores["CASH"] == 0.0).all()
        assert scores.iloc[-1].drop("CASH").abs().sum() > 0

    def test_looser_tolerance_shortens_warmup(self):
        tight = TrendPullback(long=200)
        loose = TrendPullback(long=200, ewm_tol=1e-3)
        assert loose.min_history < tight.min_history
        assert tight.min_history == ewm_min_history(200)


class TestTrendStrength:
    def test_sign_follows_the_trend_and_stays_bounded(self, trend_prices):
        noise = np.random.default_rng(3).normal(0, 0.002, size=trend_prices.shape)
        prices = trend_prices * np.exp(np.cumsum(noise, axis=0))
        raw = TrendStrength(window=60, vol_window=20).raw(DataBundle.build(prices))
        last = raw.iloc[-1]
        assert last["UP"] > 0.5 and last["DOWN"] < -0.5
        assert raw.abs().max().max() < 1.0

    def test_exclude_leaves_the_asset_unscored(self, bundle):
        raw = TrendStrength(window=60, vol_window=20, exclude=("CASH",)).raw(bundle)
        assert raw["CASH"].isna().all()
        assert raw["UP"].notna().any()

    def test_rejects_degenerate_windows(self):
        with pytest.raises(ValueError, match="window"):
            TrendStrength(window=1)

    def test_ema_variant_is_a_different_bounded_score(self, bundle):
        sma_, ema_ = (TrendStrength(window=60, vol_window=20, ma=ma).raw(bundle)
                      for ma in ("sma", "ema"))
        assert not np.allclose(sma_.fillna(0).to_numpy(), ema_.fillna(0).to_numpy())
        assert ema_.abs().max().max() < 1.0
        assert np.sign(sma_["DOWN"].iloc[-1]) == np.sign(ema_["DOWN"].iloc[-1])

    def test_ema_warmup_is_its_convergence_length(self):
        sig = TrendStrength(window=200, ma="ema", ewm_tol=1e-3)
        assert sig.min_history == ewm_min_history(200, 1e-3)
        assert TrendStrength(window=200, ma="ema").min_history == ewm_min_history(200, 1e-9)
        with pytest.raises(ValueError, match="ma"):
            TrendStrength(ma="wma")
