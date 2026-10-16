"""Risk model tests: the estimator math, and the point-in-time plumbing around it.

``estimate`` is the bare math on a returns frame; ``covariance`` is the point-in-time entry
the strategy calls. They are tested separately because the second is where look-ahead would
hide.
"""

from __future__ import annotations

import numpy as np
import pytest

from quantzy.data.bundle import DataBundle
from quantzy.risk import LedoitWolfCovariance, SampleCovariance, vol_from_cov

MODELS = [SampleCovariance, LedoitWolfCovariance]
IDS = [m.__name__ for m in MODELS]


class TestEstimator:
    @pytest.mark.parametrize("model", MODELS, ids=IDS)
    def test_is_symmetric_psd_and_labelled(self, model, random_returns):
        sigma = model(annualize=False).estimate(random_returns)
        arr = sigma.to_numpy()
        assert np.allclose(arr, arr.T)
        assert np.linalg.eigvalsh(arr).min() > -1e-10
        assert list(sigma.index) == list(random_returns.columns)
        assert sigma.shape == (4, 4)

    def test_full_shrinkage_gives_a_diagonal_target(self, random_returns):
        sigma = LedoitWolfCovariance(annualize=False, shrinkage=1.0).estimate(random_returns)
        off_diag = sigma.to_numpy() - np.diag(np.diag(sigma.to_numpy()))
        assert np.allclose(off_diag, 0.0)

    def test_shrinkage_pulls_toward_the_target(self, random_returns):
        """Shrunk off-diagonals sit between the sample's and the diagonal target's zero."""
        sample = SampleCovariance(annualize=False).estimate(random_returns).to_numpy()
        shrunk = LedoitWolfCovariance(annualize=False, shrinkage=0.5).estimate(
            random_returns
        ).to_numpy()
        off = ~np.eye(4, dtype=bool)
        assert np.all(np.abs(shrunk[off]) <= np.abs(sample[off]) + 1e-12)

    def test_vol_from_cov_matches_diagonal(self, random_returns):
        sigma = SampleCovariance(annualize=False).estimate(random_returns)
        np.testing.assert_allclose(
            vol_from_cov(sigma).to_numpy(), np.sqrt(np.diag(sigma.to_numpy()))
        )


class TestAnnualization:
    @pytest.mark.parametrize("model", MODELS, ids=IDS)
    def test_annualize_scales_by_trading_days(self, model, bundle):
        """Checked through ``covariance``, the path that actually applies the scaling."""
        daily = model(annualize=False, lookback=252).covariance(bundle)
        annual = model(annualize=True, trading_days=252, lookback=252).covariance(bundle)
        np.testing.assert_allclose(annual.to_numpy(), daily.to_numpy() * 252, rtol=1e-12)

    def test_trading_days_is_respected(self, bundle):
        a = SampleCovariance(trading_days=52, lookback=252).covariance(bundle)
        b = SampleCovariance(trading_days=252, lookback=252).covariance(bundle)
        np.testing.assert_allclose(b.to_numpy(), a.to_numpy() * (252 / 52), rtol=1e-12)


class TestPointInTime:
    @pytest.mark.parametrize("model", MODELS, ids=IDS)
    def test_asof_equals_truncated_history(self, model, bundle):
        asof = bundle.index[900]
        sliced = model(lookback=252).covariance(bundle, asof=asof)
        truncated = model(lookback=252).covariance(bundle.upto(asof))
        np.testing.assert_allclose(sliced.to_numpy(), truncated.to_numpy(), rtol=1e-12)

    @pytest.mark.parametrize("model", MODELS, ids=IDS)
    def test_future_data_cannot_change_the_estimate(self, model, bundle, panel):
        asof = panel.index[900]
        corrupted = panel.copy()
        corrupted.loc[corrupted.index > asof] *= 1000.0
        base = model(lookback=252).covariance(bundle, asof=asof)
        after = model(lookback=252).covariance(DataBundle.build(corrupted), asof=asof)
        np.testing.assert_allclose(base.to_numpy(), after.to_numpy(), rtol=1e-12)

    def test_lag_excludes_the_return_being_earned(self, bundle):
        """Sigma at t with lag=1 is Sigma at t-1 with lag=0 -- it cannot see day t's move."""
        asof = bundle.index[900]
        lagged = LedoitWolfCovariance(lag=1, lookback=252).covariance(bundle, asof=asof)
        prior = LedoitWolfCovariance(lag=0, lookback=252).covariance(
            bundle, asof=bundle.index[899]
        )
        np.testing.assert_allclose(lagged.to_numpy(), prior.to_numpy(), rtol=1e-12)

    def test_lookback_bounds_the_window(self, bundle):
        """A short lookback must not quietly widen as history accumulates."""
        early = SampleCovariance(lookback=60).covariance(bundle, asof=bundle.index[400])
        late = SampleCovariance(lookback=60).covariance(bundle, asof=bundle.index[1300])
        assert not np.allclose(early.to_numpy(), late.to_numpy())

        wide = SampleCovariance(lookback=None).covariance(bundle, asof=bundle.index[1300])
        assert not np.allclose(late.to_numpy(), wide.to_numpy())

    def test_min_history_accounts_for_lookback_and_lag(self):
        assert SampleCovariance(lookback=60, lag=1).min_history == 62
        assert SampleCovariance(lookback=None, lag=1).min_history == 4

    def test_rejects_too_few_observations(self, bundle):
        with pytest.raises(ValueError, match="at least 2 return observations"):
            SampleCovariance().covariance(bundle, asof=bundle.index[1])

    def test_returns_panel_is_memoized(self, bundle):
        model = SampleCovariance(lookback=60)
        model.covariance(bundle, asof=bundle.index[500])
        first = model._returns(bundle)
        model.covariance(bundle, asof=bundle.index[900])
        assert model._returns(bundle) is first

    def test_accepts_a_bare_price_frame(self, panel):
        sigma = SampleCovariance(lookback=252).covariance(panel)
        assert list(sigma.index) == list(panel.columns)


class TestEwmVariant:
    def test_halflife_weights_recent_observations(self, bundle):
        plain = SampleCovariance(lookback=252).covariance(bundle)
        ewm = SampleCovariance(lookback=252, halflife=20).covariance(bundle)
        assert not np.allclose(plain.to_numpy(), ewm.to_numpy())
        assert np.linalg.eigvalsh(ewm.to_numpy()).min() > -1e-10


class TestStateScaledRisk:
    @staticmethod
    def _model(downside=0.5, assets=None):
        from quantzy.risk import StateScaledRisk
        from quantzy.signals import Identity, TrendStrength

        return StateScaledRisk(
            base=SampleCovariance(lookback=60),
            state=TrendStrength(window=60, vol_window=20, normalizer=Identity()),
            downside=downside,
            assets=assets,
        )

    def test_scales_only_assets_in_a_downtrend(self, bundle):
        model, asof = self._model(), bundle.index[900]
        base = model.base.covariance(bundle, asof).to_numpy()
        m = model.multiplier(bundle, asof)
        s = model.state.scores(bundle).loc[asof]
        assert m["DOWN"] > 1.0 and s["DOWN"] < 0
        assert m["UP"] == 1.0 and s["UP"] > 0
        np.testing.assert_allclose(
            model.covariance(bundle, asof).to_numpy(), base * np.outer(m, m), rtol=1e-12
        )

    def test_keeps_correlations(self, bundle):
        model, asof = self._model(downside=2.0), bundle.index[900]

        def corr(c):
            v = np.sqrt(np.diag(c))
            return c / np.outer(v, v)

        np.testing.assert_allclose(corr(model.covariance(bundle, asof).to_numpy()),
                                   corr(model.base.covariance(bundle, asof).to_numpy()),
                                   rtol=1e-10)

    def test_zero_downside_reproduces_the_base(self, bundle):
        model, asof = self._model(downside=0.0), bundle.index[900]
        np.testing.assert_allclose(model.covariance(bundle, asof).to_numpy(),
                                   model.base.covariance(bundle, asof).to_numpy())

    def test_assets_restricts_the_scaling(self, bundle):
        m = self._model(assets=["UP"]).multiplier(bundle, bundle.index[900])
        assert m["DOWN"] == 1.0

    def test_future_data_cannot_change_the_estimate(self, bundle, panel):
        asof = panel.index[900]
        corrupted = panel.copy()
        corrupted.loc[corrupted.index >= asof] *= 0.5  # a crash on the decision day itself
        before = self._model().covariance(bundle, asof)
        after = self._model().covariance(DataBundle.build(corrupted), asof)
        np.testing.assert_allclose(before.to_numpy(), after.to_numpy(), rtol=1e-12)

    def test_min_history_covers_base_and_state(self):
        model = self._model()
        assert model.min_history == max(model.base.min_history, 60 + 1 + 1)

    def test_rejects_negative_downside(self):
        with pytest.raises(ValueError, match="downside"):
            self._model(downside=-0.1)


class TestEwmFastPath:
    """The precomputed EWM panel must match the per-date estimate it replaces."""

    @staticmethod
    def _slow(model, bundle, asof):
        from quantzy.risk.base import PanelRiskModel

        return PanelRiskModel.covariance(model, bundle, asof).to_numpy()

    @pytest.mark.parametrize("halflife,lookback", [(10, 250), (60, 120), (20, None)])
    def test_matches_the_per_date_estimate(self, bundle, halflife, lookback):
        model = SampleCovariance(halflife=halflife, lookback=lookback)
        for asof in bundle.index[[5, 130, 700, 1399]]:
            np.testing.assert_allclose(model.covariance(bundle, asof).to_numpy(),
                                       self._slow(model, bundle, asof), rtol=1e-8, atol=1e-14)

    def test_window_with_a_gap_takes_the_per_date_path(self, panel):
        gappy = panel.copy()
        gappy.iloc[700, 0] = np.nan
        bundle = DataBundle.build(gappy)
        model = SampleCovariance(halflife=10, lookback=60)
        _, ok = model._ewm_panel(bundle, model._returns(bundle))
        assert not ok[702] and ok[800]  # pct_change puts the gap in returns rows 700 and 701
        for asof in bundle.index[[720, 800]]:
            np.testing.assert_allclose(model.covariance(bundle, asof).to_numpy(),
                                       self._slow(model, bundle, asof), rtol=1e-8, atol=1e-14)

    def test_future_data_cannot_change_the_estimate(self, bundle, panel):
        asof = panel.index[900]
        shocked = panel.copy()
        shocked.loc[shocked.index >= asof] *= 0.5
        a = SampleCovariance(halflife=10, lookback=250).covariance(bundle, asof)
        b = SampleCovariance(halflife=10, lookback=250).covariance(DataBundle.build(shocked), asof)
        np.testing.assert_allclose(a.to_numpy(), b.to_numpy(), rtol=1e-8)

    def test_large_universe_skips_the_panel(self, bundle, monkeypatch):
        import quantzy.risk.sample as sample

        monkeypatch.setattr(sample, "MAX_PANEL_CELLS", 10)
        model = SampleCovariance(halflife=10, lookback=60)
        asof = bundle.index[900]
        np.testing.assert_allclose(model.covariance(bundle, asof).to_numpy(),
                                   self._slow(model, bundle, asof))
        assert model._ewm_cache[1] is None


class TestBlendedRisk:
    def test_equal_blend_is_the_mean(self, bundle):
        from quantzy.risk import BlendedRisk, ewma_blend

        asof = bundle.index[900]
        parts = [SampleCovariance(halflife=h, lookback=250) for h in (5, 20, 60)]
        mean = sum(p.covariance(bundle, asof).to_numpy() for p in parts) / 3
        np.testing.assert_allclose(ewma_blend().covariance(bundle, asof).to_numpy(), mean,
                                   rtol=1e-12)
        assert BlendedRisk(parts).min_history == max(p.min_history for p in parts)

    def test_weights_are_normalized(self, bundle):
        from quantzy.risk import BlendedRisk

        a, b = SampleCovariance(halflife=5), SampleCovariance(halflife=60)
        blend = BlendedRisk([a, b], weights=[3, 1])
        assert blend.weights == (0.75, 0.25)
        want = 0.75 * a.covariance(bundle).to_numpy() + 0.25 * b.covariance(bundle).to_numpy()
        np.testing.assert_allclose(blend.covariance(bundle).to_numpy(), want, rtol=1e-12)

    def test_rejects_bad_weights(self):
        from quantzy.risk import BlendedRisk

        with pytest.raises(ValueError):
            BlendedRisk([SampleCovariance()], weights=[1, 1])
        with pytest.raises(ValueError):
            BlendedRisk([SampleCovariance()], weights=[-1])
