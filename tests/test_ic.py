"""Information-coefficient tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.research import ic_decay, ic_summary, information_coefficient
from quantzy.returns import forward_returns
from quantzy.signals import Momentum


class TestForwardReturns:
    def test_is_aligned_at_the_decision_date(self, panel):
        fwd = forward_returns(panel, horizon=5)
        expected = panel["UP"].iloc[5] / panel["UP"].iloc[0] - 1.0
        assert fwd["UP"].iloc[0] == pytest.approx(expected)

    def test_tail_is_unknowable(self, panel):
        fwd = forward_returns(panel, horizon=3)
        assert fwd.iloc[-3:].isna().all().all()
        assert not fwd.iloc[:-3].isna().any().any()

    def test_rejects_a_non_positive_horizon(self, panel):
        with pytest.raises(ValueError, match="horizon must be >= 1"):
            forward_returns(panel, horizon=0)


class TestInformationCoefficient:
    def test_a_perfect_signal_scores_one(self, panel):
        """A score equal to the forward return must have IC = 1 everywhere it is defined."""
        fwd = forward_returns(panel, horizon=1)
        ic = information_coefficient(fwd, panel, horizon=1, min_names=3)
        np.testing.assert_allclose(ic.dropna().to_numpy(), 1.0, atol=1e-9)

    def test_an_inverted_signal_scores_minus_one(self, panel):
        fwd = forward_returns(panel, horizon=1)
        ic = information_coefficient(-fwd, panel, horizon=1, min_names=3)
        np.testing.assert_allclose(ic.dropna().to_numpy(), -1.0, atol=1e-9)

    def test_rejects_min_names_above_the_universe(self, bundle):
        """The default of 5 on a 4-asset universe would silently discard every date."""
        scores = Momentum(lookback=60, skip=5).scores(bundle)
        with pytest.raises(ValueError, match="exceeds the 4-asset universe"):
            information_coefficient(scores, bundle.prices, min_names=5)

    def test_rejects_an_unknown_method(self, bundle):
        scores = Momentum(lookback=60, skip=5).scores(bundle)
        with pytest.raises(ValueError, match="method must be"):
            information_coefficient(scores, bundle.prices, method="kendall", min_names=3)

    def test_real_signal_produces_finite_ic(self, bundle):
        scores = Momentum(lookback=252, skip=21).scores(bundle)
        ic = information_coefficient(scores, bundle.prices, horizon=21, min_names=3)
        assert ic.notna().sum() > 500
        assert ic.abs().max() <= 1.0 + 1e-12


class TestICSummary:
    def test_de_overlaps_the_t_stat(self):
        """Overlapping forward windows must not be counted as independent observations."""
        ic = pd.Series(np.full(252, 0.05), index=pd.bdate_range("2020-01-01", periods=252))
        # A constant series has zero dispersion -> undefined IR, so add a little noise.
        rng = np.random.default_rng(0)
        ic = ic + rng.normal(0, 0.01, len(ic))

        daily = ic_summary(ic, horizon=1)
        monthly = ic_summary(ic, horizon=21)
        assert monthly.t_stat < daily.t_stat
        assert monthly.t_stat == pytest.approx(daily.t_stat / np.sqrt(21), rel=1e-6)

    def test_empty_series_is_reported_not_crashed(self):
        summary = ic_summary(pd.Series(dtype=float), horizon=1)
        assert summary.n_obs == 0
        assert np.isnan(summary.mean_ic)

    def test_hit_rate_counts_positive_dates(self):
        ic = pd.Series([0.1, 0.2, -0.1, -0.2, 0.3])
        assert ic_summary(ic).hit_rate == pytest.approx(0.6)


class TestICDecay:
    def test_reports_one_row_per_horizon(self, bundle):
        scores = Momentum(lookback=252, skip=21).scores(bundle)
        table = ic_decay(scores, bundle.prices, horizons=(1, 5, 21), min_names=3)
        assert list(table.index) == [1, 5, 21]
        assert set(table.columns) == {"mean_ic", "ic_std", "ic_ir", "t_stat", "hit_rate", "n_obs"}

    def test_longer_horizons_use_fewer_observations(self, bundle):
        scores = Momentum(lookback=252, skip=21).scores(bundle)
        table = ic_decay(scores, bundle.prices, horizons=(1, 126), min_names=3)
        assert table.loc[126, "n_obs"] < table.loc[1, "n_obs"]


class TestTimeSeriesIC:
    @staticmethod
    def _panel(seed=0, n=600, k=3):
        rng = np.random.default_rng(seed)
        idx = pd.bdate_range("2020-01-01", periods=n)
        rets = pd.DataFrame(rng.normal(0, 0.01, (n, k)), index=idx, columns=list("ABC")[:k])
        prices = (1 + rets).cumprod() * 100
        return prices, rets

    def test_perfect_foresight_scores_one(self):
        from quantzy.research import time_series_ic

        prices, _ = self._panel()
        oracle = forward_returns(prices, 5)
        ic = time_series_ic(oracle, prices, horizon=5, method="pearson")
        np.testing.assert_allclose(ic.to_numpy(), 1.0, atol=1e-12)

    def test_noise_scores_near_zero_and_pooled_matches_average(self):
        from quantzy.research import pooled_time_series_ic, time_series_ic

        prices, _ = self._panel(seed=1)
        noise = pd.DataFrame(np.random.default_rng(2).normal(size=prices.shape),
                             index=prices.index, columns=prices.columns)
        per_asset = time_series_ic(noise, prices, horizon=1)
        pooled = pooled_time_series_ic(noise, prices, horizon=1)
        assert per_asset.abs().max() < 0.15
        assert pooled.mean() == pytest.approx(per_asset.mean(), abs=1e-12)

    def test_nan_cells_are_ignored_not_zero(self):
        """An asset's pre-inception rows must not count as a zero score."""
        from quantzy.research import time_series_ic

        prices, _ = self._panel(seed=3)
        oracle = forward_returns(prices, 1)
        oracle.iloc[:200, 0] = np.nan
        assert time_series_ic(oracle, prices, horizon=1, method="pearson")["A"] == \
            pytest.approx(1.0)

    def test_vol_scaling_uses_only_past_returns(self):
        """Prices after the last scored row's forward window cannot change the IC."""
        from quantzy.research import time_series_ic

        prices, _ = self._panel(seed=4)
        scores = forward_returns(prices, 1).shift(1)  # yesterday's return: a legitimate score
        scores.iloc[500:] = np.nan
        corrupted = prices.copy()
        corrupted.iloc[550:] *= 3.0
        base = time_series_ic(scores, prices, horizon=5, vol_window=20)
        after = time_series_ic(scores, corrupted, horizon=5, vol_window=20)
        pd.testing.assert_series_equal(base, after)
        assert base.notna().all()


class TestChunkFolds:
    def test_blocks_alternate(self):
        from quantzy.research import chunk_folds

        f = chunk_folds(pd.RangeIndex(10), chunk=3)
        assert f.tolist() == [0, 0, 0, 1, 1, 1, 0, 0, 0, 1]

    def test_rejects_bad_args(self):
        from quantzy.research import chunk_folds

        with pytest.raises(ValueError):
            chunk_folds(pd.RangeIndex(10), chunk=0)


class TestNeweyWest:
    def test_matches_plain_t_without_lags(self):
        from quantzy.research import newey_west_t

        x = pd.Series(np.random.default_rng(0).normal(0.1, 1, 2000))
        plain = x.mean() / (x.std(ddof=0) / np.sqrt(len(x)))
        assert newey_west_t(x, lags=0) == pytest.approx(plain)

    def test_shrinks_t_for_autocorrelated_series(self):
        from quantzy.research import newey_west_t

        e = np.random.default_rng(1).normal(size=5000)
        x = pd.Series(np.convolve(e, np.ones(20) / 20, mode="same") + 0.02)
        assert abs(newey_west_t(x, lags=40)) < abs(newey_west_t(x, lags=0))
