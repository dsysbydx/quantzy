"""Volatility-forecast scoring: the metrics must reward the true volatility path."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.data.bundle import DataBundle
from quantzy.research import evaluate_vol, forward_realized_vol, vol_forecast
from quantzy.risk import SampleCovariance


@pytest.fixture
def regime_returns():
    """Daily returns whose true annual vol switches between 10% and 40% in long regimes."""
    dates = pd.bdate_range("2000-01-01", periods=4000)
    true_vol = pd.Series(np.where((np.arange(4000) // 250) % 2 == 0, 0.10, 0.40), index=dates)
    rng = np.random.default_rng(11)
    r = pd.Series(rng.standard_normal(4000), index=dates) * true_vol / np.sqrt(252)
    return r, true_vol


class TestForwardRealizedVol:
    def test_indexed_at_the_start_of_its_window(self):
        r = pd.Series([0.0, 0.01, 0.0, 0.0], index=pd.bdate_range("2020-01-01", periods=4))
        rv = forward_realized_vol(r, 2)
        assert rv.iloc[0] == pytest.approx(np.sqrt(0.01**2 / 2 * 252))  # sees r[0], r[1]
        assert rv.iloc[2] == 0.0
        assert np.isnan(rv.iloc[3])


class TestEvaluateVol:
    def test_true_vol_is_calibrated(self, regime_returns):
        r, true_vol = regime_returns
        out = evaluate_vol({"true": true_vol}, r, horizon=1).loc["true"]
        assert out["bias"] == pytest.approx(1.0, abs=0.05)
        assert out["z_std"] == pytest.approx(1.0, abs=0.05)
        assert out["left_tail"] == pytest.approx(0.01, abs=0.005)

    def test_qlike_prefers_the_truth_over_scaled_versions(self, regime_returns):
        r, true_vol = regime_returns
        out = evaluate_vol({"true": true_vol, "high": true_vol * 1.5, "low": true_vol / 1.5},
                           r, horizon=21)
        assert out["qlike"].idxmin() == "true"
        assert out.loc["high", "bias"] < 1.0 < out.loc["low", "bias"]
        assert out.loc["low", "left_tail"] > out.loc["true", "left_tail"]

    def test_a_flat_forecast_explains_nothing(self, regime_returns):
        r, true_vol = regime_returns
        flat = pd.Series(true_vol.mean(), index=true_vol.index) + 1e-9 * np.arange(len(r))
        out = evaluate_vol({"true": true_vol, "flat": flat}, r, horizon=21)
        assert out.loc["true", "mz_r2"] > 0.5 > out.loc["flat", "mz_r2"]

    def test_by_scores_each_group(self, regime_returns):
        r, true_vol = regime_returns
        groups = pd.Series(np.where(true_vol > 0.2, "calm?", "wild?"), index=r.index)
        out = evaluate_vol({"true": true_vol}, r, horizon=1, by=groups)
        assert set(out.index) == {("true", "calm?"), ("true", "wild?")}

    def test_rejects_disjoint_dates(self, regime_returns):
        r, true_vol = regime_returns
        with pytest.raises(ValueError, match="share no dates"):
            evaluate_vol({"x": true_vol.shift(10000, freq="D")}, r)


class TestVolForecast:
    def test_matches_the_model_and_cannot_see_the_day_it_sizes(self, bundle, panel):
        model = SampleCovariance(lookback=60)
        f = vol_forecast(model, bundle, "UP", start=bundle.index[900], step=100)
        d = f.index[0]
        assert f.loc[d] == pytest.approx(np.sqrt(model.covariance(bundle, d).loc["UP", "UP"]))

        shocked = panel.copy()
        shocked.loc[shocked.index >= d, "UP"] *= 0.5
        g = vol_forecast(SampleCovariance(lookback=60), DataBundle.build(shocked), "UP",
                         start=d, step=100)
        assert g.loc[d] == pytest.approx(f.loc[d])
