import numpy as np
import pandas as pd

from quantzy.metrics import (
    annualized_vol,
    max_drawdown,
    sharpe_ratio,
    turnover,
)


class TestMetrics:
    def test_sharpe_zero_vol_is_nan(self):
        r = pd.Series([0.01] * 10)
        assert np.isnan(sharpe_ratio(r))

    def test_sharpe_sign_and_scale(self):
        rng = np.random.default_rng(0)
        r = pd.Series(rng.normal(0.001, 0.01, 1000))
        sr = sharpe_ratio(r, periods=252)
        # mean/std * sqrt(252), positive drift -> positive Sharpe
        expected = r.mean() / r.std(ddof=1) * np.sqrt(252)
        assert np.isclose(sr, expected)
        assert sr > 0

    def test_annualized_vol_scales_with_sqrt_periods(self):
        r = pd.Series([0.01, -0.01, 0.02, -0.02, 0.0])
        assert np.isclose(annualized_vol(r, periods=252), r.std(ddof=1) * np.sqrt(252))

    def test_max_drawdown_known_path(self):
        # +10% then -50% -> trough at 1.1 * 0.5 = 0.55, drawdown = -0.5
        r = pd.Series([0.10, -0.50])
        assert np.isclose(max_drawdown(r), -0.5)

    def test_turnover_from_weight_history(self):
        w = pd.DataFrame(
            {"A": [0.5, 0.7, 0.7], "B": [0.5, 0.3, 0.3]},
            index=pd.bdate_range("2020-01-01", periods=3),
        )
        to = turnover(w)
        # first step: 0.5*(|0.2|+|-0.2|)=0.2 ; second step: 0
        assert np.isclose(to.iloc[0], 0.2)
        assert np.isclose(to.iloc[1], 0.0)
