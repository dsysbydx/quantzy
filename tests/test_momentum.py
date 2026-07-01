import numpy as np
import pytest

from quantzy.alpha import AlphaCombiner, Momentum, Value


class TestMomentum:
    def test_momentum_ranks_by_trend(self, trend_prices):
        mom = Momentum(lookback=200, skip=5)
        scores = mom.forecast(trend_prices)
        # Uptrending asset scores highest, downtrending lowest.
        assert scores["UP"] > scores["FLAT"] > scores["DOWN"]
        # z-scored -> roughly mean zero across assets.
        assert abs(scores.mean()) < 1e-9

    def test_momentum_needs_enough_history(self, trend_prices):
        mom = Momentum(lookback=10_000, skip=21)
        with pytest.raises(ValueError):
            mom.forecast(trend_prices)

    def test_combiner_weights_signals(self, trend_prices):
        combiner = AlphaCombiner(
            [Momentum(lookback=200, skip=5), Value(window=200)],
            weights={"momentum": 1.0, "value": 0.0},  # value zeroed out
        )
        combined = combiner.forecast(trend_prices)
        mom_only = Momentum(lookback=200, skip=5).forecast(trend_prices)
        assert np.allclose(combined.to_numpy(), mom_only.to_numpy())
