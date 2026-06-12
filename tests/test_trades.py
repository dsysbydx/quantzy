import numpy as np
import pandas as pd

from quantzy.construction import Portfolio, diff


class TestTrades:
    def test_diff_reconciles_to_target(self):
        current = Portfolio(weights=pd.Series({"A": 0.5, "B": 0.5}), nav=1_000.0)
        target = Portfolio(weights=pd.Series({"A": 0.7, "B": 0.3}), nav=1_000.0)
        trades = diff(current, target)

        deltas = {t.ticker: t.delta_weight for t in trades.trades}
        # current + delta == target, per asset
        assert np.isclose(0.5 + deltas["A"], 0.7)
        assert np.isclose(0.5 + deltas["B"], 0.3)
        # self-financing: deltas net to zero
        assert np.isclose(sum(deltas.values()), 0.0)

    def test_diff_handles_new_and_dropped_assets(self):
        current = Portfolio(weights=pd.Series({"A": 1.0}))
        target = Portfolio(weights=pd.Series({"B": 1.0}))
        trades = diff(current, target).nonzero()
        sides = {t.ticker: t.side for t in trades.trades}
        assert sides == {"A": "sell", "B": "buy"}

    def test_notional_uses_nav(self):
        current = Portfolio.empty(["A", "B"], nav=2_000.0)
        target = Portfolio(weights=pd.Series({"A": 0.25, "B": 0.75}), nav=2_000.0)
        trades = diff(current, target)
        notional = {t.ticker: t.delta_notional for t in trades.trades}
        assert np.isclose(notional["A"], 0.25 * 2_000.0)
        assert np.isclose(notional["B"], 0.75 * 2_000.0)

    def test_turnover_is_half_sum_abs(self):
        current = Portfolio(weights=pd.Series({"A": 0.5, "B": 0.5}))
        target = Portfolio(weights=pd.Series({"A": 0.7, "B": 0.3}))
        trades = diff(current, target)
        # |0.2| + |-0.2| = 0.4 -> one-way turnover 0.2
        assert np.isclose(trades.turnover, 0.2)
