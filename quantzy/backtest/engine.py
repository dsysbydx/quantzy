"""Walk-forward backtest engine -- stub.

The intended design: step through time on a rebalance schedule, and at each date use only
data available up to that point (``asof``) to compute mu via an AlphaCombiner and Sigma via a
RiskModel, optimize target weights against the current book net of transaction cost, apply
the resulting trades, then accrue the next period's portfolio return. The output is a return
series + weight history that feeds :mod:`quantzy.backtest.metrics`.

Left as a typed placeholder; the building blocks (alpha, risk, optimizer, trades, metrics)
already exist and are exercised end-to-end by ``examples/end_to_end.py``.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class WalkForwardBacktester:
    """Periodic-rebalance simulator. Not yet implemented."""

    rebalance_every: int = 21  # trading days between rebalances (~monthly)

    def run(self, prices: pd.DataFrame, *args, **kwargs):
        raise NotImplementedError(
            "WalkForwardBacktester is a stub; see the module docstring for the intended design."
        )
