"""Treasury auction overlay, 30-year: short TLT into each nominal 30-year auction.

See :mod:`treasury_auction` for the rule, the evidence and the dates.
"""

from __future__ import annotations

import pandas as pd

from quantzy.strategies import treasury_auction as ta

NAME = "treasury_auction_30y"
ASSET = "TLT"
TERM = "30-Year"
SIZE = ta.SIZE


def asset_prices(bundle) -> pd.Series:
    return bundle.prices[ASSET]


def target_after_close(day, bundle=None, current: float = 0.0) -> float:
    """The weight in TLT to hold from ``day``'s close (a calendar rule: the data and the
    current holding are not used)."""
    return ta.target_after_close(day, TERM, SIZE)


def backtest_returns(bundle) -> pd.Series:
    return ta.backtest_returns(bundle, TERM, ASSET, SIZE)
