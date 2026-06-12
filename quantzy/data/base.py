"""The source-agnostic data interface."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

import pandas as pd

#: Price/volume fields a reader may expose. Strategies default to ``adj_close``.
Field = str


@runtime_checkable
class DataReader(Protocol):
    """Reads historical market data for a set of tickers.

    Implementations return a *wide* panel: a ``DatetimeIndex`` of dates with one column per
    requested ticker. Plugging in a new data source (e.g. the Stock Exchange of Thailand)
    means implementing this single method.
    """

    def history(
        self,
        tickers: Sequence[str],
        start: str | None = None,
        end: str | None = None,
        field: Field = "adj_close",
    ) -> pd.DataFrame:
        """Return a dates x tickers panel of ``field`` for ``tickers`` over ``[start, end]``."""
        ...
