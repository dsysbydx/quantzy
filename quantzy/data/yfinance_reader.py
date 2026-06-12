"""Reference :class:`DataReader` backed by yfinance."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

#: Map our canonical field names onto yfinance's auto-adjusted column names.
_FIELD_MAP = {
    "adj_close": "Close",  # yfinance auto_adjust=True folds dividends/splits into Close
    "close": "Close",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "volume": "Volume",
}


class YFinanceReader:
    """Fetch daily OHLCV from Yahoo Finance via the ``yfinance`` package.

    All tickers (including SGOV) are real symbols; there is no synthetic series.
    """

    def __init__(self, auto_adjust: bool = True) -> None:
        self.auto_adjust = auto_adjust

    def history(
        self,
        tickers: Sequence[str],
        start: str | None = None,
        end: str | None = None,
        field: str = "adj_close",
    ) -> pd.DataFrame:
        import yfinance as yf

        if field not in _FIELD_MAP:
            raise ValueError(f"unsupported field {field!r}; expected one of {list(_FIELD_MAP)}")
        yf_field = _FIELD_MAP[field]

        tickers = list(tickers)
        raw = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            auto_adjust=self.auto_adjust,
            progress=False,
            group_by="column",
        )
        if raw.empty:
            raise RuntimeError(f"no data returned for {tickers}")

        # With multiple tickers yfinance returns columns as a (field, ticker) MultiIndex.
        if isinstance(raw.columns, pd.MultiIndex):
            panel = raw[yf_field].copy()
        else:  # single ticker -> flat columns
            panel = raw[[yf_field]].copy()
            panel.columns = tickers

        panel = panel.reindex(columns=tickers)
        panel.index = pd.DatetimeIndex(panel.index)
        panel.index.name = "date"
        return panel.sort_index()
