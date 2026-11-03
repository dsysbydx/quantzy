"""Extend short price histories backwards with proxy return series.

A young ETF can be given a longer history by chaining the daily returns of something that
tracked the same exposure before it listed: an index, an index fund, a futures series. The
ETF's own prices are kept wherever they exist; before its first price the series is rebuilt
backwards from each proxy's returns in turn, so the levels join with no jump.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from quantzy.data.base import DataReader

#: A proxy: ``(start, end) -> daily returns`` on the proxy's own trading days.
ReturnSource = Callable[[str | None, str | None], pd.Series]


def splice(prices: pd.Series, *proxies: pd.Series) -> pd.Series:
    """``prices`` extended back before their first value by each proxy's returns in turn.

    Each proxy only fills dates before the series built so far begins, so a chain
    ``splice(etf, fund, index)`` uses the ETF where it trades, the fund before that, and the
    index before the fund. A proxy missing the join date is taken as flat up to it.
    """
    out = prices.dropna()
    for r in proxies:
        # The day before a proxy's first return has a price too (pct_change leaves it NaN).
        r = r[r.notna() | r.notna().shift(-1, fill_value=False)].fillna(0.0)
        if out.empty:
            out = (1.0 + r).cumprod()
            continue
        first = out.index[0]
        r = r[r.index <= first]
        if r.empty:
            continue
        # The price on day d is the first price over the growth from d's close to it.
        growth = (1.0 + r).iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0)
        back = out.iloc[0] / growth[growth.index < first]
        out = pd.concat([back, out])
    return out


def tracking(actual: pd.Series, proxy: pd.Series) -> dict[str, float]:
    """How well a proxy's daily returns tracked the actual's where both exist."""
    both = pd.concat([actual, proxy], axis=1, keys=["a", "p"]).dropna()
    a, p = both["a"], both["p"]
    ann = np.sqrt(252)
    return {
        "from": both.index[0],
        "days": len(both),
        "corr": float(a.corr(p)),
        "beta": float(a.cov(p) / p.var()),
        "vol_ratio": float(p.std() / a.std()),
        "tracking_error": float((a - p).std() * ann),
        "drift": float((p.mean() - a.mean()) * 252),  # proxy minus actual, a year
    }


class SplicedReader:
    """A :class:`DataReader` that extends listed tickers back with proxy returns.

    Every other ticker, and every field but ``adj_close``, passes through unchanged. On a day
    some spliced ticker priced and another did not (a proxy's holiday calendar differs), the
    missing one is carried flat, so a stray gap never blanks a rolling window.

    Args:
        reader: The underlying reader, for the tickers themselves.
        proxies: ``{ticker: [source, ...]}``, the sources in the order they are chained.
    """

    def __init__(self, reader: DataReader, proxies: Mapping[str, Sequence[ReturnSource]]) -> None:
        self.reader = reader
        self.proxies = proxies

    def history(
        self,
        tickers: Sequence[str],
        start: str | None = None,
        end: str | None = None,
        field: str = "adj_close",
    ) -> pd.DataFrame:
        raw = self.reader.history(tickers, start=start, end=end, field=field)
        keys = [t for t in tickers if t in self.proxies]
        if field != "adj_close" or not keys:
            return raw
        long = {t: splice(raw[t], *(src(start, end) for src in self.proxies[t])) for t in keys}
        idx = raw.index.union(pd.DatetimeIndex(sorted(set().union(*(s.index for s in long.values())))))
        if start is not None:
            idx = idx[idx >= pd.Timestamp(start)]
        if end is not None:
            idx = idx[idx <= pd.Timestamp(end)]
        out = raw.reindex(idx)
        for t, s in long.items():
            out[t] = s.reindex(idx)
        live = out[keys].notna().any(axis=1)
        out.loc[live, keys] = out[keys].ffill().loc[live]
        return out[list(tickers)]
