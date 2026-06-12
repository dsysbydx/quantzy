"""A parquet-backed caching wrapper around any :class:`DataReader`."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from quantzy.data.base import DataReader

DEFAULT_CACHE_DIR = Path(".cache/prices")


class CachedReader:
    """Wrap a reader and memoize each ``history`` call to a parquet file on disk.

    The cache key is the (tickers, start, end, field) tuple, so repeated runs of the same
    request avoid hitting the network. Delete ``.cache/`` to force a refresh.
    """

    def __init__(self, reader: DataReader, cache_dir: str | Path = DEFAULT_CACHE_DIR) -> None:
        self.reader = reader
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _key(self, tickers: Sequence[str], start, end, field) -> Path:
        raw = f"{sorted(tickers)}|{start}|{end}|{field}"
        digest = hashlib.sha1(raw.encode()).hexdigest()[:16]
        return self.cache_dir / f"{field}_{digest}.parquet"

    def history(
        self,
        tickers: Sequence[str],
        start: str | None = None,
        end: str | None = None,
        field: str = "adj_close",
    ) -> pd.DataFrame:
        path = self._key(tickers, start, end, field)
        if path.exists():
            return pd.read_parquet(path).reindex(columns=list(tickers))
        panel = self.reader.history(tickers, start=start, end=end, field=field)
        panel.to_parquet(path)
        return panel
