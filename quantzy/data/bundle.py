"""A bundle of aligned data panels -- the input every signal reads from.

A :class:`DataBundle` holds named ``(dates x tickers)`` frames that all share one index and
one column set: ``prices`` always, plus whatever else a signal needs (``volume``,
``market_cap``, ``book_value``, ``eps``, ...). Price signals touch ``data.prices``; a
fundamental ratio needs two panels at once, which is why signals read a bundle rather than a
bare price frame.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import pandas as pd

PRICES = "prices"


class DataBundle(Mapping[str, pd.DataFrame]):
    """Named panels sharing one index and column set.

    Build with :meth:`build`, which aligns everything to the price panel. Access panels by
    name (``data["eps"]``) or via :attr:`prices`.
    """

    __slots__ = ("_panels",)

    def __init__(self, panels: Mapping[str, pd.DataFrame]) -> None:
        if PRICES not in panels:
            raise ValueError(f"a DataBundle needs a {PRICES!r} panel, got {sorted(panels)}")
        self._panels = dict(panels)

    @classmethod
    def build(cls, prices: pd.DataFrame, /, **panels: pd.DataFrame) -> DataBundle:
        """Align ``panels`` onto the price panel's index and columns.

        Reindexing only -- no filling. A quarterly fundamental panel must already be
        forward-filled onto a daily index before it gets here; silently filling would hide
        the point-in-time question of *when* each value became knowable.

        ``prices`` is positional-only so that ``build(px, prices=other)`` reaches the check
        below with a useful message instead of a bare ``TypeError``.
        """
        prices = prices.sort_index()
        prices.index = pd.DatetimeIndex(prices.index)
        out = {PRICES: prices}
        for name, panel in panels.items():
            if name == PRICES:
                raise ValueError(f"{PRICES!r} is passed positionally, not as a keyword panel")
            out[name] = panel.reindex(index=prices.index, columns=prices.columns)
        return cls(out)

    # -- Mapping protocol ------------------------------------------------------------
    def __getitem__(self, key: str) -> pd.DataFrame:
        try:
            return self._panels[key]
        except KeyError:
            raise KeyError(
                f"no panel {key!r} in this bundle; available: {sorted(self._panels)}"
            ) from None

    def __iter__(self) -> Iterator[str]:
        return iter(self._panels)

    def __len__(self) -> int:
        return len(self._panels)

    # -- Convenience -----------------------------------------------------------------
    @property
    def prices(self) -> pd.DataFrame:
        return self._panels[PRICES]

    @property
    def index(self) -> pd.DatetimeIndex:
        return self.prices.index

    @property
    def tickers(self) -> pd.Index:
        return self.prices.columns

    def tail(self, n: int) -> DataBundle:
        """The last ``n`` rows of every panel -- the production warmup window."""
        return DataBundle({k: v.iloc[-n:] for k, v in self._panels.items()})

    def upto(self, asof: pd.Timestamp) -> DataBundle:
        """Every panel truncated at ``asof`` inclusive -- used by the look-ahead test."""
        return DataBundle({k: v.loc[:asof] for k, v in self._panels.items()})

    def fingerprint(self) -> tuple:
        """Cheap identity for memoizing derived panels across repeated calls."""
        idx = self.index
        return (
            tuple(sorted(self._panels)),
            len(idx),
            idx[0].value if len(idx) else None,
            idx[-1].value if len(idx) else None,
            tuple(self.tickers),
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"DataBundle(panels={sorted(self._panels)}, "
            f"shape={self.prices.shape}, "
            f"range={self.index[0].date()}..{self.index[-1].date()})"
        )


def as_bundle(data: DataBundle | pd.DataFrame) -> DataBundle:
    """Accept either a bundle or a bare price frame, so price-only callers stay terse."""
    return data if isinstance(data, DataBundle) else DataBundle.build(data)
