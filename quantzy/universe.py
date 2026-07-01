"""Asset and investable-universe definitions."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

AssetKind = Literal["etf", "equity", "bond", "cash_equivalent"]


@dataclass(frozen=True, slots=True)
class Asset:
    """A single tradeable instrument.

    ``SGOV`` (a short T-bill ETF) is used as the risk-off / cash-equivalent sleeve, but it
    is an ordinary asset with real prices -- no special casing anywhere in the stack.
    """

    ticker: str
    name: str = ""
    kind: AssetKind = "etf"


@dataclass(frozen=True, slots=True)
class Universe:
    """An ordered collection of assets the strategy may hold."""

    assets: Sequence[Asset]
    name: str = "universe"

    def __post_init__(self) -> None:
        seen = set()
        for a in self.assets:
            if a.ticker in seen:
                raise ValueError(f"duplicate ticker in universe: {a.ticker}")
            seen.add(a.ticker)

    @property
    def tickers(self) -> list[str]:
        return [a.ticker for a in self.assets]

    def __len__(self) -> int:
        return len(self.assets)

    def __iter__(self) -> Iterable[Asset]:
        return iter(self.assets)

    @classmethod
    def from_tickers(cls, tickers: Iterable[str], name: str = "universe") -> Universe:
        return cls(assets=[Asset(ticker=t) for t in tickers], name=name)


def default_etf_universe() -> Universe:
    """The starter universe: broad-equity ETFs plus a T-bill (cash-equivalent) sleeve."""
    return Universe(
        name="default_etf",
        assets=[
            Asset("QQQ", "Invesco QQQ (Nasdaq-100)", "etf"),
            Asset("VOO", "Vanguard S&P 500", "etf"),
            Asset("URTH", "iShares MSCI World", "etf"),
            Asset("SGOV", "iShares 0-3 Month Treasury", "cash_equivalent"),
        ],
    )
