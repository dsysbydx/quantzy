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


def sector_etf_universe() -> Universe:
    """The nine original Select Sector SPDRs plus a short-Treasury cash sleeve.

    All ten trade from mid-2002 (SHY is the binding inception), long enough to cover 2008.
    XLRE (2015) and XLC (2018) are left out: their short histories would cut the backtest
    to the last decade. SHY rather than SGOV (2020) is the cash sleeve for the same reason;
    it carries a little rate risk (~1.9y duration) that a T-bill fund would not.
    """
    sectors = [
        ("XLB", "Materials"),
        ("XLE", "Energy"),
        ("XLF", "Financials"),
        ("XLI", "Industrials"),
        ("XLK", "Technology"),
        ("XLP", "Consumer Staples"),
        ("XLU", "Utilities"),
        ("XLV", "Health Care"),
        ("XLY", "Consumer Discretionary"),
    ]
    return Universe(
        name="sector_etf",
        assets=[Asset(t, f"Select Sector SPDR: {n}", "etf") for t, n in sectors]
        + [Asset("SHY", "iShares 1-3 Year Treasury", "cash_equivalent")],
    )
