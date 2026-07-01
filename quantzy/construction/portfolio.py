"""The Portfolio domain object."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class Portfolio:
    """A set of target weights, optionally backed by a NAV (for notional sizing).

    Attributes:
        weights: Series indexed by ticker; fractions of NAV (long positive, short negative).
        nav: Optional account value, used to convert weights into traded notional.
    """

    weights: pd.Series
    nav: float | None = None

    @classmethod
    def empty(cls, tickers, nav: float | None = None) -> Portfolio:
        return cls(weights=pd.Series(0.0, index=list(tickers)), nav=nav)

    @classmethod
    def from_holdings(cls, holdings: pd.Series, nav: float | None = None) -> Portfolio:
        """Build from notional holdings (per-asset dollar values); weights = holding / NAV."""
        total = float(holdings.sum()) if nav is None else nav
        if total == 0:
            raise ValueError("cannot derive weights from holdings with zero total value")
        return cls(weights=holdings / total, nav=total)

    @property
    def gross(self) -> float:
        return float(self.weights.abs().sum())

    @property
    def net(self) -> float:
        return float(self.weights.sum())

    def notional(self) -> pd.Series:
        if self.nav is None:
            raise ValueError("portfolio has no NAV; cannot compute notional")
        return self.weights * self.nav

    def align_to(self, tickers) -> Portfolio:
        """Reindex weights onto ``tickers`` (missing assets -> 0 weight)."""
        return Portfolio(weights=self.weights.reindex(list(tickers)).fillna(0.0), nav=self.nav)
