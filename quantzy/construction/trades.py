"""Trades: the diff between a current portfolio and an optimized target."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quantzy.construction.portfolio import Portfolio


@dataclass(frozen=True, slots=True)
class Trade:
    """A single rebalancing instruction for one asset."""

    ticker: str
    delta_weight: float
    delta_notional: float | None = None

    @property
    def side(self) -> str:
        return "buy" if self.delta_weight > 0 else "sell" if self.delta_weight < 0 else "hold"


@dataclass(frozen=True, slots=True)
class TradeList:
    """The full set of trades to move from current to target weights."""

    trades: list[Trade]
    cost: float | None = None  # estimated transaction cost (weight units), if priced

    @property
    def turnover(self) -> float:
        """One-way turnover = 0.5 * sum |delta weight| (fraction of NAV traded)."""
        return 0.5 * sum(abs(t.delta_weight) for t in self.trades)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ticker": [t.ticker for t in self.trades],
                "side": [t.side for t in self.trades],
                "delta_weight": [t.delta_weight for t in self.trades],
                "delta_notional": [t.delta_notional for t in self.trades],
            }
        ).set_index("ticker")

    def nonzero(self, tol: float = 1e-9) -> TradeList:
        return TradeList([t for t in self.trades if abs(t.delta_weight) > tol], cost=self.cost)


def diff(current: Portfolio, target: Portfolio, cost: float | None = None) -> TradeList:
    """Compute the trades to move ``current`` -> ``target``.

    Weights are aligned across the union of tickers (missing -> 0). If ``target`` carries a
    NAV, per-asset notional deltas are filled in too.
    """
    tickers = current.weights.index.union(target.weights.index)
    cur = current.weights.reindex(tickers).fillna(0.0)
    tgt = target.weights.reindex(tickers).fillna(0.0)
    delta = tgt - cur

    nav = target.nav if target.nav is not None else current.nav
    trades = [
        Trade(
            ticker=str(tk),
            delta_weight=float(delta[tk]),
            delta_notional=(float(delta[tk] * nav) if nav is not None else None),
        )
        for tk in tickers
    ]
    return TradeList(trades=trades, cost=cost)
