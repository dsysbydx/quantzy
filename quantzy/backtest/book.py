"""The simulated book: the half of a backtest that live replaces with a broker.

A :class:`SimulatedBook` holds weights and NAV through a run, applies a
:class:`~quantzy.strategy.Decision` as if it filled exactly at the decision price, and
accrues each period's return. It is deliberately the *only* thing in a backtest that a live
runner throws away -- the decision itself comes from
:meth:`~quantzy.strategy.Strategy.decide`, which both worlds share.

Keeping it a named object rather than three loop variables is what makes that boundary
visible: everything here is an assumption about execution, and every assumption here is one
a live run answers with a fill instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from quantzy.construction.portfolio import Portfolio
from quantzy.strategy import Decision


@dataclass(slots=True)
class SimulatedBook:
    """Weights and NAV through a simulated run.

    Args:
        tickers: Column order the book is held in.
        nav: Starting account value. Compounds with realized returns, so notional trade
            sizes grow with the book -- the previous engine reset NAV to its starting value
            at every rebalance, which left every ``delta_notional`` after the first one
            quoted against a stale account size.
    """

    tickers: list[str]
    nav: float = 100_000.0
    weights: pd.Series = field(init=False)

    def __post_init__(self) -> None:
        self.weights = pd.Series(0.0, index=list(self.tickers))

    @property
    def portfolio(self) -> Portfolio:
        """The book as a :class:`Portfolio`, for handing to the optimizer as ``current``."""
        return Portfolio(weights=self.weights, nav=self.nav)

    def apply(self, decision: Decision) -> float:
        """Move the book to ``decision``'s target and return the cost charged.

        The fill is assumed perfect: full size, at the decision price, with cost taken from
        the optimizer's own estimate. Slippage, partial fills and market impact all live
        behind this one method.

        Returns:
            Cost as a fraction of NAV (0.0 when the optimizer priced no cost).
        """
        self.weights = decision.target.weights.reindex(self.tickers).fillna(0.0)
        cost = decision.trades.cost
        return float(cost) if cost is not None else 0.0

    def accrue(self, asset_returns: pd.Series, cost: float = 0.0) -> float:
        """Earn one period's return on the book currently held, net of ``cost``.

        The weights then drift with prices: an asset that outperformed ends the period a
        larger share of the book. Holding them fixed instead would mean trading back to
        target every period, for free -- a daily rebalance the cost model never sees, and
        one that flatters any strategy that rebalances less often than daily.

        Args:
            asset_returns: Per-asset returns for the period, indexed by ticker.
            cost: Trading cost charged this period, as a fraction of NAV.

        Returns:
            The portfolio return for the period, net of cost.
        """
        r = asset_returns.reindex(self.tickers).fillna(0.0)
        gross = float((self.weights * r).sum())
        net = gross - cost
        self.nav *= 1.0 + net
        # Cost is a proportional haircut on the whole book, so it does not move the mix.
        if 1.0 + gross > 0.0:
            self.weights = self.weights * (1.0 + r) / (1.0 + gross)
        return net
