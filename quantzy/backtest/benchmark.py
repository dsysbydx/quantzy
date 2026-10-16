"""Fixed-weight benchmarks, run through the same engine as the strategies they judge.

A benchmark computed off to the side -- an equal-weight mean of daily returns, say -- is
rebalanced every day for free, which no strategy in the backtest is allowed. Comparing a
monthly, cost-paying strategy against it charges the strategy for something the benchmark
gets as a gift. :class:`FixedWeights` instead makes the same kind of :class:`Decision` a
:class:`~quantzy.strategy.Strategy` does, so the walk-forward engine rebalances it on the same
dates, lets it drift in between, and charges it the same cost model.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pandas as pd

from quantzy.construction.portfolio import Portfolio
from quantzy.construction.trades import diff
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.optimize.cost import CostModel
from quantzy.strategy import Decision
from quantzy.universe import Universe


@dataclass
class FixedWeights:
    """Rebalance to the same target every time.

    Args:
        universe: Assets the book may hold.
        weights: Target weight per ticker. ``None`` means equal weight across ``holdings``.
        holdings: The tickers to equal-weight when ``weights`` is ``None``; defaults to the
            whole universe. Use it to leave out a cash sleeve.
        cost_model: Charged on every rebalance, exactly as the optimizer's would be.
        name: Label used in diagnostics and result objects.
    """

    universe: Universe
    weights: Mapping[str, float] | None = None
    holdings: Sequence[str] | None = None
    cost_model: CostModel | None = None
    name: str = "fixed_weights"

    def __post_init__(self) -> None:
        tickers = list(self.universe.tickers)
        if self.weights is None:
            names = list(self.holdings) if self.holdings is not None else tickers
            unknown = sorted(set(names) - set(tickers))
            if unknown or not names:
                raise ValueError(f"holdings must be a non-empty subset of the universe, "
                                 f"got unknown {unknown}")
            self.weights = {t: 1.0 / len(names) for t in names}
        unknown = sorted(set(self.weights) - set(tickers))
        if unknown:
            raise ValueError(f"weights name tickers outside the universe: {unknown}")
        self._target = pd.Series(self.weights, dtype=float).reindex(tickers).fillna(0.0)

    @property
    def min_history(self) -> int:
        """A fixed book needs no history to decide."""
        return 1

    def decide(
        self,
        data: DataBundle | pd.DataFrame,
        asof: pd.Timestamp | None = None,
        current: Portfolio | None = None,
    ) -> Decision:
        bundle = as_bundle(data)
        tickers = list(self.universe.tickers)
        if current is None:
            current = Portfolio.empty(tickers)
        target = Portfolio(weights=self._target.copy(), nav=current.nav)
        delta = (target.weights - current.weights.reindex(tickers).fillna(0.0)).to_numpy()
        cost = self.cost_model.estimate(delta) if self.cost_model is not None else None
        return Decision(
            asof=bundle.index[-1] if asof is None else pd.Timestamp(asof),
            target=target,
            trades=diff(current, target, cost=cost).nonzero(),
            mu=pd.Series(0.0, index=tickers),
            sigma=pd.DataFrame(index=tickers, columns=tickers, dtype=float),
            status="fixed",
            diagnostics={"strategy": self.name},
        )
