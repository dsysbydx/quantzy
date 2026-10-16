"""Walk-forward backtest engine.

The engine is *tooling*: it supplies a clock and a simulated book, and asks the strategy the
same question a production job asks, once per rebalance date. Every decision comes from
:meth:`~quantzy.strategy.Strategy.decide`, so what is being tested here is the thing that
would run live -- not a reimplementation of it.

**Timing.** At rebalance date ``t`` the engine rebalances *before* accruing, because a
signal's score at row ``t`` is already lagged to be tradeable at ``t``
(:attr:`~quantzy.signals.base.Signal.lag`, default 1): it was computed from the close of
``t-1``, so it sizes the book that earns ``t``. The risk model carries the same lag, so
``mu`` and ``sigma`` rest on identical information. Nothing in a decision at ``t`` has seen
any data dated ``t`` or later.

The previous engine accrued first and rebalanced second, with the lag living implicitly in
that statement order. It happened to be right, and nothing tested it. Here the lag is a
declared property of each component and ``tests/test_signals.py`` asserts it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pandas as pd

from quantzy.backtest.book import SimulatedBook
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.returns import to_returns

if TYPE_CHECKING:
    from quantzy.strategy import Decision, Strategy


@dataclass(slots=True)
class BacktestResult:
    """Output of a walk-forward run.

    Attributes:
        returns: Per-period portfolio returns, net of estimated cost on rebalance days.
        weights: Held weights through time (dates x tickers); the row for date ``t`` is the
            book that earns ``returns[t]``.
        nav: Account value through time, compounding ``returns``.
        decisions: Every :class:`~quantzy.strategy.Decision` the strategy made, in order.
            Keeping them is what lets you ask "what did it think on this date, and why?"
            after the run -- and what a live run's replay check compares against.
    """

    returns: pd.Series
    weights: pd.DataFrame
    nav: pd.Series
    decisions: list[Decision] = field(default_factory=list)

    @property
    def rebalance_dates(self) -> pd.DatetimeIndex:
        """The dates on which the optimizer was re-run."""
        return pd.DatetimeIndex([d.asof for d in self.decisions])

    def decision_at(self, asof: pd.Timestamp | str) -> Decision:
        """The decision made on ``asof``.

        Raises:
            KeyError: If no rebalance happened on that date.
        """
        asof = pd.Timestamp(asof)
        for d in self.decisions:
            if d.asof == asof:
                return d
        raise KeyError(f"no rebalance on {asof.date()}; see .rebalance_dates")


@dataclass(slots=True)
class WalkForwardBacktester:
    """Periodic-rebalance simulator.

    Args:
        rebalance_every: Rows between rebalances (~21 = monthly).
        warmup: Rows of history required before the first rebalance. ``None`` -- the default
            -- takes :attr:`~quantzy.strategy.Strategy.min_history` from the strategy, so
            the warmup tracks the signals actually being run instead of being a constant the
            caller has to remember to raise alongside a longer lookback.
    """

    rebalance_every: int = 21
    warmup: int | None = None

    def run(
        self,
        data: DataBundle | pd.DataFrame,
        strategy: Strategy,
        *,
        nav: float = 100_000.0,
    ) -> BacktestResult:
        """Simulate ``strategy`` over ``data`` and return its returns, weights and decisions.

        Raises:
            ValueError: If the history is shorter than the warmup the strategy needs.
        """
        bundle = as_bundle(data)
        warmup = self.warmup if self.warmup is not None else strategy.min_history
        if len(bundle.index) <= warmup:
            raise ValueError(
                f"{strategy.name} needs > {warmup} rows of history "
                f"({'explicit warmup' if self.warmup is not None else 'strategy.min_history'}), "
                f"got {len(bundle.index)}"
            )

        asset_returns = to_returns(bundle.prices)
        tickers = list(strategy.universe.tickers)
        book = SimulatedBook(tickers=tickers, nav=nav)

        index = bundle.index
        rebal_dates = set(index[warmup :: self.rebalance_every])
        first_date = index[warmup]

        port_returns: dict[pd.Timestamp, float] = {}
        weight_rows: dict[pd.Timestamp, pd.Series] = {}
        nav_rows: dict[pd.Timestamp, float] = {}
        decisions: list[Decision] = []

        for date in asset_returns.index[asset_returns.index >= first_date]:
            # Rebalance first: the score at ``date`` is lagged to be tradeable at ``date``,
            # so it sizes the book that earns ``date``'s return.
            cost = 0.0
            if date in rebal_dates:
                decision = strategy.decide(bundle, asof=date, current=book.portfolio)
                cost = book.apply(decision)
                decisions.append(decision)

            # Record the book before accruing: it is what earns ``date``'s return. After
            # accruing, the weights have drifted to where the day's prices left them.
            weight_rows[date] = book.weights.copy()
            port_returns[date] = book.accrue(asset_returns.loc[date], cost=cost)
            nav_rows[date] = book.nav

        return BacktestResult(
            returns=pd.Series(port_returns, name="return").sort_index(),
            weights=pd.DataFrame(weight_rows).T.reindex(columns=tickers).sort_index(),
            nav=pd.Series(nav_rows, name="nav").sort_index(),
            decisions=decisions,
        )
