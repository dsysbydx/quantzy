"""BACKTEST: what would this strategy have done, decided step by step through history?

    prices -> walk forward, calling strategy.decide() at each rebalance -> returns + metrics

The engine supplies a clock and a simulated book. Every decision comes from the same
``Strategy.decide`` that ``daily_rebalance.py`` calls, so this measures the strategy that
would actually run, not a reimplementation of it.

Run:  uv run python -m examples.backtest
"""

from __future__ import annotations

from examples.strategy_def import build
from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.metrics import (
    annualized_return,
    annualized_vol,
    max_drawdown,
    sharpe_ratio,
    turnover,
)


def main() -> None:
    strategy = build()
    reader = CachedReader(YFinanceReader())
    prices = reader.history(strategy.universe.tickers, start="2015-01-01").dropna(how="any")
    bundle = DataBundle.build(prices)

    print(f"Strategy: {strategy.name}")
    print(f"Loaded {len(bundle.index)} rows for {strategy.universe.tickers}")
    print(f"Warmup: {strategy.min_history} rows\n")

    result = WalkForwardBacktester(rebalance_every=21).run(bundle, strategy)

    print(f"Traded {result.returns.index[0].date()} .. {result.returns.index[-1].date()} "
          f"over {len(result.decisions)} rebalances\n")
    print(f"  Annualized return : {annualized_return(result.returns):>8.2%}")
    print(f"  Annualized vol    : {annualized_vol(result.returns):>8.2%}")
    print(f"  Sharpe            : {sharpe_ratio(result.returns):>8.2f}")
    print(f"  Max drawdown      : {max_drawdown(result.returns):>8.2%}")
    print(f"  Avg turnover/rebal: {turnover(result.weights).mean():>8.2%}")
    print(f"  NAV               : {result.nav.iloc[0]:>8,.0f} -> {result.nav.iloc[-1]:,.0f}\n")

    print("Average weights:")
    print(result.weights.mean().round(4).to_string(), "\n")

    # Every decision is kept, so the run explains itself after the fact.
    last = result.decisions[-1]
    print(f"Most recent decision ({last.asof.date()}):")
    print(last.to_frame().round(4).to_string())


if __name__ == "__main__":
    main()
