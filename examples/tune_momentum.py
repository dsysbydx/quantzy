"""Tune the momentum lookback/skip by train/test holdout.

    prices --(holdout)--> sweep Momentum(lookback, skip) on the train slice, rank by Sharpe,
    then report the winning combo's Sharpe on the held-out test slice.

Run:  python examples/tune_momentum.py
First run hits Yahoo Finance; results are cached to .cache/ thereafter.
"""

from __future__ import annotations

from quantzy.alpha import Momentum
from quantzy.backtest import holdout_tune
from quantzy.data import CachedReader, YFinanceReader
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.risk import LedoitWolfCovariance
from quantzy.universe import default_etf_universe


def main() -> None:
    universe = default_etf_universe()
    reader = CachedReader(YFinanceReader())
    prices = reader.history(universe.tickers, start="2015-01-01").dropna(how="any")
    print(f"Loaded {len(prices)} days for {universe.tickers}\n")

    # The same risk model + optimizer are reused at every rebalance and across every combo.
    risk_model = LedoitWolfCovariance()
    optimizer = MeanVariance(
        risk_aversion=8.0,
        constraints=[C.budget(), C.long_only()],
        cost_model=LinearCost(cost_bps=5.0),
    )

    param_grid = {
        # short-, medium-, and long-term momentum windows (trading days). All must exceed the
        # skip below, or the window collapses to zero width (a flat signal -> parks in cash).
        "lookback": [42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252],  # ~2..12 months
        "skip": [7, 14, 21],                                               # ~1.5, 3, 4 weeks
    }

    result = holdout_tune(
        prices,
        param_grid,
        build_alpha=lambda lookback, skip: Momentum(lookback=lookback, skip=skip),
        risk_model=risk_model,
        optimizer=optimizer,
        train_frac=0.7,
        rebalance_every=21,
    )

    print("Train-slice ranking (annualized Sharpe):")
    print(result.table.round(3).to_string(index=False), "\n")

    # Heatmap pivot: rows = lookback, cols = skip, cell = train Sharpe. Makes the hump-shaped
    # sweet spot easy to spot at a glance; trailing row/col show the per-axis averages.
    pivot = result.table.pivot(index="lookback", columns="skip", values="train_score")
    pivot.loc["mean"] = pivot.mean()
    pivot["mean"] = pivot.mean(axis=1)
    print("Train Sharpe by lookback (rows) x skip (cols):")
    print(pivot.round(3).to_string(), "\n")

    print(f"Best params (chosen in-sample): {result.best_params}")
    print(f"  train Sharpe: {result.train_score:.3f}")
    print(f"  test  Sharpe (held-out): {result.test_score:.3f}")


if __name__ == "__main__":
    main()
