"""RESEARCH: is this signal worth trading, and at what parameters?

Two questions, in the order worth asking them:

1. **Does the score predict the forward return at all?** The information coefficient judges
   a signal before any portfolio exists, so it is not confounded by the risk model, the
   optimizer's risk aversion or the constraint set. A signal with no IC cannot be rescued by
   portfolio construction.
2. **Which parameters?** Only once the answer to (1) is yes. Swept by holdout: ranked on a
   train slice, reported on a held-out test slice.

Nothing here runs in production -- this is the exploratory half of the repo.

Run:  uv run python -m examples.research_signal
"""

from __future__ import annotations

from examples.strategy_def import build
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import holdout_tune, ic_decay
from quantzy.signals import Momentum
from quantzy.universe import default_etf_universe


def main() -> None:
    universe = default_etf_universe()
    reader = CachedReader(YFinanceReader())
    prices = reader.history(universe.tickers, start="2015-01-01").dropna(how="any")
    bundle = DataBundle.build(prices)
    print(f"Loaded {len(bundle.index)} rows for {universe.tickers}\n")

    # --- 1. does the raw signal predict anything? ---
    signal = Momentum(lookback=252, skip=21)
    scores = signal.scores(bundle)
    # min_names is the floor for a usable cross-section; this universe only has four names,
    # so it has to come down from the default of 5. A four-asset IC is noisy by construction.
    print(f"IC decay for {type(signal).__name__}({signal.params()}):")
    print(ic_decay(scores, bundle.prices, min_names=3).round(4).to_string())
    print("\n  mean_ic  : average rank correlation of score with forward return")
    print("  ic_ir    : mean_ic / its own volatility")
    print("  t_stat   : de-overlapped, so a 21-day horizon is not counted 21 times\n")

    # --- 2. only now, parameters ---
    result = holdout_tune(
        bundle,
        {"lookback": [63, 126, 189, 252], "skip": [5, 21]},
        build_strategy=build,
        train_frac=0.7,
        rebalance_every=21,
    )

    print("Sweep, ranked on the train slice:")
    print(result.table.round(3).to_string(index=False), "\n")

    pivot = result.table.pivot(index="lookback", columns="skip", values="train_score")
    print("Train Sharpe by lookback (rows) x skip (cols):")
    print(pivot.round(3).to_string(), "\n")

    print(f"Split at {result.split_date.date()}")
    print(f"Best params (chosen in-sample): {result.best_params}")
    print(f"  train Sharpe : {result.train_score:.3f}")
    print(f"  test  Sharpe : {result.test_score:.3f}  (held out)")
    print(f"  survival     : {result.decay:.1%} of the in-sample score")


if __name__ == "__main__":
    main()
