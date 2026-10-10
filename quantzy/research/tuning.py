"""Parameter search over strategies, scored by walk-forward backtest.

Implements **train/test holdout**: sweep a parameter grid, rank the combos on an early
``train`` slice, then report the winner's score on a held-out ``test`` slice -- an honest
out-of-sample number that is harder to overfit than an in-sample sweep.

Each combo is run as a *single* backtest over the whole history, and the resulting return
series is split. Running the two slices as separate backtests, as this previously did, made
the test run start from an empty book: the first out-of-sample rebalance paid full turnover
to get invested from cash, a cost the train run had already absorbed, which biased every
out-of-sample number downward. One continuous run also matches what actually happens in
production, where the book does not reset on New Year's Day.

Note that only the *ranking* may use train scores. The test score is read once, for the
winner. Re-running the sweep and picking on the test column is how a holdout quietly becomes
an in-sample fit.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from quantzy.backtest.engine import BacktestResult, WalkForwardBacktester
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.metrics import sharpe_ratio

if TYPE_CHECKING:
    from quantzy.strategy import Strategy

#: Builds a strategy from one parameter combo, e.g.
#: ``lambda lookback, skip: Strategy(universe=u, signals=[Momentum(lookback, skip)], ...)``.
StrategyBuilder = Callable[..., "Strategy"]
Metric = Callable[[pd.Series], float]


@dataclass(slots=True)
class TuneResult:
    """Outcome of a holdout tune.

    Attributes:
        table: One row per combo with ``train_score`` and ``test_score``, sorted by train
            score, best first. The test column is reported for transparency; selecting on it
            is the thing this design exists to prevent.
        best_params: The combo selected on the train slice.
        train_score: ``best_params`` score on the train slice (in-sample, optimistic).
        test_score: ``best_params`` score on the held-out test slice (out-of-sample).
        split_date: First date of the test slice.
        best_result: The full backtest of ``best_params`` over the whole history.
    """

    table: pd.DataFrame
    best_params: dict
    train_score: float
    test_score: float
    split_date: pd.Timestamp
    best_result: BacktestResult

    @property
    def decay(self) -> float:
        """How much of the in-sample score survived out of sample, as a fraction.

        Well below 1.0 is the norm and not by itself alarming; near zero or negative means
        the sweep found noise.
        """
        if not self.train_score:
            return float("nan")
        return self.test_score / self.train_score


def _expand_grid(param_grid: Mapping[str, Sequence]) -> list[dict]:
    keys = list(param_grid)
    return [
        dict(zip(keys, values, strict=True))
        for values in itertools.product(*param_grid.values())
    ]


def holdout_tune(
    data: DataBundle | pd.DataFrame,
    param_grid: Mapping[str, Sequence],
    build_strategy: StrategyBuilder,
    *,
    train_frac: float = 0.7,
    metric: Metric = sharpe_ratio,
    rebalance_every: int = 21,
    nav: float = 100_000.0,
) -> TuneResult:
    """Tune ``param_grid`` by train/test holdout and return the ranked results.

    Args:
        data: A bundle, or a bare price panel.
        param_grid: Mapping of parameter name -> values to sweep, e.g.
            ``{"lookback": [126, 252], "skip": [5, 21]}``.
        build_strategy: Builds a :class:`~quantzy.strategy.Strategy` from one combo (kwargs).
        train_frac: Fraction of the *scored* date range used for the train slice.
        metric: Scores a return series; higher is better. Defaults to annualized Sharpe.
        rebalance_every: Rows between rebalances, passed to the backtester.
        nav: Starting account value.

    Returns:
        A :class:`TuneResult`. Each strategy sizes its own warmup from
        ``Strategy.min_history``, so a grid mixing short and long lookbacks needs no manual
        warmup bookkeeping.

    Raises:
        ValueError: If the grid is empty, or a slice ends up with no scored dates.
    """
    if not param_grid:
        raise ValueError("param_grid is empty")
    if not 0.0 < train_frac < 1.0:
        raise ValueError(f"train_frac must be strictly between 0 and 1, got {train_frac}")

    bundle = as_bundle(data)
    combos = _expand_grid(param_grid)
    backtester = WalkForwardBacktester(rebalance_every=rebalance_every)

    rows: list[dict] = []
    results: dict[tuple, BacktestResult] = {}
    split_date: pd.Timestamp | None = None

    for params in combos:
        strategy = build_strategy(**params)
        result = backtester.run(bundle, strategy, nav=nav)
        train, test, split = _split(result.returns, train_frac)
        split_date = split if split_date is None else split_date
        key = tuple(sorted(params.items()))
        results[key] = result
        rows.append(
            {
                **params,
                "train_score": float(metric(train)),
                "test_score": float(metric(test)),
                "n_rebalances": len(result.decisions),
            }
        )

    table = pd.DataFrame(rows).sort_values("train_score", ascending=False).reset_index(drop=True)
    best_params = {k: table.loc[0, k] for k in param_grid}
    best_key = tuple(sorted(best_params.items()))

    return TuneResult(
        table=table,
        best_params=best_params,
        train_score=float(table.loc[0, "train_score"]),
        test_score=float(table.loc[0, "test_score"]),
        split_date=split_date,
        best_result=results[best_key],
    )


def _split(
    returns: pd.Series, train_frac: float
) -> tuple[pd.Series, pd.Series, pd.Timestamp]:
    """Split a return series into train/test at ``train_frac`` of its length."""
    n = len(returns)
    cut = int(n * train_frac)
    if cut < 1 or cut >= n:
        raise ValueError(
            f"train_frac={train_frac} leaves an empty slice for {n} scored dates; "
            "lengthen the history or move the split"
        )
    return returns.iloc[:cut], returns.iloc[cut:], returns.index[cut]
