"""Parameter tuning for strategies via a walk-forward backtest.

Currently implements **train/test holdout**: sweep a parameter grid on an early ``train`` slice
of history, pick the combo with the best score, then report that combo's score on a held-out
``test`` slice -- an honest out-of-sample number that is harder to overfit than an in-sample
sweep. The same ``build_alpha`` factory pattern extends to other parameters (Value window,
risk aversion) and, later, to walk-forward re-optimization.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from quantzy.backtest.engine import BacktestResult, WalkForwardBacktester
from quantzy.backtest.metrics import sharpe_ratio

if TYPE_CHECKING:
    from quantzy.alpha.base import Alpha
    from quantzy.optimize.base import BaseOptimizer
    from quantzy.risk.base import RiskModel

#: A factory that builds an alpha from one parameter combo, e.g.
#: ``lambda lookback, skip: Momentum(lookback=lookback, skip=skip)``.
AlphaBuilder = Callable[..., "Alpha"]
Metric = Callable[[pd.Series], float]


@dataclass(slots=True)
class TuneResult:
    """Outcome of a holdout tune.

    Attributes:
        table: One row per parameter combo with its ``train_score``, sorted best-first.
        best_params: The combo selected on the train slice.
        train_score: ``best_params`` score on the train slice (in-sample, optimistic).
        test_score: ``best_params`` score on the held-out test slice (out-of-sample, honest).
        test_result: The full backtest of ``best_params`` on the test slice.
    """

    table: pd.DataFrame
    best_params: dict
    train_score: float
    test_score: float
    test_result: BacktestResult


def _expand_grid(param_grid: Mapping[str, Sequence]) -> list[dict]:
    keys = list(param_grid)
    return [
        dict(zip(keys, values, strict=True))
        for values in itertools.product(*param_grid.values())
    ]


def holdout_tune(
    prices: pd.DataFrame,
    param_grid: Mapping[str, Sequence],
    build_alpha: AlphaBuilder,
    risk_model: RiskModel,
    optimizer: BaseOptimizer,
    *,
    train_frac: float = 0.7,
    metric: Metric = sharpe_ratio,
    rebalance_every: int = 21,
    warmup: int = 252,
    nav: float = 100_000.0,
) -> TuneResult:
    """Tune ``param_grid`` by train/test holdout and return the ranked results.

    Args:
        prices: Wide price panel (dates x tickers).
        param_grid: Mapping of parameter name -> values to sweep, e.g.
            ``{"lookback": [126, 252, 504], "skip": [5, 21, 42]}``.
        build_alpha: Builds an alpha from one combo of grid parameters (kwargs).
        risk_model, optimizer: Reused unchanged at every rebalance and across combos.
        train_frac: Fraction of the date range used for the (earlier) train slice.
        metric: Scores a return series; higher is better. Defaults to annualized Sharpe.
        rebalance_every, warmup, nav: Passed to :class:`WalkForwardBacktester`. ``warmup`` is
            auto-raised to clear the largest ``lookback``/``skip`` in the grid.

    Raises:
        ValueError: If the grid is empty or the train/test slices are too short for warmup.
    """
    if not param_grid:
        raise ValueError("param_grid is empty")
    combos = _expand_grid(param_grid)

    warmup = _min_warmup(param_grid, warmup)
    backtester = WalkForwardBacktester(rebalance_every=rebalance_every, warmup=warmup)

    split = int(len(prices) * train_frac)
    train_px, test_px = prices.iloc[:split], prices.iloc[split:]
    for label, frame in (("train", train_px), ("test", test_px)):
        if len(frame) <= warmup:
            raise ValueError(
                f"{label} slice has {len(frame)} rows but needs > {warmup} for warmup; "
                f"lengthen history, lower train_frac, or shrink warmup/lookback"
            )

    rows = []
    for params in combos:
        alpha = build_alpha(**params)
        result = backtester.run(train_px, alpha, risk_model, optimizer, nav=nav)
        rows.append({**params, "train_score": float(metric(result.returns))})

    table = pd.DataFrame(rows).sort_values("train_score", ascending=False).reset_index(drop=True)
    best_params = {k: table.loc[0, k] for k in param_grid}
    train_score = float(table.loc[0, "train_score"])

    best_alpha = build_alpha(**best_params)
    test_result = backtester.run(test_px, best_alpha, risk_model, optimizer, nav=nav)
    test_score = float(metric(test_result.returns))

    return TuneResult(
        table=table,
        best_params=best_params,
        train_score=train_score,
        test_score=test_score,
        test_result=test_result,
    )


def _min_warmup(param_grid: Mapping[str, Sequence], warmup: int) -> int:
    """Ensure warmup clears the largest lookback (+skip) so ``forecast`` has enough history."""
    longest = 0
    if "lookback" in param_grid:
        longest = max(param_grid["lookback"])
        if "skip" in param_grid:
            longest += max(param_grid["skip"])
    elif "window" in param_grid:  # Value(window)
        longest = max(param_grid["window"])
    return max(warmup, longest + 1)
