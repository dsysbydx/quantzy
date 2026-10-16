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

Every combo trades the same dates by default. Each strategy needs its own warmup, so left to
itself a slow-EMA combo would start trading years after a fast one, and the two Sharpe
ratios would be measured over different markets. The tuner instead starts every run at the
warmup of the slowest combo, so differences in score are differences in parameters.

Note that only the *ranking* may use train scores. The test score is read once, for the
winner. Re-running the sweep and picking on the test column is how a holdout quietly becomes
an in-sample fit.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pandas as pd

from quantzy.backtest.engine import BacktestResult, WalkForwardBacktester
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.metrics import annualized_vol, sharpe_ratio
from quantzy.research.compare import compare

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
        results: Every combo's backtest, keyed by :func:`label` of its params, in the
            order of :attr:`table`.
    """

    table: pd.DataFrame
    best_params: dict
    train_score: float
    test_score: float
    split_date: pd.Timestamp
    best_result: BacktestResult
    results: dict[str, BacktestResult] = field(default_factory=dict)

    def compare(self, top: int | None = None, **benchmarks: pd.Series) -> pd.DataFrame:
        """Full metrics for each combo, split at :attr:`split_date` into train and test.

        Args:
            top: Keep only the best ``top`` combos by train score.
            **benchmarks: Extra return series to set alongside, e.g. ``equal_weight=...``.
        """
        labels = list(self.results)[:top]
        runs: dict[str, BacktestResult | pd.Series] = {k: self.results[k] for k in labels}
        runs.update(benchmarks)
        return compare(runs, self.split_date)

    @property
    def decay(self) -> float:
        """How much of the in-sample score survived out of sample, as a fraction.

        Well below 1.0 is the norm and not by itself alarming; near zero or negative means
        the sweep found noise.
        """
        if not self.train_score:
            return float("nan")
        return self.test_score / self.train_score


def label(params: Mapping) -> str:
    """A compact, stable name for one combo, e.g. ``"long=200 short=10"``."""
    return " ".join(f"{k}={v}" for k, v in params.items())


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
    align: bool = True,
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
        align: Start every combo at the slowest combo's warmup, so all of them trade the
            same dates. ``False`` lets each start as soon as its own warmup allows, which
            uses more history per combo but makes their scores incomparable.

    Returns:
        A :class:`TuneResult`. Warmups come from each ``Strategy.min_history``, so a grid
        mixing short and long lookbacks needs no manual warmup bookkeeping.

    Raises:
        ValueError: If the grid is empty, or a slice ends up with no scored dates.
    """
    if not param_grid:
        raise ValueError("param_grid is empty")
    if not 0.0 < train_frac < 1.0:
        raise ValueError(f"train_frac must be strictly between 0 and 1, got {train_frac}")

    bundle = as_bundle(data)
    combos = [(params, build_strategy(**params)) for params in _expand_grid(param_grid)]
    warmup = max(s.min_history for _, s in combos) if align else None
    backtester = WalkForwardBacktester(rebalance_every=rebalance_every, warmup=warmup)

    rows: list[dict] = []
    results: dict[str, BacktestResult] = {}
    split_date: pd.Timestamp | None = None

    for params, strategy in combos:
        result = backtester.run(bundle, strategy, nav=nav)
        train, test, split = _split(result.returns, train_frac)
        split_date = split if split_date is None else split_date
        results[label(params)] = result
        rows.append(
            {
                **params,
                "train_score": float(metric(train)),
                "test_score": float(metric(test)),
                "n_rebalances": len(result.decisions),
            }
        )

    table = pd.DataFrame(rows).sort_values("train_score", ascending=False)
    # Labels are taken from the original combos, by row position, rather than rebuilt from
    # the table, where a mixed-dtype row would print 200 as 200.0.
    ranked = [label(combos[i][0]) for i in table.index]
    table = table.reset_index(drop=True)
    best_params = {k: _py(table.loc[0, k]) for k in param_grid}

    return TuneResult(
        table=table,
        best_params=best_params,
        train_score=float(table.loc[0, "train_score"]),
        test_score=float(table.loc[0, "test_score"]),
        split_date=split_date,
        best_result=results[ranked[0]],
        results={k: results[k] for k in ranked},
    )


def _py(value):
    """A numpy scalar as its Python equivalent, so best_params prints as {'long': 150}."""
    return value.item() if hasattr(value, "item") else value


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


@dataclass(slots=True)
class RiskAversionFit:
    """Outcome of :func:`calibrate_risk_aversion`.

    Attributes:
        risk_aversion: The calibrated lambda.
        realized_vol: Annualized vol of the calibration run at ``risk_aversion``.
        target_vol: The vol asked for.
        trials: Every ``(lambda, realized vol)`` tried, in order.
        result: The backtest at ``risk_aversion``, over the calibration window only.
    """

    risk_aversion: float
    realized_vol: float
    target_vol: float
    trials: list[tuple[float, float]]
    result: BacktestResult


def calibrate_risk_aversion(
    data: DataBundle | pd.DataFrame,
    build_strategy: Callable[[float], Strategy],
    target_vol: float,
    *,
    end: pd.Timestamp | str | None = None,
    backtester: WalkForwardBacktester | None = None,
    bounds: tuple[float, float] = (0.05, 50.0),
    tol: float = 0.0025,
    max_iter: int = 12,
) -> RiskAversionFit:
    """Find the mean-variance risk aversion whose backtest runs at ``target_vol``.

    Risk aversion only means something relative to the scale of ``mu``: the unconstrained
    optimum is ``Sigma^-1 mu / (2 lambda)``, so adding an alpha to ``mu`` makes the same
    lambda run a bigger book. Fixing the vol target and solving for lambda keeps the book's
    size stable as alphas come and go. Re-run this whenever ``mu`` changes.

    Lambda stays constant through the run, so the book still drifts with its own forecast
    Sharpe; the target holds on average, not every day. Holding it every day is the hard
    :func:`~quantzy.optimize.constraints.max_vol` constraint, which trades on every move in
    the vol forecast.

    The search bisects ``log(lambda)``: realized vol falls as lambda rises. Pass ``end`` to
    calibrate on history before it only, so data after ``end`` stays out of sample.

    Args:
        data: A bundle, or a bare price panel.
        build_strategy: Builds the strategy for one lambda.
        target_vol: Annualized vol to hit, e.g. ``0.10``.
        end: Last date the calibration may see. ``None`` uses the whole history.
        backtester: The backtester to run; defaults to daily rebalancing.
        bounds: Lambda search range.
        tol: Stop once realized vol is within this of the target.
        max_iter: Bisection steps after the two bound checks.

    Raises:
        ValueError: If ``target_vol`` is not positive, or the bounds are invalid.
    """
    if target_vol <= 0:
        raise ValueError(f"target_vol must be > 0, got {target_vol}")
    lo, hi = bounds
    if not 0 < lo < hi:
        raise ValueError(f"bounds must satisfy 0 < lo < hi, got {bounds}")

    bundle = as_bundle(data)
    if end is not None:
        bundle = bundle.upto(pd.Timestamp(end))
    bt = backtester or WalkForwardBacktester(rebalance_every=1)
    trials: list[tuple[float, float]] = []
    runs: dict[float, BacktestResult] = {}

    def vol_at(lam: float) -> float:
        res = bt.run(bundle, build_strategy(lam))
        runs[lam] = res
        v = float(annualized_vol(res.returns))
        trials.append((lam, v))
        return v

    def fit(lam: float, v: float) -> RiskAversionFit:
        return RiskAversionFit(lam, v, target_vol, trials, runs[lam])

    v_lo, v_hi = vol_at(lo), vol_at(hi)
    if v_lo <= target_vol:  # even the boldest book is too quiet, e.g. long-only, no leverage
        return fit(lo, v_lo)
    if v_hi >= target_vol:
        return fit(hi, v_hi)
    for _ in range(max_iter):
        mid = math.sqrt(lo * hi)
        v = vol_at(mid)
        if abs(v - target_vol) <= tol:
            return fit(mid, v)
        lo, hi = (mid, hi) if v > target_vol else (lo, mid)
    best = min(trials, key=lambda t: abs(t[1] - target_vol))
    return fit(*best)
