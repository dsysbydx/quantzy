"""Side-by-side performance of several runs over one common window.

Comparing two backtests is only meaningful over the same dates. A slow-EMA variant that
starts trading two years after a fast one has skipped -- or been spared -- whatever happened
in those two years, and its Sharpe says as much about the calendar as about the parameters.
:func:`compare` therefore trims every run to the dates they all share before measuring.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from quantzy.backtest.engine import BacktestResult
from quantzy.config import TRADING_DAYS
from quantzy.metrics import annualized_return, annualized_vol, max_drawdown, sharpe_ratio

Run = BacktestResult | pd.Series


def summarize(returns: pd.Series, result: BacktestResult | None = None) -> dict[str, float]:
    """Headline metrics for one return series.

    Args:
        returns: Per-period returns over the window to measure.
        result: The run they came from, if any. Supplies turnover, which is counted from
            the decisions traded inside the window -- not from the held weights, whose
            day-to-day drift is not trading.
    """
    ann_ret = annualized_return(returns)
    mdd = max_drawdown(returns)
    out = {
        "ann_return": ann_ret,
        "ann_vol": annualized_vol(returns),
        "sharpe": sharpe_ratio(returns),
        "max_dd": mdd,
        "calmar": ann_ret / -mdd if mdd < 0 else float("nan"),
        "turnover": float("nan"),
    }
    if result is not None and len(returns):
        start, end = returns.index[0], returns.index[-1]
        traded = sum(d.turnover for d in result.decisions if start <= d.asof <= end)
        years = len(returns) / TRADING_DAYS
        out["turnover"] = traded / years if years else float("nan")
    return out


def _aligned_returns(runs: Mapping[str, Run], align: bool) -> dict[str, pd.Series]:
    if not runs:
        raise ValueError("no runs to compare")
    series = {k: (v.returns if isinstance(v, BacktestResult) else v).dropna()
              for k, v in runs.items()}
    if align:
        common = None
        for s in series.values():
            common = s.index if common is None else common.intersection(s.index)
        if common is None or common.empty:
            raise ValueError("the runs share no dates; pass align=False to compare as-is")
        series = {k: s.loc[common] for k, s in series.items()}
    return series


def yearly(runs: Mapping[str, Run], *, align: bool = True) -> pd.DataFrame:
    """Calendar-year metrics for each run, over the dates they share.

    Returns:
        A frame indexed by year with ``(metric, run)`` columns, so ``out["return"]`` is a
        years x runs table. ``return`` is the compounded return within the year -- not
        annualized, so a partial first or last year reads as year-to-date. ``vol`` and
        ``sharpe`` are annualized from that year's days; ``max_dd`` is the worst drawdown
        within the year. ``days`` counts the trading days, flagging partial years.
    """
    series = _aligned_returns(runs, align)
    tables: dict[str, pd.DataFrame] = {}
    for label, s in series.items():
        g = s.groupby(s.index.year)
        tables[label] = pd.DataFrame(
            {
                "return": g.apply(lambda r: float((1.0 + r).prod() - 1.0)),
                "vol": g.apply(annualized_vol),
                "sharpe": g.apply(sharpe_ratio),
                "max_dd": g.apply(max_drawdown),
                "days": g.size(),
            }
        )
    out = pd.concat(tables, axis=1).swaplevel(axis=1).sort_index(axis=1, level=0)
    out.index.name = "year"
    # Keep runs in the order given, not alphabetical.
    order = list(series)
    return out.reindex(columns=pd.MultiIndex.from_product(
        [["return", "vol", "sharpe", "max_dd", "days"], order]))


def compare(
    runs: Mapping[str, Run],
    split_date: pd.Timestamp | str | None = None,
    *,
    align: bool = True,
) -> pd.DataFrame:
    """One row per run, one column per metric -- optionally split into train and test.

    Args:
        runs: Mapping of label -> :class:`BacktestResult`, or a bare return series for a
            benchmark that is not a strategy (turnover is then NaN).
        split_date: If given, report metrics separately for the slices before and from this
            date, under a ``(period, metric)`` column MultiIndex with periods ``full``,
            ``train`` and ``test``.
        align: Trim every run to the dates all of them share. Leave on unless you mean to
            compare different windows.

    Returns:
        A frame indexed by label. ``turnover`` is annualized one-way turnover (1.0 = the
        whole book replaced once a year); ``calmar`` is annual return over max drawdown.
    """
    series = _aligned_returns(runs, align)

    def table(window: slice) -> pd.DataFrame:
        rows = {
            label: summarize(
                s.loc[window],
                runs[label] if isinstance(runs[label], BacktestResult) else None,
            )
            for label, s in series.items()
        }
        return pd.DataFrame(rows).T

    if split_date is None:
        return table(slice(None))
    split = pd.Timestamp(split_date)
    before = slice(None, split - pd.Timedelta(np.timedelta64(1, "ns")))
    return pd.concat(
        {"full": table(slice(None)), "train": table(before), "test": table(slice(split, None))},
        axis=1,
    )
