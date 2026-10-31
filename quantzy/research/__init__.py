"""Research tooling: signal and risk diagnostics, parameter search and run comparison.

Everything here is *exploratory* -- it answers "is this signal worth trading?" and "what
parameters?" before anything is committed to. None of it runs in production; production
calls :meth:`~quantzy.strategy.Strategy.decide` and nothing else in this package.
"""

from quantzy.research.compare import compare, summarize, yearly
from quantzy.research.distribution import (
    clip_scores,
    compare_tail_treatments,
    describe_scores,
    tail_treatments,
    winsorize_scores,
)
from quantzy.research.ic import (
    ICSummary,
    chunk_folds,
    ic_decay,
    ic_summary,
    information_coefficient,
    newey_west_t,
    pooled_time_series_ic,
    time_series_ic,
)
from quantzy.research.tuning import (
    RiskAversionFit,
    TuneResult,
    calibrate_risk_aversion,
    holdout_tune,
    label,
)
from quantzy.research.volfit import (
    evaluate_vol,
    forward_realized_vol,
    realized_returns,
    vol_forecast,
)

__all__ = [
    "information_coefficient",
    "ic_summary",
    "ic_decay",
    "ICSummary",
    "time_series_ic",
    "pooled_time_series_ic",
    "chunk_folds",
    "newey_west_t",
    "describe_scores",
    "clip_scores",
    "winsorize_scores",
    "tail_treatments",
    "compare_tail_treatments",
    "holdout_tune",
    "TuneResult",
    "label",
    "calibrate_risk_aversion",
    "RiskAversionFit",
    "compare",
    "summarize",
    "yearly",
    "vol_forecast",
    "forward_realized_vol",
    "evaluate_vol",
    "realized_returns",
]
