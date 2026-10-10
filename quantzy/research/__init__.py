"""Research tooling: signal diagnostics and parameter search.

Everything here is *exploratory* -- it answers "is this signal worth trading?" and "what
parameters?" before anything is committed to. None of it runs in production; production
calls :meth:`~quantzy.strategy.Strategy.decide` and nothing else in this package.
"""

from quantzy.research.ic import ICSummary, ic_decay, ic_summary, information_coefficient
from quantzy.research.tuning import TuneResult, holdout_tune

__all__ = [
    "information_coefficient",
    "ic_summary",
    "ic_decay",
    "ICSummary",
    "holdout_tune",
    "TuneResult",
]
