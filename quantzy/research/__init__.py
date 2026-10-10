"""Research tooling: parameter search.

Everything here is *exploratory* -- it answers "is this signal worth trading?" and "what
parameters?" before anything is committed to. None of it runs in production; production
calls :meth:`~quantzy.strategy.Strategy.decide` and nothing else in this package.
"""

from quantzy.research.tuning import TuneResult, holdout_tune

__all__ = [
    "holdout_tune",
    "TuneResult",
]
