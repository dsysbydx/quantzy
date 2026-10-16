"""Saved strategies: the ones a production job may run.

The library (signals, risk, optimizers, research tools) is generic. A *saved strategy* is
one concrete, frozen choice made from it, in one module that defines:

* ``UNIVERSE`` and ``load()`` -- what it trades and the data it needs;
* ``PORTFOLIO`` -- a :class:`~quantzy.portfolio.PortfolioConfig`, the book-level settings;
* ``ALPHAS`` -- ``{name: (signal, weight)}``, the forecasts blended into ``mu``;
* ``build(alphas=None, portfolio=None)`` -- the :class:`~quantzy.strategy.Strategy`.

Lifecycle of an idea:

1. **Explore** in ``examples/research_*.py``: IC with interleaved folds, then a backtest.
2. **Test against a saved strategy.** Call ``build(alphas={**ALPHAS, "new": (sig, w)})``
   with ``PORTFOLIO`` unchanged, and judge the incremental Sharpe over ``build()``, not the
   book's total. Portfolio settings never move to make one alpha look better.
3. **Promote** by editing ``ALPHAS`` in the strategy module, in its own commit, and update
   the snapshot in its docstring. Retire an alpha the same way.

``REGISTRY`` maps a strategy's name to its module, for jobs that pick one by name.
"""

from __future__ import annotations

from types import ModuleType

from quantzy.strategies import core_multiasset

REGISTRY: dict[str, ModuleType] = {
    core_multiasset.NAME: core_multiasset,
}


def get(name: str) -> ModuleType:
    """The saved strategy module called ``name``."""
    try:
        return REGISTRY[name]
    except KeyError:
        raise KeyError(f"no saved strategy {name!r}; have {sorted(REGISTRY)}") from None


__all__ = ["REGISTRY", "get", "core_multiasset"]
