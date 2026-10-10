"""The Signal base class: one implementation, three ways to read it.

A signal author writes :meth:`Signal.raw` and nothing else. Normalization, the execution lag,
the as-of view used by the backtest engine, and the single-row view used by the production job
are all provided here, so every signal behaves identically and research cannot drift from prod.

Three read paths, one computation:

- :meth:`Signal.scores` -- the whole ``(dates x tickers)`` panel, for research and diagnostics.
- :meth:`Signal.forecast` -- one row as of a date, satisfying the ``Alpha`` protocol so the
  existing :class:`~quantzy.strategy.Strategy` blends it into mu.
- :meth:`Signal.latest` -- today's row, for the daily production job.

``forecast`` and ``latest`` are slices of the panel, never a separate calculation.

**The invariant that licenses this.** Slicing a full-history panel at ``asof`` equals computing
as of that date *only if* every operation in ``raw`` is backward-looking. ``ewm``, ``rolling``
and per-row cross-sectional operations are; a centered window or a full-sample mean is not.
``tests/test_signals.py`` asserts it for every signal rather than trusting the author.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from typing import Any, ClassVar

import pandas as pd

from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.signals.normalize import Normalizer, ZScore


@dataclass(kw_only=True)
class Signal(ABC):
    """A cross-sectional forecast in z-score units.

    Subclasses implement :meth:`raw` and override :attr:`min_history`. Configuration fields go
    on the subclass positionally; ``normalizer`` and ``lag`` stay keyword-only here so they can
    be overridden without disturbing a subclass's own argument order.

    Args:
        normalizer: Raw-panel -> z-unit-panel map. Rank-spread is an option, not a default.
        lag: Rows between observing the data and being able to trade on it. ``1`` means a
            signal computed from the close of day ``t`` sizes the book that earns day ``t+1``.
            ``0`` asserts you can trade the close you just measured.
    """

    #: Display name. The key a :class:`~quantzy.strategy.Strategy` files it under wins.
    name: ClassVar[str] = "signal"

    normalizer: Normalizer = field(default_factory=ZScore)
    lag: int = 1
    _cache: tuple[tuple, pd.DataFrame] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    @abstractmethod
    def raw(self, data: DataBundle) -> pd.DataFrame:
        """Return the raw signal panel (dates x tickers) in whatever units are natural.

        Must be causal: row ``t`` may not depend on any row after ``t``. NaN means "no view"
        for that asset on that date and is handled by the normalizer.
        """

    @property
    def min_history(self) -> int:
        """Rows of history needed before the last row is trustworthy.

        Used by :meth:`latest` to size the production warmup window. Override it -- the default
        of ``0`` claims the signal has no memory, which is almost never true.
        """
        return 0

    def scores(self, data: DataBundle | pd.DataFrame) -> pd.DataFrame:
        """The full z-unit score panel, lagged and ready to trade on."""
        bundle = as_bundle(data)
        key = bundle.fingerprint()
        if self._cache is not None and self._cache[0] == key:
            return self._cache[1]
        panel = self._compute(bundle)
        self._cache = (key, panel)
        return panel

    def forecast(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.Series:
        """One score row as of ``asof`` -- the ``Alpha`` protocol the optimizer engine calls.

        The panel is memoized on the frame's identity, so a backtest that calls this at every
        rebalance computes the panel once rather than once per rebalance.
        """
        panel = self.scores(data)
        if asof is None:
            return panel.iloc[-1]
        window = panel.loc[:asof]
        if window.empty:
            raise ValueError(f"no signal history at or before {asof}")
        return window.iloc[-1]

    def latest(self, data: DataBundle | pd.DataFrame) -> pd.Series:
        """Today's score row -- the production entry point.

        Pass only ``min_history + lag + 1`` rows or more; a short window silently returns a
        converged-looking number that is wrong, so this checks rather than trusting the caller.
        """
        bundle = as_bundle(data)
        needed = self.min_history + self.lag + 1
        if len(bundle.index) < needed:
            raise ValueError(
                f"{type(self).__name__} needs >= {needed} rows to produce a trustworthy last "
                f"row (min_history={self.min_history}, lag={self.lag}), got {len(bundle.index)}"
            )
        return self.scores(bundle).iloc[-1]

    def params(self) -> dict[str, Any]:
        """Configuration fields, for sweep tables and run labelling.

        Raises:
            TypeError: If the subclass declares configuration attributes but is not itself a
                ``@dataclass``, so they never became fields. That failure is otherwise
                silent and costly: the parameter vanishes from sweep tables, and the
                causality suite -- which rebuilds a signal from ``params()`` -- would test a
                default-valued object while appearing to pass.
        """
        self._check_is_dataclass()
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.init and f.name not in ("normalizer", "lag")
        }

    def _check_is_dataclass(self) -> None:
        known = {f.name for f in fields(self)}
        own = getattr(type(self), "__annotations__", {})
        lost = [
            name
            for name, ann in own.items()
            if not name.startswith("_")
            and name not in known
            and "ClassVar" not in str(ann)
        ]
        if lost:
            raise TypeError(
                f"{type(self).__name__} declares {lost} but is not a @dataclass, so they are "
                f"not fields and would be dropped from params(). Decorate the class with "
                f"@dataclass, and annotate `name` as ClassVar[str]."
            )

    def _compute(self, bundle: DataBundle) -> pd.DataFrame:
        raw = self.raw(bundle)
        if not isinstance(raw, pd.DataFrame):
            raise TypeError(f"{type(self).__name__}.raw must return a DataFrame, got {type(raw)}")
        scores = self.normalizer(raw)
        if self.lag:
            scores = scores.shift(self.lag)
        return scores.fillna(0.0)
