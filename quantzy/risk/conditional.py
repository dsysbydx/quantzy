"""Risk models that condition another model's estimate on the market's state.

A trailing covariance only knows how volatile the recent past was. Some states carry
information about the volatility *ahead* that the trailing window has not caught up with
yet. The classic example is a market sitting below its long moving average: risk there tends
to stay high and losses tend to cluster. :class:`StateScaledRisk` lets a signal describing
that state scale the base model's volatilities.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.risk.base import RiskModel
from quantzy.signals.base import Signal


@dataclass
class StateScaledRisk:
    """Scale a base model's volatilities up when a state signal is negative.

    Each asset's volatility is multiplied by ``1 + downside * max(-s, 0)``, where ``s`` is the
    state signal's score for that asset as of the decision. Correlations are left as the base
    model has them. A positive score leaves the base estimate alone, so with a score bounded
    to (-1, 1) the volatility is scaled by at most ``1 + downside``.

    The state signal carries its own lag (a :class:`~quantzy.signals.base.Signal` defaults to
    one row), so the scaling at ``asof`` rests on the same information as the base model's
    lagged estimate.

    Args:
        base: The model whose covariance is scaled.
        state: Signal whose score describes each asset's state, e.g.
            :class:`~quantzy.signals.trend.TrendStrength` with an ``Identity`` normalizer.
        downside: Extra volatility, as a fraction, at a score of -1. ``0`` reproduces ``base``.
        assets: Restrict the scaling to these tickers. ``None`` scales every asset the state
            signal scores.
    """

    base: RiskModel
    state: Signal
    downside: float = 0.5
    assets: Sequence[str] | None = None
    _cache: tuple[tuple, pd.DataFrame] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if self.downside < 0:
            raise ValueError(f"downside must be >= 0, got {self.downside}")

    def multiplier(self, data: DataBundle | pd.DataFrame,
                   asof: pd.Timestamp | None = None) -> pd.Series:
        """The volatility multiplier per asset as of ``asof``."""
        panel = self._multipliers(as_bundle(data))
        # A row lookup, not panel.loc[:asof]: this runs once per decision, and slicing a frame
        # per call cost more than the rest of the risk model.
        pos = len(panel) - 1 if asof is None else panel.index.searchsorted(asof, "right") - 1
        if pos < 0:
            raise ValueError(f"no state history at or before {pd.Timestamp(asof).date()}")
        return panel.iloc[pos]

    def _multipliers(self, bundle: DataBundle) -> pd.DataFrame:
        """The multiplier for every date, computed once per bundle like a signal panel."""
        key = bundle.fingerprint()
        if self._cache is not None and self._cache[0] == key:
            return self._cache[1]
        s = self.state.scores(bundle).fillna(0.0)
        if self.assets is not None:
            s = s * s.columns.isin(list(self.assets))
        panel = 1.0 + self.downside * (-s).clip(lower=0.0)
        self._cache = (key, panel)
        return panel

    def covariance(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        bundle = as_bundle(data)
        cov = self.base.covariance(bundle, asof)
        m = self.multiplier(bundle, asof).reindex(cov.index).fillna(1.0).to_numpy()
        return cov * np.outer(m, m)

    @property
    def min_history(self) -> int:
        return max(self.base.min_history, self.state.min_history + self.state.lag + 1)
