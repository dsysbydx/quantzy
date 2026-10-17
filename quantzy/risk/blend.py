"""Blends of risk models: one forecast mixing fast and slow memory."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.risk.base import RiskModel
from quantzy.risk.sample import SampleCovariance


@dataclass
class BlendedRisk:
    """A weighted average of several risk models' covariances.

    Averaging EWMAs with short and long halflives is a simple HAR-style forecast: the short
    one reacts to a volatility spike within days, the long one keeps the estimate from
    collapsing in a calm week. On QQQ, a 5/20/60 blend forecast 21-day volatility better than
    any single halflife, in and out of sample (``research/research_qqq_volmodel.py``).

    Args:
        models: The models to blend. They must cover the same tickers.
        weights: One weight per model, normalized to sum to one. ``None`` weights equally.
    """

    models: Sequence[RiskModel]
    weights: Sequence[float] | None = None

    def __post_init__(self) -> None:
        self.models = tuple(self.models)
        if not self.models:
            raise ValueError("BlendedRisk needs at least one model")
        w = [1.0] * len(self.models) if self.weights is None else [float(x) for x in self.weights]
        if len(w) != len(self.models):
            raise ValueError(f"got {len(w)} weights for {len(self.models)} models")
        if any(x < 0 for x in w) or sum(w) <= 0:
            raise ValueError(f"weights must be >= 0 with a positive sum, got {w}")
        self.weights = tuple(x / sum(w) for x in w)

    def covariance(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        bundle = as_bundle(data)
        covs = [m.covariance(bundle, asof) for m in self.models]
        out = covs[0] * self.weights[0]
        for w, c in zip(self.weights[1:], covs[1:], strict=True):
            out = out + c.reindex(index=out.index, columns=out.columns) * w
        return out

    @property
    def min_history(self) -> int:
        return max(m.min_history for m in self.models)


def ewma_blend(halflives: Sequence[float] = (5, 20, 60), lookback: int | None = 250,
               **kwargs) -> BlendedRisk:
    """An equal-weight blend of EWMA covariances, one per halflife.

    Extra keyword arguments (``lag``, ``annualize``, ...) go to every
    :class:`~quantzy.risk.sample.SampleCovariance`.
    """
    return BlendedRisk([SampleCovariance(halflife=h, lookback=lookback, **kwargs)
                        for h in halflives])
