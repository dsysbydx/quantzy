"""Blend several alpha signals into a single expected-return vector (mu)."""

from __future__ import annotations

import pandas as pd

from quantzy.alpha.base import Alpha


class AlphaCombiner:
    """Model-weighted blend of alpha forecasts.

    This is the "model-weight" step: each underlying signal produces a z-scored forecast,
    and the combiner takes their weighted sum to form the expected-return vector mu that
    feeds the optimizer.

    Args:
        alphas: Mapping of name -> Alpha, or a sequence of Alphas (names taken from ``.name``).
        weights: Optional mapping name -> weight. Missing names default to 1.0.
        scale: Multiplies the combined score (interpret mu in expected-return units).
    """

    def __init__(
        self,
        alphas: dict[str, Alpha] | list[Alpha],
        weights: dict[str, float] | None = None,
        scale: float = 1.0,
    ) -> None:
        if isinstance(alphas, list):
            alphas = {a.name: a for a in alphas}
        self.alphas = alphas
        self.weights = weights or {name: 1.0 for name in alphas}
        self.scale = scale

    def forecast(self, prices: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.Series:
        combined: pd.Series | None = None
        for name, alpha in self.alphas.items():
            w = self.weights.get(name, 1.0)
            contribution = alpha.forecast(prices, asof) * w
            if combined is None:
                combined = contribution
            else:
                combined = combined.add(contribution, fill_value=0.0)
        if combined is None:
            raise ValueError("AlphaCombiner has no alphas")
        return combined * self.scale
