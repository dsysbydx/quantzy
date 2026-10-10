"""Normalizers: turn a raw signal panel into z-score units.

A normalizer maps a ``(dates x tickers)`` panel to a panel of the same shape in z-units,
row by row. Which one to use is a per-signal choice -- rank-spread is an option, not a
requirement -- so each signal carries its own.

Two conventions hold across all of them:

**NaN means "no view".** Missing entries are excluded from the mean/standard-deviation of
their row, then emitted as ``0.0`` (neutral). Filling with zero *before* standardizing would
drag the cross-sectional mean toward zero and bias every other name in the row.

**Thin rows are skipped.** A row with fewer than ``min_names`` observations produces zeros
rather than a standardization computed from two or three points.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd
from scipy.stats import norm

EPS = 1e-12


@runtime_checkable
class Normalizer(Protocol):
    """Maps a raw signal panel to a z-unit panel of the same shape."""

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame: ...


class DegenerateCrossSection(ValueError):
    """Raised when a row has real coverage but no cross-sectional dispersion.

    The usual cause is a macro or timing signal -- yield-curve slope, VIX level -- that takes
    the same value for every asset. Cross-sectional demeaning annihilates it: every score
    becomes exactly 0.0, the strategy holds nothing, and no error is raised anywhere. Such a
    signal needs :class:`TimeSeriesZ`, or a timing framework rather than a cross-sectional one.
    """


@dataclass(slots=True)
class ZScore:
    """Cross-sectional z-score, the default.

    Winsorization runs as standardize -> clip -> re-standardize -> clip. The re-standardization
    is the point: one violent outlier inflates the row's standard deviation, which squashes every
    other name toward zero, so re-measuring dispersion *after* clipping restores the spread among
    the names actually being compared. The final clip is also the point -- without it
    re-standardizing simply re-inflates the outlier it just clipped.

    Note that ``|z| <= sqrt(n-1)`` for ``n`` assets, so the default threshold does nothing below
    ten names. On a small universe winsorization is not what is protecting you.

    Args:
        winsorize: Clip to +/-k standard deviations. ``None`` disables clipping.
        min_names: Rows with fewer observations emit zeros.
        on_degenerate: ``"raise"`` (default) or ``"zero"`` for rows with no dispersion.
    """

    winsorize: float | None = 3.0
    min_names: int = 3
    on_degenerate: str = "raise"

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        x = panel.astype(float)
        ok = x.notna().sum(axis=1) >= self.min_names
        z = _standardize(x, ok, self.on_degenerate)
        if self.winsorize is not None:
            k = self.winsorize
            z = _standardize(z.clip(-k, k), ok, self.on_degenerate).clip(-k, k)
        return _neutralize_missing(z, ok)


@dataclass(slots=True)
class RankNormal:
    """Rank across assets, then map to a normal score -- the "rank-spread" option.

    Ranks are mapped to ``(rank - 0.5) / n`` and pushed through the inverse normal CDF. This
    discards magnitude entirely, which is what you want when a signal's tails are unreliable
    but its ordering is not.
    """

    min_names: int = 3

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        x = panel.astype(float)
        counts = x.notna().sum(axis=1)
        ok = counts >= self.min_names
        ranks = x.rank(axis=1, method="average")
        uniform = ranks.sub(0.5).div(counts.where(counts > 0), axis=0)
        z = pd.DataFrame(norm.ppf(uniform.to_numpy()), index=x.index, columns=x.columns)
        return _neutralize_missing(z, ok)


@dataclass(slots=True)
class Demean:
    """Subtract the cross-sectional mean without rescaling."""

    min_names: int = 3

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        x = panel.astype(float)
        ok = x.notna().sum(axis=1) >= self.min_names
        return _neutralize_missing(x.sub(x.mean(axis=1), axis=0), ok)


@dataclass(slots=True)
class Identity:
    """Pass through -- for a signal already expressed in z-units."""

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        return panel.astype(float).fillna(0.0)


@dataclass(slots=True)
class TimeSeriesZ:
    """Standardize each asset against its own trailing history, not against its peers.

    The right choice for timing signals and for universes too small for a meaningful
    cross-section. Uses a trailing window, so it stays causal.
    """

    window: int = 252
    min_periods: int | None = None

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        x = panel.astype(float)
        minp = self.min_periods or self.window
        roll = x.rolling(self.window, min_periods=minp)
        std = roll.std(ddof=0)
        z = (x - roll.mean()) / std.where(std > EPS)
        return z.fillna(0.0)


@dataclass(slots=True)
class GroupNeutral:
    """Demean within each group (sector, industry, region, asset class).

    Rarely optional in practice: a raw value signal is mostly a sector bet and raw momentum is
    mostly a beta bet, so without neutralization the P&L is dominated by exposures that were
    never the idea being tested. Chain it *before* :class:`ZScore`.
    """

    groups: Mapping[str, str]

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        g = pd.Series(dict(self.groups)).reindex(panel.columns)
        if g.isna().any():
            missing = list(g.index[g.isna()])
            raise ValueError(f"no group assigned for {missing}")
        demeaned = panel.astype(float).T.groupby(g, observed=True).transform(
            lambda block: block - block.mean()
        )
        return demeaned.T


@dataclass(slots=True)
class BetaNeutral:
    """Project out a cross-sectional exposure (typically market beta).

    Removes the component of the score parallel to ``betas``: ``z - b * (b'z / b'b)``.
    """

    betas: pd.Series | pd.DataFrame

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        x = panel.astype(float)
        if isinstance(self.betas, pd.Series):
            b = self.betas.reindex(x.columns).astype(float)
            b_panel = pd.DataFrame(
                np.tile(b.to_numpy(), (len(x), 1)), index=x.index, columns=x.columns
            )
        else:
            b_panel = self.betas.reindex(index=x.index, columns=x.columns).astype(float)
        mask = x.notna() & b_panel.notna()
        xf, bf = x.where(mask), b_panel.where(mask)
        denom = (bf * bf).sum(axis=1)
        loading = (bf * xf).sum(axis=1) / denom.where(denom > EPS)
        return (xf - bf.mul(loading, axis=0)).fillna(0.0)


@dataclass(slots=True)
class FromReturns:
    """Escape hatch for a signal that is natively a return forecast.

    Carry, expected dividend yield and analyst targets already carry return units. Dividing by
    ``vol`` moves them into z-units so they can be blended with everything else, keeping the
    return/z boundary in exactly one place.
    """

    vol: pd.DataFrame

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        x = panel.astype(float)
        v = self.vol.reindex(index=x.index, columns=x.columns).astype(float)
        return (x / v.where(v > EPS)).fillna(0.0)


@dataclass(slots=True)
class Chain:
    """Apply normalizers in order, e.g. ``Chain(GroupNeutral(sectors), ZScore())``."""

    steps: Sequence[Normalizer] = field(default_factory=tuple)

    def __init__(self, *steps: Normalizer) -> None:
        self.steps = tuple(steps)

    def __call__(self, panel: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            panel = step(panel)
        return panel


def _standardize(x: pd.DataFrame, ok: pd.Series, on_degenerate: str) -> pd.DataFrame:
    """Row-wise ``(x - mean) / std``, skipping NaN, guarding flat rows."""
    std = x.std(axis=1, ddof=0)
    flat = ok & (std.fillna(0.0) <= EPS)
    if flat.any():
        if on_degenerate == "raise":
            first = x.index[flat][0]
            raise DegenerateCrossSection(
                f"no cross-sectional dispersion on {first.date()} across "
                f"{int(x.notna().sum(axis=1).loc[first])} assets: every value is "
                f"{x.loc[first].dropna().iloc[0]!r}. A signal that is identical across assets "
                "is erased by cross-sectional demeaning -- use TimeSeriesZ, or pass "
                "on_degenerate='zero' if a flat row is expected here."
            )
        if on_degenerate != "zero":
            raise ValueError(f"on_degenerate must be 'raise' or 'zero', got {on_degenerate!r}")
    return x.sub(x.mean(axis=1), axis=0).div(std.where(std > EPS), axis=0)


def _neutralize_missing(z: pd.DataFrame, ok: pd.Series) -> pd.DataFrame:
    """Thin rows and no-view cells become 0.0 -- neutral, not a position."""
    return z.where(ok, 0.0).fillna(0.0)
