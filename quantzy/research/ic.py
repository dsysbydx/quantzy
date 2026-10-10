"""Information coefficient: does the score predict the return that follows?

The IC is the cross-sectional correlation between today's tradeable score and the forward
return, measured per date. It judges a signal before any portfolio is built, so it is not
contaminated by sizing rules, the risk model or the optimizer's risk aversion.

Rank IC (Spearman) is the default. A signal's ordering is usually more reliable than its
magnitudes, and one extreme value cannot dominate the statistic.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from quantzy.returns import forward_returns

EPS = 1e-12


def information_coefficient(
    scores: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int = 1,
    method: str = "spearman",
    min_names: int = 5,
) -> pd.Series:
    """Per-date cross-sectional correlation of score with forward return.

    Args:
        scores: Z-unit score panel, already lagged so row ``t`` is tradeable at ``t``.
        prices: Price panel covering the same dates.
        horizon: Forward-return horizon in rows.
        method: ``"spearman"`` (rank IC, default) or ``"pearson"``.
        min_names: Dates with fewer paired observations return NaN. Must not exceed the
            universe size -- a threshold above it discards every date.

    Returns:
        Series of IC by date; NaN where the cross-section was too thin or had no dispersion.
    """
    if min_names > scores.shape[1]:
        raise ValueError(
            f"min_names={min_names} exceeds the {scores.shape[1]}-asset universe, so every "
            f"date would be discarded and the IC would be silently empty. Lower min_names "
            f"(3 is the floor for a meaningful cross-section) or widen the universe."
        )
    fwd = forward_returns(prices, horizon).reindex(index=scores.index, columns=scores.columns)
    return _rowwise_corr(scores, fwd, method=method, min_names=min_names)


@dataclass(slots=True)
class ICSummary:
    """Aggregate IC statistics. ``horizon`` rows of overlap are accounted for in ``t_stat``."""

    horizon: int
    mean_ic: float
    ic_std: float
    ic_ir: float
    t_stat: float
    hit_rate: float
    n_obs: int

    def as_dict(self) -> dict[str, float]:
        return {
            "horizon": self.horizon,
            "mean_ic": self.mean_ic,
            "ic_std": self.ic_std,
            "ic_ir": self.ic_ir,
            "t_stat": self.t_stat,
            "hit_rate": self.hit_rate,
            "n_obs": self.n_obs,
        }


def ic_summary(ic: pd.Series, horizon: int = 1) -> ICSummary:
    """Collapse an IC series to mean, IR, t-stat and hit rate.

    The t-stat divides the observation count by ``horizon``. With daily dates and a 21-day
    forward return, consecutive ICs share 20 of 21 days of return, so treating them as
    independent overstates significance by roughly ``sqrt(21)`` -- enough to turn noise into a
    "highly significant" signal. This is the single most common way a backtest lies.
    """
    ic = ic.dropna()
    n = len(ic)
    if n == 0:
        return ICSummary(horizon, float("nan"), float("nan"), float("nan"), float("nan"),
                         float("nan"), 0)
    mean = float(ic.mean())
    sd = float(ic.std(ddof=1)) if n > 1 else float("nan")
    ir = mean / sd if sd and sd > EPS else float("nan")
    effective_n = max(n / max(horizon, 1), 1.0)
    t_stat = ir * math.sqrt(effective_n) if np.isfinite(ir) else float("nan")
    return ICSummary(
        horizon=horizon,
        mean_ic=mean,
        ic_std=sd,
        ic_ir=ir,
        t_stat=t_stat,
        hit_rate=float((ic > 0).mean()),
        n_obs=n,
    )


def ic_decay(
    scores: pd.DataFrame,
    prices: pd.DataFrame,
    horizons: tuple[int, ...] = (1, 5, 21, 63, 126),
    method: str = "spearman",
    min_names: int = 5,
) -> pd.DataFrame:
    """IC statistics across forward horizons -- how fast the edge evaporates.

    A real signal decays monotonically: strongest at short horizons, fading as the forecast
    window lengthens. A flat profile usually means the score is a slow-moving proxy for
    something structural; an IC that *rises* with horizon deserves suspicion before celebration.
    """
    rows = [
        ic_summary(
            information_coefficient(scores, prices, h, method=method, min_names=min_names), h
        ).as_dict()
        for h in horizons
    ]
    return pd.DataFrame(rows).set_index("horizon")


def _rowwise_corr(
    a: pd.DataFrame, b: pd.DataFrame, method: str, min_names: int
) -> pd.Series:
    """Correlation within each row, over cells where both frames are present."""
    if method not in ("spearman", "pearson"):
        raise ValueError(f"method must be 'spearman' or 'pearson', got {method!r}")
    mask = a.notna() & b.notna()
    x, y = a.where(mask), b.where(mask)
    if method == "spearman":
        x, y = x.rank(axis=1), y.rank(axis=1)
    xc = x.sub(x.mean(axis=1), axis=0)
    yc = y.sub(y.mean(axis=1), axis=0)
    denom = np.sqrt((xc**2).sum(axis=1) * (yc**2).sum(axis=1))
    corr = (xc * yc).sum(axis=1) / denom.where(denom > EPS)
    return corr.where(mask.sum(axis=1) >= min_names).rename("ic")
