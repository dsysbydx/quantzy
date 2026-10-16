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


# --- time-series IC ----------------------------------------------------------------------------

def _ts_pairs(
    scores: pd.DataFrame, prices: pd.DataFrame, horizon: int, method: str,
    vol_window: int | None, fit: pd.Series | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Score and forward-return panels, aligned, masked to common cells and standardized per
    asset over time (ranked first for ``spearman``). With ``fit``, the mean and std come from
    the rows where it is True only, so a held-out fold is scored with train-fold stats."""
    if method not in ("spearman", "pearson"):
        raise ValueError(f"method must be 'spearman' or 'pearson', got {method!r}")
    fwd = forward_returns(prices, horizon).reindex(index=scores.index, columns=scores.columns)
    if vol_window is not None:
        # Ex-ante vol: returns through row t, which is known when row t's score is traded.
        vol = prices.pct_change().rolling(vol_window, min_periods=vol_window).std()
        vol = vol.reindex(index=scores.index, columns=scores.columns) * math.sqrt(horizon)
        fwd = fwd / vol.where(vol > EPS)
    mask = scores.notna() & fwd.notna()
    x, y = scores.where(mask), fwd.where(mask)
    if method == "spearman":
        x, y = x.rank(), y.rank()

    rows = slice(None) if fit is None else fit.reindex(scores.index, fill_value=False).to_numpy()

    def z(p: pd.DataFrame) -> pd.DataFrame:
        sd = p.loc[rows].std(ddof=0)
        return (p - p.loc[rows].mean()) / sd.where(sd > EPS)

    return z(x), z(y)


def time_series_ic(
    scores: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int = 21,
    method: str = "spearman",
    vol_window: int | None = None,
    fit: pd.Series | None = None,
) -> pd.Series:
    """Per asset, the correlation *over time* of its score with its own forward return.

    The cross-sectional :func:`information_coefficient` asks "does the score rank assets?";
    this asks "does an asset's score say when to own it?" -- the question for a timing signal
    such as trend, and the only one a single asset can answer.

    Args:
        scores: Score panel, lagged so row ``t`` is tradeable at ``t``. Cells to ignore (an
            asset before it existed, a warmup) must be NaN, not zero.
        prices: Price panel covering the same dates.
        horizon: Forward-return horizon in rows.
        method: ``"spearman"`` (rank IC, default) or ``"pearson"``.
        vol_window: If set, divide each forward return by the asset's trailing realized vol
            over this many rows. Without it, a Pearson IC is dominated by the most volatile
            stretches (crashes); with it, every period counts in risk units.
        fit: Boolean per date. If set, standardize with stats from these rows only, e.g. the
            train fold of :func:`chunk_folds`, so a test fold is not demeaned with its own mean.

    Returns:
        IC per asset, NaN where an asset has no paired observations.
    """
    zx, zy = _ts_pairs(scores, prices, horizon, method, vol_window, fit)
    return (zx * zy).mean().rename("ic")


def pooled_time_series_ic(
    scores: pd.DataFrame,
    prices: pd.DataFrame,
    horizon: int = 21,
    method: str = "spearman",
    vol_window: int | None = None,
    fit: pd.Series | None = None,
) -> pd.Series:
    """The time-series IC pooled across assets, as one contribution per date.

    Each asset's score and forward return are standardized over time (as in
    :func:`time_series_ic`); the series holds, per date, the average across assets of their
    product. Its mean is the pooled IC. Collapsing to one number per date before computing a
    t-stat clusters by date: assets that move together on the same day count once, not once
    per asset. Pass the result to :func:`ic_summary` with the same ``horizon`` to correct for
    overlapping forward windows too. ``fit`` is as in :func:`time_series_ic`.
    """
    zx, zy = _ts_pairs(scores, prices, horizon, method, vol_window, fit)
    return (zx * zy).mean(axis=1).dropna().rename("ic")


def chunk_folds(index: pd.Index, chunk: int, n_folds: int = 2) -> pd.Series:
    """Interleaved folds: consecutive blocks of ``chunk`` rows, dealt to folds in turn.

    A chronological split puts different regimes in train and test, so a signal that decays
    and a signal that simply met a new market look the same. Interleaving gives every fold
    every regime. A prime ``chunk`` (e.g. 47) keeps the blocks from lining up with weekly,
    monthly or quarterly cycles. Use with a short forward horizon: a label longer than
    ``chunk`` leaks across the block boundary.

    Returns:
        Fold number (``0 .. n_folds - 1``) per row of ``index``.
    """
    if chunk < 1 or n_folds < 2:
        raise ValueError(f"need chunk >= 1 and n_folds >= 2, got {chunk}, {n_folds}")
    return pd.Series((np.arange(len(index)) // chunk) % n_folds, index=index, name="fold")


def newey_west_t(series: pd.Series, lags: int = 10) -> float:
    """t-stat of the mean with Newey-West (Bartlett) standard errors.

    A daily IC series of a slow signal is serially correlated even with 1-day labels: the
    score barely moves, and volatility clusters. The plain t-stat then overstates
    significance; this one widens the error bar for autocorrelation up to ``lags`` rows.
    """
    x = series.dropna().to_numpy(dtype=float)
    n = len(x)
    if n < 2:
        return float("nan")
    e = x - x.mean()
    var = e @ e / n
    for k in range(1, min(lags, n - 1) + 1):
        var += 2 * (1 - k / (lags + 1)) * (e[k:] @ e[:-k]) / n
    return float(x.mean() / math.sqrt(var / n)) if var > EPS else float("nan")
