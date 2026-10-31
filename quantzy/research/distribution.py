"""Score distribution and tail treatment: what to do with a signal's extreme values.

A unit score (the signal before it is multiplied by vol, in Sharpe units per unit of weight) is
only as good as its tails. A fat tail lets a handful of days set the position, and usually the
IC too. Before adopting a signal, look at its distribution, then test whether clipping or
winsorizing the tails makes its IC more *robust* -- steadier across held-out folds and periods --
not merely higher.

Two traps this module is written around:

- **Rank IC cannot see a tail treatment.** Clipping and winsorizing are monotone: they keep the
  order of the values and only tie the extremes, so a Spearman IC barely moves. Compare tail
  treatments with a Pearson IC (the default here), where magnitudes count.
- **Winsorizing must be causal.** Percentiles of the full sample use the future. The thresholds
  in :func:`winsorize_scores` come from each asset's own history through row ``t``.
"""

from __future__ import annotations

import pandas as pd

from quantzy.research.ic import chunk_folds, newey_west_t, pooled_time_series_ic

#: Five-year blocks for the by-period robustness check.
PERIOD_YEARS = 5


def describe_scores(scores: pd.DataFrame, by_asset: bool = False) -> pd.DataFrame:
    """Shape of a score panel: moments, quantiles and how much mass sits beyond 2 and 3.

    Read it before choosing a tail treatment. Excess kurtosis well above 0, a ``max_abs`` far
    beyond the 99th percentile or a ``share_gt3`` above ~0.3% (a normal's) say the tails are
    fatter than a normal's and worth testing. Moments are of the raw values; the shares beyond
    2 and 3 read as standard deviations only if the score is already in z units.

    Args:
        scores: Unit-score panel (dates x tickers); NaN cells are ignored.
        by_asset: One row per asset as well as the pooled row.
    """
    cols = {"pooled": scores.stack()}
    if by_asset:
        cols |= {t: scores[t] for t in scores.columns}
    rows = {}
    for name, x in cols.items():
        x = x.dropna().astype(float)
        if x.empty:
            continue
        q = x.quantile([0.01, 0.05, 0.5, 0.95, 0.99])
        rows[name] = {
            "n": len(x), "mean": x.mean(), "std": x.std(), "skew": x.skew(),
            "ex_kurt": x.kurt(), "q01": q[0.01], "q05": q[0.05], "q50": q[0.5],
            "q95": q[0.95], "q99": q[0.99], "max_abs": x.abs().max(),
            "share_gt2": (x.abs() > 2).mean(), "share_gt3": (x.abs() > 3).mean(),
        }
    return pd.DataFrame(rows).T


def clip_scores(scores: pd.DataFrame, k: float) -> pd.DataFrame:
    """Cap ``|score|`` at ``k``. A fixed cap needs no history, so it is causal by construction."""
    if k <= 0:
        raise ValueError(f"need k > 0, got {k}")
    return scores.clip(-k, k)


def winsorize_scores(scores: pd.DataFrame, q: float = 0.01, min_periods: int = 252) -> pd.DataFrame:
    """Clip each asset to its own trailing ``q`` and ``1 - q`` quantiles, through row ``t``.

    The thresholds are expanding-window quantiles of the asset's history up to and including
    the current row, so no row is clipped with information from after it. Rows before an asset
    has ``min_periods`` observations are left unchanged: there is no trustworthy threshold yet.

    Args:
        scores: Unit-score panel (dates x tickers).
        q: Tail probability per side, e.g. ``0.01`` for the 1st / 99th percentiles.
        min_periods: Observations an asset needs before it is winsorized.
    """
    if not 0 < q < 0.5:
        raise ValueError(f"need 0 < q < 0.5, got {q}")
    exp = scores.expanding(min_periods=min_periods)
    lo, hi = exp.quantile(q), exp.quantile(1 - q)
    return scores.clip(lo, hi, axis=None).where(lo.notna(), scores)


def tail_treatments(
    scores: pd.DataFrame,
    clips: tuple[float, ...] = (2.0, 3.0),
    winsor: tuple[float, ...] = (0.01,),
    min_periods: int = 252,
) -> dict[str, pd.DataFrame]:
    """The score as is, clipped at each ``clips`` level and winsorized at each ``winsor`` tail."""
    out = {"none": scores}
    out |= {f"clip +/-{k:g}": clip_scores(scores, k) for k in clips}
    out |= {f"winsor {100 * w:g}/{100 * (1 - w):g}": winsorize_scores(scores, w, min_periods)
            for w in winsor}
    return out


def compare_tail_treatments(
    variants: dict[str, pd.DataFrame],
    prices: pd.DataFrame,
    *,
    horizon: int = 1,
    method: str = "pearson",
    vol_window: int | None = 60,
    chunk: int = 47,
    nw_lags: int = 21,
) -> pd.DataFrame:
    """Pooled time-series IC of each variant, judged on robustness as well as level.

    ``variants`` maps a name to a score panel already lagged so row ``t`` is tradeable at ``t``
    (typically ``tail_treatments(unit).items()`` shifted by the signal's lag). Every variant is
    scored on the same cells: those where the first variant is live.

    Columns:
        altered: share of live cells the treatment changed.
        ic, t: IC on all dates and its Newey-West t.
        held_out, held_out_t: IC on each interleaved fold, standardized with the *other* fold's
            statistics, both directions concatenated -- the out-of-sample number.
        worst_period, periods_pos: lowest IC of the five-year blocks, and how many are positive.

    Prefer the variant with the best held-out t and worst period, not the highest ``ic``. A
    treatment that only helps on all dates helped by fitting the tails it can see.
    """
    base = next(iter(variants.values()))
    live = base.notna()
    folds = chunk_folds(base.index, chunk)
    period = pd.Series(base.index.year // PERIOD_YEARS * PERIOD_YEARS, index=base.index)
    lags = max(nw_lags, 2 * horizon)

    def ic(s: pd.DataFrame, fit: pd.Series | None = None) -> pd.Series:
        return pooled_time_series_ic(s.where(live), prices, horizon, method, vol_window, fit)

    rows = {}
    for name, s in variants.items():
        full = ic(s)
        held = pd.concat([ic(s, folds == 1).loc[lambda x: folds.reindex(x.index) == 0],
                          ic(s, folds == 0).loc[lambda x: folds.reindex(x.index) == 1]])
        by_period = full.groupby(period.reindex(full.index)).mean()
        changed = (s.where(live) - base.where(live)).abs() > 1e-12
        rows[name] = {
            "altered": changed.to_numpy().sum() / max(live.to_numpy().sum(), 1),
            "ic": full.mean(), "t": newey_west_t(full, lags),
            "held_out": held.mean(), "held_out_t": newey_west_t(held.sort_index(), lags),
            "worst_period": by_period.min(), "periods_pos": int((by_period > 0).sum()),
            "n_periods": len(by_period),
        }
    return pd.DataFrame(rows).T
