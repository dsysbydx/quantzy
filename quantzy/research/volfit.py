"""Is the volatility forecast any good? Score it against the volatility that followed.

The optimizer sizes positions as ``mu / (2 * lambda * sigma^2)``, so an error in ``sigma`` is
an error in size. A model that is too slow to see a crash coming holds full size into it.
Testing a risk model only through backtest P&L mixes that error up with everything else in
the strategy, so this module scores the forecast on its own first.

Every forecast is the risk model's annualized volatility as of ``t``, which by the risk
model's own lag rests on returns through ``t-1``. It is scored against the returns from
``t`` onward, the ones the book sized at ``t`` actually earns.

Metrics, per model:

- ``qlike``: the mean of ``log F + RV / F``, with ``F`` the forecast variance and ``RV`` the
  realized variance. This is the standard loss for ranking volatility forecasts (Patton,
  2011). It stays consistent when the realized variance is itself noisy, and it punishes
  under-forecasting more than over-forecasting. Lower is better. Only differences between
  models mean anything.
- ``bias``: mean realized variance over mean forecast variance. 1 is unbiased; above 1
  means the model under-forecasts risk.
- ``mz_slope``, ``mz_r2``: from the Mincer-Zarnowitz regression of realized on forecast
  variance. A slope of 1 means the forecast moves one-for-one with what follows. ``R^2`` is
  how much of the realized variance it explains.
- ``z_std``: standard deviation of ``R / predicted vol``, where ``R`` is the cumulative
  ``horizon``-day return. 1 means well calibrated.
- ``left_tail``: share of ``z < -2.33``, which a calibrated normal model puts at 1%. This is
  the number that matters for drawdowns: how often a loss was bigger than the model
  allowed for.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.returns import to_returns
from quantzy.risk.base import RiskModel

LEFT_TAIL_Z = -2.326  # 1% quantile of the standard normal


def vol_forecast(
    model: RiskModel,
    data: DataBundle | pd.DataFrame,
    asset: str,
    *,
    start: pd.Timestamp | str | None = None,
    step: int = 1,
) -> pd.Series:
    """The model's annualized volatility forecast for ``asset`` at each date.

    Args:
        model: An *annualized* risk model.
        data: Bundle or price panel.
        asset: Ticker whose volatility to forecast.
        start: First forecast date. Defaults to the first date the model has enough history
            for.
        step: Forecast every ``step``-th date, to save time on slow models.
    """
    bundle = as_bundle(data)
    index = bundle.index
    first = index[model.min_history] if start is None else pd.Timestamp(start)
    dates = index[index >= first][::step]
    out = {d: float(np.sqrt(model.covariance(bundle, d).loc[asset, asset])) for d in dates}
    return pd.Series(out, name=asset)


def forward_realized_vol(returns: pd.Series, horizon: int) -> pd.Series:
    """Annualized realized volatility over ``t .. t + horizon - 1``, indexed at ``t``.

    Uses the zero-mean convention, the square root of the mean squared return, as is standard
    for realized volatility. Over short horizons the mean is noise.
    """
    sq = returns.pow(2).rolling(horizon).mean().shift(-(horizon - 1))
    return np.sqrt(sq * TRADING_DAYS)


def _forward_return(returns: pd.Series, horizon: int) -> pd.Series:
    growth = np.log1p(returns).rolling(horizon).sum().shift(-(horizon - 1))
    return np.expm1(growth)


def _score(f: pd.Series, rv: pd.Series, fwd: pd.Series, horizon: int) -> dict[str, float]:
    fvar, rvar = f.pow(2), rv.pow(2)
    if fvar.std() > 0:
        slope = float(np.cov(fvar, rvar)[0, 1] / fvar.var())
        r2 = float(np.corrcoef(fvar, rvar)[0, 1] ** 2)
    else:  # a constant forecast has no slope to fit
        slope = r2 = float("nan")
    z = fwd / (f * np.sqrt(horizon / TRADING_DAYS))
    return {
        "n": float(len(f)),
        "avg_forecast": float(f.mean()),
        "avg_realized": float(np.sqrt(rvar.mean())),
        "qlike": float((np.log(fvar) + rvar / fvar).mean()),
        "bias": float(rvar.mean() / fvar.mean()),
        "mz_slope": slope,
        "mz_r2": r2,
        "z_std": float(z.std()),
        "left_tail": float((z < LEFT_TAIL_Z).mean()),
    }


def evaluate_vol(
    forecasts: Mapping[str, pd.Series],
    returns: pd.Series,
    *,
    horizon: int = 21,
    by: pd.Series | None = None,
) -> pd.DataFrame:
    """Score several volatility forecasts of one asset over the dates they share.

    Args:
        forecasts: Label -> annualized forecast series, as from :func:`vol_forecast`.
        returns: The asset's daily simple returns.
        horizon: Days ahead the forecast is judged over. Match it to how long a sized position
            is typically held.
        by: Optional label per date (e.g. "downtrend" / "uptrend") to score each group
            separately. A model can be accurate on average and still wrong exactly where it
            matters.

    Returns:
        One row per model, or per ``(model, group)`` when ``by`` is given. Columns are
        described in the module docstring.
    """
    if not forecasts:
        raise ValueError("no forecasts to evaluate")
    rv = forward_realized_vol(returns, horizon)
    fwd = _forward_return(returns, horizon)
    frame = pd.DataFrame({k: v for k, v in forecasts.items()})
    frame = frame[frame.gt(0).all(axis=1)]
    common = frame.index.intersection(rv.dropna().index)
    if by is not None:
        common = common.intersection(by.dropna().index)
    if common.empty:
        raise ValueError("the forecasts and the realized volatility share no dates")

    rows: dict = {}
    for label in forecasts:
        f = frame.loc[common, label]
        if by is None:
            rows[label] = _score(f, rv.loc[common], fwd.loc[common], horizon)
            continue
        groups = by.loc[common]
        for g in pd.unique(groups):
            sel = common[groups == g]
            rows[(label, g)] = _score(f.loc[sel], rv.loc[sel], fwd.loc[sel], horizon)
    out = pd.DataFrame(rows).T
    out["n"] = out["n"].astype(int)
    return out


def realized_returns(data: DataBundle | pd.DataFrame, asset: str) -> pd.Series:
    """Daily simple returns for ``asset``, the input :func:`evaluate_vol` scores against."""
    return to_returns(as_bundle(data).prices)[asset]
