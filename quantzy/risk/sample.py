"""Sample covariance, optionally exponentially weighted."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from quantzy.config import TRADING_DAYS


@dataclass(slots=True)
class SampleCovariance:
    """Empirical covariance of returns, annualized.

    Args:
        annualize: If True, scale the per-period covariance by ``trading_days``.
        trading_days: Periods per year used for annualization.
        halflife: If set, use an exponentially-weighted covariance with this halflife
            (in periods), giving more weight to recent observations.
    """

    annualize: bool = True
    trading_days: int = TRADING_DAYS
    halflife: float | None = None

    def covariance(self, returns: pd.DataFrame, asof: pd.Timestamp | None = None) -> pd.DataFrame:
        rets = returns if asof is None else returns.loc[:asof]
        rets = rets.dropna(how="any")
        if len(rets) < 2:
            raise ValueError("need at least 2 return observations to estimate covariance")

        if self.halflife is not None:
            cov = rets.ewm(halflife=self.halflife).cov(bias=True)
            # ewm.cov returns a stacked (date, asset) x asset frame; take the last block.
            last_date = rets.index[-1]
            cov = cov.loc[last_date]
        else:
            cov = rets.cov()

        if self.annualize:
            cov = cov * self.trading_days
        return _symmetrize(cov)


def _symmetrize(cov: pd.DataFrame) -> pd.DataFrame:
    arr = cov.to_numpy()
    arr = 0.5 * (arr + arr.T)
    return pd.DataFrame(arr, index=cov.index, columns=cov.columns)
