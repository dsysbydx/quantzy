"""Sample covariance, optionally exponentially weighted."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy.signal import fftconvolve

from quantzy.data.bundle import DataBundle, as_bundle
from quantzy.risk.base import PanelRiskModel, symmetrize

#: Largest ``dates x assets x assets`` panel the EWM fast path will hold in memory.
MAX_PANEL_CELLS = 50_000_000


@dataclass(kw_only=True)
class SampleCovariance(PanelRiskModel):
    """Empirical covariance of returns.

    Args:
        halflife: If set, use an exponentially-weighted covariance with this halflife (in
            periods), giving more weight to recent observations.

    **Fast path.** With a halflife, a daily backtest would otherwise recompute the same
    weighted sums over the window at every date. Instead the windowed sums are computed for
    every date at once, by convolving the returns with the window's weights, and
    :meth:`covariance` looks up the date it is asked for. The weights, window and formula are
    the ones :meth:`estimate` uses. The convolution runs through an FFT, so the result matches
    the per-date path to about 1e-10 relative, not bit for bit.
    A date whose window contains missing returns, or a universe too large to hold a panel
    for, takes the per-date path instead.
    """

    halflife: float | None = None
    _ewm_cache: tuple[tuple, np.ndarray, np.ndarray] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def estimate(self, returns: pd.DataFrame) -> pd.DataFrame:
        if self.halflife is not None:
            cov = returns.ewm(halflife=self.halflife).cov(bias=True)
            # ewm.cov returns a stacked (date, asset) x asset frame; take the last block.
            cov = cov.loc[returns.index[-1]]
        else:
            cov = returns.cov()
        return symmetrize(cov)

    def covariance(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        if self.halflife is None:
            return super().covariance(data, asof)
        bundle = as_bundle(data)
        rets = self._returns(bundle)
        panel, ok = self._ewm_panel(bundle, rets)
        if panel is None:
            return super().covariance(bundle, asof)
        last = len(rets) - 1 if asof is None else rets.index.searchsorted(asof, "right") - 1
        end = last - self.lag  # the window's last row, after dropping the lagged rows
        if end < 0 or not ok[end]:
            return super().covariance(bundle, asof)
        cov = panel[end] * (self.trading_days if self.annualize else 1.0)
        return pd.DataFrame(cov, index=rets.columns, columns=rets.columns)

    def _ewm_panel(self, bundle: DataBundle,
                   rets: pd.DataFrame) -> tuple[np.ndarray | None, np.ndarray]:
        """``(panel, ok)``: the windowed EWM covariance ending at each row, and whether the
        per-date path would see exactly that window.

        The per-date path drops rows with any missing return, then weights row ``t - i`` of
        the window by ``(1 - alpha) ** i``. Leading rows with gaps (e.g. the first row of
        ``pct_change``) are dropped here too, which matches. A gap inside a window would shift
        the weights of every older row, so such windows are marked not-ok and left to the
        per-date path.
        """
        key = bundle.fingerprint()
        if self._ewm_cache is not None and self._ewm_cache[0] == key:
            return self._ewm_cache[1], self._ewm_cache[2]

        x = rets.to_numpy(dtype=float)
        t_len, n = x.shape
        ok = np.zeros(t_len, dtype=bool)
        if t_len * n * n > MAX_PANEL_CELLS:
            self._ewm_cache = (key, None, ok)
            return None, ok

        bad = np.isnan(x).any(axis=1)
        clean = np.flatnonzero(~bad)
        panel = np.full((t_len, n, n), np.nan)
        if clean.size:
            first = clean[0]
            panel[first:], ok[first:] = self._ewm_windows(x[first:], bad[first:])
        self._ewm_cache = (key, panel, ok)
        return panel, ok

    def _ewm_windows(self, x: np.ndarray, bad: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        t_len, n = x.shape
        lam = 0.5 ** (1.0 / self.halflife)  # 1 - alpha, as pandas derives it from a halflife
        span = t_len if self.lookback is None else min(self.lookback, t_len)
        kernel = lam ** np.arange(span)

        # Centre each column first: covariance is shift-invariant, and it keeps
        # E[xy] - E[x]E[y] from cancelling away the precision of a near-constant asset.
        x = np.where(bad[:, None], 0.0, x)
        x = x - x[~bad].mean(axis=0)
        iu, ju = np.triu_indices(n)
        series = np.concatenate([x, x[:, iu] * x[:, ju]], axis=1)
        sums = fftconvolve(series, kernel[:, None], axes=0)[:t_len]

        rows = np.minimum(np.arange(1, t_len + 1), span)  # rows in each window
        weight = (1.0 - lam ** rows) / (1.0 - lam)
        mean = sums[:, :n] / weight[:, None]
        second = sums[:, n:] / weight[:, None]
        upper = second - mean[:, iu] * mean[:, ju]
        panel = np.empty((t_len, n, n))
        panel[:, iu, ju] = upper
        panel[:, ju, iu] = upper

        # A window must hold no gap and at least two rows (the per-date path's minimum).
        gaps = np.convolve(bad.astype(float), np.ones(span))[:t_len]
        ok = (gaps < 0.5) & (rows >= 2)
        return panel, ok
