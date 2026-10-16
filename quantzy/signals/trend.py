"""Trend strength: how far price sits above its moving average, in units of its own volatility."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.data.bundle import DataBundle
from quantzy.signals.base import Signal
from quantzy.signals.indicators import ema, ewm_min_history, realized_vol, sma


@dataclass
class TrendStrength(Signal):
    """``tanh(log(P / MA_window) / (daily vol * sqrt(horizon)))``, bounded to (-1, 1).

    The gap to the moving average is measured in units of the volatility expected over
    ``horizon`` days. A 5% gap therefore reads as a strong trend in a calm market and a weak
    one in a turbulent market. The ``tanh`` squash keeps one violent move from dominating.

    Being bounded and already scaled through time, the score is usable without a
    cross-sectional normalizer, so pass ``normalizer=Identity()`` to time a single asset. On a
    multi-asset universe the default :class:`~quantzy.signals.normalize.ZScore` ranks assets by
    trend strength instead.

    Args:
        window: Moving-average length in rows (the span, for an EMA).
        vol_window: Trailing window for the daily volatility.
        horizon: Days of volatility the gap is measured against (21 = one month).
        exclude: Tickers to leave unscored (NaN), e.g. a cash sleeve.
        ma: ``"sma"`` or ``"ema"``. At the same window both lag a steady trend by about
            ``(window - 1) / 2`` rows. The EMA puts more weight on the latest prices but also
            keeps a long tail of old ones, and it has no jump when an old price leaves the
            window. Which turns first after a peak depends on the path.
        ewm_tol: Seed influence an EMA may retain at its first trusted row. It sets
            :attr:`min_history` (see :func:`~quantzy.signals.indicators.ewm_min_history`).
    """

    name: ClassVar[str] = "trend_strength"

    window: int = 200
    vol_window: int = 60
    horizon: int = 21
    exclude: tuple[str, ...] = ()
    ma: str = "sma"
    ewm_tol: float = 1e-9

    def __post_init__(self) -> None:
        if self.ma not in ("sma", "ema"):
            raise ValueError(f"ma must be 'sma' or 'ema', got {self.ma!r}")
        if self.window < 2 or self.vol_window < 2 or self.horizon < 1:
            raise ValueError(
                f"need window >= 2, vol_window >= 2 and horizon >= 1, got "
                f"{self.window}, {self.vol_window}, {self.horizon}"
            )

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        avg = sma(px, self.window) if self.ma == "sma" else ema(px, self.window)
        gap = np.log(px / avg)
        vol = realized_vol(px, self.vol_window) * np.sqrt(self.horizon)
        out = np.tanh(gap / vol.where(vol > 0))
        excluded = [c for c in self.exclude if c in out.columns]
        out[excluded] = np.nan
        return out

    @property
    def min_history(self) -> int:
        need = self.window if self.ma == "sma" else ewm_min_history(self.window, self.ewm_tol)
        return max(need, self.vol_window + 1)


@dataclass
class VolScaledTrend(TrendStrength):
    """:class:`TrendStrength` times annualized trailing vol: a trend view in return units.

    The score is in (-1, 1), so with ``normalizer=Identity()`` a weight ``k`` on this signal
    adds ``k * score`` to the asset's Sharpe ratio, the same units as
    :class:`~quantzy.signals.prior.SharpePrior`. That keeps ``k`` comparable across assets and
    with the IC: by Grinold's rule an IC of about 0.1 supports a ``k`` near 0.1.
    """

    name: ClassVar[str] = "trend"

    def raw(self, data: DataBundle) -> pd.DataFrame:
        vol = realized_vol(data.prices, self.vol_window, annualize=True)
        return super().raw(data) * vol


@dataclass
class EwmacTrend(Signal):
    """Carver's EWMAC crossover in return units: ``clip(f, +/-cap) * annualized vol``.

    ``f = scalar / 10 * (EMA_fast - EMA_slow) / (price * daily vol)``. The gap between the two
    EMAs is measured in daily price vols, and the forecast scalar brings the average ``|f|``
    near 1 (Carver's forecasts divided by 10). Capped at ``+/-cap``, then multiplied by
    annualized vol so that, with ``normalizer=Identity()``, a weight ``k`` adds ``k * f`` to
    the asset's Sharpe ratio, as :class:`VolScaledTrend` does.

    Args:
        fast: Span of the fast EMA.
        slow: Span of the slow EMA; ``None`` means ``4 * fast``, Carver's ratio.
        scalar: Forecast scalar; ``None`` uses Carver's 1.87 for 64/256, scaled by
            ``sqrt(64 / fast)`` for other speeds (his published values follow that rule).
        cap: Cap on ``|f|``.
        vol_window: Trailing window for the daily vol.
        exclude: Tickers to leave unscored (NaN), e.g. a cash sleeve.
        ewm_tol: Seed influence the slow EMA may retain at its first trusted row.
    """

    name: ClassVar[str] = "ewmac"

    fast: int = 64
    slow: int | None = None
    scalar: float | None = None
    cap: float = 2.0
    vol_window: int = 60
    exclude: tuple[str, ...] = ()
    ewm_tol: float = 1e-9

    def __post_init__(self) -> None:
        if self.fast < 1 or self.slow_span <= self.fast:
            raise ValueError(f"need 1 <= fast < slow, got {self.fast}, {self.slow_span}")
        if self.cap <= 0 or self.vol_window < 2:
            raise ValueError(f"need cap > 0 and vol_window >= 2, got {self.cap}, "
                             f"{self.vol_window}")

    @property
    def slow_span(self) -> int:
        return 4 * self.fast if self.slow is None else self.slow

    @property
    def forecast_scalar(self) -> float:
        return 1.87 * np.sqrt(64 / self.fast) if self.scalar is None else self.scalar

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        price_vol = px * realized_vol(px, self.vol_window)
        gap = ema(px, self.fast) - ema(px, self.slow_span)
        f = (gap / price_vol.where(price_vol > 0) * self.forecast_scalar / 10.0)
        out = f.clip(-self.cap, self.cap) * realized_vol(px, self.vol_window, annualize=True)
        out[[c for c in self.exclude if c in out.columns]] = np.nan
        return out

    @property
    def min_history(self) -> int:
        return max(ewm_min_history(self.slow_span, self.ewm_tol), self.vol_window + 1)
