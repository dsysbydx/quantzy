"""Pension rebalancing overlay: trade against balanced funds' month-end rebalancing in SPY.

What it is
    A calendar rule with a signed size, not an optimizer book. Balanced and pension funds
    hold fixed stock / bond weights and rebalance near month-end, so when stocks have beaten
    bonds this month they sell stocks into its last days, and buy when stocks have lagged
    (Harvey, Mazzoleni and Melone, 2025). Over the same last ``DAYS`` trading days as the
    Treasury month-end overlay, hold ``-SIZE * clip(z / 2, -1, 1)`` of SPY -- short when
    stocks led, long when they lagged, full size at ``|z| >= 2`` -- funded from or added to
    cash; otherwise hold nothing. It runs on top of :mod:`core_multiasset`, which lists it in
    ``OVERLAYS``, and the daily job adds its position to the core's.
Signal
    ``z`` is SPY minus IEF total return from the previous month's last close through the
    close *before* the entry close (the close the position is taken at, the day before the
    window), over its expected spread: the trailing 252-day sd of daily SPY - IEF returns
    times the square root of the days counted. Measured a day before it is traded, so the
    daily job can compute it from the last complete close.
Evidence (``research/research_month_end_followups.py``)
    At 0.5x, 2008-09 .. 2026-10: the book with both other overlays 1.340 -> 1.396 (train
    1.316 -> 1.410, test 1.364 -> 1.388), max drawdown unchanged; the overlay's own return
    t 2.96 since 2002-08, correlation with the book +0.04; unchanged with the signal through
    t-2 or t-3. The magnitude matters: a sign-only rule needed the same-close signal
    (``research_month_end_flows.py``).
Size: 0.25x
    Half the tested size, as for the Treasury overlay: the gain is lumpy (the best five months
    gave 58% of it; full size in about 3% of months) and fading since publication (overlay
    Sharpe 0.91 / 0.77 / 0.35 in 2002-08 / 2008-16 / 2017-26).
Trading
    Market-on-close orders: enter at the close the day before the window, exit at the
    month's last close; short SPY in the ETF account when ``z > 0`` (about 61% of months). One
    basis point each way.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.strategies import treasury_month_end as tme

NAME = "pension_rebalance"
ASSET = "SPY"
BOND = "IEF"
SIZE = 0.25
CAP_Z = 2.0  # full size at |z| >= CAP_Z
SD_WINDOW = TRADING_DAYS
COST_BPS = 1.0  # one way
PANEL = "market"  # the bundle panel with SPY's closes


def asset_prices(bundle) -> pd.Series:
    return bundle[PANEL].iloc[:, 0]


def z_after_close(spy: pd.Series, bond: pd.Series) -> pd.Series:
    """The month-to-date SPY - bond deviation through each close, in expected sds.

    Row ``p`` uses closes through ``p`` only: the month-to-date return since the previous
    month's last close, over ``sd * sqrt(n)`` with ``n`` the month's days through ``p`` and
    ``sd`` the 252-day sd of daily SPY - bond returns through ``p``.
    """
    r = pd.DataFrame({"s": spy, "b": bond}).ffill().pct_change(fill_method=None)
    month = r.index.to_period("M")
    growth = (1 + r.fillna(0.0)).groupby(month).cumprod()
    mtd = growth["s"] - growth["b"]
    n = r["s"].groupby(month).cumcount() + 1
    sd = (r["s"] - r["b"]).rolling(SD_WINDOW, min_periods=SD_WINDOW).std()
    return mtd / (sd * np.sqrt(n))


def weight(z: float) -> float:
    """The overlay's weight in SPY for a signal ``z`` (NaN: no position)."""
    if not np.isfinite(z):
        return 0.0
    return float(-SIZE * np.clip(z / CAP_Z, -1.0, 1.0))


def _entry_day(window_day: pd.Timestamp, cal: pd.DatetimeIndex) -> pd.Timestamp:
    """The trading day before the first window day of ``window_day``'s month."""
    flags = tme.in_window(cal)
    month = cal[cal.to_period("M") == window_day.to_period("M")]
    first = month[flags[month].to_numpy()][0]
    return cal[cal < first][-1]


def target_after_close(day, bundle, current: float = 0.0) -> float:
    """The weight in SPY to hold from ``day``'s close: :func:`weight` of the signal measured
    at the close before the month's entry close when the next trading day is in the window,
    else 0. ``current`` is not used (the position is set once a month)."""
    day = pd.Timestamp(day).normalize()
    cal = tme.trading_days(day - pd.Timedelta(days=40), day + pd.offsets.MonthEnd(2))
    nxt = cal[cal > day][0]
    if not bool(tme.in_window(cal)[nxt]):
        return 0.0
    entry = _entry_day(nxt, cal)
    measured = cal[cal < entry][-1]
    z = z_after_close(asset_prices(bundle), bundle.prices[BOND]).loc[:measured]
    return weight(z.iloc[-1]) if len(z) else 0.0


def backtest_returns(bundle) -> pd.Series:
    """The overlay's return over cash on the bundle's dates, after costs."""
    spy = asset_prices(bundle)
    z = z_after_close(spy, bundle.prices[BOND])
    idx = bundle.index
    window = tme.in_window(idx)
    entry = window.shift(-1, fill_value=False) & ~window
    signal = z.shift(1).where(entry)  # measured at the close before the entry close
    held = signal.ffill().where(window).map(weight).fillna(0.0)
    r = spy.pct_change(fill_method=None).fillna(0.0)
    cash = bundle.prices["TBILL"].pct_change(fill_method=None).fillna(0.0)
    trades = held.diff().abs().fillna(held.abs())
    return held * (r - cash) - trades * COST_BPS / 1e4
