"""Treasury month-end overlay: hold TLT over the last two trading days of each month.

What it is
    A calendar rule, not an optimizer book. From the close three trading days before
    month-end to the month's last close, hold ``SIZE`` (0.5x NAV) of TLT, funded from cash;
    otherwise hold nothing. It runs on top of :mod:`core_multiasset`, which lists it in
    ``OVERLAYS``, and the daily job adds its position to the core's.

Why it should work
    Bond index funds extend duration at month-end, when the index rolls into new issues, and
    the buying is predictable. Over 2002-2026 TLT earned more than all of its excess return
    over T-bills on these two days a month (131%; the rest of the days netted slightly
    negative). On a 30-year par bond rolled from ^TYX, 1977-2026, t 5.1 for in-window against
    other days, positive in every period (1994-2007 t 4.0, 2008-16 2.7, 2017-26 2.2)
    (``research/research_calendar_flows.py``).

Size: 0.5x
    The overlay's Sharpe on its excess return has fallen by period (1.54 in 2002-08, 0.82 in
    2008-17, 0.43 in 2017-26, each +/-0.3-0.6), as published anomalies tend to. Sized on the
    recent estimate and halved for that uncertainty; 0.5x keeps gross under about 1.8x on
    those days (Reg T) and the worst window near -1.3% of NAV. Correlation with the core book
    +0.05. Core + 0.5x overlay, 2008-09 .. 2026-10: Sharpe 1.21 against 1.13, max drawdown
    -17.0% against -15.9%.

Trading
    Market-on-close orders: buy at the close three trading days before month-end, sell at the
    last close; 24 trades a year at about 1 bp each way. The schedule is known in advance from
    the NYSE calendar below (regular holidays; one-off closures are not modelled).
"""

from __future__ import annotations

import pandas as pd
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    GoodFriday,
    Holiday,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
    nearest_workday,
    sunday_to_monday,
)

NAME = "treasury_month_end"
ASSET = "TLT"
SIZE = 0.5
DAYS = 2  # last trading days of the month in the window
COST_BPS = 1.0  # one way


class NYSEHolidays(AbstractHolidayCalendar):
    """The NYSE's regular full-day holidays (not one-off closures)."""

    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=sunday_to_monday),
        USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay,
        Holiday("Juneteenth", month=6, day=19, start_date="2022-01-01",
                observance=nearest_workday),
        Holiday("Independence Day", month=7, day=4, observance=nearest_workday),
        USLaborDay, USThanksgivingDay,
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


def trading_days(start, end) -> pd.DatetimeIndex:
    holidays = NYSEHolidays().holidays(pd.Timestamp(start), pd.Timestamp(end))
    return pd.bdate_range(start, end).difference(holidays)


def in_window(days: pd.DatetimeIndex) -> pd.Series:
    """True on the last ``DAYS`` trading days of each month in ``days`` (complete months)."""
    s = pd.Series(days, index=days)
    from_end = s.groupby(days.to_period("M")).cumcount(ascending=False)
    return from_end < DAYS


def target_after_close(day, bundle=None, current: float = 0.0) -> float:
    """The weight in ``ASSET`` to hold from ``day``'s close: ``SIZE`` when the next trading
    day is in the window (its return is the one earned), else 0. A calendar rule: the data
    and the current holding are not used."""
    day = pd.Timestamp(day).normalize()
    cal = trading_days(day - pd.Timedelta(days=7), day + pd.offsets.MonthEnd(2))
    nxt = cal[cal > day][0]
    flags = in_window(cal)
    return SIZE if bool(flags[nxt]) else 0.0


def asset_prices(bundle) -> pd.Series:
    return bundle.prices[ASSET]


def backtest_returns(bundle) -> pd.Series:
    """:func:`excess_returns` on the bundle's TLT and cash sleeve."""
    return excess_returns(asset_prices(bundle).pct_change(fill_method=None),
                          bundle.prices["TBILL"].pct_change(fill_method=None))


def excess_returns(asset_returns: pd.Series, cash_returns: pd.Series) -> pd.Series:
    """The overlay's return over cash on realized dates, after costs (for backtests).

    The window comes from the realized trading days, so it equals the live schedule wherever
    the exchange kept its regular calendar."""
    r = asset_returns.dropna()
    held = in_window(r.index).astype(float) * SIZE
    trades = held.diff().abs().fillna(held)
    return held * (r - cash_returns.reindex(r.index).fillna(0.0)) - trades * COST_BPS / 1e4
