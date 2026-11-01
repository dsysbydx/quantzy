"""Shared logic of the Treasury auction overlays: short Treasuries into each auction.

What it is
    A calendar rule, not an optimizer book, run as two overlays on top of
    :mod:`core_multiasset`: :mod:`treasury_auction_10y` (IEF, into each nominal 10-year
    auction) and :mod:`treasury_auction_30y` (TLT, into each 30-year auction). Each holds
    ``-SIZE`` of its ETF from the close ``PRE`` trading days before an auction to the
    auction-day close, and nothing otherwise. Reopenings count; TIPS do not.
Why it should work
    Dealers absorb each auction's new supply, so Treasuries cheapen in the days before it
    (Lou, Yan and Zhang, 2013). In the book's window the concession is significant (IEF t
    -2.4, TLT t -2.0 since 2017-07), and the same shorts 10-15 days away from auctions lose
    (``research/research_auction_preshort.py``).
Evidence and size
    At 0.25x, 2008-09 .. 2026-10: the overlay's own return t 2.02 (Sharpe 0.47), correlation
    -0.11 with the book; the book with its other overlays 1.371 -> 1.489 (train 1.368 ->
    1.430, test 1.379 -> 1.543). It was chosen after the full-cycle overlay failed, its t is
    at the bar and 2002-08 went the other way, so it runs at half that, ``SIZE = 0.125``;
    revisit the size from live Sharpe.
Dates
    History from TreasuryDirect's securities search; upcoming auctions from Treasury's
    tentative auction schedule (published months ahead: about half of auctions are
    *announced* less than five trading days before, so the announcement alone would come
    too late). Both cached under ``.cache/auctions/`` and fetched with ``curl``, which uses the
    system's certificate store. Tests inject dates with :func:`set_dates`.
"""

from __future__ import annotations

import json
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.strategies import treasury_month_end as tme

SIZE = 0.125
PRE = 5  # trading days held: the returns of the 5 days into the auction close
COST_BPS = 1.0  # one way
BORROW = 0.005  # a year, on the short
CACHE = Path(".cache/auctions")
MAX_AGE_DAYS = 7  # refetch the schedule when the cache is older
HISTORY_API = "https://www.treasurydirect.gov/TA_WS/securities/search"
SCHEDULE_URL = "https://home.treasury.gov/system/files/221/Tentative-Auction-Schedule.xml"
#: original term -> (security type, the terms its auctions are listed under in the history)
TERMS = {
    "10-Year": ("Note", ["10-Year", "9-Year 11-Month", "9-Year 10-Month", "9-Year 9-Month"]),
    "30-Year": ("Bond", ["30-Year", "29-Year 11-Month", "29-Year 10-Month", "29-Year 9-Month"]),
}

_injected: dict[str, pd.DatetimeIndex] | None = None


def set_dates(dates: dict[str, pd.DatetimeIndex] | None) -> None:
    """Use these auction dates instead of fetching them (for tests); ``None`` resets."""
    global _injected
    _injected = None if dates is None else {k: pd.DatetimeIndex(v) for k, v in dates.items()}


def _curl(*args: str) -> bytes:
    return subprocess.run(["curl", "-sfL", *args], check=True, capture_output=True,
                          timeout=120).stdout


def _stale(path: Path) -> bool:
    return not path.exists() or time.time() - path.stat().st_mtime > MAX_AGE_DAYS * 86400


def _history() -> pd.DataFrame:
    path = CACHE / "history.json"
    if _stale(path):
        rows = []
        for sec_type, listed in TERMS.values():
            for term in listed:
                rows += json.loads(_curl("-G", HISTORY_API, "--data-urlencode",
                                         f"securityType={sec_type}", "--data-urlencode",
                                         f"securityTerm={term}", "--data-urlencode",
                                         "format=json"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows))
    df = pd.DataFrame(json.loads(path.read_text()))
    orig = df["originalSecurityTerm"].astype(str)
    return pd.DataFrame({
        "term": orig.where(orig != "", df["securityTerm"]),
        "date": pd.to_datetime(df["competitiveAuctionCloseDate"], errors="coerce"),
        "nominal": (df["inflationProtected"].astype(str) != "Yes")
                   & (df["floatingRate"].astype(str) != "Yes"),
    })


def _schedule() -> pd.DataFrame:
    path = CACHE / "tentative.xml"
    if _stale(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_curl(SCHEDULE_URL))
    root = ET.fromstring(path.read_bytes())
    rows = [{"term": e.findtext("SecurityTermWeekYear"),
             "date": pd.Timestamp(e.findtext("AuctionDate")),
             "nominal": e.findtext("TIPS") == "N" and e.findtext("FloatingRate") == "N"}
            for e in root.iter("AuctionCalendarDate") if e.findtext("AuctionDate")]
    out = pd.DataFrame(rows)
    out.attrs["end"] = pd.Timestamp(root.findtext("EndDate"))
    return out


def auction_dates(term: str) -> pd.DatetimeIndex:
    """Nominal auction dates of ``term`` ("10-Year" or "30-Year"): history and schedule."""
    if _injected is not None:
        return _injected[term]
    both = pd.concat([_history(), _schedule()], ignore_index=True)
    both = both[both["nominal"] & both["date"].notna() & (both["term"] == term)]
    return pd.DatetimeIndex(sorted(both["date"].unique()))


def schedule_end() -> pd.Timestamp:
    """The last day the tentative schedule covers (the live rule needs the weeks ahead)."""
    return _schedule().attrs["end"] if _injected is None else pd.Timestamp.max


def in_pre_window(days: pd.DatetimeIndex, dates: pd.DatetimeIndex) -> pd.Series:
    """True on each trading day whose return the overlay earns: the ``PRE`` days ending at
    an auction day (an auction on a non-trading day counts on the next trading day)."""
    flag = np.zeros(len(days), dtype=bool)
    for d in dates:
        i = days.searchsorted(d)
        if i < len(days):
            flag[max(i - PRE + 1, 0): i + 1] = True
    return pd.Series(flag, index=days)


def target_after_close(day, term: str, size: float = SIZE) -> float:
    """The weight to hold from ``day``'s close: ``-size`` when the next trading day's return
    is in a pre-auction window, else 0."""
    day = pd.Timestamp(day).normalize()
    if _injected is None and schedule_end() < day + pd.Timedelta(days=30):
        raise RuntimeError(
            f"the tentative auction schedule ends {schedule_end().date()}: fewer than 30 days "
            f"after {day.date()}; delete {CACHE / 'tentative.xml'} to refetch it")
    cal = tme.trading_days(day - pd.Timedelta(days=10), day + pd.Timedelta(days=40))
    nxt = cal[cal > day][0]
    flags = in_pre_window(cal, auction_dates(term))
    return -size if bool(flags[nxt]) else 0.0


def backtest_returns(bundle, term: str, asset: str, size: float = SIZE) -> pd.Series:
    """The overlay's return over cash on the bundle's dates, after costs and short borrow."""
    idx = bundle.index
    held = -size * in_pre_window(idx, auction_dates(term)).astype(float)
    r = bundle.prices[asset].pct_change(fill_method=None).fillna(0.0)
    cash = bundle.prices["TBILL"].pct_change(fill_method=None).fillna(0.0)
    trades = held.diff().abs().fillna(held.abs())
    return held * (r - cash) - trades * COST_BPS / 1e4 - held.abs() * BORROW / TRADING_DAYS
