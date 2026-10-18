"""Calendar and flow effects as a separate, mostly-in-cash sleeve.

The core book trades slowly by design, so short-horizon effects never reach it. These three
hold an asset for a few days around a predictable event and sit in T-bills otherwise, so
they are close to independent of the core. Each has an economic reason, and each is
published, so some of the edge may have gone. **Fixed before any test.**

1. *Pre-FOMC drift* (Lucca and Moench, 2015): US equities earned a large share of their
   excess return just before scheduled FOMC announcements (uncertainty resolving). Hold SPY
   on announcement days (previous close to that day's close); variant: the day before and
   the day. Scheduled meetings from the Federal Reserve's calendars, 1994 on.
2. *Turn of the month*: hold SPY on the last trading day of a month and the first three of
   the next (payroll, pension and fund flows). ^GSPC (price only) from 1950 as a long check.
3. *Treasury month-end*: hold TLT on the last two trading days of a month (bond index funds
   extend duration at month-end). A 30-year par bond rolled daily from ^TYX, 1977 on, as a
   long check.

Per effect: mean excess return a day inside the window against all other days (Welch t),
the share of all excess return earned inside it, by period; and the sleeve (the asset in the
window, T-bills otherwise, 1 bp each way per entry and exit): Sharpe, return, vol, drawdown,
time invested, correlation with the core book, and the core plus the combined sleeve.

**Result (2026-10-18): Treasury month-end adopted (0.5x overlay); the others rejected.**
Pre-FOMC drift: t 2.6 since 1994 but -0.1 since 2017 (after publication). Turn of the month:
SPY t 1.2; ^GSPC t 5.2 since 1950 but almost all before 1994, ~0 since 2008. Treasury
month-end: TLT t 4.2 (131% of its excess return earned on 10% of days), 30-year par bond
t 5.1 since 1977, positive in every period (2017-26 t 1.7-2.2); overlay excess +2.7% a year
since 2008-09 at 4.2% vol, correlation with the core +0.05. Its Sharpe has fallen by period
(1.54, 0.82, 0.43), so it is sized at half the recent-estimate optimum.

Usage:
    uv run python research/research_calendar_flows.py
"""

from __future__ import annotations

import re
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core

FED = "https://www.federalreserve.gov/monetarypolicy/"
CACHE = Path.home() / ".cache" / "quantzy" / "fomc"
UA = {"User-Agent": "Mozilla/5.0 (research; quantzy)"}
COST = 1e-4  # one way
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september",
     "october", "november", "december"], start=1)}
PERIODS = [("1950", "1993"), ("1994", "2007"), ("2008", "2016"), ("2017", "2026")]


def _get(url: str, name: str) -> str:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / name
    if not path.exists():
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=60) as r:
            path.write_bytes(r.read())
    return path.read_text(errors="ignore")


def _last_day(year: int, text: str) -> pd.Timestamp | None:
    """'January 31-February 1' -> Feb 1; 'April/May 30-1' -> May 1; 'July 5-6' -> Jul 6."""
    text = text.strip().lower().replace("–", "-")
    m = re.match(r"([a-z/]+)\s+(\d+)(?:\s*-\s*(?:([a-z]+)\s+)?(\d+))?$", text)
    if not m:
        return None
    month = (m.group(3) or m.group(1)).split("/")[-1]
    day = m.group(4) or m.group(2)
    month = next((full for full in MONTHS if full.startswith(month[:3])), None)
    if month is None:
        return None
    return pd.Timestamp(year, MONTHS[month], int(day))


def fomc_dates() -> list[pd.Timestamp]:
    """Scheduled FOMC announcement days (the last day of each scheduled meeting)."""
    out = []
    for year in range(1994, 2021):
        html = _get(f"{FED}fomchistorical{year}.htm", f"{year}.htm")
        for title, kind in re.findall(r"<h5[^>]*>\s*([A-Za-z0-9/\- ]+?)\s+(Meeting|Conference Call)"
                                      rf"\s*-\s*{year}", html):
            if kind == "Meeting":
                d = _last_day(year, title)
                if d is not None:
                    out.append(d)
    html = _get(f"{FED}fomccalendars.htm", "calendars.htm")
    for block in re.split(r'<h4><a id="\d+">', html)[1:]:
        year = int(block[:4])
        if year < 2021:
            continue
        months = re.findall(r'fomc-meeting__month[^>]*><strong>([^<]+)</strong>', block)
        dates = re.findall(r'fomc-meeting__date[^>]*>([^<]+)<', block)
        for mo, dt in zip(months, dates, strict=False):
            dt = dt.strip().rstrip("*").strip()
            if not re.match(r"^\d+(-\d+)?$", dt):
                continue  # notation votes, unscheduled
            mo = mo.split("/")[-1].strip().lower()
            full = {m[:3]: m for m in MONTHS}.get(mo[:3], mo)
            d = _last_day(year, f"{full} {dt.split('-')[-1]}")
            if d is not None:
                out.append(d)
    return sorted(set(out))


def daily_par_bond(yld_pct: pd.Series, maturity: float) -> pd.Series:
    """Daily return of a par bond bought at yesterday's yield, repriced at today's."""
    y = yld_pct.dropna() / 100
    y0, y1 = y.shift(1), y
    n = maturity - 1 / TRADING_DAYS
    price = y0 / y1 * (1 - (1 + y1) ** -n) + (1 + y1) ** -n
    return (price - 1 + y0 / TRADING_DAYS).dropna()


def windows(index: pd.DatetimeIndex, fomc: list[pd.Timestamp]) -> dict[str, pd.Series]:
    s = pd.Series(index, index=index)
    month = index.to_period("M")
    pos_from_end = s.groupby(month).cumcount(ascending=False)  # 0 = last trading day
    pos_from_start = s.groupby(month).cumcount()  # 0 = first trading day
    fomc_set = set(fomc)
    fomc_day = pd.Series(index.isin(list(fomc_set)), index=index)
    return {
        "FOMC day": fomc_day,
        "FOMC day before + day": fomc_day | fomc_day.shift(-1, fill_value=False),
        "turn of month": (pos_from_end == 0) | (pos_from_start <= 2),
        "Treasury month-end": pos_from_end <= 1,
    }


def effect_stats(excess: pd.Series, inside: pd.Series) -> dict[str, float]:
    x = excess.dropna()
    w = inside.reindex(x.index).fillna(False)
    a, b = x[w], x[~w]
    t = (a.mean() - b.mean()) / np.sqrt(a.var() / len(a) + b.var() / len(b))
    return {"days": len(a), "in, % a day": a.mean() * 100, "out, % a day": b.mean() * 100,
            "t (in - out)": t,
            "share of excess earned in": a.sum() / x.sum() if x.sum() else np.nan,
            "share of days": w.mean()}


def sleeve(asset_ret: pd.Series, rf: pd.Series, inside: pd.Series) -> pd.Series:
    w = inside.reindex(asset_ret.index).fillna(False).astype(float)
    held = w.shift(0)  # the window flags the day whose return is earned; set at prior close
    trades = held.diff().abs().fillna(held)
    return held * asset_ret + (1 - held) * rf.reindex(asset_ret.index).fillna(0) - trades * COST


def perf(r: pd.Series) -> dict[str, float]:
    nav = (1 + r).cumprod()
    return {"Sharpe": r.mean() / r.std() * np.sqrt(TRADING_DAYS),
            "return": nav.iloc[-1] ** (TRADING_DAYS / len(r)) - 1,
            "vol": r.std() * np.sqrt(TRADING_DAYS), "max DD": (nav / nav.cummax() - 1).min()}


def main() -> None:
    pd.set_option("display.width", 300)
    fomc = fomc_dates()
    print(f"scheduled FOMC announcement days: {len(fomc)}, {fomc[0].date()} .. {fomc[-1].date()}"
          f"; per year (recent): "
          + ", ".join(f"{y} {sum(d.year == y for d in fomc)}" for y in (2019, 2024, 2025, 2026)))
    yf = CachedReader(YFinanceReader())
    px = yf.history(["SPY", "TLT", "^GSPC", "^IRX", "^TYX"], start="1950-01-01")
    irx = px["^IRX"].ffill() / 100
    rf = ((1 + irx) ** (1 / TRADING_DAYS) - 1).shift(1).fillna(0)
    rets = {"SPY": px["SPY"].pct_change(fill_method=None),
            "TLT": px["TLT"].pct_change(fill_method=None),
            "^GSPC (price)": px["^GSPC"].pct_change(fill_method=None),
            "30y par bond": daily_par_bond(px["^TYX"], 30)}
    tests = [("FOMC day", "SPY"), ("FOMC day before + day", "SPY"), ("turn of month", "SPY"),
             ("turn of month", "^GSPC (price)"), ("Treasury month-end", "TLT"),
             ("Treasury month-end", "30y par bond")]
    rows, sleeves = {}, {}
    for eff, asset in tests:
        r = rets[asset].dropna()
        r = r[r.index >= (pd.Timestamp("1994-01-01") if eff.startswith("FOMC") else r.index[0])]
        win = windows(r.index, fomc)[eff]
        ex = r - rf.reindex(r.index).fillna(0)
        row = {"from": f"{r.index[0]:%Y}", **effect_stats(ex, win)}
        for a, b in PERIODS:
            x = ex.loc[a:b]
            if len(x) > 500:
                row[f"t {a}-{b[2:]}"] = effect_stats(x, win)["t (in - out)"]
        rows[(eff, asset)] = row
        sl = sleeve(r, rf, win)
        sleeves[(eff, asset)] = sl
    print("\n1. EFFECTS: mean excess return a day inside the window against all other days")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    b = latest_complete(core.load(), core.RISKY)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(b, core.build())
    rc = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    prow = {}
    for key, sl in sleeves.items():
        z = pd.concat([sl.rename("s"), rc.rename("c")], axis=1).dropna()
        p_full = perf(sl)
        p_core_era = perf(z["s"])
        prow[key] = {**{f"{k} (full)": v for k, v in p_full.items()},
                     "Sharpe since 2008-09": p_core_era["Sharpe"],
                     "excess over T-bills a year": (sl - rf.reindex(sl.index).fillna(0)).mean()
                     * TRADING_DAYS,
                     "corr with core": z["s"].corr(z["c"])}
    print("\n2. SLEEVES: the asset inside the window, T-bills otherwise, 1 bp each way")
    print(pd.DataFrame(prow).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    # combined sleeve: FOMC day + turn of month on SPY, Treasury month-end on TLT, as excess
    # returns over T-bills (an overlay needs no extra capital in futures; in ETFs, margin)
    idx = rc.index
    rfi = rf.reindex(idx).fillna(0)
    parts = {k: (sleeves[k].reindex(idx) - rfi).fillna(0) for k in
             [("FOMC day", "SPY"), ("turn of month", "SPY"), ("Treasury month-end", "TLT")]}
    combo = sum(parts.values())
    print("\n3. CORE + COMBINED OVERLAY (FOMC day + turn of month on SPY, month-end on TLT), "
          f"{idx[0].date()} .. {idx[-1].date()}")
    print(f"   overlay alone: excess {combo.mean() * TRADING_DAYS:+.2%} a year, vol "
          f"{combo.std() * np.sqrt(TRADING_DAYS):.2%}, Sharpe of excess "
          f"{combo.mean() / combo.std() * np.sqrt(TRADING_DAYS):.2f}, corr with core "
          f"{combo.corr(rc):+.2f}")
    for scale in (0.0, 0.5, 1.0):
        r = rc + scale * combo
        p = perf(r)
        print(f"   core + {scale:.1f}x overlay: Sharpe {p['Sharpe']:.2f}, "
              f"return {p['return']:.1%}, vol {p['vol']:.1%}, max DD {p['max DD']:.1%}")


if __name__ == "__main__":
    main()
