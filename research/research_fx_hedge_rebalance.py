"""FX month-end hedge rebalancing as an overlay (idea 14).

Foreign investors in US equities hedge their dollar exposure and reset the hedges at the
month-end London 4 pm fix (11 am ET on the last business day). When US equities rallied in
the month their dollar exposure grew, so they sell dollars into month-end; the reverse
after a fall. US holders of hedged foreign equities do the mirror trade, so the net flow
follows US *minus* foreign equity returns (Melvin and Prins, 2015). The same mechanism family
as the pension rebalancing overlay, on the same days. **Fixed before any test.**

* Signal: ``z``, SPY minus EFA total return month to date through the close before the entry
  close, over its expected spread (``pension_rebalance.z_after_close`` with EFA for IEF).
* Candidate: over the last two trading days of each month, ``-0.5 * clip(z / 2, -1, 1)`` of
  UUP (long the dollar index): short the dollar after US equities led. 5 bp each way.
* Longer check of the effect: EUR/USD (Yahoo, from 2003-12), the dollar's return being
  minus EUR/USD's; daily closes include the 11 am ET fix on the last day, and any reversal
  after it.

Rule: the slope of the window's dollar return on ``z`` negative in every period (2004-08 on
EUR/USD; 2008-16 and 2017-26 on UUP), *and* the saved book -- core plus its overlays, the
pension overlay included, so the test is what this adds beyond it -- up at least 0.02 Sharpe
over the window and in both halves (split 2017-07-26). Reported also: the overlay's t, its
correlation with the pension overlay and with the book.

**Result (2026-11-01): fails; in daily data the sign is mostly the other way.** The window's
dollar return on ``z``: +15.6 bp per z (t +1.6) in 2004-08 on EUR/USD, +11.7 (t +1.4) in
2008-16 and -6.9 (t -1.4) in 2017-26 on UUP -- the expected negative only in the last. The
overlay loses -0.28% a year (t -2.1) since 2008-09; the book 1.414 against 1.443 (train
1.350 / test 1.473). The signal is 0.21 correlated with the pension overlay's and the returns
0.05. Before 2017 the dollar tended to *strengthen* into month-end after US equities led:
other month-end dollar flows swamp this one at a daily close. Melvin and Prins measured it in
the minutes around the fix itself, which daily data cannot isolate. The reverse sign is not
taken up: it would be fitted, and the latest period has the expected sign.

Usage:
    uv run python research/research_fx_hedge_rebalance.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402
from quantzy.strategies import pension_rebalance as pr  # noqa: E402
from quantzy.strategies import treasury_month_end as tme  # noqa: E402

SIZE, CAP_Z, COST_BPS = 0.5, 2.0, 5.0
SPLIT = pd.Timestamp("2017-07-26")
PERIODS = (("2004-01", "2008-08", "EUR/USD"), ("2008-09", "2016-12", "UUP"),
           ("2017-01", "2026-12", "UUP"))


def signal(spy: pd.Series, efa: pd.Series, idx: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    """``z`` held through each month's window (NaN outside), and the window flag."""
    z = pr.z_after_close(spy.reindex(idx), efa.reindex(idx))
    window = tme.in_window(idx)
    entry = window.shift(-1, fill_value=False) & ~window
    return z.shift(1).where(entry).ffill().where(window), window


def main() -> None:
    pd.set_option("display.width", 220)
    raw = CachedReader(YFinanceReader()).history(["EURUSD=X", "SPY", "EFA", "UUP"],
                                                 start="2001-01-01")
    b = core.load()
    idx = b.index[b.index >= "2003-12-01"]
    spy, efa = b["market"].iloc[:, 0], raw["EFA"]
    z, window = signal(spy, efa, idx)
    usd_eur = -raw["EURUSD=X"].reindex(idx).ffill().pct_change(fill_method=None)
    uup = b.prices["UUP"].reindex(idx).pct_change(fill_method=None)

    # 1. The effect: the window's dollar return on z, by period.
    month = idx.to_period("M")
    zm = z.groupby(month).first()
    print(f"{idx[0].date()} .. {idx[-1].date()}")
    print("\n1. EFFECT: the dollar's return over the window (bp) on z, slope per 1 unit of z")
    for a, e, src in PERIODS:
        r = (usd_eur if src == "EUR/USD" else uup).where(window).groupby(month).sum()
        d = pd.DataFrame({"z": zm, "r": r * 1e4}).loc[a:e].dropna()
        d = d[d.r != 0]
        res = stats.linregress(d.z, d.r)
        t_ = res.slope / res.stderr
        print(f"   {a}..{e} ({src}): slope {res.slope:+.2f} bp per z (t {t_:+.1f}), "
              f"{len(d)} months")
    for src, ser in (("EUR/USD", usd_eur), ("UUP", uup)):
        r = ser.where(window).groupby(month).sum()
        d = pd.DataFrame({"z": zm, "r": r * 1e4}).loc["2008-09":].dropna()
        d = d[d.r != 0]
        res = stats.linregress(d.z, d.r)
        print(f"   2008-09 on, {src}: slope {res.slope:+.2f} (t {res.slope / res.stderr:+.1f})")

    # 2. The overlay, on UUP, over the cash rate.
    rf = b.prices[core.CASH].pct_change(fill_method=None).reindex(idx).fillna(0.0)
    held = (-SIZE * np.clip(z / CAP_Z, -1, 1)).fillna(0.0)
    trades = held.diff().abs().fillna(held.abs())
    o = held * (uup.fillna(0.0) - rf) - trades * COST_BPS / 1e4
    o = o.loc["2008-09-10":]
    ann = np.sqrt(TRADING_DAYS)
    pension = pr.backtest_returns(b).reindex(o.index).fillna(0.0)
    print(f"\n2. OVERLAY (0.5x UUP), 2008-09 on: {o.mean() * TRADING_DAYS:+.2%}/yr, vol "
          f"{o.std() * ann:.2%}, Sharpe {o.mean() / o.std() * ann:+.2f}, "
          f"t {o.mean() / o.std() * np.sqrt(len(o)):+.2f}; corr with the pension overlay "
          f"{o.corr(pension):+.2f}")
    zp, _ = signal(spy, b.prices["IEF"], idx)  # the pension overlay's z
    print(f"   signal corr with the pension overlay's z: "
          f"{zm.corr(zp.groupby(month).first()):+.2f}")

    # 3. The book.
    r_core, *_ = export.run("strategy")
    book = export.with_overlays(r_core, b)
    oo = o.reindex(book.index).fillna(0.0)
    t = compare({"saved (core + all overlays)": book, "+ FX hedge rebalancing": book + oo},
                split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    print(f"\n3. BOOK, {book.index[0].date()} .. {book.index[-1].date()}, split {SPLIT.date()}; "
          f"overlay corr with the book {oo.corr(book):+.2f}")
    print(m.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
