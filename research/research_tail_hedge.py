"""Buying protection when volatility is low: a regime-timed tail hedge.

The idea: options are cheap when the market is calm, and bad times always come. Hold SPX puts
(long volatility) while the VIX is low, cash them in when it spikes, and so carry insurance
that should be negatively correlated with the book's equity. The question is whether low-VIX
protection pays for itself in the book, and whether timing it beats holding it always.
**Fixed before any test.**

Instruments (no options history here, so puts are priced off the VIX):

* Simulated SPX puts, 21 trading days to expiry, strike 95% or 90% of spot, held to expiry and
  rolled, marked daily by Black-Scholes. Implied vol for strike/spot ``m`` is
  ``VIX * clip(1 + 4 * (0.95 - m), 0.75, 1.6)`` (ATM ~0.8x VIX, 95% ~1x, 90% ~1.2x: a typical
  1-month skew); rate from ^IRX. Each buy and sale costs 5% of the premium. Sensitivity: every
  implied vol 20% higher. Check: an ATM put-write priced the same way against Cboe's ^PUT.
* VIXY (VIX futures ETF, 2011 on), excess over cash: real prices, contango included.

Sizing: puts on SPX notional 0.5x NAV (VIXY 0.05x NAV), reset at each roll.

Rules, all on the VIX through the prior close:

* H0 always on.
* H1 the regime rule: protection switches on when VIX <= low and off (sold at its mark) when
  VIX >= high; primary low 15 / high 25, grid low 13 / 15 / 18 by high 25 / 30 / 40.
* H2 H1 plus the short-vol leg: while off after a spike, until VIX <= low again, hold ^PUT
  (Cboe's collateralized ATM put-write) at 0.25x NAV, excess over cash.

Tests: (1) is low VIX cheap: 30-day implied variance (VIX^2) against realized variance over
the next 21 days, and the simulated put's return per trade, by VIX at purchase, 1990 on;
(2) each hedge alone: return a year, correlation with the saved book, returns in the
sell-offs; (3) the saved book (core plus overlays) plus each hedge: Sharpe, test half,
drawdown.

**Result (2026-10-19): not added; the timing helps, the "cheap" premise does not.** Low VIX
is not cheap: implied variance is 1.3-1.5x the next month's realized at every VIX level
(1.44x at or below 13), and the rolled 95% put lost money on average in every bucket (69 of
69 trades bought at VIX 13-15 expired at a loss). Held always, the puts cost 1.2-2.2% of NAV
a year in every decade and leave the book flat or worse (95%: 1.231 -> 1.202). The regime
rule cuts the cost to about zero (-0.3 to +0.2%/yr since 2008), because it sells into the
spike, and every 95% grid cell lifts the book (1.237-1.294, test half 1.20-1.27 against 1.184;
primary 15/25: 1.247, test 1.224; 2020 -8.4% -> -5% with a 30-40 exit). But it was off in
2008 (VIX above 25 from September), and the gain is within the pricing error: the simulated
put-write earns 5.1%/yr against ^PUT's real 7.0%, so the simulation under-prices puts, and
with implied vol 20% higher the primary falls to 1.210. VIXY, with real prices, is the
cleanest read: 15/25 gives 1.244 (test 1.257), +0.013. Selling vol after spikes (H2) is
harmful: 1.142, max drawdown -24.9%. A decision needs real option prices.

Usage:
    uv run python research/research_tail_hedge.py
"""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
from research_bounce_filtered import saved_book, stats
from scipy.stats import norm

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, YFinanceReader
from quantzy.jobs.daily import latest_complete
from quantzy.strategies import core_multiasset as core

TENOR = 21
COST = 0.05
NOTIONAL = 0.5
VIXY_SIZE = 0.05
PUTWRITE_SIZE = 0.25
ANN = np.sqrt(TRADING_DAYS)
EVENTS = {"2008-09..2009-03": ("2008-09-01", "2009-03-09"), "2011-08": ("2011-07-22", "2011-10-03"),
          "2015-08": ("2015-08-10", "2015-08-31"), "2018-02": ("2018-01-26", "2018-02-08"),
          "2018-Q4": ("2018-10-01", "2018-12-24"), "2020-02..03": ("2020-02-19", "2020-03-23"),
          "2022": ("2022-01-03", "2022-12-30")}


def implied_vol(vix: float, m: float, bump: float) -> float:
    return vix / 100 * float(np.clip(1 + 4 * (0.95 - m), 0.75, 1.6)) * bump


def bs_put(s: float, k: float, t: float, r: float, vol: float) -> float:
    if t <= 0:
        return max(k - s, 0.0)
    d1 = (np.log(s / k) + (r + vol * vol / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)
    return k * np.exp(-r * t) * norm.cdf(-d2) - s * norm.cdf(-d1)


def put_book(spx: pd.Series, vix: pd.Series, rate: pd.Series, on: pd.Series, moneyness: float,
             bump: float = 1.0, short: bool = False) -> tuple[pd.Series, pd.DataFrame]:
    """Daily P&L as a fraction of NAV for rolled puts held while ``on``; and one row a trade.

    ``on`` is decided at each close (from the VIX through it): a put is bought at that close
    when ``on`` and none is held, and a held put is sold at that close when ``on`` turns off.
    """
    s, v, r, o = spx.to_numpy(), vix.to_numpy(), rate.to_numpy(), on.to_numpy()
    pnl = np.zeros(len(s))
    trades = []
    held = None  # (units, strike, days left, price, entry date, entry VIX, entry premium)
    for i in range(len(s)):
        if held is not None:
            units, k, left, p0, d0, v0, prem = held
            left -= 1
            p = bs_put(s[i], k, left / TRADING_DAYS, r[i], implied_vol(v[i], k / s[i], bump))
            pnl[i] += units * (p - p0)
            held = (units, k, left, p, d0, v0, prem)
            if left == 0 or not o[i]:
                pnl[i] -= units * p * COST if left else 0.0
                trades.append({"date": d0, "vix": v0, "premium": prem,
                               "return": (p * (1 - COST * (left > 0)) - prem * (1 + COST)) / prem})
                held = None
        if held is None and o[i] and not np.isnan(v[i]):
            k = moneyness * s[i]
            p = bs_put(s[i], k, TENOR / TRADING_DAYS, r[i], implied_vol(v[i], moneyness, bump))
            units = NOTIONAL / s[i]
            pnl[i] -= units * p * COST
            held = (units, k, TENOR, p, spx.index[i], v[i], p)
    out = pd.Series(pnl, index=spx.index)
    return (-out if short else out), pd.DataFrame(trades)


def regime(vix: pd.Series, low: float, high: float) -> pd.Series:
    """True from a close at or below ``low`` until a close at or above ``high``."""
    state, out = False, []
    for x in vix.to_numpy():
        if not np.isnan(x):
            state = True if x <= low else False if x >= high else state
        out.append(state)
    return pd.Series(out, index=vix.index)


def sharpe(x: pd.Series) -> float:
    x = x.dropna()
    return x.mean() / x.std() * ANN if x.std() > 0 else np.nan


def alone(x: pd.Series, book: pd.Series) -> dict:
    x = x.reindex(book.index).fillna(0)
    return {"%/yr": x.mean() * TRADING_DAYS * 100, "corr book": x.corr(book),
            **{k: (1 + x.loc[a:z]).prod() - 1 for k, (a, z) in EVENTS.items()}}


def main() -> None:
    pd.set_option("display.width", 340)
    raw = CachedReader(YFinanceReader()).history(["^GSPC", "^VIX", "^IRX", "^PUT", "VIXY"],
                                                 start="1990-01-01")
    raw = raw[raw["^GSPC"].notna()]
    spx, vix = raw["^GSPC"], raw["^VIX"].ffill()
    rate = np.log(1 + raw["^IRX"].ffill().fillna(4.0) / 100)
    cash = ((1 + raw["^IRX"].ffill().fillna(4.0) / 100) ** (1 / TRADING_DAYS) - 1).shift(1)

    # 1. is low VIX cheap?
    rv = np.log(spx).diff().pow(2).rolling(TENOR).sum().shift(-TENOR) * TRADING_DAYS / TENOR
    iv = (vix / 100) ** 2
    always = pd.Series(True, index=spx.index)
    _, tr95 = put_book(spx, vix, rate, always, 0.95)
    _, tr90 = put_book(spx, vix, rate, always, 0.90)
    buckets = [0, 13, 15, 18, 22, 30, 100]
    b_iv = pd.cut(vix, buckets)
    rows = {}
    for lab, g in pd.concat([iv.rename("iv"), rv.rename("rv"), b_iv.rename("b")],
                            axis=1).dropna().groupby("b", observed=True):
        rows[str(lab)] = {"days": len(g), "implied vol": np.sqrt(g["iv"].mean()),
                          "next-21d realized": np.sqrt(g["rv"].mean()),
                          "implied var / realized": g["iv"].mean() / g["rv"].mean(),
                          "% days realized > implied": (g["rv"] > g["iv"]).mean()}
    for name, tr in (("95% put", tr95), ("90% put", tr90)):
        tr["b"] = pd.cut(tr["vix"], buckets)
        for lab, g in tr.groupby("b", observed=True):
            rows[str(lab)][f"{name} trades"] = len(g)
            rows[str(lab)][f"{name} avg return"] = g["return"].mean()
            rows[str(lab)][f"{name} % paid off"] = (g["return"] > 0).mean()
    print("1. IS LOW VIX CHEAP? Implied vs next-21-day realized, and the rolled put's return per "
          "trade (net), by VIX at the time, 1990 on")
    print(pd.DataFrame(rows).T.to_string(float_format=lambda v: f"{v:+.3f}"))

    # check the pricing: a collateralized ATM put-write priced this way vs Cboe ^PUT
    pw, _ = put_book(spx, vix, rate, always, 1.0, short=True)
    sim_pw = pw / NOTIONAL
    put_ix = raw["^PUT"].pct_change(fill_method=None) - cash
    yrs = pd.concat([sim_pw.rename("sim"), put_ix.rename("cboe")], axis=1).dropna()
    yr = (1 + yrs).groupby(yrs.index.year).prod() - 1
    print(f"\nPricing check, ATM put-write excess over cash, {yrs.index[0]:%Y} on: simulated "
          f"{yrs['sim'].mean() * TRADING_DAYS:+.1%}/yr vs ^PUT "
          f"{yrs['cboe'].mean() * TRADING_DAYS:+.1%}/yr; "
          f"annual corr {yr['sim'].corr(yr['cboe']):+.2f}, daily corr "
          f"{yrs['sim'].corr(yrs['cboe']):+.2f}")

    # 2-3. the hedges, alone and in the book
    b = latest_complete(core.load(), core.RISKY)
    book = saved_book(b)
    vixy = (raw["VIXY"].pct_change(fill_method=None) - cash) * VIXY_SIZE
    putw = (put_ix * PUTWRITE_SIZE).fillna(0)
    hedges = {}
    for m in (0.95, 0.90):
        hedges[f"H0 {m:.0%} put always"] = put_book(spx, vix, rate, always, m)[0]
        hedges[f"H0 {m:.0%} put always, vol +20%"] = put_book(spx, vix, rate, always, m, 1.2)[0]
    hedges["H0 VIXY always"] = vixy
    for low, high in itertools.product((13, 15, 18), (25, 30, 40)):
        on = regime(vix, low, high)
        tag = f"{low}/{high}"
        for m in (0.95, 0.90):
            hedges[f"H1 {m:.0%} put {tag}"] = put_book(spx, vix, rate, on, m)[0]
        if (low, high) == (15, 25):
            hedges[f"H1 95% put {tag}, vol +20%"] = put_book(spx, vix, rate, on, 0.95, 1.2)[0]
            hedges[f"H1 VIXY {tag}"] = vixy.where(on.shift(1, fill_value=False), 0.0)
            hedges[f"H2 95% put {tag} + put-write when off"] = (
                hedges[f"H1 95% put {tag}"] + putw.where(~on.shift(1, fill_value=True), 0.0))
    t2, t3 = {}, {"saved book": stats(book)}
    for name, x in hedges.items():
        t2[name] = alone(x, book)
        t3[f"+ {name}"] = stats(book + x.reindex(book.index).fillna(0))
    print(f"\n2. HEDGES ALONE (fraction of NAV), {book.index[0].date()} on")
    print(pd.DataFrame(t2).T.to_string(float_format=lambda v: f"{v:+.3f}"))
    print("\n3. SAVED BOOK (core + overlays) PLUS EACH HEDGE")
    print(pd.DataFrame(t3).T.to_string(float_format=lambda v: f"{v:+.3f}"))
    long = {}
    for name in ("H0 95% put always", "H1 95% put 15/25", "H0 90% put always", "H1 90% put 15/25"):
        on = always if name.startswith("H0") else regime(vix, 15, 25)
        x = put_book(spx, vix, rate, on, 0.95 if "95" in name else 0.90)[0]
        long[name] = {f"{a}-{z[2:]}": x.loc[a:z].mean() * TRADING_DAYS * 100
                      for a, z in (("1990", "1999"), ("2000", "2009"), ("2010", "2019"),
                                   ("2020", "2026"))}
    print("\n4. LONG HISTORY: hedge return, % of NAV a year, by decade")
    print(pd.DataFrame(long).T.to_string(float_format=lambda v: f"{v:+.2f}"))


if __name__ == "__main__":
    main()
