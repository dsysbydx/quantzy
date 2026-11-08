"""Follow-up: the *flipped* dollar net purchase ratio (insider dollar selling as a bullish sign).

``research_insider`` found the dollar-weighted NPR (63 days, z-scored, capped) wrong-way at
every horizon (held-out t -3.2 at 63 days, 0 of 5 periods positive) and uncorrelated with
EWMAC (+0.04). Flipping it after seeing that is fitting, and the SEC bulk data start in 2006,
so there is no untouched history of the score itself. This study asks what it is, with the
tests that can still fail. **Fixed before running (2026-11-05), the user's request.**

Score: ``flip = -z(value NPR)``, z against its trailing 3 years, capped +/-2, on the 9 US
sectors and QQQ; data and filters exactly as in ``research_insider``.

A. New constructions, not run before (held-out Pearson IC, 1 / 21 / 63 / 126 days):
   A1 ``flip 21`` and ``flip 126``: the same on 21- and 126-day windows.
   A2 The legs: ``-z(log sell $)`` and ``+z(log buy $)``, 63 days. If insiders' views drive
      it, buys should carry it; if scheduled mega-cap selling does, sells will.
   A3 ``flip ex-top-50``: drop the 50 issuers with the most insider sale dollars over the
      sample (a diagnostic; the selection uses the whole sample).
B. What it is: correlation of ``flip`` with the market's own price state -- SPY's log price
   against its trailing 3-year mean (z-scored, ``price z``), SPY's 252-day return, and VRP --
   and the IC of ``flip`` after the price state is regressed out (full-sample fit; a
   diagnostic).
C. Out of sample for the mechanism: ``price z`` on the S&P 500 (``^GSPC``) 1953-2005, which
   no study here has scored (IC at 21 / 63 / 126 days, NW t, by decade). Its sign is fixed by
   B: the sign of ``flip``'s correlation with it.
D. Book with overlays, ``PORTFOLIO`` fixed: ``flip`` at the weight matching VRP's train-window
   dispersion, and 0.5x / 2x; ``price z`` the same way if C is run.

Adoption rule: ``flip`` is adopted only if (1) A1's new windows keep the sign with held-out
t >= 2 at 63 or 126 days, (2) its residual IC after B keeps t >= 2 (it is not the price
state), and (3) D's matched weight lifts Sharpe +0.02 with neither half lower. If (2) fails
because ``flip`` *is* the price state, ``price z`` replaces it as the candidate, and is
adopted only if C has the same sign with NW t >= 2 over 1953-2005 and it passes (3).

**Result (2026-11-05): not adopted; fails the book test. The original lead was a data error.**

* The first run's numbers (and ``research_insider``'s first ``value NPR 63``) were typo filings: one
  AMD sale recorded at $2.1e16, the top 10 trades 96% of all sale dollars, so the dollar
  NPR jumped whenever one entered the window. Cleaning (added after that run, fixed before
  the rerun): drop the 695 trades over $10bn and cap events at $100m, now in
  ``research_insider.events`` (``clean=True``). Every number below is on clean data; the
  script prints part A on the raw data too, for the record.
* (1) passes: ``flip 63`` held-out t 0.5 / 1.0 / 2.8 / 2.6 at 1 / 21 / 63 / 126 days, positive
  in 5 of 5 periods; the new windows ``flip 126`` 2.3 at 63 days, ``flip 21`` 1.9 at 126;
  without the top 50 sellers 2.9. The buying leg carries it (``+buy $`` t -3.0 at 63 days:
  fewer insider dollars bought, higher returns), not scheduled mega-cap selling (``-sell $``
  -1.2).
* (2) passes, with a caveat: on clean data ``flip`` is +0.68 correlated with ``price z`` and
  +0.63 with the 252-day return (R^2 0.50), so half of it is the market's own momentum and
  price level; the residual keeps t 2.7 at 63 days (3 of 5 periods). ``price z`` alone has
  no IC (t 0.7) and fails out of sample (1953-2005 t 0.6 at 63 days; +2.7 in the 2000s,
  -1.8 in the 1980s), so it is not the candidate.
* (3) fails: the book is lower at every weight, 1.414 / 1.407 / 1.385 at 0.5x / 1x / 2x
  against 1.433 (train up to 1.436, test down to 1.388); 2021 +5.8 points, 2022 -5.4.
  ``price z`` 1.398 / 1.410 / 1.326. A 63-day IC from 19 years (~75 independent windows) on a
  score half made of momentum does not survive mean-variance sizing next to trend.

Usage:
    uv run python research/research_insider_value.py
"""

from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from research_insider import (  # noqa: E402
    CAP, DATA_END, US_EQ, _count, _npr, _z, events,
)

from quantzy.backtest import WalkForwardBacktester  # noqa: E402
from quantzy.data import CachedReader, DataBundle, YFinanceReader  # noqa: E402
from quantzy.research import compare, compare_tail_treatments, newey_west_t  # noqa: E402
from quantzy.research.ic import _ts_pairs  # noqa: E402
from quantzy.signals import Identity, PanelScore  # noqa: E402
from quantzy.signals.indicators import realized_vol  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402
from quantzy.strategies import overlays  # noqa: E402

SPLIT = pd.Timestamp("2017-07-26")
PANELS = Path(".cache/insider/value_panels.pkl")
HS = (1, 21, 63, 126)


def price_z(px: pd.Series) -> pd.Series:
    """Log price against its trailing 3-year mean, z-scored against the trailing 3 years."""
    x = np.log(px.ffill())
    dev = x - x.rolling(756, min_periods=504).mean()
    return _z(dev)


def build(index: pd.DatetimeIndex, clean: bool = True) -> dict[str, pd.Series]:
    ev = events(index, clean=clean)
    n = len(index)
    first = index.searchsorted(pd.Timestamp("2006-01-03"))
    last = index.searchsorted(DATA_END + pd.Timedelta(days=1))
    pos = np.arange(n)
    top = (ev[~ev["buy"]].groupby("cik")["value"].sum().sort_values(ascending=False)
           .index[:50])

    def ser(a: np.ndarray, w: int) -> pd.Series:
        return _z(pd.Series(np.where((pos >= first + w - 1) & (pos < last), a, np.nan),
                            index=index))

    def dollars(e: pd.DataFrame, w: int) -> np.ndarray:
        return _count(e["row"].to_numpy(), n, w, e["value"].to_numpy())

    out = {}
    for w in (21, 63, 126):
        b, s = ev[ev["buy"]], ev[~ev["buy"]]
        out[f"flip {w}"] = -ser(_npr(dollars(b, w), dollars(s, w)), w)
    b, s = ev[ev["buy"]], ev[~ev["buy"]]
    out["-sell $ 63"] = -ser(np.log1p(dollars(s, 63)), 63)
    out["+buy $ 63"] = ser(np.log1p(dollars(b, 63)), 63)
    ex = ev[~ev["cik"].isin(top)]
    b, s = ex[ex["buy"]], ex[~ex["buy"]]
    out["flip ex-top-50"] = -ser(_npr(dollars(b, 63), dollars(s, 63)), 63)
    share = ev[~ev["buy"] & ev["cik"].isin(top)]["value"].sum() / ev[~ev["buy"]]["value"].sum()
    out["_top50_share"] = share
    out["_top50"] = list(top)
    return out


def frame(s: pd.Series, cols: list[str]) -> pd.DataFrame:
    return pd.DataFrame({t: s for t in cols}, index=s.index)


def ic_table(scores: dict[str, pd.Series], px: pd.DataFrame) -> pd.DataFrame:
    rows = {}
    for name, s in scores.items():
        v = {name: frame(s.clip(-CAP, CAP), list(px.columns)).shift(1)}
        r = {}
        for h in HS:
            t = compare_tail_treatments(v, px, horizon=h).iloc[0]
            r[f"t{h}"] = t["held_out_t"]
            r[f"ic{h}"] = t["ic"]
        r["periods+ 63"] = f"{int(t['periods_pos'])}/{int(t['n_periods'])}"
        rows[name] = r
    return pd.DataFrame(rows).T


def part_a_b(b: DataBundle, p: dict) -> dict:
    px = b.prices[US_EQ]
    idx = b.index
    spy = b["market"].iloc[:, 0].astype(float)
    pz = price_z(spy)
    r252 = np.log(spy / spy.shift(252))
    vol = realized_vol(b.prices, 60, annualize=True)
    vrp_unit = (core.ALPHAS["vrp"][0].raw(b) / vol)["QQQ"]
    print(f"top 50 sellers hold {p['_top50_share']:.0%} of insider sale dollars")
    scores = {k: v for k, v in p.items() if not k.startswith("_")}
    print("\nA. held-out Pearson IC t (and IC) on the 9 sectors + QQQ, capped +/-2")
    print(ic_table(scores, px).to_string(float_format="{:+.3f}".format))

    f = p["flip 63"].clip(-CAP, CAP)
    d = pd.DataFrame({"flip": f, "price z": pz.clip(-CAP, CAP), "ret 252": r252,
                      "vrp": vrp_unit}).dropna()
    print("\nB. correlations, 2007-2026")
    print(d.corr().to_string(float_format="{:+.2f}".format))
    X = np.column_stack([np.ones(len(d)), d["price z"], d["ret 252"]])
    beta, *_ = np.linalg.lstsq(X, d["flip"].to_numpy(), rcond=None)
    resid = pd.Series(d["flip"].to_numpy() - X @ beta, index=d.index)
    r2 = 1 - resid.var() / d["flip"].var()
    resid = (resid / resid.std()).reindex(idx)
    print(f"   flip on [price z, 252d return]: R^2 {r2:.2f}, betas {beta[1]:+.2f}, {beta[2]:+.2f}")
    t = ic_table({"flip 63": p["flip 63"], "flip residual": resid,
                  "price z (SPY)": pz}, px)
    print(t.to_string(float_format="{:+.3f}".format))
    sign = float(np.sign(d["flip"].corr(d["price z"])))
    return {"price z": pz, "resid": resid, "sign": sign}


def part_c(sign: float) -> None:
    raw = CachedReader(YFinanceReader()).history(["^GSPC"], start="1950-01-01")["^GSPC"].dropna()
    s = sign * price_z(raw)
    px = raw.to_frame("SPX")
    sc = s.clip(-CAP, CAP).to_frame("SPX").shift(1)
    print(f"\nC. price z on the S&P 500 (sign {sign:+.0f}), out of sample 1953-2005 and after")
    rows = {}
    for lab, sl in (("1953-2005", slice("1953", "2005")), ("2006-2026", slice("2006", None)),
                    *((f"{y}s", slice(str(y), str(y + 9))) for y in range(1950, 2030, 10))):
        r = {}
        for h in (21, 63, 126):
            zx, zy = _ts_pairs(sc.loc[sl], px.loc[sl], h, "pearson", 60)
            prod = (zx * zy)["SPX"].dropna()
            r[f"ic{h}"] = prod.mean()
            r[f"t{h}"] = newey_west_t(prod, max(21, 2 * h))
        rows[lab] = r
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.3f}".format))


def run_book(args):
    name, panel_name, k = args
    b = core.load()
    warm = core.warmup(b, core.build())
    alphas = dict(core.ALPHAS)
    if panel_name is not None:
        s = pd.read_pickle(PANELS)[panel_name].reindex(b.index).clip(-CAP, CAP)
        panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
        for t in US_EQ:
            panel[t] = s
        b = DataBundle({**b, "x": panel})
        alphas["x"] = (PanelScore("x", normalizer=Identity()), k)
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return name, r


def part_d(b: DataBundle, panels: dict[str, pd.Series]) -> None:
    vol = realized_vol(b.prices, 60, annualize=True)
    vrp_sd = float((core.ALPHAS["vrp"][0].raw(b) / vol).loc[:SPLIT, US_EQ].stack().std())
    pd.to_pickle(panels, PANELS)
    jobs = [("saved", None, 0.0)]
    for n, s in panels.items():
        k = 0.091 * vrp_sd / float(s.clip(-CAP, CAP).loc[:SPLIT].std())
        jobs += [(f"{n} k={k * m:.3f}" + (" (matched)" if m == 1 else ""), n, k * m)
                 for m in (0.5, 1.0, 2.0)]
    with ProcessPoolExecutor(7) as pool:
        runs = dict(pool.map(run_book, jobs))
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    m = m.loc[[j[0] for j in jobs]]
    r0 = runs["saved"]
    print(f"\nD. book with overlays, {r0.index[0].date()} .. {r0.index[-1].date()}, PORTFOLIO "
          f"fixed; flip NaN after {DATA_END.date()}")
    print(m.to_string(float_format="{:.3f}".format))
    yrs = pd.DataFrame({n: (1 + runs[n]).groupby(runs[n].index.year).prod() - 1 for n in runs})
    diff = yrs.sub(yrs["saved"], axis=0)[[j[0] for j in jobs if "matched" in j[0]]]
    print("   calendar-year return minus saved, matched weight")
    print(diff.to_string(float_format="{:+.1%}".format))


def main() -> None:
    pd.set_option("display.width", 240)
    b = core.load()
    print("=== RAW (as in research_insider), part A only")
    raw = build(b.index, clean=False)
    print(f"top 50 sellers hold {raw['_top50_share']:.0%} of insider sale dollars")
    print(ic_table({k: v for k, v in raw.items() if not k.startswith("_")},
                   b.prices[US_EQ]).to_string(float_format="{:+.3f}".format))
    print("\n=== CLEAN")
    p = build(b.index)
    ab = part_a_b(b, p)
    part_c(ab["sign"])
    part_d(b, {"flip 63": p["flip 63"], "price z": ab["sign"] * ab["price z"]})


if __name__ == "__main__":
    main()
