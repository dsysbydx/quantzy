"""Insider buying as an equity alpha: market-wide and by sector (the user's idea, 2026-11-05).

Insiders' open-market purchases have predicted returns: in aggregate, the market over the
next 6-12 months (Seyhun 1988, 1992; Lakonishok and Lee 2001, who use a net purchase ratio);
by stock, mostly purchases, mostly "opportunistic" ones (Cohen, Malloy and Pomorski 2012).
Sales carry little information (diversification, taxes). Part of the aggregate effect is
contrarian: insiders buy after prices fall (Jiang and Zaman 2010). Since 2002 Form 4 is due
within two business days, so the market may price it faster now. By sector the evidence is
thin. Tested here as an *alpha* (a score in ``mu``), not an overlay. **Fixed before any
test.**

Data (``research/insider_data.py``): SEC Form 3/4/5 bulk sets, filings 2006-01 .. 2026-09-30;
Form 4 open-market purchases (P) and sales (S), price and shares > 0, traded at most 30 days
before filing. Dropped: owners who are only 10% holders (funds, not insiders), trades under
$1,000, and issuers with SIC 6722 / 6726 / 6770 (funds and blank-check companies). An
*event* is one owner's buys (or sells) in one issuer on one filing day. Each event becomes
known on the first trading day after its filing date (filings land after the close), and the
signal's usual one-day lag applies on top.

Hypotheses (unit score = z-score against its own trailing 3 years, from 1 year, capped +/-2,
positive = more buying):

Market-wide, the same score on every US equity ETF (9 sectors and QQQ), like VRP:
  A1 ``NPR 63``       net purchase ratio (buys - sells) / (buys + sells), event counts, 63d
  A2 ``NPR 126``      the same over 126 days (Lakonishok and Lee's six months)
  A3 ``NPR 21``       the same over 21 days (fast; earnings blackouts make it seasonal)
  A4 ``value NPR 63`` the same on dollar value (the volume hypothesis; mega-cap sales weigh)
  A5 ``buy firms 63`` log number of distinct issuers with insider buying, 63d (the count /
                      breadth hypothesis)
  A6 ``officer NPR 63`` the NPR on events by officers only (CEO, CFO etc.; directors out)
By sector, issuers mapped to the nine SPDR sectors by SIC (an approximation; telecom to XLK
and REITs to XLF, as the SPDRs held them before 2016-18):
  S1 ``sector NPR 63``      each sector's NPR, z against its own history (time-series tilt)
  S2 ``sector NPR 63 rel``  each sector's NPR minus the nine-sector mean that day, then z
                            (relative: which sector's insiders buy more than usual)
  S3 ``sector buy share 63`` distinct issuers with buys / distinct issuers with any trade

Steps (``research/README.md``): (1) IC: pooled time-series Pearson IC at 1, 21, 63 and 126
days, held-out 47-day folds, Newey-West t, five-year periods; for S2 also the cross-
sectional rank IC across the nine sectors. (2) Distribution and tail treatments. (3)
Correlation with VRP and EWMAC, and the score around the 2008-09, 2020 and 2022 lows. (4)
Book with overlays, ``PORTFOLIO`` fixed: market-wide scores at a weight matching VRP's
train-window dispersion (0.091), sector scores matching EWMAC's (0.037); A1 and S2 also at
0.5x and 2x, and A1 on all 12 equity ETFs (EFA, EEM too). The score is NaN (no view) after
the data ends, 2026-09-30.

Adoption rule: held-out t >= 2 at one of the four horizons, positive in at least half the
five-year periods; and in the book at the matched weight, Sharpe +0.02 or more with neither
train nor test lower. Nine hypotheses: one pass at t ~2 is weak evidence on its own.

**Result (2026-11-05, filings to 2026-09-30; rerun on clean data): fails, wrong sign; not
adopted.** 3.2M trades, ~240k buy and ~700k sell events, 97% mapped to a sector (XLF holds
45% of buy events: small bank directors). The data hold typo filings (one AMD sale at
$2.1e16; the top 10 trades 96% of all sale dollars), found after the first run: trades over
$10bn are dropped and events capped at $100m (``DROP_ABOVE``, ``EVENT_CAP``). That changed
only ``value NPR 63`` (first run: t -3.2 at 63 days, +0.04 with EWMAC, book 1.396); the count
scores use dollars only for the $1,000 floor and match the first run to the third decimal.

* Every market-wide score has a *negative* IC, growing with the horizon: held-out t at
  1 / 21 / 63 / 126 days ``NPR 63`` -0.7 / -0.7 / -1.5 / -1.9, ``NPR 126`` to -1.7, ``NPR 21``
  to -1.7, ``officer NPR 63`` to -1.9, ``buy firms 63`` to -2.5, ``value NPR 63`` -0.5 / -1.0 /
  -2.8 / -2.6. Insiders did buy the lows (the scores were at +2 at the 2008, 2016, 2018 and
  2020 lows) but also through the 2007 peak and the 2008 fall, and every market-wide score
  is -0.5 to -0.75 correlated with EWMAC (``value NPR 63`` -0.69): insiders buy after prices
  fall, which in 2007-26 was a bet against trend. The literature's positive aggregate result
  (mostly pre-2002, monthly reporting) does not show up here.
* By sector: ``sector NPR 63`` and ``sector buy share 63`` also negative (t to -1.7, -0.6
  with EWMAC); the relative version is ~0 (pooled +0.5 to +0.7 at 63-126 days, cross-
  sectional rank IC -0.01, t -0.6).
* Book (saved 1.433): every US-equity variant is lower, 1.29-1.43 (``value NPR 63`` 1.369).
  ``NPR 63`` on all 12 equity ETFs is 1.456 (test 1.50) but train 1.408 against 1.412, so it
  fails the rule, and its US IC is negative: the gain is EFA / EEM noise.
* The flipped dollar score is tested in ``research_insider_value``: it is half the market's
  momentum and price level and lowers the book at every weight.

Usage:
    uv run python research/insider_data.py trades && uv run python research/insider_data.py sic_bulk
    uv run python research/research_insider.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import DataBundle
from quantzy.research import (
    compare,
    compare_tail_treatments,
    describe_scores,
    information_coefficient,
    newey_west_t,
    tail_treatments,
)
from quantzy.signals import Identity, PanelScore
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays

ROOT = Path(".cache/insider")
PANELS = ROOT / "panels.pkl"
SPLIT = pd.Timestamp("2017-07-26")
SECTORS = core.CLASSES["US sectors"]
US_EQ = [*SECTORS, "QQQ"]
Z_WIN, Z_MIN, CAP = 756, 252, 2.0
DATA_END = pd.Timestamp("2026-09-30")
MARKET = ("NPR 63", "NPR 126", "NPR 21", "value NPR 63", "buy firms 63", "officer NPR 63")
SECTOR = ("sector NPR 63", "sector NPR 63 rel", "sector buy share 63")
LOWS = {"2008-11-20": "2008 low", "2009-03-09": "2009 low", "2011-10-03": "2011 low",
        "2016-02-11": "2016 low", "2018-12-24": "2018 low", "2020-03-23": "2020 low",
        "2022-10-12": "2022 low", "2025-04-08": "2025 low", "2007-10-09": "2007 peak",
        "2020-02-19": "2020 peak", "2022-01-03": "2022 peak"}


def sic_sector(sic: str) -> str | None:
    """Approximate SPDR sector for a SIC code (pre-2018 SPDR conventions)."""
    try:
        s = int(sic)
    except (TypeError, ValueError):
        return None
    d2 = s // 100
    if s in (6722, 6726, 6770):
        return "drop"
    if 100 <= s < 1000 or d2 in (20, 21, 54) or s in (2844, 5140, 5141, 5149, 5411, 5412, 5912):
        return "XLP"
    if d2 in (12, 13, 29) or 4920 <= s <= 4924 or 5170 <= s <= 5172:
        return "XLE"
    if d2 in (10, 14, 24, 26, 30, 32, 33) or (d2 == 28 and not (2830 <= s <= 2839)
                                              and s != 2844):
        return "XLB"
    if 2830 <= s <= 2839 or s in (3826, 5122, 8731) or 3841 <= s <= 3851 or d2 == 80:
        return "XLV"
    if 3570 <= s <= 3579 or 3660 <= s <= 3699 or 3810 <= s <= 3829 or 7370 <= s <= 7379 \
            or (d2 == 48 and s not in (4833, 4841)):
        return "XLK"
    if 4900 <= s <= 4949 or d2 == 49 and s != 4955:
        return "XLU"
    if 6000 <= s <= 6799:
        return "XLF"
    if s == 1531 or d2 in (22, 23, 25, 27, 31, 39, 70, 72, 75, 78, 79, 82) or 3630 <= s <= 3659 \
            or 3710 <= s <= 3716 or d2 in (52, 53, 55, 56, 57, 58, 59) or s in (4833, 4841):
        return "XLY"
    if d2 in (15, 16, 17, 34, 35, 36, 37, 38, 40, 41, 42, 44, 45, 46, 47, 50, 51, 73, 87) \
            or s == 4955:
        return "XLI"
    return None


# --- panels ----------------------------------------------------------------------------------

#: Typo filings (one AMD sale recorded at $2.1e16; the top 10 trades held 96% of all sale
#: dollars): drop trades over $10bn and cap each event at $100m. Added 2026-11-05 after the
#: first run; see ``research_insider_value``.
DROP_ABOVE, EVENT_CAP = 1e10, 1e8


def events(index: pd.DatetimeIndex, clean: bool = True) -> pd.DataFrame:
    tr = pd.read_parquet(ROOT / "trades.parquet")
    if clean:
        tr = tr[tr["value"] <= DROP_ABOVE]
    sic = pd.read_parquet(ROOT / "sic.parquet")
    tr = tr.merge(sic[["cik", "sic"]], on="cik", how="left")
    tr["sector"] = tr["sic"].map(sic_sector)
    tr = tr[(tr["sector"] != "drop") & (tr["value"] >= 1000)
            & ~(tr["ten_pct"] & ~tr["director"] & ~tr["officer"])]
    ev = (tr.groupby(["owner", "cik", "filed", "buy"], as_index=False)
          .agg(value=("value", "sum"), officer=("officer", "max"), sector=("sector", "first")))
    if clean:
        ev["value"] = ev["value"].clip(upper=EVENT_CAP)
    known = ev["filed"] + pd.Timedelta(days=1)
    ev["row"] = index.searchsorted(known)
    return ev[ev["row"] < len(index)]


def _count(rows: np.ndarray, n: int, w: int, weights: np.ndarray | None = None) -> np.ndarray:
    daily = np.bincount(rows, weights=weights, minlength=n).astype(float)
    return pd.Series(daily).rolling(w, min_periods=1).sum().to_numpy()


def _distinct(ev: pd.DataFrame, n: int, w: int) -> np.ndarray:
    """Distinct issuers with at least one event in the trailing ``w`` rows."""
    diff = np.zeros(n + w + 1)
    for _, r in ev.groupby("cik")["row"]:
        r = np.unique(r.to_numpy())
        end = -1
        for x in r:
            start = max(x, end)
            diff[start] += 1
            diff[x + w] -= 1
            end = x + w
    return np.cumsum(diff)[:n]


def _z(x: pd.Series) -> pd.Series:
    r = x.rolling(Z_WIN, min_periods=Z_MIN)
    return (x - r.mean()) / r.std()


def _npr(b: np.ndarray, s: np.ndarray) -> np.ndarray:
    tot = b + s
    return np.where(tot > 0, (b - s) / np.where(tot > 0, tot, 1), np.nan)


def build_panels(index: pd.DatetimeIndex) -> dict[str, pd.Series | pd.DataFrame]:
    """Uncapped z-scores: market-wide (Series) and by sector (DataFrame), on ``index``."""
    ev = events(index)
    n = len(index)
    first = index.searchsorted(pd.Timestamp("2006-01-03"))
    last = index.searchsorted(DATA_END + pd.Timedelta(days=1))
    live = (np.arange(n) >= first) & (np.arange(n) < last)
    buy, sell = ev[ev["buy"]], ev[~ev["buy"]]
    out: dict = {}

    def mk(a: np.ndarray, w: int) -> pd.Series:
        s = pd.Series(np.where(live & (np.arange(n) >= first + w - 1), a, np.nan), index=index)
        return _z(s)

    for w in (21, 63, 126):
        out[f"NPR {w}"] = mk(_npr(_count(buy["row"].to_numpy(), n, w),
                                  _count(sell["row"].to_numpy(), n, w)), w)
    out["value NPR 63"] = mk(_npr(_count(buy["row"].to_numpy(), n, 63, buy["value"].to_numpy()),
                                  _count(sell["row"].to_numpy(), n, 63,
                                         sell["value"].to_numpy())), 63)
    out["buy firms 63"] = mk(np.log1p(_distinct(buy, n, 63)), 63)
    ob, os_ = buy[buy["officer"]], sell[sell["officer"]]
    out["officer NPR 63"] = mk(_npr(_count(ob["row"].to_numpy(), n, 63),
                                    _count(os_["row"].to_numpy(), n, 63)), 63)

    npr, share, raw_npr = {}, {}, {}
    for sec in SECTORS:
        b, s = buy[buy["sector"] == sec], sell[sell["sector"] == sec]
        raw_npr[sec] = pd.Series(np.where(live, _npr(_count(b["row"].to_numpy(), n, 63),
                                                     _count(s["row"].to_numpy(), n, 63)),
                                          np.nan), index=index)
        anyt = ev[ev["sector"] == sec]
        nb, na = _distinct(b, n, 63), _distinct(anyt, n, 63)
        share[sec] = _z(pd.Series(np.where(live & (na > 0), nb / np.maximum(na, 1), np.nan),
                                  index=index))
        npr[sec] = _z(raw_npr[sec])
    rn = pd.DataFrame(raw_npr)
    out["sector NPR 63"] = pd.DataFrame(npr)
    out["sector NPR 63 rel"] = rn.sub(rn.mean(axis=1), axis=0).apply(_z)
    out["sector buy share 63"] = pd.DataFrame(share)
    # coverage diagnostics
    cov = ev.assign(year=index[ev["row"]].year).groupby(["year", "buy"]).size().unstack()
    out["_coverage"] = cov
    out["_sector_events"] = ev.groupby(["sector", "buy"]).size().unstack()
    out["_unmapped"] = float(ev["sector"].isna().mean())
    return out


def panel_frame(p: dict, name: str, cols: list[str], index: pd.DatetimeIndex) -> pd.DataFrame:
    """The score as a (dates x assets) panel, uncapped."""
    x = p[name]
    if isinstance(x, pd.Series):
        return pd.DataFrame({t: x for t in cols}, index=index)
    return x[cols].reindex(index)


# --- 1-3. IC, distribution, correlations ----------------------------------------------------

def ic_report(b: DataBundle, p: dict) -> dict[str, float]:
    px = b.prices
    idx = px.index
    vrp = core.ALPHAS["vrp"][0]
    vol = realized_vol(px, 60, annualize=True)
    vrp_unit = (vrp.raw(b) / vol)[core.EQUITY]
    ew = core.ALPHAS["ewmac"][0]
    ew_unit = (ew.raw(DataBundle.build(px[core.RISKY])) / vol[core.RISKY])
    sd = {"vrp": float(vrp_unit.loc[:SPLIT, US_EQ].stack().std()),
          "ewmac": float(ew_unit.loc[:SPLIT, SECTORS].stack().std())}

    print(f"events by year (known date), buys / sells; unmapped sector {p['_unmapped']:.1%}")
    print(p["_coverage"].T.to_string())
    print("\nevents by sector, buys / sells")
    print(p["_sector_events"].T.to_string())

    summary = {}
    for name in (*MARKET, *SECTOR):
        cols = US_EQ if name in MARKET else SECTORS
        unit = panel_frame(p, name, cols, idx)
        live = unit.notna().any(axis=1)
        print(f"\n=== {name}: {live.idxmax().date()} .. {unit.loc[live].index[-1].date()}")
        print(describe_scores(unit if name in SECTOR else unit[[cols[0]]]).round(3)
              .to_string())
        variants = {k: v.shift(1) for k, v in tail_treatments(unit).items()}
        rows = []
        for h in (1, 21, 63, 126):
            t = compare_tail_treatments(variants, px[cols], horizon=h)
            for k, r in t.iterrows():
                rows.append({"h": h, "treatment": k, **r.to_dict()})
        t = pd.DataFrame(rows).set_index(["h", "treatment"])
        print(t.drop(columns=["altered", "n_periods"]).to_string(float_format="{:+.4f}".format))
        main = t.xs("clip +/-2", level="treatment")
        summary[name] = {f"t{h}": main.loc[h, "held_out_t"] for h in (1, 21, 63, 126)} | {
            f"ic{h}": main.loc[h, "ic"] for h in (1, 21, 63, 126)} | {
            "periods+ (63)": f"{int(main.loc[63, 'periods_pos'])}/{int(main.loc[63, 'n_periods'])}"}
        capped = unit.clip(-CAP, CAP)
        if name == "sector NPR 63 rel":
            for h in (21, 63):
                xs = information_coefficient(capped.shift(1), px[SECTORS], horizon=h,
                                             min_names=5)
                print(f"   cross-sectional rank IC h={h}: {xs.mean():+.4f} "
                      f"(NW t {newey_west_t(xs.dropna(), max(21, 2 * h)):+.2f})")
        ref = vrp_unit[cols[0]] if name in MARKET else None
        if name in MARKET:
            c_v = capped[cols[0]].corr(ref)
            c_e = capped[cols[0]].corr(ew_unit[US_EQ].mean(axis=1))
            print(f"   corr with VRP {c_v:+.2f}, with mean EWMAC over US equity {c_e:+.2f}")
        else:
            c_e = pd.concat([capped.stack(), ew_unit[SECTORS].stack()], axis=1).dropna().corr()
            print(f"   corr with EWMAC on the sectors (pooled) {c_e.iloc[0, 1]:+.2f}")
        sd[name] = float(capped.loc[:SPLIT, cols].stack().std())

    print("\nSUMMARY: held-out Pearson IC t of the capped score, by horizon (rule: >= 2 at one)")
    print(pd.DataFrame(summary).T.to_string(float_format="{:+.3f}".format))

    print("\nmarket-wide scores (capped) around lows and peaks: value at the date, and its "
          "max over the 63 days after")
    rows = {}
    for d, lab in LOWS.items():
        i = idx.searchsorted(pd.Timestamp(d))
        rows[f"{lab} {d}"] = {n: f"{p[n].clip(-CAP, CAP).iloc[i]:+.1f} / "
                                 f"{p[n].clip(-CAP, CAP).iloc[i:i + 63].max():+.1f}"
                              for n in MARKET}
    print(pd.DataFrame(rows).T.to_string())
    return sd


# --- 4. book ---------------------------------------------------------------------------------

def run_book(args):
    name, alpha, k, cols = args
    b = core.load()
    warm = core.warmup(b, core.build())
    alphas = dict(core.ALPHAS)
    if alpha is not None:
        p = pd.read_pickle(PANELS)
        panel = pd.DataFrame(np.nan, index=b.index, columns=b.prices.columns)
        sc = panel_frame(p, alpha, cols, b.index).clip(-CAP, CAP)
        for t in cols:
            panel[t] = sc[t]
        b = DataBundle({**b, "insider": panel})
        alphas["insider"] = (PanelScore("insider", normalizer=Identity()), k)
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(b, core.build(alphas=alphas))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return name, r


def book_report(sd: dict[str, float]) -> None:
    jobs = [("saved", None, 0.0, [])]
    for n in MARKET:
        k = 0.091 * sd["vrp"] / sd[n]
        jobs.append((f"{n} k={k:.3f}", n, k, US_EQ))
        if n == "NPR 63":
            jobs += [(f"{n} k={k * m:.3f} ({m}x)", n, k * m, US_EQ) for m in (0.5, 2.0)]
            jobs.append((f"{n} k={k:.3f}, all 12 equity", n, k, core.EQUITY))
    for n in SECTOR:
        k = 0.037 * sd["ewmac"] / sd[n]
        jobs.append((f"{n} k={k:.3f}", n, k, SECTORS))
        if n == "sector NPR 63 rel":
            jobs += [(f"{n} k={k * m:.3f} ({m}x)", n, k * m, SECTORS) for m in (0.5, 2.0)]
    with ProcessPoolExecutor(7) as pool:
        runs = dict(pool.map(run_book, jobs))
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    m = m.loc[[j[0] for j in jobs]]
    r0 = runs["saved"]
    ex = pd.DataFrame({n: runs[n] - r0 for n in runs if n != "saved"})
    m["excess t"] = [np.nan] + [float(ex[n].mean() / ex[n].std() * np.sqrt(len(ex[n])))
                                for n in m.index[1:]]
    print(f"\n4. book with overlays, {r0.index[0].date()} .. {r0.index[-1].date()}, split "
          f"{SPLIT.date()}; PORTFOLIO fixed; score NaN after {DATA_END.date()}")
    print(m.to_string(float_format="{:.3f}".format))


def main() -> None:
    pd.set_option("display.width", 240)
    pd.set_option("display.max_rows", 400)
    b = core.load()
    p = build_panels(b.index)
    pd.to_pickle(p, PANELS)
    sd = ic_report(b, p)
    book_report(sd)


if __name__ == "__main__":
    main()
