"""Does trend predict returns once tested across many assets instead of one?

On QQQ alone the trend score showed no reliable IC. One asset over 26 years has about 300
independent months, too few to see a small IC; the published evidence for time-series
momentum rests on breadth. This tests the same :class:`~quantzy.signals.TrendStrength` score
on 17 ETFs across asset classes, then trades it.

1. **Time-series IC.** Per asset, the correlation over time of the score with the asset's own
   forward return, pooled across assets with a t-stat clustered by date and corrected for
   overlapping windows (:func:`~quantzy.research.pooled_time_series_ic`). Reported for
   rank and Pearson, raw and vol-scaled forward returns, several MA types, windows and
   horizons, by asset class and per asset. The MA spec is chosen on the train window.
2. **Portfolio.** One rule for every asset, with no per-asset tuning:

       mu_i = vol_i * (SR + k * trend_i)

   A constant Sharpe prior ``SR`` (the same for every asset, so the prior alone is a
   risk-balanced book) plus a trend tilt in the same risk units. ``vol_i`` is the asset's
   trailing 60-day realized vol. The risk model is the shared ``ewma_blend((5, 20, 60))``,
   with no trend in it. ``k`` and risk aversion are chosen on the train window.

Cash is a T-bill price compounded from ``^IRX``, as in ``research_qqq_timing.py``.

Usage:
    uv run python research/research_trend_breadth.py [--workers 8]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import FixedWeights, WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare, ic_summary, pooled_time_series_ic, time_series_ic
from quantzy.risk import ewma_blend
from quantzy.signals import Identity, SharpePrior, TrendStrength, VolScaledTrend
from quantzy.signals.base import Signal
from quantzy.strategies import core_multiasset as core
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

CASH = "TBILL"
CLASSES = {
    "US sectors": ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"],
    "equity index": ["QQQ", "EFA", "EEM"],
    "Treasuries": ["IEF", "TLT"],
    "gold & commodities": ["GLD", "DBC"],
    "US dollar": ["UUP"],
}
RISKY = [t for ts in CLASSES.values() for t in ts]
def asset_class(t: str) -> str:
    return next(c for c, ts in CLASSES.items() if t in ts)


UNIVERSE = Universe(
    name="trend_breadth",
    assets=[Asset(t, t, "etf") for t in RISKY]
    + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")],
)
START = "1998-12-22"  # the sector SPDRs' first day

IC_SPLIT = pd.Timestamp("2013-07-31")  # midpoint of the IC sample
IC_SPECS = [(ma, w) for ma in ("sma", "ema") for w in (50, 100, 200)]
IC_HORIZONS = (21, 63, 126)
IC_VOL_WINDOW = 60
EWM_TOL = 0.05  # seed influence an EMA may keep at its first trusted row; 1e-9 needs 8 years

COST_BPS, COST_AVERSION, RISK_AVERSION = 5.0, 12.0, 1.0
# Long-run Sharpe priors, set by judgment rather than fitted: equities earn the most per unit
# of risk, Treasuries a little less, gold and commodities less again (commodity futures lose
# roll yield), and a long dollar position has no premium at all.
PRIORS = {
    "flat 0.3": {t: 0.3 for t in RISKY},
    "by class": {t: {"US sectors": 0.3, "equity index": 0.3, "Treasuries": 0.25,
                     "gold & commodities": 0.15 if t == "DBC" else 0.2,
                     "US dollar": 0.0}[asset_class(t)] for t in RISKY},
}
BASELINE = {"prior": ["flat 0.3", "by class"], "cap": [None, 0.2], "vol": [0.08, 0.10, 0.12]}
K_GRID = [0.0, 0.1, 0.2, 0.3]
TURNOVER = {
    "hls": [(5, 20, 60), (20, 60), (60,)],
    # (hard vol limit, risk aversion): a hard 8% limit, or no limit and a risk aversion that
    # lands near 8% on average.
    "sizing": [(0.08, 1.0), (None, 2.0), (None, 3.0)],
    "ca": [12.0, 24.0, 48.0],
}


def load() -> DataBundle:
    raw = CachedReader(YFinanceReader()).history([*RISKY, "^IRX"], start=START)
    yld = raw["^IRX"].ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    prices = raw[RISKY].copy()
    prices[CASH] = (1.0 + cash_ret).cumprod()
    return DataBundle.build(prices)


def trend(ma: str, window: int) -> TrendStrength:
    return TrendStrength(window, exclude=(CASH,), ma=ma, ewm_tol=EWM_TOL, normalizer=Identity())


# --- 1. IC -------------------------------------------------------------------------------------

def tradeable_scores(signal: Signal, bundle: DataBundle) -> pd.DataFrame:
    """Scores with NaN wherever the signal was not yet trustworthy for that asset.

    ``Signal.scores`` fills those cells with 0, which is right for a book (no view) but would
    count as real zero-score observations in an IC.
    """
    px = bundle.prices[RISKY]
    rows_live = px.notna().cumsum()  # rows of history each asset has at each date
    ok = (rows_live > signal.min_history).shift(signal.lag, fill_value=False)
    return signal.scores(bundle)[RISKY].where(ok)


def ic_cell(scores: pd.DataFrame, prices: pd.DataFrame, h: int, method: str,
            vol: int | None, period: slice) -> str:
    c = pooled_time_series_ic(scores, prices, h, method, vol).loc[period]
    s = ic_summary(c, h)
    return f"{s.mean_ic:+.3f} ({s.t_stat:+.1f})"


def ic_report(bundle: DataBundle) -> tuple[str, int]:  # (ma, window)
    px = bundle.prices[RISKY]
    train, test = slice(None, IC_SPLIT), slice(IC_SPLIT, None)
    panels = {spec: tradeable_scores(trend(*spec), bundle) for spec in IC_SPECS}

    print("\n1a. POOLED TIME-SERIES IC, all 17 assets: mean ic (t, clustered by date and "
          "overlap-corrected)")
    print(f"    vol-scaled = forward return / trailing {IC_VOL_WINDOW}d vol; split "
          f"{IC_SPLIT.date()}")
    rows = {}
    for (ma, w), h in itertools.product(IC_SPECS, IC_HORIZONS):
        s = panels[(ma, w)]
        rows[(f"{ma}{w}", h)] = {
            ("rank, vol-scaled", "train"): ic_cell(s, px, h, "spearman", IC_VOL_WINDOW, train),
            ("rank, vol-scaled", "test"): ic_cell(s, px, h, "spearman", IC_VOL_WINDOW, test),
            ("pearson, vol-scaled", "train"): ic_cell(s, px, h, "pearson", IC_VOL_WINDOW, train),
            ("pearson, vol-scaled", "test"): ic_cell(s, px, h, "pearson", IC_VOL_WINDOW, test),
            ("pearson, raw", "train"): ic_cell(s, px, h, "pearson", None, train),
            ("pearson, raw", "test"): ic_cell(s, px, h, "pearson", None, test),
        }
    tab = pd.DataFrame(rows).T
    tab.index.names = ["spec", "h"]
    print(tab.to_string())

    # Choose the spec and horizon on train rank IC, vol-scaled: the most robust of the three.
    score = {(spec, h): ic_summary(pooled_time_series_ic(
        panels[spec], px, h, "spearman", IC_VOL_WINDOW).loc[train], h).t_stat
        for spec, h in itertools.product(IC_SPECS, IC_HORIZONS)}
    (best, best_h) = max(score, key=score.get)
    print(f"\n    Best on train (rank, vol-scaled t-stat): {best[0]}{best[1]} at h={best_h}")

    s = panels[best]
    print(f"\n1b. BY ASSET CLASS, {best[0]}{best[1]}, rank IC vol-scaled (t)")
    rows = {}
    for cls, ts in CLASSES.items():
        rows[cls] = {f"h={h} {p}": ic_cell(s[ts], px[ts], h, "spearman", IC_VOL_WINDOW, sl)
                     for h in IC_HORIZONS for p, sl in (("train", train), ("test", test))}
    print(pd.DataFrame(rows).T.to_string())

    print(f"\n1c. PER ASSET, {best[0]}{best[1]}, h={best_h}: IC over each asset's full history")
    per = pd.DataFrame({
        "class": [asset_class(t) for t in RISKY],
        "rank vol-scaled": time_series_ic(s, px, best_h, "spearman", IC_VOL_WINDOW),
        "pearson vol-scaled": time_series_ic(s, px, best_h, "pearson", IC_VOL_WINDOW),
        "pearson raw": time_series_ic(s, px, best_h, "pearson", None),
        "years": s.notna().sum() / TRADING_DAYS,
    }, index=RISKY)
    print(per.to_string(float_format="{:+.3f}".format,
                        formatters={"years": "{:.1f}".format}))
    print(f"    positive rank IC on {int((per['rank vol-scaled'] > 0).sum())} of {len(per)} "
          "assets")
    return best


# --- 2. portfolio ------------------------------------------------------------------------------

BORROW_SPREAD_BPS = core.PORTFOLIO.borrow_spread_bps

#: The selected book's settings, taken from the saved strategy
#: (:mod:`quantzy.strategies.core_multiasset`) so research and production cannot drift. The
#: research grids above override individual keys.
_P = core.PORTFOLIO
PORTFOLIO = {"ra": _P.risk_aversion, "lev": _P.max_leverage, "cap": _P.max_position,
             "cap_of": _P.position_cap_of, "ca": _P.cost_aversion,
             "hls": tuple(_P.risk_halflives), "vol": None}
ALPHA = {"prior": "by class", "spec": ("ema", 200), "k": 0.1}  # EMA-200 as the only trend
SELECTED = {**PORTFOLIO, **ALPHA}


def key(p: dict) -> str:
    cap = "no cap" if p["cap"] is None else f"cap {p['cap']:.0%}"
    if p["cap"] is not None and p.get("cap_of") == "book":
        cap += " of book"
    risk = "vol " + (f"{p['vol']:.0%}" if p.get("vol") else f"soft ra={p['ra']:g}")
    extra = ""
    if "hls" in p:
        extra = f" | hl {'/'.join(map(str, p['hls']))} | ca={p['ca']:g}"
    lev = f" | lev {p['lev']:g}" if p.get("lev", 1.0) != 1.0 else ""
    return f"{p['prior']} | {cap} | {risk}{extra}{lev} | k={p['k']:g}"


def build(p: dict) -> Strategy:
    ma, w = p["spec"]
    signals = {"prior": SharpePrior(PRIORS[p["prior"]], normalizer=Identity())}
    if p["k"]:
        signals["trend"] = VolScaledTrend(w, exclude=(CASH,), ma=ma, ewm_tol=EWM_TOL,
                                    normalizer=Identity())
    cons = [C.budget(), C.long_only(exclude=(CASH,)), C.max_leverage(p.get("lev", 1.0), CASH)]
    if p.get("vol"):  # a hard volatility limit; without one, risk aversion alone sets the size
        cons.append(C.max_vol(p["vol"]))
    if p["cap"] is not None:  # a share of capital, or of the invested book (grows with leverage)
        cap = C.max_book_share if p.get("cap_of") == "book" else C.max_weight
        cons.append(cap(p["cap"], exclude=(CASH,)))
    return Strategy(
        universe=UNIVERSE,
        signals=signals,
        weights={"trend": p["k"]} if p["k"] else {},
        risk_model=ewma_blend(p.get("hls", (5, 20, 60))),
        optimizer=MeanVariance(
            risk_aversion=p.get("ra", RISK_AVERSION),
            constraints=cons,
            cost_model=LinearCost(COST_BPS),
            cost_aversion=p.get("ca", COST_AVERSION),
        ),
        name=key(p),
    )


def warmup(bundle: DataBundle, spec: tuple[str, int]) -> int:
    """First row at which every asset has enough history for the signals and the risk model."""
    need = max(trend(*spec).min_history, 250) + 2
    first = bundle.prices[RISKY].apply(lambda s: bundle.index.get_loc(s.first_valid_index()))
    return int(first.max()) + need


def run_one(p: dict) -> tuple[str, pd.Series, pd.DataFrame, float, float]:
    bundle = load()
    res = WalkForwardBacktester(rebalance_every=1, warmup=warmup(bundle, p["spec"])).run(
        bundle, build(p))
    borrowed = (-res.weights[CASH]).clip(lower=0)
    returns = res.returns - borrowed * BORROW_SPREAD_BPS / 1e4 / TRADING_DAYS
    years = len(returns) / TRADING_DAYS
    days = sum(d.turnover > 1e-3 for d in res.decisions) / years
    turnover = sum(d.turnover for d in res.decisions) / years
    return key(p), returns, res.weights, days, turnover


def run_all(params: list[dict], workers: int) -> dict[str, tuple]:
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return {k: (r, w, d, t) for k, r, w, d, t in pool.map(run_one, params)}


def table(out: dict[str, tuple], bench: dict[str, pd.Series], split: pd.Timestamp) -> pd.DataFrame:
    runs = {k: v[0] for k, v in out.items()} | bench
    t = compare(runs, split_date=split)
    m = pd.DataFrame({
        "tr_ret": t["train"]["ann_return"], "tr_vol": t["train"]["ann_vol"],
        "tr_sharpe": t["train"]["sharpe"], "tr_dd": t["train"]["max_dd"],
        "te_ret": t["test"]["ann_return"], "te_vol": t["test"]["ann_vol"],
        "te_sharpe": t["test"]["sharpe"], "te_dd": t["test"]["max_dd"],
        "full_ret": t["full"]["ann_return"], "full_sharpe": t["full"]["sharpe"],
        "full_dd": t["full"]["max_dd"],
    })
    m["avg_cash"] = [out[k][1][CASH].mean() if k in out else 0.0 for k in m.index]
    m["max_asset"] = [out[k][1][RISKY].mean().max() if k in out else 1 / len(RISKY)
                      for k in m.index]
    m["trade_days"] = [out[k][2] if k in out else np.nan for k in m.index]
    m["turnover"] = [out[k][3] if k in out else np.nan for k in m.index]
    return m


FMT = {c: "{:.1%}".format for c in ("tr_ret", "tr_vol", "tr_dd", "te_ret", "te_vol", "te_dd",
                                    "full_ret", "full_dd", "avg_cash", "max_asset")}
FMT |= {c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "full_sharpe")}
FMT |= {"trade_days": "{:.0f}".format, "turnover": "{:.1f}".format}


def by_class(weights: pd.DataFrame) -> pd.DataFrame:
    w = weights[RISKY]
    cls = w.T.groupby([asset_class(t) for t in RISKY]).sum().T
    cls[CASH] = weights[CASH]
    return pd.DataFrame({"avg": cls.mean(), "min": cls.min(), "max": cls.max()})


def portfolio_report(bundle: DataBundle, spec: tuple[str, int], workers: int) -> None:
    warm = warmup(bundle, spec)
    cost = LinearCost(COST_BPS)
    ew = WalkForwardBacktester(rebalance_every=21, warmup=warm).run(
        bundle, FixedWeights(UNIVERSE, holdings=RISKY, cost_model=cost)).returns
    bench = {"equal weight 17, monthly": ew}
    split = ew.index[len(ew.index) // 2]
    print(f"\n2. PORTFOLIO, {ew.index[0].date()} .. {ew.index[-1].date()}, train before "
          f"{split.date()} | mu = vol * (SR + k * trend {spec[0]}{spec[1]}) | risk "
          f"ewma_blend(5, 20, 60) | max_vol constraint, ra={RISK_AVERSION:g} | cost "
          f"{COST_BPS:g}bp, ca={COST_AVERSION:g}")
    print("   priors (SR): " + "; ".join(
        f"{c} {PRIORS['by class'][ts[0]]:g}" for c, ts in CLASSES.items()) + " (DBC 0.15)")

    # --- 2a. the baseline: prior, cap and vol target, no trend ---
    base = [{"spec": spec, "k": 0.0, "prior": pr, "cap": cap, "vol": v}
            for pr, cap, v in itertools.product(*BASELINE.values())]
    out = run_all(base, workers)
    m = table(out, bench, split)
    print("\n2a. BASELINE, no trend")
    print(m.to_string(formatters=FMT))
    # The baseline is a judgment call (priors) plus two risk limits; pick on train Sharpe among
    # the capped books, since an uncapped book's concentration is itself the problem.
    capped = [key(p) for p in base if p["cap"] is not None and p["prior"] == "by class"]
    win = m.loc[capped, "tr_sharpe"].idxmax()
    bp = next(p for p in base if key(p) == win)
    print(f"\n   Baseline: {win}")
    print(by_class(out[win][1]).to_string(float_format="{:.0%}".format))

    # --- 2b. trend on top of the baseline ---
    tr = [{**bp, "k": k} for k in K_GRID]
    out |= run_all([p for p in tr if key(p) not in out], workers)
    m = table({key(p): out[key(p)] for p in tr}, bench, split)
    print("\n2b. TREND ON THE BASELINE, k in Sharpe units (trend +-1 moves SR by +-k)")
    print(m.to_string(formatters=FMT))

    # --- 2c. turnover: slower risk, soft vs hard vol limit, cost aversion ---
    tv = [{**bp, "hls": hls, "vol": v, "ra": ra, "ca": ca}
          for hls, (v, ra), ca in itertools.product(TURNOVER["hls"], TURNOVER["sizing"],
                                                    TURNOVER["ca"])]
    out |= run_all([p for p in tv if key(p) not in out], workers)
    m = table({key(p): out[key(p)] for p in tv}, bench, split)
    print("\n2c. TURNOVER FIXES on the baseline (no trend): risk-model halflives, hard vol "
          "limit vs soft (risk aversion only), cost aversion")
    print(m.to_string(formatters=FMT))
    # The risk model stays the shared one, chosen on forecast accuracy (5/20/60); the slower
    # ones are shown only to see how much of the churn they explain. Among the shared-model
    # runs, keep turnover down without paying for it: the lowest turnover within 0.03 of the
    # best train Sharpe.
    shared = [key(p) for p in tv if p["hls"] == (5, 20, 60)]
    good = m.loc[shared]
    good = good[good["tr_sharpe"] >= good["tr_sharpe"].max() - 0.03]
    win2 = good["turnover"].idxmin()
    tp = next(p for p in tv if key(p) == win2)
    print(f"\n   Pick (lowest turnover within 0.03 of the best train Sharpe): {win2}")
    print(by_class(out[win2][1]).to_string(float_format="{:.0%}".format))

    tr2 = [{**tp, "k": k} for k in K_GRID]
    out |= run_all([p for p in tr2 if key(p) not in out], workers)
    m = table({key(p): out[key(p)] for p in tr2}, bench, split)
    print("\n2d. TREND ON THE LOW-TURNOVER BASELINE")
    print(m.to_string(formatters=FMT))


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    bundle = load()
    print(f"\n17 risky ETFs + {CASH}, {bundle.index[0].date()} .. {bundle.index[-1].date()}; "
          "each asset enters at its own inception")
    best = ic_report(bundle)
    portfolio_report(bundle, best, workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    main(ap.parse_args().workers)
