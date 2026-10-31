"""Tail treatment of the saved book's alpha scores: is the +/-2 cap the robust choice?

Three of the four alphas cap their unit score at +/-2 (EWMAC's forecast, the VRP z-score, the
dollar value z-score); trend is a ``tanh``, bounded in (-1, 1). The caps were set by
convention, not tested. For each alpha, on the uncapped unit score (the score before the
times-vol):

1. The distribution: moments, quantiles, mass beyond 2 and 3.
2. Pooled time-series Pearson IC of: no cap, clip +/-2 (saved), clip +/-3, and a causal
   winsorize at the 1st / 99th percentiles (each asset's expanding history). Next-day return
   over 60-day vol, 47-day interleaved folds, Newey-West t; also 21 days, and 252 days for
   the dollar (its earlier study's horizon). A rank IC is printed as a check only: it cannot
   see a monotone tail treatment.
3. For any alpha where another treatment passes the rule below, the book with it, ``PORTFOLIO``
   and the overlays unchanged.

Pre-specified rule: keep the saved treatment unless another has a higher held-out t *and* a
worst five-year period no lower, at the 1-day horizon. A gain on all dates alone does not count.

**Result (2026-10-31): keep every saved treatment.** By alpha, 1-day held-out Pearson IC t:

* VRP is the one with real tails: excess kurtosis 63, a left tail to -21 (realized variance
  spiking past the VIX in crashes), 2.6% of days beyond -3. Uncapped, its IC is noise (t 0.15,
  worst period -0.084); the saved +/-2 cap is what makes it a signal (t 0.67, worst -0.021).
  +/-3 and winsor 1/99 come close at 1 day but have worse periods, and at 21 days +/-2 is
  clearly best (t 1.58 against 1.29 and 1.19).
* EWMAC has mild tails (excess kurtosis 0.5, 4.9% beyond 2). The saved +/-2 has the best
  held-out t at 1 and 21 days (0.61 against 0.42-0.48 at 1 day).
* Trend is a ``tanh``, so the caps do nothing; it piles up near +/-1 (excess kurtosis -0.9),
  saturation rather than tails. Winsorizing changes nothing (t 0.04 either way).
* The dollar is the exception: thin tails (excess kurtosis -0.8), but +/-2 clips 9% of days,
  mostly an expensive dollar. No cap, +/-3 and winsor 1/99 each pass the rule at 1 day (held-
  out t 1.83-1.89 against 1.81), on one asset. In the book all three lower Sharpe a little
  (1.334-1.337 against 1.340): noise, so the cap stays.

Rank IC moved by less than 0.001 for every treatment of every alpha, as expected.

Usage:
    uv run python research/research_tail_treatment.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.research import compare, compare_tail_treatments, describe_scores, tail_treatments
from quantzy.signals import EwmacTrend, VarianceRiskPremium
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays

SPLIT = pd.Timestamp("2017-07-26")
SAVED = {"trend": "none", "ewmac": "clip +/-2", "vrp": "clip +/-2", "dollar_value": "clip +/-2"}
HORIZONS = {"trend": (1, 21), "ewmac": (1, 21), "vrp": (1, 21), "dollar_value": (1, 21, 252)}


def uncapped_dollar_value(dxy: pd.Series) -> pd.Series:
    """``core.dollar_value_score`` without its +/-2 clip."""
    dxy = dxy.ffill()
    x = -np.log(dxy / dxy.rolling(core.VALUE_WINDOW, min_periods=core.VALUE_WINDOW).mean())
    r = x.rolling(core.VALUE_Z_WINDOW, min_periods=core.VALUE_WINDOW)
    return (x - r.mean()) / r.std()


def unit_scores(bundle: DataBundle) -> dict[str, pd.DataFrame]:
    """Each alpha's uncapped score before the times-vol, NaN until the asset has warmed up."""
    px = bundle.prices[core.RISKY]
    b = DataBundle.build(px, vix=bundle["vix"][core.RISKY], market=bundle["market"][core.RISKY])
    trend = core.ALPHAS["trend"][0]
    ewmac = EwmacTrend(64, cap=np.inf, ewm_tol=core.EWM_TOL)
    vrp = VarianceRiskPremium(tuple(core.EQUITY), cap=np.inf)
    rows_live = px.notna().cumsum()
    out = {}
    for name, sig, warm in (("trend", trend, trend.min_history),
                            ("ewmac", ewmac, ewmac.min_history),
                            ("vrp", vrp, vrp.asset_history)):
        vol = realized_vol(px, sig.vol_window, annualize=True)
        out[name] = (sig.raw(b)[core.RISKY] / vol.where(vol > 0)).where(rows_live > warm)
    # load() keeps only the clipped score, so read DXY again (same cached request as load()).
    dxy = CachedReader(YFinanceReader()).history(
        [*core.RISKY, "^IRX", "^VIX", "SPY", *core.CARRY_YIELDS.values(), core.DOLLAR_INDEX,
         core.HEDGE], start=core.START)[core.DOLLAR_INDEX].reindex(px.index)
    dv = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
    dv["UUP"] = uncapped_dollar_value(dxy)
    out["dollar_value"] = dv
    return out


def book_with_dollar(variant: str) -> tuple[str, pd.Series]:
    """The saved book and overlays, with UUP's dollar value score under ``variant``."""
    b = core.load()
    if variant != SAVED["dollar_value"]:
        dv = tail_treatments(unit_scores(b)["dollar_value"][["UUP"]])[variant]
        panel = b["dollar_value"].copy()
        panel["UUP"] = dv["UUP"]
        b = DataBundle({**b, "dollar_value": panel})
    s = core.build()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return variant, r


def book_report() -> None:
    names = ["clip +/-2", "none", "clip +/-3", "winsor 1/99"]
    with ProcessPoolExecutor(len(names)) as pool:
        runs = dict(pool.map(book_with_dollar, names))
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "vol": t["full"]["ann_vol"],
                      "max_dd": t["full"]["max_dd"]}).loc[names]
    m.index = [f"{i} (saved)" if i == SAVED["dollar_value"] else i for i in m.index]
    print("\n=== 3. book with overlays, dollar value under each treatment, PORTFOLIO fixed")
    print(m.to_string(float_format="{:.3f}".format))


def main() -> None:
    bundle = core.load()
    px = bundle.prices[core.RISKY]
    pd.set_option("display.width", 200)
    for name, unit in unit_scores(bundle).items():
        cols = [c for c in unit.columns if unit[c].notna().any()]
        unit = unit[cols]
        live = unit.notna().any(axis=1)
        print(f"\n=== {name}: {len(cols)} assets, {live.idxmax().date()} .. "
              f"{unit.index[-1].date()}, saved treatment: {SAVED[name]}")
        print("1. distribution of the uncapped unit score")
        print(describe_scores(unit).round(3).to_string())
        variants = {k: v.shift(1) for k, v in tail_treatments(unit).items()}
        for h in HORIZONS[name]:
            print(f"\n2. Pearson IC, h={h}")
            t = compare_tail_treatments(variants, px[cols], horizon=h)
            t.index = [f"{i} (saved)" if i == SAVED[name] else i for i in t.index]
            print(t.to_string(float_format="{:+.4f}".format))
        r = compare_tail_treatments(variants, px[cols], horizon=1, method="spearman")
        print("   rank IC h=1 (check only): "
              + ", ".join(f"{k} {v:+.4f}" for k, v in r["ic"].items()))
    book_report()


if __name__ == "__main__":
    main()
