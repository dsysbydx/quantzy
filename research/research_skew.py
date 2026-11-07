"""Skew forecast (the user's Pine script, 2026-11-05): does negative skew predict returns here?

The idea, from the user's TradingView script: Carver-style skew forecasts over 60, 120 and 240
days with forecast scalars 21.61, 23.01 and 22.70, capped at +/-20 (Carver's units; /10 here).
Assets whose recent returns are negatively skewed are paid for bearing crash risk, so the rule
leans toward them. The script's lines that compute skew were not shared, so the forecast is
Carver's ``skewabs``, fixed before running:

    f_N    = clip(scalar_N / 10 * -skew_N, +/-2)       skew_N: rolling skew of daily returns
    f      = mean(f_60, f_120, f_240)                  (no diversification multiplier)
    score  = f * annualized 60-day vol                 (return units, like ``EwmacTrend``)

Two candidates, both pre-specified:

* ``skew abs`` -- as above, the user's rule.
* ``skew demeaned`` -- ``-(skew_N - its own expanding mean)``, the same scalars. Equities are
  negatively skewed almost always, so ``skew abs`` is mostly a standing overweight of equities
  (a prior shift, which the class priors already set); demeaning keeps only the timing.

Breadth (the same script's high-low index, MAHN/MALN, HIGN/LOWN, ...) waits on the user's
exchange new-high/new-low data; yfinance has none.

Steps (``research/README.md``, "Exploring a new signal"):

1. IC of the unit score (``f``, before the times-vol): pooled time-series Pearson IC vs next-day
   return over 60-day vol, 47-day interleaved folds, Newey-West t; 21 and 63 days; by window,
   by class, by five-year period.
2. Distribution of the uncapped score, and the tail treatments.
3. Book: the saved book and overlays plus the skew alpha, ``PORTFOLIO`` fixed. Weight ``k``
   gives the score the same train-window dispersion as EWMAC's at 0.037 (half the trend
   budget), plus 0.5x and 2x that for sensitivity.

Adoption rule (fixed before running): held-out t >= 2 at 1 or 21 days, positive in at least
4 of the five-year periods, *and* in the book at the matched ``k`` a full-period Sharpe gain of
at least +0.02 with neither train nor test half lower.

**Result (2026-11-05, data to 2026-11-04): fails; not adopted.** Neither candidate passes
the IC bar, and both lower the book's Sharpe at every weight tried.

* IC, ``skew abs`` with the +/-2 cap: held-out t 1.58 at 1 day, 1.84 at 21 days, 1.49 at 63;
  positive in 6 / 5 / 5 of 7 periods. The 120-day window alone reaches t 2.06 at 1 day, one of
  six window-horizon cells, after the fact. By class the 21-day IC is carried by Treasuries
  (held-out t 2.61) and gold and commodities (1.52); equities, most of the book, have ~0
  (t 0.75 sectors, 0.80 indexes). ``skew demeaned`` is weaker everywhere (t 1.08 / 1.28).
* Distribution: mild tails (excess kurtosis 3.7, 0.9% beyond 3, max 10). The +/-2 cap is as
  good as any treatment. Mean ``|f|`` 0.45-0.99 by asset against Carver's target of 1: the
  user's scalars run a little low on these ETFs. ``skew abs`` is a standing tilt (mean f
  +0.28, most toward DBC, GLD, EFA, XLU), as expected. +0.25-0.29 correlated with EWMAC.
* Book with overlays, Sharpe 1.433 saved (train 1.41 / test 1.46): ``skew abs`` 1.37 / 1.31 /
  1.24 at 0.5x / 1x / 2x the matched k = 0.043, ``skew demeaned`` 1.38 / 1.32 / 1.28. Test
  falls more than train (1.46 to 1.25 at the matched k) and the excess is negative in every
  five-year block for ``skew abs``. The modest 21-day IC on Treasuries and commodities does
  not survive mean-variance sizing next to trend and the priors.

Usage:
    uv run python research/research_skew.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.data import DataBundle
from quantzy.research import (
    compare,
    compare_tail_treatments,
    describe_scores,
    tail_treatments,
)
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import realized_vol
from quantzy.signals.normalize import Normalizer
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays

SPLIT = pd.Timestamp("2017-07-26")
#: The user's Pine forecast scalars, Carver units (average |forecast| ~10).
SCALARS = {60: 21.6092248822864, 120: 23.014133310933, 240: 22.6957696605146}
DEMEAN_MIN = 252  # rows of skew history before the expanding mean is trusted


@dataclass
class SkewForecast(Signal):
    """Carver's skew forecast, ``mean_N clip(scalar_N / 10 * -skew_N, +/-cap)`` x annual vol."""

    name: ClassVar[str] = "skew"

    windows: tuple[int, ...] = (60, 120, 240)
    demean: bool = False
    cap: float = 2.0
    vol_window: int = 60
    exclude: tuple[str, ...] = ()
    normalizer: Normalizer = field(default_factory=Identity)

    def unit(self, px: pd.DataFrame) -> pd.DataFrame:
        r = px.pct_change(fill_method=None)
        parts = []
        for n in self.windows:
            sk = r.rolling(n, min_periods=n).skew()
            if self.demean:
                sk = sk - sk.expanding(min_periods=DEMEAN_MIN).mean()
            parts.append((-sk * SCALARS[n] / 10.0).clip(-self.cap, self.cap))
        f = sum(parts) / len(parts)
        f[[c for c in self.exclude if c in f.columns]] = np.nan
        return f

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        return self.unit(px) * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return max(self.windows) + 1 + (DEMEAN_MIN if self.demean else 0)


CANDIDATES = {"skew abs": False, "skew demeaned": True}


# --- 1-2. IC and distribution --------------------------------------------------------------------

def ic_report(bundle: DataBundle) -> dict[str, float]:
    px = bundle.prices[core.RISKY]
    sd = {}
    for name, dm in CANDIDATES.items():
        sig = SkewForecast(demean=dm, cap=np.inf)
        unit = sig.unit(px)
        live = unit.notna().any(axis=1)
        print(f"\n=== {name}: {live.idxmax().date()} .. {unit.index[-1].date()}")
        print("distribution of the uncapped unit score (pooled, then mean |f| per asset)")
        print(describe_scores(unit).round(3).to_string())
        print("   mean |f| by asset: " + ", ".join(
            f"{t} {v:.2f}" for t, v in unit.abs().mean().items()))
        print("   mean f by asset:   " + ", ".join(
            f"{t} {v:+.2f}" for t, v in unit.mean().items()))
        variants = {k: v.shift(1) for k, v in tail_treatments(unit).items()}
        for h in (1, 21, 63):
            print(f"\nPearson IC, h={h} ('clip +/-2' is the rule as specified)")
            print(compare_tail_treatments(variants, px, horizon=h)
                  .to_string(float_format="{:+.4f}".format))
        capped = sig.unit(px).clip(-2, 2)
        print("\nby window, clip +/-2, h=1 and h=21")
        for n in sig.windows:
            one = SkewForecast(windows=(n,), demean=dm).unit(px).shift(1)
            for h in (1, 21):
                t = compare_tail_treatments({f"{n}d": one}, px, horizon=h)
                row = t.iloc[0]
                print(f"   {n:>3}d h={h:<2}: ic {row['ic']:+.4f} (t {row['t']:+.2f}), held-out "
                      f"{row['held_out']:+.4f} (t {row['held_out_t']:+.2f}), periods "
                      f"{int(row['periods_pos'])}/{int(row['n_periods'])}")
        print("\nby class, clip +/-2, h=21")
        lagged = capped.shift(1)
        for c, ts in core.CLASSES.items():
            row = compare_tail_treatments({c: lagged[ts]}, px[ts], horizon=21).iloc[0]
            print(f"   {c:<20} ic {row['ic']:+.4f} (t {row['t']:+.2f}), held-out t "
                  f"{row['held_out_t']:+.2f}")
        sd[name] = float(sig.unit(px).clip(-2, 2).loc[:SPLIT].stack().std())

    ew = core.ALPHAS["ewmac"][0]
    vol = realized_vol(px, ew.vol_window, annualize=True)
    ew_unit = (ew.raw(DataBundle.build(px)) / vol.where(vol > 0))
    sd["ewmac"] = float(ew_unit.loc[:SPLIT].stack().std())
    # Correlation with the saved trend scores: is skew just (minus) trend?
    for name, dm in CANDIDATES.items():
        u = SkewForecast(demean=dm).unit(px)
        c = pd.concat([u.stack(), ew_unit.stack()], axis=1).dropna().corr().iloc[0, 1]
        print(f"\ncorrelation, {name} vs EWMAC 64/256 unit score (pooled): {c:+.2f}")
    return sd


# --- 3. book ---------------------------------------------------------------------------------

def run_book(args: tuple[str, dict]) -> tuple[str, pd.Series]:
    name, alphas = args
    b = core.load()
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, core.build())).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    for ov in overlays(core):
        r = r + ov.backtest_returns(b).reindex(r.index).fillna(0.0)
    return name, r


def book_report(sd: dict[str, float]) -> None:
    k0 = core.ALPHAS["ewmac"][1] * sd["ewmac"]
    jobs = {"saved": dict(core.ALPHAS)}
    for name, dm in CANDIDATES.items():
        k = k0 / sd[name]
        sig = SkewForecast(demean=dm, exclude=(core.CASH,))
        for m in (0.5, 1.0, 2.0):
            jobs[f"{name} k={k * m:.3f}" + (" (matched)" if m == 1 else "")] = {
                **core.ALPHAS, "skew": (sig, k * m)}
    with ProcessPoolExecutor(min(len(jobs), 7)) as pool:
        runs = dict(pool.map(run_book, jobs.items()))
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]}).loc[list(jobs)]
    r0 = runs["saved"]
    m["corr_excess"] = [np.nan] + [
        float((runs[n] - r0).corr(r0)) for n in list(jobs)[1:]]
    first = r0.index[0].date()
    print(f"\n=== 3. book with overlays, {first} .. {r0.index[-1].date()}, split {SPLIT.date()}, "
          f"PORTFOLIO fixed; k matched to EWMAC's train dispersion")
    print(m.to_string(float_format="{:.3f}".format))
    ex = pd.DataFrame({n: runs[n] - r0 for n in jobs if n.endswith("(matched)")})
    g = ex.groupby(ex.index.year // 5 * 5)
    ir = (g.mean() / g.std() * np.sqrt(252)).rename(lambda y: f"{y}-{y + 4}").T
    print("\n   what skew adds over the saved book: return / tracking vol, by period")
    print(ir.to_string(float_format="{:+.2f}".format))


def main() -> None:
    pd.set_option("display.width", 220)
    sd = ic_report(core.load())
    print("\ntrain-window dispersion of the capped unit score: "
          + ", ".join(f"{k} {v:.3f}" for k, v in sd.items()))
    book_report(sd)


if __name__ == "__main__":
    main()
