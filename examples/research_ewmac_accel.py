"""Multi-speed MA crossover (EWMAC) and trend acceleration, tested against the saved book.

The saved book's trend alpha is one slow measure: price against its 200-day EMA. Carver-style
systems run the same idea at several speeds and add a second rule, acceleration, the change in
a crossover forecast. This asks whether either adds anything to the 17-ETF book.

**Rules, fixed before any test** (Carver, *Systematic Trading*, no fitting here):

* ``EWMAC(N, 4N)`` for N in 8, 16, 32, 64: ``(EMA_N - EMA_4N) / (price * daily vol)``, times
  Carver's forecast scalar (5.3, 3.75, 2.65, 1.87), divided by 10 and capped at +/-2. The
  combined rule is the average of the four speeds. "EWMAC fast" averages N = 8 and 16 only:
  that is the speed that could exit a sell-off before EMA-200 does.
* ``accel_N = f_N(t) - f_N(t - N)`` for N in 16, 32, 64, where ``f_N`` is the uncapped EWMAC
  forecast above. Each speed is divided by its own RMS over the train window (to 2017-07-26)
  so the speeds count equally, then capped at +/-2 and averaged.
* Daily vol is the 60-day realized vol, as in every other signal in the book.

**Sizing, the only scale choice.** Each rule enters ``mu`` like the saved trend alpha: score
times annualized vol, times a weight ``k``. The weight is set so the rule's score has the same
dispersion over the train window as the saved trend alpha at ``k = 0.1``. Every variant then
spends the same trend budget, and only the rule changes. ``PORTFOLIO`` is untouched.

Tests:

1. Pooled time-series IC against the next day's return over trailing vol (47-day interleaved
   folds, z-stats fit on fold A, Newey-West t), per speed and per asset class; and how
   correlated each rule is with the saved trend alpha.
2. Backtests of the book with each rule replacing or joining the saved trend alpha, including
   the three sell-offs a faster trend should help in: 2008, 2020 and 2022.

**Result (2026-10-16): negative, not adopted.** The IC comes from the slow speeds only (32/128
t 2.7, 64/256 t 3.2); 8/32 and 16/64 show none and are slightly negative since 2010. The
4-speed rule is 0.92 correlated with EMA-200 and gave Sharpe 1.11 against 1.13 (2008-12 ..
2026-10); the half-and-half blend 1.15 is within noise. The fast rule did not cut the 2020 or
2022 losses in any useful way and cost 0.07 Sharpe. Acceleration has no IC (16 and 32 are
negative), cut Sharpe by about 0.18, deepened the drawdown by 5 points and doubled turnover.
The slowest speed's IC suggests a slower trend than EMA-200; that is a post-hoc idea and
would need its own pre-specified test (fold A t 3.6 but fold B only 1.1).

Usage:
    uv run python examples/research_ewmac_accel.py [--workers 4]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import ClassVar

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import DataBundle
from quantzy.research import chunk_folds, compare, newey_west_t, pooled_time_series_ic
from quantzy.research.ic import _ts_pairs
from quantzy.signals import Identity
from quantzy.signals.base import Signal
from quantzy.signals.indicators import ema, ewm_min_history, realized_vol
from quantzy.strategies import core_multiasset as core

CARVER_SCALAR = {8: 5.3, 16: 3.75, 32: 2.65, 64: 1.87}
ACCEL_SPEEDS = (16, 32, 64)
CAP = 2.0
SPLIT = pd.Timestamp("2017-07-26")  # the train/test split used for the saved book
CHUNK, NW_LAGS, VOL_IC = 47, 21, 60
SELLOFFS = {"2008-09 .. 2009-03": ("2008-09-01", "2009-03-09"),
            "2020-02 .. 2020-03": ("2020-02-19", "2020-03-23"),
            "2022-01 .. 2022-10": ("2022-01-03", "2022-10-12")}


def forecast_scalar(n: int) -> float:
    """Carver's scalar, or for other speeds its sqrt rule from the 64/256 value."""
    return CARVER_SCALAR.get(n, CARVER_SCALAR[64] * (64 / n) ** 0.5)


def ewmac_forecasts(px: pd.DataFrame, speeds: tuple[int, ...], vol_window: int,
                    ewm_tol: float) -> dict[int, pd.DataFrame]:
    """Uncapped EWMAC forecasts per speed, in Carver units / 10 (average |f| near 1)."""
    price_vol = px * realized_vol(px, vol_window)
    out = {}
    for n in speeds:
        fast, slow = ema(px, n), ema(px, 4 * n)
        f = (fast - slow) / price_vol.where(price_vol > 0) * forecast_scalar(n) / 10.0
        # mask the slow EMA's burn-in, as TrendStrength does through min_history
        burn = px.notna().cumsum() <= ewm_min_history(4 * n, ewm_tol)
        out[n] = f.mask(burn)
    return out


@dataclass
class EWMAC(Signal):
    """Average of capped EWMAC(N, 4N) forecasts, times annualized vol (return units)."""

    name: ClassVar[str] = "ewmac"
    speeds: tuple[int, ...] = (8, 16, 32, 64)
    vol_window: int = 60
    exclude: tuple[str, ...] = ()
    ewm_tol: float = 0.05

    def score(self, px: pd.DataFrame) -> pd.DataFrame:
        f = ewmac_forecasts(px, self.speeds, self.vol_window, self.ewm_tol)
        out = sum(v.clip(-CAP, CAP) for v in f.values()) / len(f)
        out[[c for c in self.exclude if c in out.columns]] = np.nan
        return out

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        return self.score(px) * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        return max(ewm_min_history(4 * max(self.speeds), self.ewm_tol), self.vol_window + 1)


@dataclass
class Acceleration(Signal):
    """Average of capped ``f_N(t) - f_N(t-N)``, each over its train-window RMS, times vol."""

    name: ClassVar[str] = "accel"
    speeds: tuple[int, ...] = ACCEL_SPEEDS
    rms: dict[int, float] = field(default_factory=dict)  # from :func:`accel_rms`
    vol_window: int = 60
    exclude: tuple[str, ...] = ()
    ewm_tol: float = 0.05

    def changes(self, px: pd.DataFrame) -> dict[int, pd.DataFrame]:
        f = ewmac_forecasts(px, self.speeds, self.vol_window, self.ewm_tol)
        return {n: f[n] - f[n].shift(n) for n in self.speeds}

    def score(self, px: pd.DataFrame) -> pd.DataFrame:
        d = self.changes(px)
        out = sum((d[n] / self.rms[n]).clip(-CAP, CAP) for n in self.speeds) / len(self.speeds)
        out[[c for c in self.exclude if c in out.columns]] = np.nan
        return out

    def raw(self, data: DataBundle) -> pd.DataFrame:
        px = data.prices
        return self.score(px) * realized_vol(px, self.vol_window, annualize=True)

    @property
    def min_history(self) -> int:
        n = max(self.speeds)
        return max(ewm_min_history(4 * n, self.ewm_tol) + n, self.vol_window + 1)


def rms(panel: pd.DataFrame) -> float:
    """Pooled root-mean-square over the train window."""
    x = panel.loc[:SPLIT].to_numpy().ravel()
    x = x[np.isfinite(x)]
    return float(np.sqrt(np.mean(x ** 2)))


def accel_rms(px: pd.DataFrame) -> dict[int, float]:
    d = Acceleration(rms={n: 1.0 for n in ACCEL_SPEEDS}).changes(px)
    return {n: rms(d[n]) for n in ACCEL_SPEEDS}


def unit_scores(px: pd.DataFrame, rms_accel: dict[int, float]) -> dict[str, pd.DataFrame]:
    """Each rule's dimensionless score (before times-vol), NaN where not yet trustworthy."""
    rules: dict[str, Signal] = {
        "EMA-200 (saved)": core.ALPHAS["trend"][0],
        "EWMAC 4-speed": EWMAC(),
        "EWMAC fast (8,16)": EWMAC(speeds=(8, 16)),
        "acceleration": Acceleration(rms=rms_accel),
        **{f"  EWMAC {n}/{4 * n}": EWMAC(speeds=(n,)) for n in CARVER_SCALAR},
        **{f"  accel {n}": Acceleration(speeds=(n,), rms={n: rms_accel[n]})
           for n in ACCEL_SPEEDS},
    }
    rows_live = px.notna().cumsum()
    out = {}
    for name, sig in rules.items():
        vol = realized_vol(px, sig.vol_window, annualize=True)
        score = sig.raw(DataBundle.build(px)) / vol  # undo the times-vol: the unit score
        out[name] = score.where(rows_live > sig.min_history)
    return out


# --- 1. IC -------------------------------------------------------------------------------------

def ic_report(bundle: DataBundle, rms_accel: dict[int, float]) -> dict[str, float]:
    """Prints the IC tables; returns each rule's train-window score std (for matching k)."""
    px = bundle.prices[core.RISKY]
    raw = unit_scores(px, rms_accel)
    scores = {k: v.shift(1) for k, v in raw.items()}  # tradeable at t: known at t-1's close
    folds = chunk_folds(bundle.index, CHUNK)
    in_a = folds == 0
    a_rows, b_rows = folds.index[in_a], folds.index[~in_a]

    def fmt(s: pd.Series) -> str:
        return f"{s.mean():+.4f} ({newey_west_t(s, NW_LAGS):+.1f})"

    rows = {}
    for name, s in scores.items():
        ic = pooled_time_series_ic(s, px, 1, "spearman", VOL_IC, fit=in_a)
        full = pooled_time_series_ic(s, px, 1, "spearman", VOL_IC)
        rows[name] = {"fold A (fit)": fmt(ic[ic.index.isin(a_rows)]),
                      "fold B (held out)": fmt(ic[ic.index.isin(b_rows)]),
                      "all": fmt(full),
                      "pre-2010": fmt(full.loc[:"2009"]), "2010+": fmt(full.loc["2010":])}
    print(f"\n1. POOLED TIME-SERIES RANK IC, 17 ETFs, vs next-day return / trailing {VOL_IC}d vol."
          f" {CHUNK}-day interleaved folds, z-stats from fold A. Mean IC (Newey-West t, "
          f"{NW_LAGS} lags)")
    print(pd.DataFrame(rows).T.to_string())

    print("\n   by asset class, rank IC, all dates (Newey-West t)")
    by_cls = {}
    for name in ("EMA-200 (saved)", "EWMAC 4-speed", "EWMAC fast (8,16)", "acceleration"):
        zx, zy = _ts_pairs(scores[name], px, 1, "spearman", VOL_IC)
        prod = zx * zy
        by_cls[name] = {c: fmt(prod[ts].mean(axis=1).dropna()) for c, ts in core.CLASSES.items()}
    print(pd.DataFrame(by_cls).to_string())

    print("\n   correlation of scores, pooled over assets and dates")
    stacked = pd.DataFrame({k: v.stack() for k, v in raw.items()
                            if not k.startswith("  ")})
    print(stacked.corr().to_string(float_format="{:+.2f}".format))

    return {k: float(v.loc[:SPLIT].stack().std()) for k, v in raw.items()}


# --- 2. portfolio ------------------------------------------------------------------------------

def variants(k: dict[str, float], rms_accel: dict[int, float]) -> dict[str, dict]:
    prior, (trend, k0) = core.ALPHAS["prior"], core.ALPHAS["trend"]
    ex = dict(exclude=(core.CASH,), normalizer=Identity())
    ewmac = (EWMAC(**ex), k["EWMAC 4-speed"])
    fast = (EWMAC(speeds=(8, 16), **ex), k["EWMAC fast (8,16)"])
    accel = (Acceleration(rms=rms_accel, **ex), k["acceleration"])
    return {
        "prior only": {"prior": prior},
        "EMA-200 (saved)": {"prior": prior, "trend": (trend, k0)},
        "EWMAC 4-speed": {"prior": prior, "ewmac": ewmac},
        "EMA-200 + EWMAC, half each": {"prior": prior, "trend": (trend, k0 / 2),
                                       "ewmac": (ewmac[0], ewmac[1] / 2)},
        "EWMAC fast (8,16)": {"prior": prior, "ewmac": fast},
        "EMA-200 + acceleration": {"prior": prior, "trend": (trend, k0), "accel": accel},
        "EWMAC 4-speed + acceleration": {"prior": prior, "ewmac": ewmac, "accel": accel},
    }


def run_one(args: tuple[str, dict]) -> tuple[str, pd.Series, pd.DataFrame, float]:
    name, alphas = args
    bundle = core.load()
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(bundle, s)).run(bundle, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    turnover = sum(d.turnover for d in res.decisions) / (len(r) / TRADING_DAYS)
    return name, r, res.weights, turnover


def portfolio_report(jobs: dict[str, dict], workers: int) -> None:
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w, t) for n, r, w, t in pool.map(run_one, jobs.items())}
    start = max(v[0].index[0] for v in out.values())
    runs = {n: v[0].loc[start:] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    })
    m["turnover"] = [out[n][2] for n in m.index]
    for label, (a, z) in SELLOFFS.items():
        m[label] = [(1 + runs[n].loc[a:z]).prod() - 1 for n in m.index]
    m = m.loc[list(jobs)]
    pct = "{:.1%}".format
    print(f"\n2. PORTFOLIO, {start.date()} .. {runs[next(iter(runs))].index[-1].date()}, split "
          f"{SPLIT.date()} | {core.PORTFOLIO}")
    print(m.to_string(formatters={**{c: pct for c in ("ret", "vol", "max_dd", *SELLOFFS)},
                                  **{c: "{:.2f}".format for c in
                                     ("tr_sharpe", "te_sharpe", "sharpe")},
                                  "turnover": "{:.1f}".format}))
    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    print("\n   calendar years")
    print(yr[list(jobs)].to_string(float_format="{:+.1%}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    bundle = core.load()
    rms_accel = accel_rms(bundle.prices[core.RISKY])
    print(f"acceleration train-window RMS per speed: "
          f"{ {n: round(v, 3) for n, v in rms_accel.items()} }")
    sd = ic_report(bundle, rms_accel)
    k0 = core.ALPHAS["trend"][1]
    k = {n: k0 * sd["EMA-200 (saved)"] / sd[n] for n in sd}
    print("\n   k matched to the saved trend alpha's train-window dispersion: "
          + ", ".join(f"{n} {k[n]:.3f}" for n in
                      ("EMA-200 (saved)", "EWMAC 4-speed", "EWMAC fast (8,16)", "acceleration")))
    portfolio_report(variants(k, rms_accel), workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    main(ap.parse_args().workers)
