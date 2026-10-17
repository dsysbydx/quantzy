"""Can a better volatility model replace the trend term in QQQ's ``mu``?

``research_qqq_optimizer.py`` found the trend score has no IC for returns but does carry
information about the risk ahead: in a downtrend, realized vol runs well above what a trailing
estimate expects. This puts that information where it belongs, in the risk model, and checks
two things in order:

1. **Forecast quality.** Is the volatility forecast closer to what followed? Scored with
   :func:`quantzy.research.evaluate_vol` over a 21-day horizon, in-sample (train) and after
   the split (test), and separately on downtrend days, where errors cost the most.
2. **Backtest.** With ``mu`` held at the flat premium (no trend term), does the better risk
   model alone reproduce the Sharpe and drawdown of the trend-in-``mu`` strategy?

Candidates:

- ``ewma hl=N``: exponentially weighted covariance with an ``N``-day halflife. The current
  model is ``hl=20``.
- ``blend 5/20/60``: the average of three EWMA variances, a crude HAR-style mix of fast and
  slow memory.
- ``ewma hl=N +trend d``: the EWMA scaled by :class:`~quantzy.risk.StateScaledRisk`, with
  volatility multiplied by ``1 + d * max(-trend, 0)``.

Usage:
    uv run python research/research_qqq_volmodel.py [--workers 8]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np
import pandas as pd
from research_qqq_optimizer import Premium, trade_days
from research_qqq_timing import CASH, RISKY, UNIVERSE, load, warmup_rows

from quantzy.backtest import BacktestResult, WalkForwardBacktester
from quantzy.data import DataBundle
from quantzy.data.bundle import as_bundle
from quantzy.metrics import sharpe_ratio
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare, evaluate_vol, realized_returns, vol_forecast
from quantzy.risk import RiskModel, SampleCovariance, StateScaledRisk
from quantzy.signals import Identity, TrendStrength
from quantzy.strategy import Strategy

SPLIT = pd.Timestamp("2013-07-31")  # midpoint of the backtest sample, as in the other scripts
HORIZON = 21
PREMIUM, RISK_AVERSION, COST_AVERSION, COST_BPS = 0.05, 0.5, 12.0, 5.0
REFERENCE = "ewma hl=20"

SPECS: dict[str, dict] = {
    **{f"ewma hl={hl}": {"kind": "ewma", "hl": hl} for hl in (5, 10, 20, 40, 60)},
    "blend 5/20/60": {"kind": "blend", "hls": (5, 20, 60)},
    **{f"ewma hl={hl} +trend {d:g}": {"kind": "scaled", "hl": hl, "downside": d}
       for hl in (10, 20, 40) for d in (0.25, 0.5, 1.0)},
}


@dataclass
class BlendRisk:
    """Average of several risk models' covariances."""

    models: tuple[RiskModel, ...]

    def covariance(self, data: DataBundle | pd.DataFrame,
                   asof: pd.Timestamp | None = None) -> pd.DataFrame:
        bundle = as_bundle(data)
        covs = [m.covariance(bundle, asof) for m in self.models]
        return sum(covs[1:], covs[0]) / len(covs)

    @property
    def min_history(self) -> int:
        return max(m.min_history for m in self.models)


def trend() -> TrendStrength:
    return TrendStrength(200, exclude=(CASH,), normalizer=Identity())


def ewma(hl: float) -> SampleCovariance:
    return SampleCovariance(halflife=hl, lookback=250)


def risk_model(spec: dict) -> RiskModel:
    if spec["kind"] == "ewma":
        return ewma(spec["hl"])
    if spec["kind"] == "blend":
        return BlendRisk(tuple(ewma(h) for h in spec["hls"]))
    return StateScaledRisk(ewma(spec["hl"]), trend(), downside=spec["downside"],
                           assets=(RISKY,))


def strategy(spec: dict, k: float = 0.0) -> Strategy:
    signals = {"premium": Premium(PREMIUM, normalizer=Identity())}
    if k:
        signals["trend"] = trend()
    return Strategy(
        universe=UNIVERSE,
        signals=signals,
        weights={"trend": k} if k else {},
        risk_model=risk_model(spec),
        optimizer=MeanVariance(
            risk_aversion=RISK_AVERSION,
            constraints=[C.budget(), C.long_only()],
            cost_model=LinearCost(COST_BPS),
            cost_aversion=COST_AVERSION,
        ),
    )


# --- workers (module level so a process pool can pickle them) --------------------------------

def forecast_one(label: str) -> tuple[str, pd.Series]:
    return label, vol_forecast(risk_model(SPECS[label]), load(), RISKY)


def backtest_one(job: tuple[str, dict, float]) -> tuple[str, BacktestResult]:
    label, spec, k = job
    bundle = load()
    engine = WalkForwardBacktester(rebalance_every=1, warmup=warmup_rows(bundle))
    return label, engine.run(bundle, strategy(spec, k))


# --- report ----------------------------------------------------------------------------------

def forecast_table(forecasts: dict[str, pd.Series], rets: pd.Series,
                   state: pd.Series) -> pd.DataFrame:
    """Forecast quality by model: train/test overall, and test on downtrend days."""
    train = {k: v.loc[:SPLIT - pd.Timedelta(days=1)] for k, v in forecasts.items()}
    test = {k: v.loc[SPLIT:] for k, v in forecasts.items()}
    tr = evaluate_vol(train, rets, horizon=HORIZON)
    te = evaluate_vol(test, rets, horizon=HORIZON)
    regime = pd.Series(np.where(state < 0, "down", "up"), index=state.index)
    full = evaluate_vol(forecasts, rets, horizon=HORIZON, by=regime)
    down = full.xs("down", level=1)
    return pd.DataFrame({
        "qlike_train": tr["qlike"] - tr.loc[REFERENCE, "qlike"],
        "qlike_test": te["qlike"] - te.loc[REFERENCE, "qlike"],
        "bias_test": te["bias"],
        "mz_r2_test": te["mz_r2"],
        "left_tail_test": te["left_tail"],
        "bias_down": down["bias"],
        "left_tail_down": down["left_tail"],
        "avg_vol_down": down["avg_forecast"],
        "real_vol_down": down["avg_realized"],
    })


def backtest_table(runs: dict[str, BacktestResult]) -> pd.DataFrame:
    out = compare(runs)
    for name, period in (("sh_1st", slice(None, SPLIT - pd.Timedelta(days=1))),
                         ("sh_2nd", slice(SPLIT, None))):
        out[name] = [sharpe_ratio(runs[k].returns.loc[period]) for k in out.index]
    out["avg_exp"] = [float(runs[k].weights[RISKY].mean()) for k in out.index]
    out["trade_days"] = [trade_days(runs[k]) for k in out.index]
    return out[["ann_return", "ann_vol", "sharpe", "max_dd", "calmar", "avg_exp", "turnover",
                "trade_days", "sh_1st", "sh_2nd"]]


def main(workers: int) -> None:
    bundle = load()
    rets = realized_returns(bundle, RISKY)
    state = trend().scores(bundle)[RISKY]

    jobs = [(label, spec, 0.0) for label, spec in SPECS.items()]
    jobs += [("REF trend in mu k=0.05", SPECS[REFERENCE], 0.05),
             ("REF trend in mu k=0.1", SPECS[REFERENCE], 0.10)]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        forecasts = dict(pool.map(forecast_one, SPECS))
        runs = dict(pool.map(backtest_one, jobs))

    pd.set_option("display.width", 250)
    print(f"\n1. Volatility forecast quality, {HORIZON}-day horizon, split at {SPLIT.date()}")
    print("   qlike columns are differences from the current model "
          f"({REFERENCE}); negative = better.")
    print("   bias = realized / forecast variance (>1: under-forecasts risk); left_tail: share "
          "of 21-day returns below -2.33 predicted sigmas (1% if calibrated).\n")
    ft = forecast_table(forecasts, rets, state)
    print(ft.to_string(formatters={
        "qlike_train": "{:+.3f}".format, "qlike_test": "{:+.3f}".format,
        "bias_test": "{:.2f}".format, "mz_r2_test": "{:.2f}".format,
        "left_tail_test": "{:.1%}".format, "bias_down": "{:.2f}".format,
        "left_tail_down": "{:.1%}".format, "avg_vol_down": "{:.1%}".format,
        "real_vol_down": "{:.1%}".format}))

    bt = backtest_table(runs)
    idx = runs[REFERENCE].returns.index
    print(f"\n2. Backtest, daily decisions, mu = {PREMIUM:.0%} premium only (no trend term) "
          f"unless REF | risk_aversion {RISK_AVERSION} | cost_aversion {COST_AVERSION:g} | "
          f"{idx[0].date()} .. {idx[-1].date()}\n")
    print(bt.to_string(formatters={
        "ann_return": "{:.1%}".format, "ann_vol": "{:.1%}".format, "sharpe": "{:.2f}".format,
        "max_dd": "{:.1%}".format, "calmar": "{:.2f}".format, "avg_exp": "{:.0%}".format,
        "turnover": "{:.1f}".format, "trade_days": "{:.0f}".format, "sh_1st": "{:.2f}".format,
        "sh_2nd": "{:.2f}".format}))

    both = ft[["qlike_train", "qlike_test"]].join(bt[["sharpe", "max_dd"]], how="inner")
    print("\n3. Does a better forecast give a better strategy? Rank correlation across models:")
    for col in ("qlike_train", "qlike_test"):
        print(f"   {col} vs sharpe: {both[col].corr(both['sharpe'], method='spearman'):+.2f}   "
              f"{col} vs max_dd: {both[col].corr(both['max_dd'], method='spearman'):+.2f}")
    print("   (negative on both means models that forecast better also traded better: higher\n"
          "    Sharpe and a shallower drawdown)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    main(ap.parse_args().workers)
