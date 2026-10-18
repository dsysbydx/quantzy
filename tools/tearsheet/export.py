"""Build the core_multiasset backtest tear-sheet: one self-contained HTML page.

    uv run python tools/tearsheet/export.py [--out .cache/tearsheet/core_multiasset.html]

Runs the saved strategy and a priors-only baseline through the walk-forward engine, plus an
equal-weight benchmark, and embeds the results as JSON in ``core_multiasset.html`` (the
page's charts, tables, period selector and signal heatmap). The page text describes
``core_multiasset`` by hand: update it when the strategy changes.
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.backtest import FixedWeights, WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.optimize import LinearCost
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays

TEMPLATE = Path(__file__).with_name("core_multiasset.html")
SPLIT = pd.Timestamp("2017-07-26")
HEAT_STEP = 5  # sample the signal heatmap weekly
LABELS = {"trend": ("EMA-200 trend", "tanh score, -1..1"),
          "ewmac": ("EWMAC 64/256", "forecast / 10, capped +/-2"),
          "vrp": ("Variance risk premium", "z-score, capped +/-2")}


def run(which: str):
    b = core.load()
    alphas = core.ALPHAS if which == "strategy" else {"prior": core.ALPHAS["prior"]}
    warm = core.warmup(b, core.build())  # the saved book's start, for both
    res = WalkForwardBacktester(rebalance_every=1, warmup=warm).run(b, core.build(alphas=alphas))
    turnover = pd.Series({d.asof: d.turnover for d in res.decisions})
    cost = pd.Series({d.asof: d.trades.cost or 0.0 for d in res.decisions})
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return r, res.weights, turnover, cost


def _values(frame: pd.DataFrame, assets: list[str], digits: int = 3) -> list[list]:
    return [[None if not np.isfinite(v) else round(float(v), digits) for v in frame[t]]
            for t in assets]


def signal_heatmap(b, weights: pd.DataFrame, idx: pd.DatetimeIndex) -> dict:
    """Unit scores per alpha (as traded, lagged), the total view and weights, sampled weekly."""
    rows = list(range(0, len(idx), HEAT_STEP)) + (
        [len(idx) - 1] if (len(idx) - 1) % HEAT_STEP else [])
    di = idx[rows]
    assets = list(core.RISKY)
    series, total = {}, pd.DataFrame(0.0, index=di, columns=assets)
    for name, (s, k) in core.ALPHAS.items():
        vol = realized_vol(b.prices, s.vol_window, annualize=True).shift(s.lag)
        live = s.raw(b).shift(s.lag).notna()  # no view (excluded, warming up) stays empty
        sc = (s.scores(b) / vol.where(vol > 0)).where(live).reindex(di)[assets]
        total += sc.fillna(0) * k
        label, unit = ("Prior Sharpe", "Sharpe ratio") if name == "prior" else LABELS.get(
            name, (name, "score"))
        series[name] = {"label": label, "k": k, "unit": unit, "values": _values(sc, assets)}
    series["total"] = {"label": "Total view (prior + all alphas)", "k": 1.0,
                       "unit": "Sharpe units, mu / vol", "values": _values(total, assets)}
    series["weight"] = {"label": "Portfolio weight", "k": 1.0, "unit": "share of capital",
                        "values": _values(weights.reindex(di)[assets], assets, 4)}
    return {"dates": [d.strftime("%Y-%m-%d") for d in di], "assets": assets, "series": series}


def with_overlays(r: pd.Series, b) -> pd.Series:
    """The core's returns plus each overlay's excess return over cash, on the same dates."""
    cash = b.prices[core.CASH].pct_change(fill_method=None)
    out = r.copy()
    for ov in overlays(core):
        ex = ov.excess_returns(b.prices[ov.ASSET].pct_change(fill_method=None), cash)
        out = out + ex.reindex(out.index).fillna(0.0)
    return out


def export(out: Path) -> dict[str, pd.Series]:
    b = core.load()
    with ProcessPoolExecutor(2) as pool:
        (r_core, w1, t1, c1), (r0, *_) = pool.map(run, ["strategy", "baseline"])
    r1 = with_overlays(r_core, b)
    print(f"core only: Sharpe {r_core.mean() / r_core.std() * np.sqrt(TRADING_DAYS):.2f}")
    equal = FixedWeights(core.UNIVERSE, holdings=core.RISKY,
                         cost_model=LinearCost(0.0, spread_bps=dict(core.EXECUTION_COST_BPS)))
    ew = WalkForwardBacktester(rebalance_every=21, warmup=core.warmup(b)).run(b, equal).returns
    px, idx = b.prices, r1.index
    rets = {"strategy": r1, "baseline": r0.reindex(idx).fillna(0),
            "equal": ew.reindex(idx).fillna(0),
            "qqq": px["QQQ"].pct_change(fill_method=None).reindex(idx).fillna(0)}
    w1 = w1.reindex(idx).fillna(0)
    cls_of = {t: c for c, ts in core.CLASSES.items() for t in ts} | {core.CASH: "T-bill"}
    contrib = w1 * px.pct_change(fill_method=None).reindex(idx).fillna(0)[w1.columns]
    by_class = contrib.T.groupby(cls_of).sum().T
    class_w = w1.T.groupby(cls_of).sum().T
    turnover, cost = t1.reindex(idx).fillna(0), c1.reindex(idx).fillna(0)
    data = {
        "dates": [d.strftime("%Y-%m-%d") for d in idx],
        "rets": {k: v.round(7).tolist() for k, v in rets.items()},
        "class_w": {c: class_w[c].round(4).tolist() for c in class_w},
        "asset_w_avg": w1.mean().round(4).to_dict(),
        "asset_w_last": w1.iloc[-1].round(4).to_dict(),
        "asset_class": cls_of,
        "class_contrib_year": {c: s.groupby(s.index.year).sum().round(5).to_dict()
                               for c, s in by_class.items()},
        "asset_contrib_total": contrib.sum().round(5).to_dict(),
        "turnover_year": turnover.groupby(turnover.index.year).sum().round(4).to_dict(),
        "trade_days_year": (turnover > 1e-3).groupby(turnover.index.year).sum().to_dict(),
        "cost_year": cost.groupby(cost.index.year).sum().round(6).to_dict(),
        "split": SPLIT.strftime("%Y-%m-%d"),
        "gross": w1[core.RISKY].sum(axis=1).round(4).tolist(),
        "sig": signal_heatmap(b, w1, idx),
    }
    payload = json.dumps(data, default=lambda o: int(o) if isinstance(o, np.integer) else float(o))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(TEMPLATE.read_text().replace("__DATA__", payload))
    return rets


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=".cache/tearsheet/core_multiasset.html")
    out = Path(ap.parse_args().out)
    rets = export(out)
    for k, v in rets.items():
        ann = np.sqrt(TRADING_DAYS)
        print(f"{k:9s} return {v.mean() * TRADING_DAYS:6.2%}  vol {v.std() * ann:6.2%}"
              f"  Sharpe {v.mean() / v.std() * ann:.2f}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
