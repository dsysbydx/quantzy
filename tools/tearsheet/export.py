"""Build the core_multiasset backtest tear-sheet: one self-contained HTML page.

    uv run python tools/tearsheet/export.py [--out .cache/tearsheet/core_multiasset.html]

Runs the saved strategy and a priors-only baseline through the walk-forward engine, plus an
equal-weight benchmark, and embeds the results as JSON in ``core_multiasset.html`` (the
page's charts, tables, period selector and signal heatmap). The page text describes
``core_multiasset`` by hand: update it when the strategy changes.

Each is run twice: on the ETFs' own prices (from 2008-09) and on the same ETFs extended back
with proxies (``quantzy.strategies.core_proxies``, from 1995-10). The page's history toggle
switches between :data:`VIEWS`, each a slice of one of the two runs.

To share it, publish the output file as a Claude Artifact to the existing URL,
https://claude.ai/artifact/KagCis5282eJCN3qJmnim9, so the link others hold stays the same (see
"Sharing the tear-sheet" in the README).
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
from quantzy.data import CachedReader, YFinanceReader
from quantzy.data.splice import tracking
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

TEMPLATE = Path(__file__).with_name("core_multiasset.html")
#: The evaluation periods, ``(key, label, first day)``. History is proxy data, a sanity check
#: only: settings are chosen on train, checked on test, and production is the book trading
#: live from 2026-11-01.
PERIODS = [("history", "History (proxies)", None), ("train", "Train", "2008-09-10"),
           ("test", "Test", "2017-07-26"), ("production", "Production", "2026-11-01")]
HEAT_STEP = 5  # sample the signal heatmap weekly
LABELS = {"trend": ("EMA-200 trend", "tanh score, -1..1"),
          "ewmac": ("EWMAC 64/256", "forecast / 10, capped +/-2"),
          "vrp": ("Variance risk premium", "z-score, capped +/-2"),
          "dollar_value": ("Dollar value (DXY vs 5-year mean)", "z-score, capped +/-2")}
#: The history toggle: ``(key, label, run, first date, last date, note)``. The first is the
#: page's default.
VIEWS = [
    ("actual", "Full actual", "actual", None, None,
     "Train, test and production on the ETFs' own prices, from when the youngest (UUP, "
     "2007-03) has warmed up."),
    ("full", "Full, with proxies", "proxy", None, None,
     "Adds the history period, 1995-10 to 2008-09: each ETF extended back before it listed "
     "with an index fund, index or futures proxy (table in section 12). A sanity check, not a "
     "period the settings are chosen or judged on."),
    ("since2000", "Since 2000", "proxy", "2000-01-01", None,
     "The proxy-extended run from 2000, which takes in the dot-com bust; history is proxy "
     "data, a sanity check."),
]
LOADERS = {"actual": core.load, "proxy": core_proxies.load}


def run(which: str, data: str = "actual"):
    b = LOADERS[data]()
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
    out = r.copy()
    for ov in overlays(core):
        out = out + ov.backtest_returns(b).reindex(out.index).fillna(0.0)
    return out


def results(data: str, strategy: tuple, baseline: tuple) -> dict:
    """The strategy, baseline and benchmarks over one run's full history."""
    b = LOADERS[data]()
    (r_core, w1, t1, c1), (r0, *_) = strategy, baseline
    r1 = with_overlays(r_core, b)
    print(f"{data}: core only Sharpe {r_core.mean() / r_core.std() * np.sqrt(TRADING_DAYS):.2f}")
    equal = FixedWeights(core.UNIVERSE, holdings=core.RISKY,
                         cost_model=LinearCost(0.0, spread_bps=dict(core.EXECUTION_COST_BPS)))
    ew = WalkForwardBacktester(rebalance_every=21, warmup=core.warmup(b)).run(b, equal).returns
    idx = r1.index
    rets = pd.DataFrame({"strategy": r1, "baseline": r0.reindex(idx).fillna(0),
                         "equal": ew.reindex(idx).fillna(0),
                         "qqq": b.prices["QQQ"].pct_change(fill_method=None).reindex(idx).fillna(0)})
    return {"b": b, "rets": rets, "w": w1.reindex(idx).fillna(0),
            "turnover": t1.reindex(idx).fillna(0), "cost": c1.reindex(idx).fillna(0)}


def view(res: dict, start: str | None, end: str | None) -> dict:
    """The page's data for one slice of a run, everything restarted at its first day."""
    b = res["b"]
    rets = res["rets"].loc[start:end]
    idx = rets.index
    w1, turnover, cost = (res[k].reindex(idx) for k in ("w", "turnover", "cost"))
    px = b.prices
    cls_of = {t: c for c, ts in core.CLASSES.items() for t in ts} | {core.CASH: "T-bill"}
    contrib = w1 * px.pct_change(fill_method=None).reindex(idx).fillna(0)[w1.columns]
    by_class = contrib.T.groupby(cls_of).sum().T
    class_w = w1.T.groupby(cls_of).sum().T
    return {
        "dates": [d.strftime("%Y-%m-%d") for d in idx],
        "rets": {k: rets[k].round(7).tolist() for k in rets},
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
        "periods": periods(idx),
        "gross": w1[core.RISKY].sum(axis=1).round(4).tolist(),
        "sig": signal_heatmap(b, w1, idx),
    }


def periods(idx: pd.DatetimeIndex) -> list[dict]:
    """Each of :data:`PERIODS` the dates touch, as a first and last row of ``idx``."""
    starts = [idx[0] if s is None else pd.Timestamp(s) for *_, s in PERIODS]
    ends = [*starts[1:], idx[-1] + pd.Timedelta(days=1)]
    out = []
    for (key, label, _), lo, hi in zip(PERIODS, starts, ends):
        a, b = int(idx.searchsorted(lo)), int(idx.searchsorted(hi)) - 1
        if b >= a:
            out.append({"k": key, "label": label, "a": a, "b": b})
    return out


def proxy_table() -> list[dict]:
    """Each proxy against its ETF's own daily returns, where both trade."""
    own = CachedReader(YFinanceReader()).history(
        list(core_proxies.PROXIES), start=core_proxies.START).pct_change(fill_method=None)
    rows = []
    for t, chain in core_proxies.PROXIES.items():
        for label, src in zip(core_proxies.LABELS[t], chain):
            r = src(core_proxies.START, None)
            tr = tracking(own[t], r)
            rows.append({"etf": t, "proxy": label, "from": r.dropna().index[0].strftime("%Y-%m-%d"),
                         "etf_from": own[t].first_valid_index().strftime("%Y-%m-%d"),
                         **{k: round(v, 3) for k, v in tr.items() if k in (
                             "corr", "beta", "vol_ratio", "tracking_error", "drift")}})
    return rows


def export(out: Path) -> dict[str, pd.DataFrame]:
    with ProcessPoolExecutor(4) as pool:
        jobs = {(d, w): pool.submit(run, w, d) for d in LOADERS for w in ("strategy", "baseline")}
        runs = {d: results(d, jobs[d, "strategy"].result(), jobs[d, "baseline"].result())
                for d in LOADERS}
    views = {key: {"label": label, "note": note, **view(runs[data], a, z)}
             for key, label, data, a, z, note in VIEWS}
    data = {"views": views, "order": [v[0] for v in VIEWS], "proxies": proxy_table()}
    payload = json.dumps(data, default=lambda o: int(o) if isinstance(o, np.integer) else float(o))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(TEMPLATE.read_text().replace("__DATA__", payload))
    return {key: runs[d]["rets"].loc[a:z] for key, _, d, a, z, _ in VIEWS}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=".cache/tearsheet/core_multiasset.html")
    out = Path(ap.parse_args().out)
    ann = np.sqrt(TRADING_DAYS)
    for name, rets in export(out).items():
        print(f"{name}: {rets.index[0].date()} .. {rets.index[-1].date()}")
        for k, v in rets.items():
            print(f"  {k:9s} return {v.mean() * TRADING_DAYS:6.2%}  vol {v.std() * ann:6.2%}"
                  f"  Sharpe {v.mean() / v.std() * ann:.2f}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
