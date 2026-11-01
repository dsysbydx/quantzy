"""Universe: drop QQQ, or complete the sector set with XLC and XLRE?

The sector SPDRs explain 96.5% of QQQ's daily variance (``research_corr_factor.py``) and the
book holds ~3% of NAV in it, so QQQ looks redundant. But the universe has only the nine
original SPDRs: no communication services (XLC -- GOOGL, META, NFLX since the 2018 GICS
change moved them out of XLK and XLY) and no real estate (XLRE), so part of what QQQ
carries -- the mega-cap communication names -- no other ETF in the book does. **Fixed before
any test.**

Universes, everything else as saved (``PORTFOLIO``, the alphas and their weights, the
overlays added after):

* ``saved``: the 17.
* ``no QQQ``: 16.
* ``no QQQ + XLC, XLRE``: 18, the two new sectors treated as the other sectors (US sectors
  class, Sharpe prior 0.30, trend, EWMAC and VRP, 2 bp to trade).

XLC starts 2018-06-19, and the book needs about a year of history before an asset enters, so
two comparisons: ``saved`` against ``no QQQ`` over 2008-09 .. 2026-10 (split 2017-07-26), and
all three over the window the XLC universe can run (split at its midpoint). Rule: +0.02
Sharpe over the window and in both halves. Seven years is little power: a null there is
weak evidence either way.

**Result (2026-11-01): no change; keep QQQ, do not add XLC or XLRE.** The rebuilt saved
universe reproduces the book (core 1.211, with overlays 1.371). Full window: without QQQ
1.208 (train 1.226 / test 1.199), with overlays 1.368 against 1.371. XLC window
(2019-12-31 .. 2026-10-30): saved 1.078, without QQQ 1.080, without QQQ plus XLC and XLRE
1.062 (overlays: 1.217 / 1.219 / 1.202). QQQ's ~4% of NAV just moves into the sector SPDRs,
and XLC and XLRE barely enter (5% and 1%). QQQ costs nothing and is the cleanest futures leg
(MNQ) in the hybrid, so it stays.

Usage:
    uv run python research/research_universe_qqq.py
"""

from __future__ import annotations

import dataclasses
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.backtest import WalkForwardBacktester  # noqa: E402
from quantzy.data import CachedReader, DataBundle, YFinanceReader  # noqa: E402
from quantzy.jobs.daily import latest_complete  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.signals import CarrySharpePrior, Identity, VarianceRiskPremium  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402
from quantzy.strategy import Strategy  # noqa: E402
from quantzy.universe import Asset, Universe  # noqa: E402

SPLIT = pd.Timestamp("2017-07-26")
NEW = ["XLC", "XLRE"]
NEW_COST_BPS: dict[str, float] = {}  # one-way cost per new ticker; default 2 bp
UNIVERSES = {
    "saved": core.RISKY,
    "no QQQ": [t for t in core.RISKY if t != "QQQ"],
    "no QQQ + XLC, XLRE": [t for t in core.RISKY if t != "QQQ"] + NEW,
}


def load() -> DataBundle:
    """The saved bundle with XLC and XLRE added (their panels like the other ETFs')."""
    b = core.load()
    extra = CachedReader(YFinanceReader()).history(NEW, start="2015-01-01").reindex(b.index)
    px = pd.concat([b.prices, extra], axis=1)
    panels = {}
    for k in b:
        if k == "prices":
            continue
        p = b[k].reindex(columns=px.columns)
        if k in ("vix", "market", "vixy"):  # broadcast panels: the same value in every column
            p[NEW] = np.repeat(b[k].iloc[:, [0]].to_numpy(), len(NEW), axis=1)
        panels[k] = p
    return DataBundle.build(px, **panels)


def strategy(name: str) -> Strategy:
    risky = UNIVERSES[name]
    equity = [t for t in core.EQUITY if t in risky] + [t for t in NEW if t in risky]
    prior_sr = {t: core.PRIOR_SR.get(t, core.CLASS_SR["US sectors"]) for t in risky}
    alphas = {
        "prior": (CarrySharpePrior(prior_sr, carry_vol=core.CARRY_VOL, carry_weight=0.5,
                                   normalizer=Identity()), core.ALPHAS["prior"][1]),
        "trend": core.ALPHAS["trend"], "ewmac": core.ALPHAS["ewmac"],
        "vrp": (VarianceRiskPremium(tuple(equity), normalizer=Identity()),
                core.ALPHAS["vrp"][1]),
        "dollar_value": core.ALPHAS["dollar_value"],
    }
    cost = {t: v for t, v in core.EXECUTION_COST_BPS.items() if t in risky}
    cost |= {t: NEW_COST_BPS.get(t, 2.0) for t in NEW if t in risky}
    portfolio = dataclasses.replace(core.PORTFOLIO, execution_cost_bps=cost)
    universe = Universe(name=name, assets=[Asset(t, t, "etf") for t in risky]
                        + [Asset(core.CASH, "T-bill", "cash_equivalent")])
    return Strategy(universe=universe, signals={n: s for n, (s, _) in alphas.items()},
                    weights={n: k for n, (_, k) in alphas.items()},
                    risk_model=portfolio.risk_model(), optimizer=portfolio.optimizer(core.CASH),
                    name=name)


def warmup(b: DataBundle, name: str) -> int:
    risky, s = UNIVERSES[name], strategy(name)
    first = b.prices[risky].apply(lambda x: b.index.get_loc(x.first_valid_index()))
    per_asset = max([s.risk_model.min_history]
                    + [getattr(g, "asset_history", g.min_history) + g.lag + 1
                       for g in s.signals.values()])
    return int(max(first.max() + per_asset, s.min_history))


def run_one(args):
    name, start = args
    b = load()
    risky = UNIVERSES[name]
    cols = [*risky, core.CASH]
    sub = DataBundle.build(b.prices[cols], **{k: b[k][cols] for k in b if k != "prices"})
    sub = latest_complete(sub, risky)
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(sub, strategy(name))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights.reindex(r.index).fillna(0.0)


def table(runs: dict, split: pd.Timestamp, b: DataBundle) -> pd.DataFrame:
    with_ov = {f"{n} + overlays": export.with_overlays(r, b) for n, r in runs.items()}
    t = compare({**runs, **with_ov}, split_date=split)
    return pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                         "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                         "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})


def main() -> None:
    pd.set_option("display.width", 220)
    b = load()
    full_start = max(warmup(b, n) for n in ("saved", "no QQQ"))
    xlc_start = max(warmup(b, n) for n in UNIVERSES)
    jobs = [("saved", full_start), ("no QQQ", full_start)]
    jobs += [(n, xlc_start) for n in UNIVERSES]
    with ProcessPoolExecutor(max_workers=5) as pool:
        out = list(pool.map(run_one, jobs))
    full = {n: r for (n, r, _), (_, s) in zip(out[:2], jobs[:2], strict=True)}
    short = {n: r for n, r, _ in out[2:]}
    weights = {n: w for n, _, w in out[2:]}

    print(f"1. FULL WINDOW {b.index[full_start].date()} .. {b.index[-1].date()}, "
          f"split {SPLIT.date()}")
    print(table(full, SPLIT, b).to_string(float_format="{:.3f}".format))

    idx = short["saved"].index
    mid = idx[len(idx) // 2]
    print(f"\n2. XLC WINDOW {idx[0].date()} .. {idx[-1].date()}, split at midpoint {mid.date()}")
    print(table(short, mid, b).to_string(float_format="{:.3f}".format))
    print("\n   average weight in the XLC window (x NAV)")
    rows = {n: {c: float(w[[t for t in ts if t in w.columns]].sum(axis=1).mean())
                for c, ts in (("US sectors", [*core.CLASSES["US sectors"], *NEW]),
                              ("QQQ", ["QQQ"]), ("XLC", ["XLC"]), ("XLRE", ["XLRE"]),
                              ("Treasuries", ["IEF", "TLT"]))}
            for n, w in weights.items()}
    print(pd.DataFrame(rows).T.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    main()
