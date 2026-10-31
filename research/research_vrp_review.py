"""Review of the saved equity VRP alpha: does it still earn its place?

``research_vrp_multi.py`` found the equity VRP's 1-day IC near zero since 2017 (+0.003,
t 0.2) against +0.016 (t 1.8) over the whole sample, and ``IDEAS.md`` asks for its book
contribution by period. The book with and without the ``vrp`` alpha, everything else
(other alphas, ``PORTFOLIO``, overlays) unchanged. **Rule fixed before the run:** retire it
only if it adds <= 0 Sharpe since the split (2017-07-26, the test half) *and* less than
+0.02 over the whole window, the adoption bar it once passed. A recent dip alone does not
retire an alpha, as a recent gain alone does not adopt one.

Reported: Sharpe with and without (core only, and with the overlays), both halves and
five-year periods; the return difference and its t-stat; drawdown; average equity weight;
and the 2008, 2020 and 2022 episodes, where its gain was concentrated when adopted.

**Result (2026-10-31): keep it.** 2008-09-10 .. 2026-10-30. Core 1.211 with VRP against
1.169 without (train 1.235 / 1.196, test 1.198 / 1.150); with the overlays 1.371 / 1.329.
Its gain is +0.042 Sharpe (+92 bp/yr, t 2.4) over the window and +0.048 (t 2.1) in the test
half, and positive in every five-year period (+0.073 / +0.022 / +0.070 / +0.024). Neither
retirement condition holds. The near-zero 1-day IC since 2017 understates it: its edge is
at weeks (21-day IC t 2.0), keeping equities sized up while the premium is high. It still
costs in a sudden crash (Feb-Apr 2020 -5.0% against -2.6%) and earns it back after.

Usage:
    uv run python research/research_vrp_review.py
"""

from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.backtest import WalkForwardBacktester  # noqa: E402
from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.jobs.daily import latest_complete  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

SPLIT = pd.Timestamp("2017-07-26")
EPISODES = {"2008-09..2009-03": ("2008-09-10", "2009-03-09"),
            "2020-02..04": ("2020-02-19", "2020-04-30"), "2022": ("2022-01-01", "2022-12-31")}
MODELS = {"saved": core.ALPHAS,
          "without vrp": {k: v for k, v in core.ALPHAS.items() if k != "vrp"}}


def run_one(name: str):
    b = latest_complete(core.load(), core.RISKY)
    start = core.warmup(b, core.build())
    s = core.build(alphas=MODELS[name])
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights.reindex(r.index).fillna(0.0)


def sharpe(r: pd.Series) -> float:
    return float(r.mean() / r.std() * np.sqrt(TRADING_DAYS))


def main() -> None:
    pd.set_option("display.width", 220)
    with ProcessPoolExecutor(2) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, list(MODELS))}
    b = core.load()
    runs = {n: v[0] for n, v in out.items()}
    runs |= {f"{n} + overlays": export.with_overlays(v[0], b) for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    m["equity w"] = [float(out[n.replace(" + overlays", "")][1][core.EQUITY].sum(axis=1).mean())
                     for n in m.index]
    for e, (a, z) in EPISODES.items():
        m[e] = [float((1 + runs[n].loc[a:z]).prod() - 1) for n in m.index]
    idx = runs["saved"].index
    print(f"{idx[0].date()} .. {idx[-1].date()}, split {SPLIT.date()}")
    print(m.to_string(float_format="{:.3f}".format))

    print("\nVRP's contribution (saved minus without), by period")
    rows = {}
    for label, a, z in (("2008-09 .. 2012", "2008", "2012"), ("2013-2017", "2013", "2017"),
                        ("2018-2022", "2018", "2022"), ("2023-2026", "2023", "2026"),
                        ("train", None, SPLIT - pd.Timedelta(days=1)), ("test", SPLIT, None),
                        ("full", None, None)):
        for book, (x, y) in (("core", ("saved", "without vrp")),
                             ("with overlays", ("saved + overlays", "without vrp + overlays"))):
            rx, ry = runs[x].loc[a:z], runs[y].loc[a:z]
            d = rx - ry
            rows[(label, book)] = {
                "sharpe gain": sharpe(rx) - sharpe(ry),
                "bp/yr": d.mean() * TRADING_DAYS * 1e4,
                "t": d.mean() / d.std() * np.sqrt(len(d)),
            }
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.3f}".format))


if __name__ == "__main__":
    main()
