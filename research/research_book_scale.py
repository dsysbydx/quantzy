"""Lever the saved book 1.25x (~12% vol) as the default, under Reg T.

``research_target_vol`` found that levering the saved book beats a bolder optimizer at the
same vol, and the user chose 1.25x (2026-11-02). Levering by L is the same optimization with
risk aversion / L and the leverage cap x L: the objective ``mu'w - lambda w'Sigma w - cost``
scales by L exactly when ``w`` does, the cost terms included (linear in trades, cash's too),
and the 20% position cap is of the invested book, so it scales by itself. Only the borrow
spread, charged outside the optimizer, is not linear. So the candidate is a
``PortfolioConfig`` change, not a new multiplier.

**Fixed before any test.**

* ``saved``: risk aversion 2, cap 1.3x, overlays at their sizes.
* ``A`` (**the candidate**): risk aversion 1.6, cap 1.625x, overlays at their sizes. The
  overlays are separate bets, sized on their own (the auction size is to be revisited from
  live Sharpe), so they are not levered.
* ``B``: A with every overlay x 1.25.

All with the daily job's margin scale-back (overlays shrunk to keep gross under 1.95x),
split 2017-07-26. **Rule:** adopt A as ``PORTFOLIO`` if, as traded under Reg T, its vol is
11.5-13%, its Sharpe is within 0.05 of saved in both halves, its max drawdown is no worse
than -20%, and the smallest fall to a margin call is at least 25%. B replaces A only if it
also passes and beats A's Sharpe by 0.02 in both halves.

**Result (2026-11-02): fails, not adopted; under Reg T the levered core crowds out the
overlays.** 2008-09-10 .. 2026-10-30. Saved 1.44 (train 1.41 / test 1.47), 14.5% return,
9.8% vol, -15.1% drawdown. A: 1.31 (1.28 / 1.33), 16.1%, 12.0%, -17.9%, worst month -11.6%,
smallest fall to a call 35% (median 50%), 2008 crash week on today's book -30% against -25%.
B: 1.32 (1.26 / 1.37), the same return. Vol passes and drawdown passes; Sharpe falls 0.13,
well past 0.05. The equivalence holds for the core: A's risky gross is 1.24x saved's
(median), and the core's Sharpe excess of cash is 1.060 against 1.065 for saved x 1.25
exactly (1.211 -> 1.169 total, the drop from cash not being levered and from borrowing).
The rest is the overlays: they add return at little risk and do not grow with the core, and
with the core at ~1.56x the margin scale-back trims them on 9% of days instead of 2% (B
levers them, and the scale-back takes it back). So +22% vol buys +1.6 points of return.
Revisit under portfolio margin, where the scale-back would not bind at this size.

Usage:
    uv run python research/research_book_scale.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd
import research_target_vol as tv

from quantzy.backtest import WalkForwardBacktester
from quantzy.jobs import margin as mg
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import overlays

SPLIT = tv.SPLIT
L = 1.25
CONFIGS = {"saved": (2.0, 1.3), "A": (2.0 / L, 1.3 * L)}


def run_one(n: str):
    ra, lev = CONFIGS[n]
    b = latest_complete(core.load(), core.RISKY)
    p = replace(core.PORTFOLIO, risk_aversion=ra, max_leverage=lev)
    s = core.build(portfolio=p)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    r = p.net_of_borrowing(res.returns, res.weights, core.CASH)
    return n, r, res.weights.reindex(r.index).fillna(0.0)


def main() -> None:
    pd.set_option("display.width", 250)
    b = latest_complete(core.load(), core.RISKY)
    with ProcessPoolExecutor(max_workers=2) as pool:
        out = {n: rest for n, *rest in pool.map(run_one, CONFIGS)}
    idx = out["saved"][0].index
    ov_w = tv.overlay_weights(b, idx)
    ov_r = sum(o.backtest_returns(b).reindex(idx).fillna(0.0) for o in overlays(core))
    cases = {"saved": ("saved", 1.0), "A": ("A", 1.0), "B": ("A", L)}
    runs, extra = {}, {}
    for n, (cfg, k) in cases.items():
        r, w = out[cfg]
        r, total, scale = tv.book(r.reindex(idx).fillna(0.0), w.reindex(idx).fillna(0.0),
                                  k * ov_r, k * ov_w)
        runs[n] = r
        gross = total.abs().sum(axis=1)
        extra[n] = {"avg gross": gross.mean(), "peak gross": gross.max(),
                    "days scaled": (scale < 1).mean(),
                    "borrowed": (-w[core.CASH]).clip(lower=0).mean(), **tv.margin_stats(total)}
        last = total.iloc[-1]
        extra[n]["stress 2008 now"] = mg.stress(last[last != 0], b.prices.join(
            b["market"].iloc[:, [0]].set_axis(["SPY"], axis=1))).get(
            "2008-10 crash week", {}).get("loss", np.nan)
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]}).loc[list(runs)]
    m["calmar"] = m["ret"] / -m["max_dd"]
    m["worst month"] = [(1 + r).resample("ME").prod().min() - 1 for r in runs.values()]
    m = m.join(pd.DataFrame(extra).T)
    pct = "{:.1%}".format
    print(f"{idx[0].date()} .. {idx[-1].date()}, split {SPLIT.date()}, Reg T scale-back on")
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "worst month",
                                                   "days scaled", "call: median fall",
                                                   "call: min fall", "stress 2008 now")}
                      | {c: "{:.2f}".format for c in ("sharpe", "train", "test", "calmar",
                                                      "avg gross", "peak gross", "borrowed")}))
    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    print("\n   calendar years")
    print(yr.to_string(float_format="{:+.1%}".format))


if __name__ == "__main__":
    main()
