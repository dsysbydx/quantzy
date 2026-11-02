"""A seasonal equity prior: the Halloween tilt through the optimizer (idea 18, version b).

``research_halloween.py`` found Nov-Apr beating May-Oct in 27 of 28 country markets (fading in
the US), and a raw +/-0.25x SPY overlay hurt the book by adding equity into the 2008-09 crash.
Here the tilt goes through ``mu`` instead, so the risk model and the trend alphas still size
equities down in a crash. Shaped by the first result: one candidate, the stricter rule.
**Fixed before the test.**

* Candidate: the 12 equity ETFs' Sharpe prior 0.35 when the position is held in November-
  April and 0.25 in May-October (0.30 on average over a year; the saved prior is 0.30 all
  year). The season is the month of the next trading day, the day the book is held.
  Everything else -- risk model, other alphas, ``PORTFOLIO``, overlays -- as saved.
* Diagnostic, not a candidate: the same tilt on EFA and EEM only.

Rule: the full book (core plus its overlays) up at least 0.02 Sharpe over the window and in
both halves (split 2017-07-26), *and* the daily return difference against the saved book with
t >= 2.

**Result (2026-11-02): fails; idea 18 closed.** Full book 2008-09-10 .. 2026-10-30: saved
1.443 (train 1.412 / test 1.473); the candidate 1.406 (1.411 / 1.410), return difference
-0.02% a year (t -0.07), max drawdown -16.0% against -15.1%; EFA / EEM only 1.413 (1.405 /
1.423), t -1.2. The tilt works one way only: equities rise from 70% to 75% of NAV in
Nov-Apr, but barely fall in May-Oct (72% to 71%) -- the leverage cap, the position caps and
the trading band absorb the summer cut -- so the candidate mostly holds more equity in
winter. The book's equity is mostly US, where the effect has faded (^GSPC t 0.7 since 2008),
and the non-US part is too small for its larger effect to matter.

Usage:
    uv run python research/research_seasonal_prior.py
"""

from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from types import MethodType

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402

from quantzy.backtest import WalkForwardBacktester  # noqa: E402
from quantzy.data.bundle import as_bundle  # noqa: E402
from quantzy.jobs.daily import latest_complete  # noqa: E402
from quantzy.research import compare  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402

WINTER = {11, 12, 1, 2, 3, 4}
BASE, DELTA = 0.30, 0.05
SPLIT = pd.Timestamp("2017-07-26")
MODELS = {"saved": (), "seasonal prior, all equity": tuple(core.EQUITY),
          "seasonal prior, EFA / EEM only": ("EFA", "EEM")}
CANDIDATE = "seasonal prior, all equity"


def strategy(name: str):
    s = core.build()
    assets = list(MODELS[name])
    if not assets:
        return s
    plain_mu, prior, k = s.mu, s.signals["prior"], s.weights["prior"]

    def mu(self, data, asof=None):
        bundle = as_bundle(data)
        m = plain_mu(bundle, asof).copy()
        day = bundle.index[-1] if asof is None else pd.Timestamp(asof)
        held_month = (day + pd.offsets.BDay(1)).month
        p = prior.forecast(bundle, asof).reindex(assets) * k  # BASE x vol per asset
        sign = 1.0 if held_month in WINTER else -1.0
        m[assets] = m[assets] + sign * DELTA / BASE * p.fillna(0.0).to_numpy()
        return m

    s.mu = MethodType(mu, s)
    return s


def run_one(name: str):
    b = latest_complete(core.load(), core.RISKY)
    start = core.warmup(b, core.build())
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, strategy(name))
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights.reindex(r.index).fillna(0.0)


def main() -> None:
    pd.set_option("display.width", 220)
    with ProcessPoolExecutor(max_workers=3) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, list(MODELS))}
    b = core.load()
    books = {n: export.with_overlays(r, b) for n, (r, _) in out.items()}
    t = compare(books, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
                      "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"]})
    eq = {n: w[core.EQUITY].sum(axis=1) for n, (_, w) in out.items()}
    winter = {n: e[e.index.month.isin(sorted(WINTER))].mean() for n, e in eq.items()}
    summer = {n: e[~e.index.month.isin(sorted(WINTER))].mean() for n, e in eq.items()}
    m["equity w Nov-Apr"] = [winter[n] for n in m.index]
    m["equity w May-Oct"] = [summer[n] for n in m.index]
    idx = books["saved"].index
    print(f"FULL BOOK (core + overlays), {idx[0].date()} .. {idx[-1].date()}, split {SPLIT.date()};"
          f" candidate: {CANDIDATE}")
    print(m.to_string(float_format="{:.3f}".format))
    for n in MODELS:
        if n == "saved":
            continue
        d = books[n] - books["saved"]
        t_ = d.mean() / d.std() * np.sqrt(len(d))
        print(f"   {n} minus saved: {d.mean() * 252:+.2%}/yr, t {t_:+.2f}")
    episodes = {"2008-09..2009-03": ("2008-09-10", "2009-03-09"),
                "2020-02..03": ("2020-02-19", "2020-03-23"), "2022": ("2022-01", "2022-12")}
    for e, (a, z) in episodes.items():
        print(f"   {e}: " + ", ".join(f"{n} {((1 + books[n].loc[a:z]).prod() - 1):+.1%}"
                                     for n in MODELS))


if __name__ == "__main__":
    main()
