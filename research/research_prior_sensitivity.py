"""How much does the book depend on its Sharpe priors?

The prior is the largest term in ``mu`` (the core book is 0.80 correlated with the prior's
own portfolio), so a wrong prior could matter more than any alpha. This reruns the saved core
book with the prior replaced, everything else fixed (trend, EWMAC, VRP, dollar value, the
Treasury carry half-way rule, ``PORTFOLIO``). **Fixed before any test.**

* *saved*: the judgment priors.
* *flat*: 0.25 on every asset but UUP (0); no view between classes.
* *flat incl. USD*: 0.25 on all 17.
* *hindsight*: each asset's realized Sharpe over the backtest window, rescaled to the saved
  priors' mean, so only the ranking differs. A look-ahead upper bound, not a candidate.
* *inverted*: the hindsight ranking reversed, same mean: the worst ranking one could pick.
* *random error 1-8*: the saved priors plus N(0, 0.10) noise each, floored at 0: a judgment
  error about the size of the gap between the equity prior and 100 years of data.
* *level x0.5, x1.5*: every prior scaled: the level, not the ranking.

Core book only (net of borrowing, no overlays: they do not depend on the prior), from the
saved warmup, split 2017-07-26. Sharpe is return over vol, no cash deduction.

**Result (2026-10-26).** 2008-09-10 .. 2026-10-16, core only: saved 1.15 (1.17 / 1.14).
* The class ranking adds nothing: flat 1.16, flat incl. USD 1.19. The priors move a fifth to
  a quarter of the weights (sectors 71% -> 59%, gold and commodities 15% -> 27%) but not the
  Sharpe, as the module docstring's attribution said.
* Perfect hindsight gains only +0.18 (1.33); the inverted ranking loses -0.41 (0.74).
* Random errors of 0.10 per asset cost a median -0.21 (0.77-1.13, test 0.78-1.17), drawdowns
  to -25.6%. Equal priors within a class of near-substitutes are what keep the optimizer from
  piling into whichever sector a noisy prior favours (the QUAL mechanism, in reverse).
* The level matters, against the other alphas: x0.5 gives 1.31 (1.35 / 1.28) at 8.7% vol and
  1.22x gross; x1.5 gives 1.00. Found after the fact, so only a lead: it changes both the
  prior's weight against trend, VRP and value and the book's size; needs its own study.

Usage:
    uv run python research/research_prior_sensitivity.py [--workers 6]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.signals import CarrySharpePrior, Identity
from quantzy.strategies import core_multiasset as core

SPLIT = pd.Timestamp("2017-07-26")
NOISE, DRAWS, SEED = 0.10, 8, 7


def bundle():
    return latest_complete(core.load(), core.RISKY)


def priors(b) -> dict[str, dict[str, float]]:
    saved = dict(core.PRIOR_SR)
    start = b.index[core.warmup(b)]
    r = b.prices[core.RISKY].pct_change(fill_method=None).loc[start:]
    realized = r.mean() / r.std() * np.sqrt(TRADING_DAYS)
    mean = np.mean(list(saved.values()))
    hind = (realized / realized.mean() * mean).clip(lower=0)
    ranks = realized.rank()
    inv = realized.sort_values().copy()
    inv[:] = realized.sort_values(ascending=False).to_numpy()  # worst gets the best value
    inv = (inv / inv.mean() * mean).clip(lower=0)
    out = {
        "saved": saved,
        "flat": {t: 0.0 if t == "UUP" else 0.25 for t in core.RISKY},
        "flat incl. USD": {t: 0.25 for t in core.RISKY},
        "hindsight": hind.to_dict(),
        "inverted": inv.reindex(ranks.index).to_dict(),
        "level x0.5": {t: v * 0.5 for t, v in saved.items()},
        "level x1.5": {t: v * 1.5 for t, v in saved.items()},
    }
    rng = np.random.default_rng(SEED)
    for i in range(DRAWS):
        out[f"random error {i + 1}"] = {t: max(v + rng.normal(0, NOISE), 0.0)
                                        for t, v in saved.items()}
    return out


def run_one(args):
    name, prior = args
    b = bundle()
    alphas = {**core.ALPHAS, "prior": (CarrySharpePrior(prior, carry_vol=core.CARRY_VOL,
                                                        carry_weight=0.5,
                                                        normalizer=Identity()), 1.0)}
    s = core.build(alphas=alphas)
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    return name, r, res.weights


def main(workers: int) -> None:
    pd.set_option("display.width", 300)
    b = bundle()
    ps = priors(b)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {n: (r, w) for n, r, w in pool.map(run_one, ps.items())}
    runs = {n: v[0] for n, v in out.items()}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "sharpe": t["full"]["sharpe"], "tr_sharpe": t["train"]["sharpe"],
        "te_sharpe": t["test"]["sharpe"], "ret": t["full"]["ann_return"],
        "vol": t["full"]["ann_vol"], "max_dd": t["full"]["max_dd"],
    }).loc[list(runs)]
    base = out["saved"][1]
    m["corr w/ saved"] = [runs[n].corr(runs["saved"]) for n in runs]
    m["wt diff"] = [(out[n][1] - base).abs().drop(columns=core.CASH).sum(axis=1).mean() / 2
                    for n in runs]
    m["gross"] = [out[n][1].drop(columns=core.CASH).sum(axis=1).mean() for n in runs]
    idx = runs["saved"].index
    print(f"\nCORE BOOK BY PRIOR SET, {idx[0].date()} .. {idx[-1].date()}, split {SPLIT.date()}"
          " | wt diff = average one-way weight difference from the saved book")
    pct = "{:.1%}".format
    print(m.to_string(formatters={c: pct for c in ("ret", "vol", "max_dd", "wt diff")}
                      | {c: "{:.2f}".format for c in ("sharpe", "tr_sharpe", "te_sharpe",
                                                      "corr w/ saved", "gross")}))
    rnd = m[m.index.str.startswith("random")]
    print(f"\n   random errors: Sharpe {rnd['sharpe'].min():.2f}-{rnd['sharpe'].max():.2f} "
          f"(median {rnd['sharpe'].median():.2f}), test {rnd['te_sharpe'].min():.2f}-"
          f"{rnd['te_sharpe'].max():.2f}")
    print("\n   prior sets")
    print(pd.DataFrame({n: ps[n] for n in ("saved", "flat", "hindsight", "inverted")}).T
          .to_string(float_format="{:.2f}".format))
    cls = {t: c for c, ts in core.CLASSES.items() for t in ts}
    print("\n   average weight by class (% of NAV)")
    print(pd.DataFrame({n: out[n][1][core.RISKY].T.groupby(cls).sum().T.mean()
                        for n in ("saved", "flat", "hindsight", "inverted")})
          .to_string(float_format="{:.0%}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
