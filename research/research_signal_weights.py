"""Signal weights fitted on train instead of set by hand: how much is there, and does it hold?

The question (2026-11-03): the four tilts' weights (trend 0.05, EWMAC 0.037, VRP 0.091,
dollar value 0.10) were each set once, from the signal's IC or to match another's
dispersion, never fitted jointly. If they were fitted on train, how much Sharpe is there,
and how much of it survives in test?

**Fixed before any run.** Grid: each tilt's saved weight times 0, 0.5, 1, 2, 3 (5^4 = 625
books); the prior stays at 1.0 (scaling every weight together only resizes the book,
``research_prior_weight``) and ``PORTFOLIO`` is fixed. Each book is core net of borrowing
plus the five overlays (they do not read ``mu``), on the ETFs' own prices from 2008-09-10.
Train 2008-09-10 .. 2017-07-25, test 2017-07-26 .. 2026-10-30. Selection on train only:

* ``best``: the highest train Sharpe (what naive fitting gives).
* ``bagged``: stationary block bootstrap of train days (mean block 63, 1,000 draws, the same
  days for every book); each draw's best multipliers, averaged; run as its own book.
* ``walk-forward``: through test, re-pick the best on all data to each 26 July and hold it
  for a year (switching instantly; a diagnostic of the procedure).
* Overfitting: probability of backtest overfitting (CSCV, Bailey et al. 2017) on train, 16
  blocks; and the rank correlation of train and test Sharpe across the 625 books.

Rule: adopt a fitted set only if its test Sharpe beats the saved book's by 0.02 or more.
History (proxy data, 1995-10 .. 2008-09) is reported for the candidates, never in the rule.

**Result (2026-11-03): fitting the weights on train is overfitting; keep the saved
weights.** The surface is flat: 625 books span train Sharpe 1.23-1.48 (median 1.38), and
train ranks barely predict test ranks (rank correlation 0.19). PBO 0.75: the train winner
ranks below the median out of sample in three splits of four. The saved book ranks 143rd of
625 in train and 45th in test. ``best`` (trend x0.5, VRP x2, dollar x2) squeezes train
1.41 -> 1.48 but test falls 1.47 -> 1.39, history 1.20 -> 1.08, history drawdown -21.2% ->
-26.4%. ``bagged`` (x1.07 / 1.34 / 1.25 / 1.82) is the saved book in disguise: train 1.44,
test 1.47 (saved 1.47), history 1.20. Walk-forward re-picking each July: test 1.41. No
fitted set meets the rule. One tilt at a time, only dollar value is better above x1 in both
(x2 1.43 / 1.51, x3 1.43 / 1.53, against 1.41 / 1.47): a lead seen in test, from one asset
and a handful of 5-year cycles, not a result.

Usage:
    uv run python research/research_signal_weights.py [--workers 12]
"""

from __future__ import annotations

import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays

TILTS = ("trend", "ewmac", "vrp", "dollar_value")
MULTS = (0.0, 0.5, 1.0, 2.0, 3.0)
TRAIN = ("2008-09-10", "2017-07-25")
TEST = ("2017-07-26", "2026-10-30")
HISTORY = (None, "2008-09-09")
CACHE = Path(".cache/research/signal_weights.parquet")
DRAWS, BLOCK, SEED = 1000, 63, 0
CSCV_BLOCKS = 16
LOADERS = {"actual": core.load, "proxy": core_proxies.load}

_bundle = {}


def alphas(mults: tuple[float, ...]) -> dict:
    return {**core.ALPHAS, **{n: (core.ALPHAS[n][0], core.ALPHAS[n][1] * m)
                              for n, m in zip(TILTS, mults)}}


def core_returns(mults: tuple[float, ...], data: str = "actual") -> pd.Series:
    """The core book's returns net of borrowing (overlays are added once, outside)."""
    if data not in _bundle:
        _bundle[data] = LOADERS[data]()
    b = _bundle[data]
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b)).run(
        b, core.build(alphas=alphas(mults)))
    return core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)


def overlay_returns(index: pd.Index, data: str = "actual") -> pd.Series:
    b = LOADERS[data]()
    return sum(ov.backtest_returns(b).reindex(index).fillna(0.0) for ov in overlays(core))


def name(mults) -> str:
    return " ".join(f"{m:g}" for m in mults)


def grid(workers: int) -> pd.DataFrame:
    """Daily book returns, one column per multiplier set (cached)."""
    if CACHE.exists():
        return pd.read_parquet(CACHE)
    points = list(itertools.product(MULTS, repeat=len(TILTS)))
    with ProcessPoolExecutor(workers) as pool:
        out = dict(zip(map(name, points), pool.map(core_returns, points)))
    r = pd.DataFrame(out)
    r = r.add(overlay_returns(r.index), axis=0)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    r.to_parquet(CACHE)
    return r


def sharpe(r: pd.DataFrame | pd.Series):
    return r.mean() / r.std() * np.sqrt(TRADING_DAYS)


def stats(r: pd.Series) -> dict:
    nav = (1 + r).cumprod()
    return {"sharpe": sharpe(r), "ret": r.mean() * TRADING_DAYS,
            "vol": r.std() * np.sqrt(TRADING_DAYS), "dd": (nav / nav.cummax() - 1).min()}


def mults_of(col: str) -> np.ndarray:
    return np.array([float(x) for x in col.split()])


def bootstrap_draws(n: int, rng: np.random.Generator) -> np.ndarray:
    """Row indices of one stationary block bootstrap draw (Politis-Romano), wrapping."""
    starts = rng.random(n) < 1.0 / BLOCK
    starts[0] = True
    idx = np.empty(n, dtype=int)
    pos = 0
    for i in range(n):
        pos = rng.integers(n) if starts[i] else (pos + 1) % n
        idx[i] = pos
    return idx


def bagged(train: pd.DataFrame) -> tuple[np.ndarray, pd.Series]:
    """Average of each bootstrap draw's best multipliers; and how often each book wins."""
    rng = np.random.default_rng(SEED)
    x = train.to_numpy()
    wins = np.zeros(x.shape[1])
    for _ in range(DRAWS):
        s = x[bootstrap_draws(len(x), rng)]
        wins[np.argmax(s.mean(0) / s.std(0))] += 1
    share = pd.Series(wins / DRAWS, index=train.columns)
    best = np.array([mults_of(c) for c in train.columns])
    return share.to_numpy() @ best, share.sort_values(ascending=False)


def pbo(train: pd.DataFrame) -> float:
    """CSCV probability of backtest overfitting: how often the in-sample best book ranks
    below the median out of sample, over every half-and-half split of the blocks."""
    x = train.to_numpy()
    blocks = np.array_split(np.arange(len(x)), CSCV_BLOCKS)
    n = np.array([len(b) for b in blocks])
    s1 = np.array([x[b].sum(0) for b in blocks])
    s2 = np.array([(x[b] ** 2).sum(0) for b in blocks])

    def sr(sel):
        k = n[sel].sum()
        m = s1[sel].sum(0) / k
        return m / np.sqrt(s2[sel].sum(0) / k - m**2)

    below = []
    for ins in itertools.combinations(range(CSCV_BLOCKS), CSCV_BLOCKS // 2):
        ins = np.array(ins)
        oos = np.setdiff1d(np.arange(CSCV_BLOCKS), ins)
        o = sr(oos)
        rank = (o < o[np.argmax(sr(ins))]).mean()
        below.append(rank < 0.5)
    return float(np.mean(below))


def walk_forward(r: pd.DataFrame) -> tuple[pd.Series, list[tuple[str, str]]]:
    """Re-pick the best train-to-date book each 26 July from the test start; hold a year."""
    out, picks = [], []
    for year in range(2017, 2027):
        a, z = pd.Timestamp(f"{year}-07-26"), pd.Timestamp(f"{year + 1}-07-25")
        hist = r.loc[TRAIN[0]:a - pd.Timedelta(days=1)]
        pick = sharpe(hist).idxmax()
        seg = r.loc[a:min(z, pd.Timestamp(TEST[1])), pick]
        if len(seg):
            out.append(seg)
            picks.append((str(year), pick))
    return pd.concat(out), picks


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=12)
    args = p.parse_args()
    pd.set_option("display.width", 160)

    r = grid(args.workers)
    train, test = r.loc[slice(*TRAIN)], r.loc[slice(*TEST)]
    tr, te = sharpe(train), sharpe(test)
    saved = name((1.0,) * len(TILTS))
    best = tr.idxmax()
    bag, share = bagged(train)
    bag = np.round(bag, 2)

    print(f"Grid: {r.shape[1]} books, {r.index[0].date()} .. {r.index[-1].date()}")
    print(f"Train Sharpe across the grid: min {tr.min():.2f}  median {tr.median():.2f}  "
          f"max {tr.max():.2f};  test: min {te.min():.2f}  median {te.median():.2f}  "
          f"max {te.max():.2f}")
    print(f"Rank correlation, train vs test Sharpe across books: {spearmanr(tr, te)[0]:.2f}")
    print(f"Saved book ranks {int((tr > tr[saved]).sum()) + 1} of {len(tr)} in train, "
          f"{int((te > te[saved]).sum()) + 1} in test")
    print(f"PBO (CSCV, {CSCV_BLOCKS} blocks, train): {pbo(train):.2f}")
    print("\nTop 10 by train Sharpe (multipliers on " + ", ".join(TILTS) + "):")
    top = pd.DataFrame({"train": tr, "test": te, "boot_win": share}).sort_values(
        "train", ascending=False).head(10)
    print(top.round(3).to_string())
    print("\nMost frequent bootstrap winners:")
    print(share.head(8).round(3).to_string())
    print(f"\nBagged multipliers: {dict(zip(TILTS, bag))}")

    # Marginal view: each tilt's multiplier, others at the saved weights.
    print("\nOne tilt at a time (others saved), train / test Sharpe:")
    for i, t in enumerate(TILTS):
        cells = []
        for m in MULTS:
            k = list((1.0,) * len(TILTS)); k[i] = m; c = name(k)
            cells.append(f"x{m:g} {tr[c]:.3f}/{te[c]:.3f}")
        print(f"  {t:13s} " + "  ".join(cells))

    wf, picks = walk_forward(r)
    print("\nWalk-forward picks: " + "; ".join(f"{y}: {p}" for y, p in picks))

    cands = {"saved": tuple(mults_of(saved)), "best": tuple(mults_of(best)),
             "bagged": tuple(bag)}
    with ProcessPoolExecutor(min(args.workers, 6)) as pool:
        jobs = {(k, d): pool.submit(core_returns, m, d)
                for k, m in cands.items() for d in LOADERS}
        res = {k: f.result() for k, f in jobs.items()}
    ov = {d: overlay_returns(res[("saved", d)].index, d) for d in LOADERS}
    rows = []
    for k, m in cands.items():
        a = res[(k, "actual")] + ov["actual"]
        h = res[(k, "proxy")] + ov["proxy"]
        for period, s in (("history", h.loc[slice(*HISTORY)]), ("train", a.loc[slice(*TRAIN)]),
                          ("test", a.loc[slice(*TEST)]), ("full", a.loc[:TEST[1]])):
            rows.append({"book": k, "mults": name(m), "period": period, **stats(s)})
    rows.append({"book": "walk-forward", "mults": "yearly", "period": "test",
                 **stats(wf)})
    out = pd.DataFrame(rows).set_index(["book", "mults", "period"])
    fmt = {"sharpe": "{:.3f}", "ret": "{:.1%}", "vol": "{:.1%}", "dd": "{:.1%}"}
    print("\nCandidates (core + overlays):")
    print(out.apply(lambda c: c.map(fmt[c.name].format)).unstack("period").to_string())


if __name__ == "__main__":
    main()
