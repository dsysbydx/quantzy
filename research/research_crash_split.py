"""Where do "good IC, worse book" variants lose: ordinary days or fast crashes?

Several variants forecast better than the saved book and still lowered its Sharpe: the
trend skip (``research_trend_skip``: equity IC t 0.6 -> 2.8, book 1.24 -> 1.21) and implied
vol in the risk model (``research_implied_vol_risk``, ``research_implied_vol_mu``). The
suspicion is that they win on ordinary days and lose it all in a few fast crashes. If so, a
regime gate -- one that notices a crash has *started*, not one that predicts it -- could
keep the gain; if they also lose on ordinary days, no gate helps. An exploratory
diagnostic: nothing is adopted from it. **Definitions fixed before the run.**

Regimes, marked after the fact on SPY:

* ``crash``: every peak-to-trough fall of at least 10% whose trough came within 40 trading
  days of the peak, from the peak to the trough.
* ``rebound``: the 63 trading days after each such trough (re-entry).
* ``normal``: every other day.

Variants, each re-run from its own study at a common start: ``skip 21 equity`` and
``skip 10 equity`` (trend skip), ``iv 0.5 sigma`` and ``iv 0.5 both`` (implied vol). For each:
its daily return minus the saved book's, summed per regime and annualized over the whole
window (so the three add up to the total), with a t-stat per regime; and an *oracle bound*:
the variant on normal days and the saved book in crashes and rebounds, the most a perfect
crash gate could deliver. The bound uses hindsight; a real gate gets less.

**Result (2026-10-31): no gate can rescue these; the IC gains are too small to matter.**
2008-10-09 .. 2026-10-30. The episode rule found three fast crashes (2018-01, 2020-02, 2025-02;
1.5% of days). Variant minus saved, bp/yr: ``skip 21 equity`` normal -8 (t -0.4), crash -22
(t -2.6); ``skip 10 equity`` normal +5 (t +0.3), crash -11 (t -1.8); implied vol loses on
normal days (-91 and -65, t -2.4 and -2.7) and in rebounds (t -3.4, -3.8) and gains in
crashes, so a crash gate is the wrong fix for it. Oracle bounds: 1.23-1.24 (skips), 1.19
(implied vol), against saved 1.241. The episode rule misses fast legs of longer declines, so
a sensitivity (added after the first run) labels every day SPY is >= 10% below its 60-day
high (8% of days): the skips gain +2 / +11 bp/yr on the other days (t 0.1 / 0.7) and their
oracle bound is 1.243 / 1.245 (test 1.18 against 1.20) -- +0.004, with perfect hindsight.

The trend tilt is small (k = 0.05 and 0.037 in Sharpe units), so a better IC of +0.015 on it
is worth a few bp a year; the crash days only tip a difference that was about zero anyway.
More gain will not come from refining the existing tilts, gated or not; it needs a source of
return with its own size, as the lessons say.

Usage:
    uv run python research/research_crash_split.py [--workers 5]
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import research_implied_vol_mu as ivmu  # noqa: E402
import research_implied_vol_risk as ivr  # noqa: E402
import research_trend_skip as skip  # noqa: E402

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.research import compare  # noqa: E402

START = "2008-10-09"  # the later of the studies' common starts
SPLIT = pd.Timestamp("2017-07-26")
DROP, WITHIN, REBOUND = 0.10, 40, 63
# label -> (study, model name)
VARIANTS = {
    "saved": ("skip", "saved"),
    "skip 21 equity": ("skip", "skip 21 equity"),
    "skip 10 equity": ("skip", "skip 10 equity"),
    "iv 0.5 sigma": ("ivr", "iv 0.5"),
    "iv 0.5 both": ("ivmu", "iv 0.5 both"),
}


def regimes(spy: pd.Series) -> pd.Series:
    """``crash`` / ``rebound`` / ``normal`` per date, from SPY closes (hindsight)."""
    px = spy.dropna()
    label = pd.Series("normal", index=px.index)
    peak_i, i, n = 0, 1, len(px)
    episodes = []
    while i < n:
        if px.iloc[i] >= px.iloc[peak_i]:
            peak_i = i
        elif px.iloc[i] / px.iloc[peak_i] - 1 <= -DROP:
            # walk to the trough of this decline (before a new peak)
            j, trough_i = i, i
            while j < n and px.iloc[j] < px.iloc[peak_i]:
                if px.iloc[j] < px.iloc[trough_i]:
                    trough_i = j
                j += 1
            if trough_i - peak_i <= WITHIN:
                episodes.append((peak_i, trough_i))
            peak_i, i = j if j < n else n - 1, j
            continue
        i += 1
    for p, t in episodes:
        label.iloc[p: t + 1] = "crash"
        label.iloc[t + 1: t + 1 + REBOUND] = label.iloc[t + 1: t + 1 + REBOUND].where(
            label.iloc[t + 1: t + 1 + REBOUND] == "crash", "rebound")
    return label, [(px.index[p], px.index[t]) for p, t in episodes]


def run(label: str) -> tuple[str, pd.Series]:
    study, name = VARIANTS[label]
    if study == "skip":
        b = skip.latest_complete(skip.core.load(), skip.core.RISKY)
        _, r, _ = skip.run_one((name, b.index.get_loc(pd.Timestamp(START))))
    else:
        b = ivr.load()
        mod = ivr if study == "ivr" else ivmu
        _, r, _, _ = mod.run_one((name, b.index.get_loc(pd.Timestamp(START))))
    return label, r


def main(workers: int) -> None:
    pd.set_option("display.width", 220)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        runs = dict(pool.map(run, list(VARIANTS)))
    idx = runs["saved"].index
    for k in runs:
        runs[k] = runs[k].reindex(idx).fillna(0.0)
    spy = ivr.load()["market"].iloc[:, 0].ffill().reindex(idx)
    reg, eps = regimes(spy)
    reg = reg.reindex(idx).fillna("normal")
    print(f"{idx[0].date()} .. {idx[-1].date()}; {len(eps)} fast crashes "
          f"(>= {DROP:.0%} within {WITHIN} days):")
    print("   " + ", ".join(f"{p.date()}..{t.date()} ({spy[t] / spy[p] - 1:+.0%})" for p, t in eps))
    share = reg.value_counts(normalize=True)
    print("   share of days: " + ", ".join(f"{k} {v:.1%}" for k, v in share.items()))

    years = len(idx) / TRADING_DAYS
    rows, bounds = {}, {}
    for k, r in runs.items():
        if k == "saved":
            continue
        d = r - runs["saved"]
        row = {}
        for g in ("normal", "crash", "rebound"):
            x = d[reg == g]
            row[f"{g} bp/yr"] = x.sum() / years * 1e4
            row[f"{g} t"] = x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 1 else np.nan
        row["total bp/yr"] = d.sum() / years * 1e4
        rows[k] = row
        bounds[k] = r.where(reg == "normal", runs["saved"])
    print("\n1. VARIANT MINUS SAVED, by regime (annualized over the whole window; "
          "+ = variant better)")
    print(pd.DataFrame(rows).T.to_string(float_format="{:+.1f}".format))

    # Sensitivity (added after the first run): the episode rule above keeps only declines whose
    # *final* trough came within 40 days, which misses fast legs of longer declines (2008, 2011,
    # 2015, 2018 Q4, 2022). Broader label: any day SPY is >= 10% below its 60-day high.
    stress = spy / spy.rolling(60, min_periods=1).max() - 1 <= -DROP
    alt = {}
    for k, r in runs.items():
        if k == "saved":
            continue
        d = r - runs["saved"]
        alt[k] = {f"{g} bp/yr": d[m].sum() / years * 1e4 for g, m in
                  (("not stressed", ~stress), ("stressed", stress))}
        alt[k] |= {f"{g} t": d[m].mean() / d[m].std() * np.sqrt(m.sum()) for g, m in
                   (("not stressed", ~stress), ("stressed", stress))}
        bounds[f"{k} (60d)"] = r.where(~stress, runs["saved"])
    print(f"\n   sensitivity: SPY >= {DROP:.0%} below its 60-day high "
          f"on {stress.mean():.1%} of days")
    print(pd.DataFrame(alt).T.to_string(float_format="{:+.1f}".format))

    t = compare({"saved": runs["saved"], **{k: runs[k] for k in rows},
                 **{f"{k} | oracle gate": v for k, v in bounds.items()}}, split_date=SPLIT)
    m = pd.DataFrame({"sharpe": t["full"]["sharpe"], "train": t["train"]["sharpe"],
                      "test": t["test"]["sharpe"], "max_dd": t["full"]["max_dd"]})
    print("\n2. SHARPE: each variant, and its oracle bound (variant on normal days, saved book in "
          "crashes and rebounds)")
    print(m.to_string(float_format="{:.3f}".format))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    main(ap.parse_args().workers)
