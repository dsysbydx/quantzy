"""Are correlations underestimated in downturns?

The shared risk model (5/20/60-day EWMA blend) forecasts the covariance from an effective ~48
days. If correlations jump when markets fall, the book's risk is understated exactly when it
matters. Weekly decision dates, the saved core book's own weights, and the next 21 days:

* predicted (the risk model at the decision date) against realized correlation, averaged over
  equity-equity pairs (the 12 equity ETFs) and equity-bond pairs (against IEF and TLT);
* the book's realized over predicted vol, split into a correlation part (realized
  correlations, predicted vols) and a vol part (realized vols, predicted correlations).

Two conditionings:

1. **Ex post**: SPY's next 21 days in its worst 10%. The loss you take, but biased: choosing
   windows *for* a large common move raises measured correlation even when the true one is
   unchanged (Forbes and Rigobon, 2002). Split by period as well.
2. **Ex ante**: states known at the decision date -- SPY below its 200-day EMA, VIX above 25,
   SPY more than 10% below its 1-year high. No selection bias, and the only kind a fix could
   use. The t-stat on the extra gap divides the count by 4 for the overlapping windows.

Not pre-specified: an exploratory check, run once (2026-10-31), with no candidate to adopt.

**Result (2026-10-31): only at the onset of a crash, and mostly through vols.** In the worst
10% of months equity correlations came in 0.11 above forecast (0.79 against 0.66) and the
book's realized vol was 1.31x predicted: 1.28x from vols, 1.07x from correlations. The
equity-bond hedge did better than forecast before 2022 (-0.13) and worse since (+0.12;
correlation part 1.14x in 2022-26). In the ex-ante states the model already forecasts high
correlations (0.76-0.78 against ~0.60) and they arrive as forecast: the extra gap is +0.01 to
+0.02 (t 0.9 or less) and the book's risk is slightly *over*-stated (0.88-0.93x), as vols
mean-revert. The miss is the window the model has not seen; none of these states predicts
it, so conditioning correlations on them would not help. The defences are a stress check on
the weights (the daily job's stress vol) and insurance (the VIX hedge overlay).

Usage:
    uv run python research/research_downturn_corr.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.signals.indicators import ema
from quantzy.strategies import core_multiasset as core

H = 21  # forward window, days
STEP = 5  # weekly decision dates
R, EQ, BONDS = core.RISKY, core.EQUITY, ["IEF", "TLT"]
COLS = ["eq_pred", "eq_real", "eq_gap", "sb_pred", "sb_real", "sb_gap",
        "book_ratio", "corr_part", "vol_part"]


def corr(c: np.ndarray) -> np.ndarray:
    d = np.sqrt(np.diag(c))
    return c / np.outer(d, d)


def mean_block(rho: np.ndarray, a: list[str], b: list[str]) -> float:
    """Mean correlation between ``a`` and ``b``; within ``a`` (off-diagonal) if they match."""
    ia, ib = [R.index(t) for t in a], [R.index(t) for t in b]
    m = rho[np.ix_(ia, ib)]
    return float(m[np.triu_indices(len(ia), 1)].mean() if a == b else m.mean())


def panel() -> pd.DataFrame:
    """One row per weekly decision date: predicted and realized correlations, book vol ratios,
    SPY's forward return and the ex-ante states."""
    b = latest_complete(core.load(), core.RISKY)
    s = core.build()
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, s)).run(b, s)
    rets = b.prices.pct_change(fill_method=None)
    spy, vix = b["market"].iloc[:, 0].ffill(), b["vix"].iloc[:, 0].ffill()
    fwd = spy.shift(-H) / spy - 1
    state = pd.DataFrame({
        "below EMA200": spy < ema(spy.to_frame(), 200).iloc[:, 0],
        "VIX > 25": vix > 25,
        "SPY dd > 10%": spy / spy.rolling(TRADING_DAYS, min_periods=1).max() - 1 < -0.10,
    })
    rows = []
    for d in res.weights.index[::STEP]:
        i = b.index.get_loc(d)
        f = rets[R].iloc[i + 1: i + 1 + H]
        if len(f) < H or f.isna().any().any():
            continue
        p = s.risk_model.covariance(b, d).loc[R, R].to_numpy() / TRADING_DAYS
        q = np.cov(f.to_numpy(), rowvar=False)
        w = res.weights.loc[d, R].to_numpy()
        rp, rq = corr(p), corr(q)
        dp, dq = np.sqrt(np.diag(p)), np.sqrt(np.diag(q))
        pred = np.sqrt(w @ p @ w)

        def ratio(c: np.ndarray, w=w, pred=pred) -> float:
            return float(np.sqrt(w @ c @ w) / pred)

        rows.append({
            "date": d,
            "eq_pred": mean_block(rp, EQ, EQ), "eq_real": mean_block(rq, EQ, EQ),
            "sb_pred": mean_block(rp, EQ, BONDS), "sb_real": mean_block(rq, EQ, BONDS),
            "book_ratio": ratio(q),
            "corr_part": ratio(np.outer(dp, dp) * rq),
            "vol_part": ratio(np.outer(dq, dq) * rp),
            "fwd_spy": fwd.loc[d], **state.loc[d].to_dict(),
        })
    t = pd.DataFrame(rows).set_index("date")
    t["eq_gap"] = t.eq_real - t.eq_pred
    t["sb_gap"] = t.sb_real - t.sb_pred
    return t


def split(t: pd.DataFrame, mask: pd.Series, name: str) -> pd.DataFrame:
    a, z = t[mask], t[~mask]
    return pd.DataFrame({f"{name} ({len(a)})": a[COLS].median(),
                         f"other ({len(z)})": z[COLS].median()}).T


def main() -> None:
    pd.set_option("display.width", 220)
    t = panel()
    states = ["below EMA200", "VIX > 25", "SPY dd > 10%"]
    print(f"weekly dates {t.index[0].date()} .. {t.index[-1].date()}, next {H} days; medians")

    worst = t.fwd_spy <= t.fwd_spy.quantile(0.10)
    print("\n1. EX POST: SPY's next 21 days in its worst 10%")
    print(split(t, worst, "worst 10%").round(3).to_string())
    w = t[worst]
    period = pd.cut(w.index.year, [2007, 2012, 2021, 2026],
                    labels=["2008-12", "2013-21", "2022-26"])
    g = w.groupby(period, observed=True)
    print("\n   by period")
    print(pd.concat([g.size().rename("n"), g[COLS].median()], axis=1).round(3).to_string())

    for c in states:
        m = t[c].astype(bool)
        print(f"\n2. EX ANTE: {c} at the decision date")
        print(split(t, m, c).round(3).to_string())
        x = t.eq_gap[m] - t.eq_gap[~m].median()
        print(f"   extra equity gap: median {x.median():+.3f}, mean {x.mean():+.3f}, "
              f"t {x.mean() / x.std() * np.sqrt(len(x) / 4):+.1f}")


if __name__ == "__main__":
    main()
