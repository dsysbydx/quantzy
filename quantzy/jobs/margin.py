"""Margin and stress checks on a target book: can the broker force a sale, or refuse a trade?

All figures are fractions of NAV (account equity = 1). Rules default to Reg T at a broker
such as IBKR: 25% maintenance on long ETF positions, 30% on shorts, 50% initial margin
(which caps gross exposure at 2x when positions are opened). A broker's house margin can be
higher for some ETFs and it may liquidate in real time rather than call; check these
estimates against the broker's own margin figures.

* :func:`margin` -- gross, net, borrowing, the maintenance requirement, the cushion above it
  (excess liquidity) and the fall in the long book that would exhaust it.
* :func:`stress` -- the target hit at once by each asset's actual 5-day return in a past
  crash week: the loss and the cushion left.
* :func:`scale_overlays` -- shrink the overlays so the book stays under the initial-margin
  limit, rather than let the broker reject or trim the orders.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class MarginRules:
    maint_long: float = 0.25
    maint_short: float = 0.30
    initial: float = 0.50

    @property
    def max_gross(self) -> float:
        """Gross exposure the initial requirement allows (2x under Reg T)."""
        return 1.0 / self.initial


REG_T = MarginRules()
#: Keep the target this far under the initial-margin limit (2x -> 1.95x).
GROSS_BUFFER = 0.05
#: Alert when the long book's fall to a margin call is under this.
MIN_DROP_TO_CALL = 0.30
#: Alert when a stress week would leave less cushion than this.
MIN_STRESS_CUSHION = 0.15
#: Past weeks whose 5-day returns are replayed on the target (the week ending on the date).
STRESS_WEEKS = {"2008-10 crash week": "2008-10-10", "2020-03 crash week": "2020-03-16",
                "2022-06 bond rout week": "2022-06-16"}


def margin(weights: pd.Series, rules: MarginRules = REG_T) -> dict[str, float]:
    """Margin position of a book of non-cash ``weights`` (fractions of NAV)."""
    longs = float(weights.clip(lower=0).sum())
    shorts = float(weights.clip(upper=0).abs().sum())
    maintenance = rules.maint_long * longs + rules.maint_short * shorts
    cushion = 1.0 - maintenance
    # the long book falls by x: equity drops x * longs, the requirement maint_long * x * longs
    drop = cushion / (longs * (1 - rules.maint_long)) if longs > 0 else float("inf")
    return {
        "long": longs, "short": shorts, "gross": longs + shorts, "net": longs - shorts,
        "borrowed": max(longs - shorts - 1.0, 0.0),
        "maintenance": maintenance, "cushion": cushion, "drop_to_call": drop,
        "initial_used": rules.initial * (longs + shorts),
    }


def stress(weights: pd.Series, prices: pd.DataFrame, rules: MarginRules = REG_T,
           weeks: dict[str, str] = STRESS_WEEKS) -> dict[str, dict[str, float]]:
    """Each past week's 5-day returns applied to ``weights`` at once: loss and cushion left.

    Assets without history in that week (younger ETFs) take no shock, so the loss is a floor.
    """
    r5 = prices.pct_change(5, fill_method=None)
    out = {}
    for name, end in weeks.items():
        hist = r5.loc[:end]
        # skip weeks the data does not cover (it must reach the week's last day)
        if hist.empty or hist.index[-1] < pd.Timestamp(end) - pd.Timedelta(days=7):
            continue
        shock = hist.iloc[-1].reindex(weights.index).fillna(0.0)
        loss = float((weights * shock).sum())
        moved = weights * (1 + shock)
        req = (rules.maint_long * float(moved.clip(lower=0).sum())
               + rules.maint_short * float(moved.clip(upper=0).abs().sum()))
        out[name] = {"loss": loss, "cushion": 1.0 + loss - req}
    return out


def scale_overlays(core: pd.Series, overlay: pd.Series, limit: float) -> float:
    """The largest factor in [0, 1] on ``overlay`` that keeps ``|core + s * overlay|`` gross
    at or under ``limit`` (1 when it already fits; 0 when even the core alone does not)."""
    def gross(s: float) -> float:
        return float((core + s * overlay).abs().sum())

    if gross(1.0) <= limit:
        return 1.0
    if gross(0.0) > limit:
        return 0.0
    lo, hi = 0.0, 1.0
    for _ in range(50):  # gross is convex in s, so bisection on the crossing is safe
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if gross(mid) <= limit else (lo, mid)
    return lo
