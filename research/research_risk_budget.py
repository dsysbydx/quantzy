"""Risk-budget construction against the saved mean-variance book.

The saved book's concentration (103% of capital in four US sectors on 2026-10-16, the EM
pile-up with 65 ETFs, the US tilt by ticker count) comes from mean-variance on equal priors:
it trusts expected-return differences and within-class correlations. A risk-budget book
uses the same views but sets risk by rule. **Fixed before any test.**

Inputs, shared: each ETF's *view* is its prior Sharpe (IEF and TLT carry-moved) plus the
trend, EWMAC and VRP tilts at their saved weights, in Sharpe units, as the optimizer sees it,
known at the prior close. Classes: US equity (9 sectors, QQQ), ex-US equity (EFA, EEM),
Treasuries (IEF, TLT), gold and commodities (GLD, DBC), USD (UUP).

* **A**: equal risk for each class with a positive view; within a class, risk proportional
  to the view (weight ``max(view, 0) / vol``).
* **B**: class risk proportional to the class's average positive view (so roughly to its
  prior, plus timing; USD gets risk only when its trend view is positive); within a class as
  in A.
* **C**: the saved mean-variance book.

A and B: class sleeves are sized on their standalone vol under the saved risk model, then
the whole book is scaled to 10% ex-ante vol, gross at most 1.3x, each ETF at most 26% of
capital (any excess stays in cash). Rebalanced weekly, skipping trades under 0.5% of
capital; per-ETF costs as saved; borrowing at T-bill + 0.5%.

**Result (2026-10-18): keep mean-variance.** A (equal class risk) made Sharpe 0.82 and B
(class risk ~ view) 0.90 against 1.13 for C, with double the turnover (2.0-2.3x). They put
36-41% of book variance in ex-US equity, which lagged (2013-14, 2019-21), and lost more in
2022 (-10.0% / -8.3% against -2.6%); only 2008-09 went their way. They are far more robust
today: stress vol 9.5% / 11.6% against 21.1%. C and B are 0.89 correlated, so blending
mostly dilutes C (25% in B: 1.09).

Usage:
    uv run python research/research_risk_budget.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.jobs.daily import latest_complete
from quantzy.research import compare
from quantzy.signals.indicators import realized_vol
from quantzy.strategies import core_multiasset as core

CASH = core.CASH
R = core.RISKY
SPLIT = pd.Timestamp("2017-07-26")
CLASSES = {"US equity": [*core.CLASSES["US sectors"], "QQQ"], "ex-US equity": ["EFA", "EEM"],
           "Treasuries": ["IEF", "TLT"], "gold & commodities": ["GLD", "DBC"], "USD": ["UUP"]}
CLASS_OF = {t: c for c, ts in CLASSES.items() for t in ts}
TARGET_VOL, MAX_GROSS, CAP, REBAL, MIN_TRADE = 0.10, 1.3, 0.26, 5, 0.005
SELLOFFS = {"2008-09 .. 2009-03": ("2008-09-10", "2009-03-09"),
            "2013 taper (May-Jun)": ("2013-05-01", "2013-06-30"),
            "2020-02 .. 2020-03": ("2020-02-19", "2020-03-23"),
            "2022": ("2022-01-01", "2022-12-31")}


def views(b) -> pd.DataFrame:
    """Per ETF, prior + alpha tilts in Sharpe units, tradeable at each row."""
    total = pd.DataFrame(0.0, index=b.index, columns=R)
    for sig, k in core.ALPHAS.values():
        vol = realized_vol(b.prices, sig.vol_window, annualize=True).shift(sig.lag)
        unit = (sig.scores(b) / vol.where(vol > 0))[R]
        total += unit.fillna(0.0) * k
    return total


def target(view: pd.Series, cov: pd.DataFrame, variant: str) -> pd.Series:
    vol = np.sqrt(np.diag(cov.loc[R, R]))
    vol = pd.Series(vol, index=R)
    pos = view.clip(lower=0)
    w = pd.Series(0.0, index=R)
    budgets = {}
    for c, ts in CLASSES.items():
        if pos[ts].sum() <= 0:
            continue
        sleeve = pos[ts] / vol[ts]
        sleeve = sleeve / sleeve.sum()
        s_vol = float(np.sqrt(sleeve @ cov.loc[ts, ts] @ sleeve))
        budget = 1.0 if variant == "A" else float(pos[ts].mean())
        if budget > 0 and s_vol > 0:
            budgets[c] = budget
            w[ts] = sleeve * budget / s_vol
    if not budgets:
        return w
    book_vol = float(np.sqrt(w @ cov.loc[R, R] @ w))
    w = w * (TARGET_VOL / book_vol)
    w = w.clip(upper=CAP)
    if w.sum() > MAX_GROSS:
        w = w * (MAX_GROSS / w.sum())
    return w


def rule_book(b, variant: str, start: int):
    rm = core.PORTFOLIO.risk_model()
    v = views(b)
    rets = b.prices.pct_change(fill_method=None).fillna(0.0)
    costs = pd.Series(core.EXECUTION_COST_BPS).reindex(R).fillna(0) / 1e4
    held = pd.Series(0.0, index=R)
    out_r, out_w, traded = [], [], []
    for i in range(start, len(b.index)):
        day = b.index[i]
        if (i - start) % REBAL == 0:
            tgt = target(v.iloc[i], rm.covariance(b, day), variant)
            dw = tgt - held
            dw[dw.abs() < MIN_TRADE] = 0.0
            cost = float((dw.abs() * costs).sum())
            held = held + dw
            traded.append(float(dw.abs().sum()) / 2)
        else:
            cost = 0.0
        r = rets.iloc[i]
        cash = 1.0 - held.sum()
        gross_r = float((held * r[R]).sum()) + cash * float(r[CASH])
        borrow = max(-cash, 0.0) * core.PORTFOLIO.borrow_spread_bps / 1e4 / TRADING_DAYS
        port = gross_r - borrow - cost
        out_r.append(port)
        out_w.append(held.copy())
        growth = (1 + r[R]) / (1 + port)  # drift to the next day
        held = held * growth
    idx = b.index[start:]
    years = len(idx) / TRADING_DAYS
    return pd.Series(out_r, index=idx), pd.DataFrame(out_w, index=idx), sum(traded) / years


def main() -> None:
    pd.set_option("display.width", 300)
    b = latest_complete(core.load(), R)
    start = core.warmup(b)
    s = core.build()
    res = WalkForwardBacktester(rebalance_every=1, warmup=start).run(b, s)
    rc = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, CASH)
    out = {"C: saved mean-variance": (rc, res.weights[R],
                                      sum(d.turnover for d in res.decisions)
                                      / (len(rc) / TRADING_DAYS))}
    for variant, label in (("A", "A: equal class risk"), ("B", "B: class risk ~ view")):
        out[label] = rule_book(b, variant, start)
    names = list(out)
    runs = {n: out[n][0] for n in names}
    t = compare(runs, split_date=SPLIT)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "ret": t["full"]["ann_return"], "vol": t["full"]["ann_vol"],
        "sharpe": t["full"]["sharpe"], "max_dd": t["full"]["max_dd"],
    }).loc[names]
    r_all = b.prices[R].pct_change(fill_method=None)
    for n in names:
        w = out[n][1].clip(lower=0)
        share = w.div(w.sum(axis=1).replace(0, np.nan), axis=0)
        m.loc[n, "turnover"] = out[n][2]
        m.loc[n, "gross"] = w.sum(axis=1).mean()
        m.loc[n, "top-4 wt"] = w.apply(lambda x: x.nlargest(4).sum(), axis=1).mean()
        m.loc[n, "eff. positions"] = (1 / (share ** 2).sum(axis=1)).mean()
        for lab, (a, z) in SELLOFFS.items():
            m.loc[n, lab] = (1 + runs[n].loc[a:z]).prod() - 1
    pct = "{:.1%}".format
    print(f"BOOK, {runs[names[0]].index[0].date()} .. {runs[names[0]].index[-1].date()}, "
          f"split {SPLIT.date()}")
    print(m.to_string(formatters={
        **{c: pct for c in ("ret", "vol", "max_dd", "top-4 wt", *SELLOFFS)},
        **{c: "{:.2f}".format for c in ("tr_sharpe", "te_sharpe", "sharpe", "gross")},
        "turnover": "{:.1f}".format, "eff. positions": "{:.1f}".format}))

    print("\n   average weight / share of book variance, by class")
    rows = {}
    for n in names:
        w = out[n][1]
        contrib = (w.shift(1).fillna(0) * r_all.reindex(w.index).fillna(0)).T.groupby(
            CLASS_OF).sum().T
        tot = contrib.sum(axis=1)
        cw = w.T.groupby(CLASS_OF).sum().T.mean()
        risk = contrib.apply(lambda c, tot=tot: c.cov(tot)) / tot.var()
        rows[n] = {c: f"{cw[c]:.0%} / {risk[c]:+.0%}" for c in CLASSES}
    print(pd.DataFrame(rows).T.to_string())

    cov_now = core.PORTFOLIO.risk_model().covariance(b, b.index[-1]).loc[R, R]
    vol = np.sqrt(np.diag(cov_now))
    c_hist = r_all.loc["2008":"2025"].corr().to_numpy() * np.outer(vol, vol)
    today = pd.DataFrame({
        "C: saved mean-variance": s.decide(b).target.weights.reindex(R),
        "A: equal class risk": target(views(b).iloc[-1], cov_now, "A"),
        "B: class risk ~ view": target(views(b).iloc[-1], cov_now, "B")})
    print(f"\n   today's target ({b.index[-1].date()}), % of capital")
    print(today.T.to_string(float_format="{:.0%}".format))
    print("   ex-ante vol: risk model / 2008-2025 correlations: " + ", ".join(
        f"{n} {np.sqrt(today[n] @ cov_now @ today[n]):.1%} / "
        f"{np.sqrt(today[n].to_numpy() @ c_hist @ today[n].to_numpy()):.1%}" for n in names))
    yr = pd.DataFrame({n: (1 + x).groupby(x.index.year).prod() - 1 for n, x in runs.items()})
    print("\n   calendar years")
    print(yr.to_string(float_format="{:+.1%}".format))


if __name__ == "__main__":
    main()
