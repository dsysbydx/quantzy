"""Exposure controls after the 2026-11-05 drop: an equity class cap and an exposure-based hedge.

On 2026-11-05 (QQQ -6.5%, VIX 16.6 -> 28.2) the book lost 4.41%, its second-worst day since
2008, an 8-sigma day against its trailing 60-day vol. It held 125% of capital in equities
(long-run mean 71%), no Treasuries since March, its top four positions at 99% of capital, and
no VIX hedge: the hedge arms only once the VIX closes at or below 13, which it had not since
before the build-up. Nothing in the rules limits a class, and equity weight runs -0.63 with
the VIX, so the book is most exposed when calm. **Fixed before running (2026-11-06).** These
are risk controls, judged on the trade-off between Sharpe and tail loss, not as alphas.

Class caps (``PortfolioConfig.group_caps``, the 12 equity ETFs; everything else saved):
  C1 ``eq <= 100% cap``  equities at most 100% of capital (no levered equity)
  C2 ``eq <= 85% cap``   equities at most 85% of capital
  C3 ``eq <= 70% book``  equities at most 70% of the invested book (91% of capital at 1.3x)

Exposure-based hedge (replaces the saved VIX hedge; the other overlays unchanged). ``E`` is
the core's equity weight held on day ``t`` (set at the close before), so a target set at
close ``t`` uses only data through ``t``. As in the saved rule, nothing is bought once the VIX
closes at or above 30 (sell into the spike), and the holding is reset outside a 25% band.
  H1 ``hedge E>100%``   0.05x VIXY when the saved regime is on *or* ``E > 1.0``
  H2 ``hedge 0.1(E-0.75)``  VIXY weight ``max(saved target, 0.10 x max(E - 0.75, 0))``:
                        zero at 75% equity, 0.05x at 125%
  C1+H2                 both
  C3+H2                 both (added 2026-11-07 at the user's request, after the first run)
  C4, C5 (+H2)          equities at most 80% / 90% of the invested book (104% / 117% of
                        capital at 1.3x), alone and with H2 (added 2026-11-07, likewise)

Metrics, on the ETFs' own prices (2008-09 on), split 2017-07-26: Sharpe full / train / test,
CAGR, vol, max drawdown, Calmar, worst day, worst 5-day loss, mean equity weight (all and
2026), the hedge's cost a year, and named episodes (2020-02-19 .. 03-23, 2022, 2025-02-19 ..
04-08, 2026-11-05). The caps also on proxy history (1995-10 on; VIXY only from 2011).

Adoption rule: a variant passes if (a) Sharpe full, train and test are each no more than 0.03
below the saved book, and the proxy run's full Sharpe too (caps); and (b) it improves at least
two of max drawdown (by >= 1.5 points), worst 5-day loss (>= 1.5 points) and Calmar (>= 0.05).
Among those that pass, the best Calmar. It remains a portfolio decision, the user's call.

**Result (2026-11-06, data to 2026-11-05): none passes the rule, because the book's own
history never had a tail event at these exposures. A portfolio decision, not a test result.**

* On actual prices (2008-09 on) the saved book held more than 100% equities only in 2026, so
  the caps and the exposure hedge do nothing in every earlier drawdown: max drawdown (-15.1%),
  worst 5 days (-9.0%), the 2020 crash (-6.1%) and 2022 are the same for every variant, and no
  tail metric moves. Sharpe is unchanged or slightly up (saved 1.399; caps 1.398-1.402; H2
  hedge 1.409, test 1.394 -> 1.410).
* What they change is 2026: on 2026-11-05 the loss falls from -4.4% to -3.6% (C1), -3.3% (C3),
  -3.1% (C2), -3.2% (H1, H2) or -3.0% (C1 + H2), at a cost in 2026 to date of -1.2 (C1, H1),
  -2.6 (C3), -3.7 (C2), -0.6 (H2) and -1.5 points (C1 + H2): the strong year paid for the
  exposure the caps remove.
* On proxy history (1995-10 on), where the cap binds in 1998-2008 too, the caps help both
  ways: C2 Sharpe 1.32 against 1.30 (before 2008-09 1.25 against 1.20), max drawdown -20.0%
  against -21.2%, worst 5 days -10.6% against -12.9%, worst day -6.1% against -7.0%; C3 1.32,
  -20.2%, -11.2%; C1 barely binds (1.31, -21.2%, -12.2%).
* Reading: the controls are near free in Sharpe over 30 years and cut the tail where the
  book was concentrated, but the backtest cannot show the case they insure (a crash at 125%
  equities), because the book only got there this year. C3 (70% of the book) keeps actual
  Sharpe and test at or above saved, lifts proxy Sharpe most per point of 2026 cost, and
  scales with leverage; H2 is the cheaper hedge (+0.01 Sharpe; 2018 +0.3, 2022 +0.5, 2024
  -0.7 points).
* C3 + H2 (added 2026-11-07): Sharpe 1.410 (train 1.412 / test 1.411), the best of all
  variants; 2026-11-05 -2.9% against -4.4%; 2026 to date -2.7 points, 2024 +1.0, 2022 +0.5;
  tail metrics unchanged on actual prices, like the rest. On proxy history its cap part is
  C3 above; the hedge has no data before 2011.
* C4 / C5, equities at most 80% / 90% of the book (added 2026-11-07, chosen after seeing C3):
  they barely bind before 2026. Proxy history 1995-2008: C4 worst 5 days -12.7% against
  -12.9%, C5 identical to saved; only C3's 70% carries the 1998-2008 tail gain (-11.2%,
  Sharpe 1.24 against 1.20). Full actual Sharpe 1.40 for all, 1.41 with H2. 2026 to date
  (return / Sharpe / max drawdown): H2 +17.5% / 1.84 / -5.3%; C3+H2 +15.4% / 1.70 / -6.2%;
  C4+H2 +17.1% / 1.85 / -6.0%; C5+H2 +18.0% / 1.91 / -5.6%. With H2 on, a cap at 80-90% is
  a circuit breaker for extreme concentration, not a change to the book; the backtest cannot
  tell 80% from 90%. Per-period table: ``.cache/research/exposure_periods2.csv``.
* **Adopted: H2 alone** (2026-11-07, the user's call), in ``vix_hedge``. This script's H2
  checks "VIX below 30" on the same close it trades on, a day ahead of the job; the module
  uses the close before, as the regime part does. On the module: Sharpe 1.408 (train 1.412 /
  test 1.409) against 1.399, hedge cost 0.37% of NAV a year against 0.34%, 2026-11-05 -3.2%.

Usage:
    uv run python research/research_exposure_controls.py
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace

import numpy as np
import pandas as pd

from quantzy.backtest import WalkForwardBacktester
from quantzy.portfolio import GroupCap
from quantzy.research import compare
from quantzy.strategies import core_multiasset as core
from quantzy.strategies import core_proxies, overlays
from quantzy.strategies import vix_hedge as vh

SPLIT = pd.Timestamp("2017-07-26")
CAPS = {
    "saved": (),
    "C1 eq<=100% cap": (GroupCap(tuple(core.EQUITY), 1.0, "capital"),),
    "C2 eq<=85% cap": (GroupCap(tuple(core.EQUITY), 0.85, "capital"),),
    "C3 eq<=70% book": (GroupCap(tuple(core.EQUITY), 0.70, "book"),),
    # added 2026-11-07 at the user's request, after the first run: looser versions of C3
    "C4 eq<=80% book": (GroupCap(tuple(core.EQUITY), 0.80, "book"),),
    "C5 eq<=90% book": (GroupCap(tuple(core.EQUITY), 0.90, "book"),),
}
EPISODES = {"2020 crash": ("2020-02-20", "2020-03-23"), "2022": ("2022-01-01", "2022-12-31"),
            "2025 tariff": ("2025-02-20", "2025-04-08"), "2026-11-05": ("2026-11-05", "2026-11-05")}


def run_core(args):
    data, name = args
    b = (core.load if data == "actual" else core_proxies.load)()
    s = core.build(portfolio=replace(core.PORTFOLIO, group_caps=CAPS[name]))
    res = WalkForwardBacktester(rebalance_every=1, warmup=core.warmup(b, core.build())).run(b, s)
    r = core.PORTFOLIO.net_of_borrowing(res.returns, res.weights, core.CASH)
    other = sum(o.backtest_returns(b).reindex(r.index).fillna(0.0)
                for o in overlays(core) if o is not vh)
    return data, name, r + other, res.weights


def hedge_returns(b, target: np.ndarray) -> pd.Series:
    """``vix_hedge.backtest_returns`` with a given target per close."""
    px = vh.asset_prices(b)
    r = px.pct_change(fill_method=None).fillna(0.0).to_numpy()
    c = b.prices["TBILL"].pct_change(fill_method=None).fillna(0.0).to_numpy()
    target = np.where(px.notna().to_numpy(), target, 0.0)
    w, out = 0.0, np.zeros(len(r))
    for i in range(len(r)):
        out[i] = w * (r[i] - c[i])
        w *= (1 + r[i]) / (1 + w * r[i] + (1 - w) * c[i])
        new = vh.rebalance(target[i], w)
        out[i] -= abs(new - w) * vh.COST_BPS / 1e4
        w = new
    return pd.Series(out, index=b.index)


def hedge_targets(b, weights: pd.DataFrame) -> dict[str, np.ndarray]:
    vix = vh._vix(b)
    saved = np.where(vh.regime(vix).shift(1, fill_value=False).to_numpy(), vh.SIZE, 0.0)
    e = weights[core.EQUITY].sum(axis=1).reindex(b.index).fillna(0.0).to_numpy()
    calm = (vix < vh.HIGH).to_numpy()  # nothing bought at or above 30
    h1 = np.where((saved > 0) | ((e > 1.0) & calm), vh.SIZE, 0.0)
    h2 = np.maximum(saved, np.where(calm, 0.10 * np.clip(e - 0.75, 0, None), 0.0))
    return {"saved": saved, "H1": h1, "H2": h2}


def metrics(runs: dict[str, pd.Series], eq: dict[str, pd.Series],
            hedge: dict[str, pd.Series]) -> pd.DataFrame:
    t = compare(runs, split_date=SPLIT)
    rows = {}
    for n, r in runs.items():
        nav = (1 + r).cumprod()
        dd = float((nav / nav.cummax() - 1).min())
        cagr = float(nav.iloc[-1] ** (252 / len(r)) - 1)
        row = {"sharpe": t["full"]["sharpe"][n], "train": t["train"]["sharpe"][n],
               "test": t["test"]["sharpe"][n], "cagr": cagr, "vol": r.std() * np.sqrt(252),
               "max_dd": dd, "calmar": cagr / -dd, "worst day": r.min(),
               "worst 5d": r.rolling(5).sum().min(), "eq wt": eq[n].mean(),
               "eq wt 2026": eq[n].loc["2026"].mean(),
               "hedge/yr": hedge[n].mean() * 252 if n in hedge else 0.0}
        for lab, (a, z) in EPISODES.items():
            row[lab] = float((1 + r.loc[a:z]).prod() - 1)
        rows[n] = row
    return pd.DataFrame(rows).T


def fmt(m: pd.DataFrame) -> str:
    f = {c: "{:.3f}".format for c in ("sharpe", "train", "test", "calmar")}
    return m.to_string(formatters={c: f.get(c, "{:+.1%}".format) for c in m.columns})


def main() -> None:
    pd.set_option("display.width", 260)
    jobs = [(d, n) for d in ("actual", "proxy") for n in CAPS]
    with ProcessPoolExecutor(len(jobs)) as pool:
        out = {(d, n): (r, w) for d, n, r, w in pool.map(run_core, jobs)}
    b = core.load()

    runs, eq, hedge = {}, {}, {}
    for n in CAPS:
        r, w = out[("actual", n)]
        tg = hedge_targets(b, w)
        e = w[core.EQUITY].sum(axis=1)
        h = hedge_returns(b, tg["saved"]).reindex(r.index).fillna(0.0)
        runs[n], eq[n], hedge[n] = r + h, e, h
        if n in ("saved", "C1 eq<=100% cap", "C3 eq<=70% book", "C4 eq<=80% book",
                 "C5 eq<=90% book"):
            for k in ("H1", "H2"):
                if n != "saved" and k == "H1":
                    continue
                hk = hedge_returns(b, tg[k]).reindex(r.index).fillna(0.0)
                lab = f"{k} hedge" if n == "saved" else f"{n[:2]} + {k}"
                runs[lab], eq[lab], hedge[lab] = r + hk, e, hk
        if n == "saved":
            days = (tg["H2"] > tg["saved"]).mean()
            on26 = pd.Series(tg["H2"], index=b.index).loc["2026"]
            print(f"H2 adds VIXY beyond the saved rule on {days:.0%} of days; 2026 mean VIXY "
                  f"weight {on26.mean():.3f}, on 2026-11-04 {on26.loc[:'2026-11-04'].iloc[-1]:.3f}")
    m = metrics(runs, eq, hedge)
    print("\n=== actual prices, with all overlays (the VIX hedge as named)")
    print(fmt(m))

    pruns, peq = {}, {}
    for n in CAPS:
        r, w = out[("proxy", n)]
        bp = core_proxies.load()
        h = hedge_returns(bp, hedge_targets(bp, w)["saved"]).reindex(r.index).fillna(0.0)
        pruns[n], peq[n] = r + h, w[core.EQUITY].sum(axis=1)
    pm = metrics(pruns, peq, {})
    pm["pre-2008"] = [compare({n: pruns[n].loc[:"2008-09-09"]})["sharpe"].iloc[0] for n in pm.index]
    print("\n=== proxy history (1995-10 on), caps only")
    print(fmt(pm.drop(columns=["hedge/yr"])))

    s0, p0 = m.loc["saved"], pm.loc["saved"]
    print("\n=== adoption rule")
    for n in m.index[1:]:
        sh = all(m.loc[n, c] >= s0[c] - 0.03 for c in ("sharpe", "train", "test"))
        if n in pm.index:
            sh = sh and pm.loc[n, "sharpe"] >= p0["sharpe"] - 0.03
        gains = [m.loc[n, "max_dd"] - s0["max_dd"] >= 0.015,
                 m.loc[n, "worst 5d"] - s0["worst 5d"] >= 0.015,
                 m.loc[n, "calmar"] - s0["calmar"] >= 0.05]
        print(f"   {n:<18} Sharpe within 0.03: {sh}; tail gains (dd, 5d, calmar): {gains} "
              f"-> {'PASS' if sh and sum(gains) >= 2 else 'fail'}")

    yrs = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    print("\n   calendar-year return minus saved")
    print(yrs.sub(yrs["saved"], axis=0).drop(columns="saved").to_string(
        float_format="{:+.1%}".format))


if __name__ == "__main__":
    main()
