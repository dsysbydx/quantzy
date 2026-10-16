"""Trend on a broader universe: country indexes, rates, credit, commodities and currencies.

``research_trend_breadth.py`` found the EMA-200 trend IC concentrated in equity indexes and
commodities, and absent in US sectors, which were 9 of its 17 ETFs. Sectors move together,
so they count as about two independent trend bets. The trend-following literature finds the
edge across many *unrelated* markets, so this widens the universe to 45 liquid ETFs in
nine groups and asks again, with the same rule for every asset and no per-group tuning.

1. **IC.** The time-series IC against the next day's return divided by trailing vol. Dates
   are split into 47-day chunks dealt alternately to folds A and B, so both folds span every
   regime (:func:`~quantzy.research.chunk_folds`). Each asset is standardized with fold-A
   stats only. Per date, the IC is averaged within each group and then across groups with
   equal weight, so nine sectors do not outvote six currencies. t-stats are Newey-West.
2. **Portfolio.** The same book as ``research_trend_breadth.py``, with the shared
   portfolio-level settings (``PORTFOLIO`` there): ``mu_i = vol_i * (SR_group + k *
   trend_i)``, compared with the 17-ETF universe on the same dates.

Most groups start trading in 2007, so the backtest starts in 2009 and misses 2008.

Usage:
    uv run python examples/research_trend_global.py [--workers 8]
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import research_trend_breadth as rb

from quantzy.backtest import FixedWeights, WalkForwardBacktester
from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import chunk_folds, compare, newey_west_t
from quantzy.research.ic import _ts_pairs
from quantzy.risk import ewma_blend
from quantzy.signals import Identity, SharpePrior, TrendStrength, VolScaledTrend
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

CASH = "TBILL"
GROUPS = {
    "US equity": ["SPY", "IWM", "QQQ"],
    "US sectors": ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"],
    "developed countries": ["EWJ", "EWG", "EWU", "EWC", "EWA", "EWL"],
    "EM countries": ["EWZ", "EWT", "EWY", "EWW", "FXI"],
    "rates": ["SHY", "IEF", "TLT", "TIP"],
    "credit": ["LQD", "HYG", "EMB", "BWX"],
    "commodities": ["GLD", "SLV", "USO", "DBB", "DBA"],
    "currencies": ["UUP", "FXE", "FXY", "FXB", "FXA", "FXC", "FXF"],
    "real estate": ["VNQ", "RWX"],
}
RISKY = [t for ts in GROUPS.values() for t in ts]
GROUP_OF = {t: g for g, ts in GROUPS.items() for t in ts}
UNIVERSE = Universe(
    name="trend_global",
    assets=[Asset(t, t, "etf") for t in RISKY]
    + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")],
)
START = "1998-12-22"

# Long-run Sharpe priors, by judgment: equities 0.3, rates, credit and real estate 0.25, gold
# 0.2, other commodities 0.1 (roll costs), currencies 0 (no premium in a spot FX position).
SR = {"US equity": 0.3, "US sectors": 0.3, "developed countries": 0.3, "EM countries": 0.3,
      "rates": 0.25, "credit": 0.25, "commodities": 0.1, "currencies": 0.0,
      "real estate": 0.25}
PRIOR = {t: 0.2 if t == "GLD" else SR[GROUP_OF[t]] for t in RISKY}

SPEC = ("ema", 200)
IC_SPECS = [(ma, w) for ma in ("sma", "ema") for w in (50, 100, 200)]
CHUNK, NW_LAGS, VOL_WINDOW = 47, 21, 60


def load() -> DataBundle:
    raw = CachedReader(YFinanceReader()).history([*RISKY, "^IRX"], start=START)
    yld = raw["^IRX"].ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    prices = raw[RISKY].copy()
    prices[CASH] = (1.0 + cash_ret).cumprod()
    return DataBundle.build(prices)


def trend(ma: str, window: int) -> TrendStrength:
    return TrendStrength(window, exclude=(CASH,), ma=ma, ewm_tol=rb.EWM_TOL,
                         normalizer=Identity())


def tradeable_scores(signal, bundle: DataBundle) -> pd.DataFrame:
    """Scores, NaN wherever an asset had too little history (``Signal.scores`` fills 0)."""
    px = bundle.prices[RISKY]
    ok = (px.notna().cumsum() > signal.min_history).shift(signal.lag, fill_value=False)
    return signal.scores(bundle)[RISKY].where(ok)


# --- 1. IC -------------------------------------------------------------------------------------

def ic_report(bundle: DataBundle) -> None:
    px = bundle.prices[RISKY]
    folds = chunk_folds(bundle.index, CHUNK)
    in_a, in_b = folds == 0, folds == 1

    def fmt(x: pd.Series) -> str:
        return f"{x.mean():+.4f} ({newey_west_t(x, NW_LAGS):+.1f})"

    def products(spec: tuple[str, int], method: str) -> pd.DataFrame:
        zx, zy = _ts_pairs(tradeable_scores(trend(*spec), bundle), px, 1, method, VOL_WINDOW,
                           fit=in_a)
        return zx * zy

    def grouped(prod: pd.DataFrame) -> pd.DataFrame:
        return prod.T.groupby(GROUP_OF).mean().T  # per date, mean within each group

    print(f"\n1. TIME-SERIES IC vs next-day return / trailing {VOL_WINDOW}d vol. Folds: "
          f"{CHUNK}-day chunks, alternating ({int(in_a.sum())} / {int(in_b.sum())} days); "
          f"z-stats from fold A. Mean IC (Newey-West t, {NW_LAGS} lags)")
    rows = []
    for spec in IC_SPECS:
        for method in ("spearman", "pearson"):
            g = grouped(products(spec, method))
            eq = g.mean(axis=1).dropna()  # groups equally weighted
            pooled = products(spec, method).mean(axis=1).dropna()  # assets equally weighted
            rows.append({"spec": f"{spec[0]}{spec[1]}", "method": method,
                         "groups eq: A": fmt(eq[in_a]), "groups eq: B": fmt(eq[in_b]),
                         "groups eq: all": fmt(eq), "assets eq: all": fmt(pooled)})
    print(pd.DataFrame(rows).to_string(index=False))

    for method in ("spearman", "pearson"):
        prod = products(SPEC, method)
        g = grouped(prod)
        print(f"\n   {SPEC[0]}{SPEC[1]} by group, {method}")
        print(pd.DataFrame({c: {"A": fmt(g[c][in_a].dropna()), "B": fmt(g[c][in_b].dropna()),
                                "all": fmt(g[c].dropna())} for c in GROUPS}).T.to_string())
    prod = products(SPEC, "spearman")
    per = pd.DataFrame({t: {"group": GROUP_OF[t], "ic": prod[t].mean(),
                            "t": newey_west_t(prod[t], NW_LAGS),
                            "since": px[t].first_valid_index().date()} for t in RISKY}).T
    print(f"\n   {SPEC[0]}{SPEC[1]} per asset, spearman, full history")
    print(per.to_string(formatters={"ic": "{:+.4f}".format, "t": "{:+.1f}".format}))
    print(f"   positive on {(per['ic'] > 0).sum()} of {len(per)}")

    eq = grouped(prod).mean(axis=1).dropna()
    print("\n   groups-equal IC by period")
    for a, z in (("1999", "2004"), ("2005", "2009"), ("2010", "2014"), ("2015", "2019"),
                 ("2020", "2026")):
        x = eq.loc[a:z]
        print(f"     {a}-{z}: {fmt(x)}  n={len(x)}")


# --- 2. portfolio ------------------------------------------------------------------------------

def build(p: dict) -> Strategy:
    ma, w = SPEC
    signals = {"prior": SharpePrior(PRIOR, normalizer=Identity())}
    if p["k"]:
        signals["trend"] = VolScaledTrend(w, exclude=(CASH,), ma=ma, ewm_tol=rb.EWM_TOL,
                                       normalizer=Identity())
    cap = C.max_book_share if p["cap_of"] == "book" else C.max_weight
    return Strategy(
        universe=UNIVERSE,
        signals=signals,
        weights={"trend": p["k"]} if p["k"] else {},
        risk_model=ewma_blend(p["hls"]),
        optimizer=MeanVariance(
            risk_aversion=p["ra"],
            constraints=[C.budget(), C.long_only(exclude=(CASH,)),
                         C.max_leverage(p["lev"], CASH), cap(p["cap"], exclude=(CASH,))],
            cost_model=LinearCost(rb.COST_BPS),
            cost_aversion=p["ca"],
        ),
        name=f"global k={p['k']:g}",
    )


def warmup(bundle: DataBundle) -> int:
    need = max(trend(*SPEC).min_history, 250) + 2
    first = bundle.prices[RISKY].apply(lambda s: bundle.index.get_loc(s.first_valid_index()))
    return int(first.max()) + need


def run_one(p: dict) -> tuple[str, pd.Series, pd.DataFrame, float]:
    """One backtest, in either universe, net of the borrow spread on negative cash."""
    if p["universe"] == "global":
        bundle = load()
        res = WalkForwardBacktester(rebalance_every=1, warmup=warmup(bundle)).run(bundle,
                                                                                build(p))
    else:
        bundle = rb.load()
        res = WalkForwardBacktester(rebalance_every=1, warmup=rb.warmup(bundle, SPEC)).run(
            bundle, rb.build({**p, "spec": SPEC, "prior": "by class"}))
    borrowed = (-res.weights[CASH]).clip(lower=0)
    returns = res.returns - borrowed * rb.BORROW_SPREAD_BPS / 1e4 / TRADING_DAYS
    turnover = sum(d.turnover for d in res.decisions) / (len(returns) / TRADING_DAYS)
    return f"{p['universe']} k={p['k']:g}", returns, res.weights, turnover


def portfolio_report(bundle: DataBundle, workers: int) -> None:
    ks = (0.0, 0.1, 0.2)
    params = [{**rb.PORTFOLIO, "universe": u, "k": k} for u in ("global", "17 ETF") for k in ks]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        out = {name: (r, w, t) for name, r, w, t in pool.map(run_one, params)}
    start = out["global k=0"][0].index[0]
    runs = {name: r.loc[start:] for name, (r, _, _) in out.items()}
    warm = warmup(bundle)
    runs["equal weight 45, monthly"] = WalkForwardBacktester(rebalance_every=21,
                                                             warmup=warm).run(
        bundle, FixedWeights(UNIVERSE, holdings=RISKY, cost_model=LinearCost(rb.COST_BPS))
    ).returns.loc[start:]
    split = runs["global k=0"].index[len(runs["global k=0"]) // 2]
    t = compare(runs, split_date=split)
    m = pd.DataFrame({
        "tr_sharpe": t["train"]["sharpe"], "te_sharpe": t["test"]["sharpe"],
        "full_ret": t["full"]["ann_return"], "full_vol": t["full"]["ann_vol"],
        "full_sharpe": t["full"]["sharpe"], "full_dd": t["full"]["max_dd"],
    })
    m["turnover"] = [out[n][2] if n in out else float("nan") for n in m.index]
    m["gross"] = [out[n][1].drop(columns=CASH).sum(axis=1).mean() if n in out else 1.0
                  for n in m.index]
    pct = "{:.1%}".format
    print(f"\n2. PORTFOLIO, {start.date()} .. {runs['global k=0'].index[-1].date()}, split "
          f"{split.date()} | portfolio settings {rb.PORTFOLIO}")
    print(m.to_string(formatters={"full_ret": pct, "full_vol": pct, "full_dd": pct,
                                  "tr_sharpe": "{:.2f}".format, "te_sharpe": "{:.2f}".format,
                                  "full_sharpe": "{:.2f}".format,
                                  "turnover": "{:.1f}".format, "gross": "{:.2f}".format}))
    for k in ks[1:]:
        w = out[f"global k={k:g}"][1].loc[start:]
        g = w[RISKY].T.groupby(GROUP_OF).sum().T
        print(f"\n   global k={k:g}: average weight by group (% of capital), and range")
        print(pd.DataFrame({"avg": g.mean(), "min": g.min(), "max": g.max()}).to_string(
            float_format="{:.0%}".format))
    yr = pd.DataFrame({n: (1 + r).groupby(r.index.year).prod() - 1 for n, r in runs.items()})
    print("\n   calendar-year returns")
    print(yr.to_string(float_format="{:+.1%}".format))


def main(workers: int) -> None:
    pd.set_option("display.width", 250)
    bundle = load()
    print(f"\n{len(RISKY)} risky ETFs in {len(GROUPS)} groups + {CASH}, "
          f"{bundle.index[0].date()} .. {bundle.index[-1].date()}")
    ic_report(bundle)
    portfolio_report(bundle, workers)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    main(ap.parse_args().workers)
