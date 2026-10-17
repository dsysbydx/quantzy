"""RESEARCH: long-term trend, bought on short-term pullbacks, across the sector ETFs.

The idea: own sectors in a long-term uptrend, and lean into the ones that have just dipped
below their short-term average -- expecting the dip to revert while the trend persists.
:class:`~quantzy.signals.TrendPullback` scores it as ``z(LT) - beta * z(ST)``, optionally
gated so the dip is bought only where the long-term trend is actually up.

Three steps, in the order worth taking them:

1. **IC of each leg.** Does LT trend predict sector returns? Does ST deviation predict the
   *opposite* sign (reversal)? If the ST leg has no reversal IC, beta can only add noise.
2. **Holdout sweep** over ``short``, ``long``, ``beta`` and ``gate``, ranked on the train
   slice. Every combo trades the same dates, so their scores are comparable.
3. **Comparison** of the winner, the best gated and ungated variants, pure trend
   (``beta=0``) and an equal-weight sector benchmark -- on train and on held-out test,
   then year by year.

Run:  uv run python research/research_pullback.py
"""

from __future__ import annotations

import pandas as pd

from quantzy.backtest import FixedWeights, WalkForwardBacktester
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.optimize import LinearCost, MeanVariance
from quantzy.optimize import constraints as C
from quantzy.research import compare, holdout_tune, ic_decay, label, yearly
from quantzy.risk import LedoitWolfCovariance
from quantzy.signals import TrendPullback, ZScore
from quantzy.strategy import Strategy
from quantzy.universe import sector_etf_universe

UNIVERSE = sector_etf_universe()
CASH = tuple(a.ticker for a in UNIVERSE if a.kind == "cash_equivalent")
SECTORS = [t for t in UNIVERSE.tickers if t not in CASH]

#: Rows between rebalances; override with --rebalance-every. Monthly keeps turnover, and so
#: cost, down -- at the price of acting on a short-term leg that may be stale by then.
REBALANCE_EVERY = 21

#: Linear trading cost, in bp of traded notional. The optimizer weighs it against expected
#: return when sizing trades, and the backtest charges it; the benchmark pays it too.
COST_BPS = 5.0

GRID = {
    "short": [5, 10, 21],
    "long": [100, 150, 200, 252],
    "beta": [0.0, 0.5, 1.0, 2.0],
    "gate": [True, False],
}

#: Loosened from 1e-9: on finite history the EMA seed is the first real price anyway, and
#: the default would spend ~10 of the 24 years warming up a 252-span EMA.
EWM_TOL = 1e-3


def signal(short: int, long: int, beta: float, gate: bool) -> TrendPullback:
    """The sweep's signal. ``mid`` is not swept: it is pinned at a quarter of ``long``
    (e.g. 50 against 200), and kept above ``short``."""
    return TrendPullback(
        short=short,
        mid=max(long // 4, short + 1),
        long=long,
        beta=beta,
        gate=gate,
        exclude=CASH,
        ewm_tol=EWM_TOL,
    )


def build(short: int = 10, long: int = 200, beta: float = 1.0, gate: bool = True) -> Strategy:
    return Strategy(
        universe=UNIVERSE,
        signals=[signal(short, long, beta, gate)],
        risk_model=LedoitWolfCovariance(lookback=252),
        optimizer=MeanVariance(
            risk_aversion=8.0,
            constraints=[C.budget(), C.long_only()],
            cost_model=LinearCost(cost_bps=COST_BPS),
        ),
        name=f"trend_pullback({label(dict(short=short, long=long, beta=beta, gate=gate))})",
    )


def leg_ic(bundle: DataBundle, short: int = 10, long: int = 200) -> dict[str, pd.DataFrame]:
    """IC decay of each leg, and of the gated and ungated blends, keyed by name."""
    sig = signal(short, long, beta=1.0, gate=True)
    lt, st = sig.legs(bundle)
    z = ZScore(on_degenerate="zero")
    # Lag by one row, as Signal.scores does, so row t is tradeable at t.
    panels = {
        "LT trend": z(lt).shift(1),
        "ST reversal (-ST)": z(-st).shift(1),
        "blend, gated": sig.scores(bundle),
        "blend, ungated": signal(short, long, beta=1.0, gate=False).scores(bundle),
    }
    prices = bundle.prices[SECTORS]
    return {
        name: ic_decay(scores[SECTORS], prices, horizons=(1, 5, 21, 63), min_names=5)
        for name, scores in panels.items()
    }


def _fmt_ic(ic: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = {
        name: d["mean_ic"].map("{:+.3f}".format) + " (t=" + d["t_stat"].map("{:+.1f}".format)
        + ")"
        for name, d in ic.items()
    }
    return pd.DataFrame(out).T.rename(columns=lambda h: f"{h}d")


def _fmt(frame: pd.DataFrame) -> pd.DataFrame:
    """Percent for return, vol and drawdown; plain for Sharpe and turnover."""
    out = frame.copy().astype(object)
    for c in frame.columns:
        pct = c in ("ann_return", "ann_vol", "max_dd")
        out[c] = frame[c].map(lambda v, pct=pct: f"{v:.1%}" if pct else f"{v:.2f}")
    return out


def export_json(path: str, *, bundle, res, ic, contenders, runs, cmp, by_year,
                rebalance_every: int) -> None:
    """Dump everything a results dashboard needs: the whole grid, the contenders' daily
    NAV and drawdown, yearly metrics, the IC table and the winner's weights over time."""
    import json
    import math

    def clean(v):
        if isinstance(v, dict):
            return {str(k): clean(x) for k, x in v.items()}
        if isinstance(v, list | tuple):
            return [clean(x) for x in v]
        if hasattr(v, "item"):
            v = v.item()
        if isinstance(v, float):
            return None if not math.isfinite(v) else round(v, 6)
        return v

    def metrics(frame: pd.DataFrame) -> dict:
        return {period: frame[period].to_dict(orient="index")
                for period in frame.columns.get_level_values(0).unique()}

    labels = list(res.results)
    grid_cmp = compare(res.results, res.split_date)
    grid = []
    for i, row in res.table.iterrows():
        lab = labels[i]
        grid.append({
            **{k: row[k] for k in GRID},
            "label": lab,
            "rank": i + 1,
            **{f"{period}_{m}": grid_cmp.loc[lab, (period, m)]
               for period in ("train", "test", "full")
               for m in ("ann_return", "ann_vol", "sharpe", "max_dd", "turnover")},
        })

    rets = {k: (v.returns if hasattr(v, "returns") else v) for k, v in runs.items()}
    common = None
    for r in rets.values():
        common = r.index if common is None else common.intersection(r.index)
    nav = pd.DataFrame({k: (1 + r.loc[common]).cumprod() for k, r in rets.items()})
    dd = nav / nav.cummax() - 1

    weights = res.best_result.weights.resample("ME").mean()

    payload = {
        "meta": {
            "universe": list(UNIVERSE.tickers),
            "sectors": SECTORS,
            "cash": list(CASH),
            "data_start": str(bundle.index[0].date()),
            "data_end": str(bundle.index[-1].date()),
            "trade_start": str(common[0].date()),
            "split_date": str(res.split_date.date()),
            "rebalance_every": rebalance_every,
            "grid": GRID,
            "n_combos": len(labels),
            "cost_bps": COST_BPS,
            "risk_aversion": 8.0,
            "ewm_tol": EWM_TOL,
            "best_params": res.best_params,
            "train_score": res.train_score,
            "test_score": res.test_score,
        },
        "ic": {name: d.reset_index().to_dict(orient="records") for name, d in ic.items()},
        "grid": grid,
        "contenders": {**contenders, "EW": EW_LABEL.format(rebalance_every, COST_BPS)},
        "metrics": metrics(cmp),
        "yearly": {
            m: {str(y): row for y, row in by_year[m].to_dict(orient="index").items()}
            for m in ("return", "vol", "sharpe", "max_dd", "days")
        },
        "series": {
            "dates": [d.strftime("%Y-%m-%d") for d in nav.index],
            "nav": {k: nav[k].round(5).tolist() for k in nav},
            "drawdown": {k: dd[k].round(5).tolist() for k in dd},
        },
        "weights": {
            "dates": [d.strftime("%Y-%m") for d in weights.index],
            "tickers": list(weights.columns),
            "values": weights.round(4).fillna(0.0).values.tolist(),
        },
    }
    with open(path, "w") as f:
        json.dump(clean(payload), f, separators=(",", ":"))
    print(f"\nWrote dashboard data to {path}")


EW_LABEL = "equal-weight sectors, rebalanced every {} days, {:g} bp costs"


def equal_weight(bundle: DataBundle, rebalance_every: int):
    """Equal-weight sectors, run through the same engine: same rebalance dates, drifting
    in between, same cost model. A daily, cost-free mean of returns would be a benchmark
    no strategy here is allowed to be."""
    ew = FixedWeights(UNIVERSE, holdings=SECTORS, cost_model=LinearCost(cost_bps=COST_BPS),
                      name="equal_weight")
    return WalkForwardBacktester(rebalance_every=rebalance_every).run(bundle, ew)


def main(json_path: str | None = None, rebalance_every: int = REBALANCE_EVERY) -> None:
    reader = CachedReader(YFinanceReader())
    prices = reader.history(UNIVERSE.tickers, start="2002-08-01").dropna(how="any")
    bundle = DataBundle.build(prices)
    print(f"Loaded {len(bundle.index)} rows, {bundle.index[0].date()} .. "
          f"{bundle.index[-1].date()}, {len(SECTORS)} sectors + cash {CASH}\n")

    # --- 1. does each leg predict anything? ---
    print("Rank IC by forward horizon (short=10, long=200). Positive = predicts the right way.")
    ic = leg_ic(bundle)
    print(_fmt_ic(ic).to_string(), "\n")

    # --- 2. sweep ---
    n = 1
    for v in GRID.values():
        n *= len(v)
    print(f"Sweeping {n} combos, rebalancing every {rebalance_every} days, "
          f"{COST_BPS:g} bp costs ...")
    res = holdout_tune(bundle, GRID, build, train_frac=0.7, rebalance_every=rebalance_every)
    first = res.best_result.returns.index[0].date()
    print(f"All combos trade {first} .. {bundle.index[-1].date()}; "
          f"split at {res.split_date.date()}\n")

    table = res.table.copy()
    cols = ["ann_return", "ann_vol", "sharpe", "max_dd", "turnover"]
    # With beta=0 neither ``short`` nor ``gate`` does anything, so those combos are the same
    # run repeated; show each distinct run once.
    labels = list(res.results)  # same order as ``table``
    distinct = table.drop_duplicates(["train_score", "test_score"]).head(10)
    top = compare({labels[i]: res.results[labels[i]] for i in distinct.index}, res.split_date)
    with pd.option_context("display.width", 200):
        for period in ("train", "test"):
            print(f"Top 10 distinct runs by train Sharpe -- {period.upper()} "
                  f"({'in-sample' if period == 'train' else 'held out'}):")
            print(_fmt(top[period][cols]).to_string(), "\n")

    for gate in (True, False):
        sub = table[(table["gate"] == gate) & (table["beta"] > 0)]
        best_beta = sub.iloc[0]["beta"]
        pivot = sub[sub["beta"] == best_beta].pivot(
            index="short", columns="long", values="train_score")
        print(f"Train Sharpe, gate={gate}, beta={best_beta}: short (rows) x long (cols)")
        print(pivot.round(3).to_string(), "\n")

    by_beta = table.groupby(["beta", "gate"])["train_score"].median().unstack("gate")
    print("Median train Sharpe across spans, by beta x gate:")
    print(by_beta.round(3).to_string(), "\n")

    # --- 3. compare the contenders, train vs held-out test ---
    def best(mask) -> str:
        return labels[table[mask].index[0]]

    # Short keys so the yearly tables fit across the screen; the legend gives the params.
    contenders = {
        "overall": best(table["beta"] >= 0),
        "gated": best(table["gate"] & (table["beta"] > 0)),
        "ungated": best(~table["gate"] & (table["beta"] > 0)),
        "trend": best(table["beta"] == 0),
    }
    print("Contenders (best by train Sharpe within each group):")
    for key, params in contenders.items():
        print(f"  {key:<8} {params}")
    print(f"  EW       {EW_LABEL.format(rebalance_every, COST_BPS)}\n")

    runs = {k: res.results[v] for k, v in contenders.items()}
    runs["EW"] = equal_weight(bundle, rebalance_every)

    cmp = compare(runs, res.split_date)
    with pd.option_context("display.width", 200):
        for period in ("full", "train", "test"):
            note = {"full": "whole run", "train": "in-sample", "test": "held out"}[period]
            print(f"{period.upper()} ({note}):")
            print(_fmt(cmp[period][cols]).to_string(), "\n")

    by_year = yearly(runs)
    days = by_year["days"].iloc[:, 0]
    partial = days[days < 240].index.tolist()
    print(f"By calendar year (years marked * are partial: {partial}; test starts "
          f"{res.split_date.date()}):\n")
    for metric, title in [("return", "Return (calendar year, not annualized)"),
                          ("vol", "Annualized vol"),
                          ("sharpe", "Sharpe"),
                          ("max_dd", "Max drawdown within year")]:
        t = by_year[metric].copy()
        t.index = [f"{y}*" if y in partial else str(y) for y in t.index]
        t = t.map(lambda v: f"{v:.2f}") if metric == "sharpe" else t.map(lambda v: f"{v:.1%}")
        print(f"{title}:")
        print(t.to_string(), "\n")

    print(f"Selected on train: {res.best_params}")
    print(f"  train Sharpe {res.train_score:.3f} -> test Sharpe {res.test_score:.3f} "
          f"({res.decay:.0%} survived)")

    if json_path:
        export_json(json_path, bundle=bundle, res=res, ic=ic, contenders=contenders,
                    runs=runs, cmp=cmp, by_year=by_year, rebalance_every=rebalance_every)

    w = res.best_result.weights
    print("\nWinner's average weights (test slice):")
    print(w.loc[res.split_date:].mean().sort_values(ascending=False).round(3).to_string())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", help="also write the results to this JSON file")
    parser.add_argument("--rebalance-every", type=int, default=REBALANCE_EVERY,
                        help=f"rows between rebalances (default {REBALANCE_EVERY}, ~monthly)")
    args = parser.parse_args()
    main(args.json, args.rebalance_every)
