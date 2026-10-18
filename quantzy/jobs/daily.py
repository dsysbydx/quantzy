"""The daily production job: what should the book hold today, and what to trade to get there.

    load the saved strategy's data -> strategy.decide(current book) -> orders

This is the same :meth:`~quantzy.strategy.Strategy.decide` the backtest calls at every
rebalance, run once on the latest close. It does no backtest, no metrics and no parameter
search. The only input it must not invent is the current book, so that comes from a file.

Holdings file (CSV), one row per position, either

* ``ticker,shares``: share counts, valued at the latest close; ``--nav`` is the account value,
  and whatever the positions do not account for is cash (negative means borrowed); or
* ``ticker,weight``: fractions of NAV, the rest cash.

The strategy's cash sleeve (``TBILL`` for ``core_multiasset``) is never ordered: it is the
account's cash, or a T-bill fund if you hold one.

**Stress vol.** Next to the risk model's ex-ante vol of the target, the job reports the vol
of the same book under the long-run correlations of its history (current vols, correlations
over all available data). A big gap means the book leans on unusually low or negative recent
correlations; on 2026-10-16 the saved book was 8.8% under the model and 21% under 2008-2025
correlations. It changes no trade; it is there to look at.

**Funding a new account.** With no holdings the job builds the target the way a backtest
starts, without the optimizer's no-trade band: from all cash, the band (a 5 bp x 48 penalty
on both the asset and the cash leg) is larger than most assets' expected return, so a banded
decision would leave the book in cash. Every later run, with holdings, applies the band.

Run::

    uv run python -m quantzy.jobs.daily --nav 3000000 --holdings positions.csv \\
        [--strategy core_multiasset] [--out orders.csv]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.construction import Portfolio
from quantzy.data import YFinanceReader
from quantzy.data.bundle import DataBundle
from quantzy.strategies import get, overlays

#: Orders smaller than this many dollars are dropped (a minimum commission would dominate).
MIN_ORDER = 100.0
#: Warn when the latest complete close is older than this many business days.
STALE_DAYS = 3
#: Flag the book when its stress vol exceeds the model's ex-ante vol by this factor.
STRESS_RATIO = 1.5


def ex_ante_vols(strategy, bundle: DataBundle, weights: pd.Series,
                 risky: list[str]) -> tuple[float, float]:
    """The target's vol under the risk model, and under long-run correlations with the same
    vols (pairwise over all of ``bundle``, at least 252 overlapping days a pair)."""
    cov = strategy.risk_model.covariance(bundle).loc[risky, risky]
    w = weights.reindex(risky).fillna(0.0).to_numpy()
    vol = np.sqrt(np.clip(np.diag(cov.to_numpy()), 0, None))
    model = float(np.sqrt(w @ cov.to_numpy() @ w))
    rho = bundle.prices[risky].pct_change(fill_method=None).iloc[:-1].corr(min_periods=252)
    rho = rho.fillna(cov / np.outer(vol, vol)).to_numpy().copy()  # pairs without history: model
    np.fill_diagonal(rho, 1.0)
    stress = float(np.sqrt(w @ (rho * np.outer(vol, vol)) @ w))
    return model, stress


def fresh_bundle(module) -> DataBundle:
    """The strategy's data straight from the source, bypassing the research cache."""
    return module.load(reader=YFinanceReader())


def _next_trading_day(day: pd.Timestamp) -> pd.Timestamp:
    from quantzy.strategies.treasury_month_end import trading_days

    cal = trading_days(day, day + pd.Timedelta(days=10))
    return cal[cal > day][0]


def read_holdings(path: str | Path | None, prices: pd.Series, nav: float) -> pd.Series:
    """Current weights (fractions of NAV) from a holdings file; empty means all cash."""
    if path is None:
        return pd.Series(dtype=float)
    df = pd.read_csv(path)
    df["ticker"] = df["ticker"].str.strip().str.upper()
    df = df.set_index("ticker")
    if "weight" in df:
        return df["weight"].astype(float)
    if "shares" in df:
        missing = [t for t in df.index if t not in prices.index]
        if missing:
            raise ValueError(f"holdings include tickers outside the strategy: {missing}")
        return df["shares"].astype(float) * prices.reindex(df.index) / nav
    raise ValueError("holdings file needs a 'weight' or a 'shares' column")


def latest_complete(bundle: DataBundle, tickers: list[str]) -> DataBundle:
    """Drop trailing rows where any traded ETF has no close (a holiday, or today's open)."""
    ok = bundle.prices[tickers].notna().all(axis=1)
    last = ok[ok].index[-1]
    return bundle.upto(last)


def run(strategy_name: str = "core_multiasset", holdings: pd.Series | None = None,
        nav: float = 1_000_000.0, bundle: DataBundle | None = None,
        min_order: float = MIN_ORDER) -> tuple[pd.DataFrame, dict]:
    """Decide today's target book and the orders to reach it.

    Args:
        strategy_name: A saved strategy in :data:`quantzy.strategies.REGISTRY`.
        holdings: Current weights by ticker (fractions of NAV); missing tickers are 0 and the
            remainder is cash. ``None`` means the book is all cash.
        nav: Account value in dollars, to size orders.
        bundle: The strategy's data; ``None`` loads it (from the cache, or the network).
        min_order: Orders below this many dollars are dropped.

    Returns:
        A frame with one row per traded asset (price, current and target weight, the order in
        weight, dollars and shares) and a summary dict.
    """
    module = get(strategy_name)
    strategy = module.build()
    cash = module.CASH
    risky = [t for t in strategy.universe.tickers if t != cash]
    bundle = latest_complete(fresh_bundle(module) if bundle is None else bundle, risky)
    window = bundle.tail(strategy.min_history + 1)
    prices = window.prices.iloc[-1]

    w = pd.Series(0.0, index=strategy.universe.tickers)
    if holdings is not None and len(holdings):
        unknown = [t for t in holdings.index if t not in w.index]
        if unknown:
            raise ValueError(f"holdings include tickers outside the strategy: {unknown}")
        w.update(holdings.astype(float))
    # Overlays: the job's orders execute at the next close, so an overlay holds from then on
    # what its rule says for that close; what it holds now is what it said for this one.
    # The core decides on the holdings without the overlays' part.
    asof = window.index[-1]
    execution_day = _next_trading_day(asof)
    overlay_now = pd.Series(0.0, index=w.index)
    overlay_next = pd.Series(0.0, index=w.index)
    for ov in overlays(module):
        overlay_now[ov.ASSET] += 0.0 if holdings is None or not len(holdings) \
            else ov.target_after_close(asof)
        overlay_next[ov.ASSET] += ov.target_after_close(execution_day)
    w[cash] = 1.0 - w.drop(cash).sum()
    w_total = w.copy()
    w = (w - overlay_now).clip(lower=0.0)
    w[cash] = 1.0 - w.drop(cash).sum()
    funding = holdings is None or not len(holdings)
    current = None if funding else Portfolio(weights=w, nav=nav)
    decision = strategy.decide(window, current=current)

    core_target = decision.target.weights.reindex(w.index).fillna(0.0)
    model_vol, stress_vol = ex_ante_vols(strategy, bundle, core_target, risky)
    target = core_target + overlay_next
    target[cash] = 1.0 - target.drop(cash).sum()
    w = w_total
    out = pd.DataFrame({"price": prices.reindex(w.index), "current": w, "target": target})
    out["trade_weight"] = out["target"] - out["current"]
    out["trade_dollars"] = out["trade_weight"] * nav
    out["trade_shares"] = np.where(out.index == cash, np.nan,
                                   np.round(out["trade_dollars"] / out["price"]))
    orders = out.drop(index=cash)
    orders = orders[orders["trade_dollars"].abs() >= min_order].copy()
    orders["side"] = np.where(orders["trade_weight"] > 0, "BUY", "SELL")
    summary = {
        "strategy": strategy_name, "asof": window.index[-1].date(), "nav": nav,
        "gross_target": float(target.drop(cash).sum()),
        "cash_target": float(target[cash]),
        "turnover": float(0.5 * (target - w).abs().sum()),
        "execution_day": execution_day.date(),
        "overlays": {ov.NAME: float(overlay_next[ov.ASSET]) for ov in overlays(module)},
        "estimated_cost": float(strategy.optimizer.execution_cost.estimate(
            (target - w).to_numpy(), tickers=list(w.index))
            if strategy.optimizer.execution_cost is not None
            else (decision.trades.cost or 0.0)) * nav,
        "status": decision.status,
        "funding": funding,
        "model_vol": model_vol,
        "stress_vol": stress_vol,
    }
    return out.assign(order=out.index.isin(orders.index)), summary


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--strategy", default="core_multiasset")
    ap.add_argument("--nav", type=float, required=True, help="account value in dollars")
    ap.add_argument("--holdings", help="CSV with ticker and shares (or weight)")
    ap.add_argument("--out", help="write the orders to this CSV")
    ap.add_argument("--min-order", type=float, default=MIN_ORDER)
    args = ap.parse_args(argv)

    module = get(args.strategy)
    bundle = latest_complete(fresh_bundle(module), [t for t in module.RISKY])
    prices = bundle.prices.iloc[-1]
    age = len(pd.bdate_range(bundle.index[-1], pd.Timestamp.today().normalize())) - 1
    if age > STALE_DAYS:
        print(f"WARNING: latest complete close is {bundle.index[-1].date()}, "
              f"{age} business days ago")
    holdings = read_holdings(args.holdings, prices, args.nav)
    book, summary = run(args.strategy, holdings, args.nav, bundle, args.min_order)

    pd.set_option("display.width", 200)
    print(f"{summary['strategy']} as of {summary['asof']} | NAV ${summary['nav']:,.0f} | "
          f"target gross {summary['gross_target']:.2f}x, cash {summary['cash_target']:+.1%} | "
          f"turnover {summary['turnover']:.1%}, est. cost ${summary['estimated_cost']:,.0f} | "
          f"{summary['status']}" + (" | funding from cash (no band)" if summary["funding"]
                                     else ""))
    for name, wt in summary["overlays"].items():
        print(f"overlay {name}: {wt:.0%} of NAV from the close of {summary['execution_day']}")
    flag = ("  <-- leans on unusual correlations" if summary["stress_vol"]
            > STRESS_RATIO * summary["model_vol"] else "")
    print(f"ex-ante vol of the target: {summary['model_vol']:.1%} (risk model), "
          f"{summary['stress_vol']:.1%} (long-run correlations){flag}")
    view = book[["price", "current", "target", "trade_dollars", "trade_shares"]]
    print(view.to_string(formatters={
        "price": "{:,.2f}".format, "current": "{:.1%}".format, "target": "{:.1%}".format,
        "trade_dollars": "{:+,.0f}".format,
        "trade_shares": lambda v: "" if pd.isna(v) else f"{v:+,.0f}"}))
    orders = book[book["order"]]
    if args.out:
        orders.assign(side=np.where(orders["trade_weight"] > 0, "BUY", "SELL"))[
            ["side", "trade_shares", "trade_dollars", "price"]].to_csv(args.out)
        print(f"\n{len(orders)} orders written to {args.out}")
    elif len(orders):
        print(f"\n{len(orders)} orders above ${args.min_order:,.0f} (pass --out to save them)")
    else:
        print("\nno orders: the book is within its no-trade band")


if __name__ == "__main__":
    main()
