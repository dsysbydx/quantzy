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
from quantzy.strategies import get

#: Orders smaller than this many dollars are dropped (a minimum commission would dominate).
MIN_ORDER = 100.0
#: Warn when the latest complete close is older than this many business days.
STALE_DAYS = 3


def fresh_bundle(module) -> DataBundle:
    """The strategy's data straight from the source, bypassing the research cache."""
    return module.load(reader=YFinanceReader())


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
    w[cash] = 1.0 - w.drop(cash).sum()
    funding = holdings is None or not len(holdings)
    current = None if funding else Portfolio(weights=w, nav=nav)
    decision = strategy.decide(window, current=current)

    target = decision.target.weights.reindex(w.index).fillna(0.0)
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
        "estimated_cost": float(strategy.optimizer.execution_cost.estimate(
            (target - w).to_numpy(), tickers=list(w.index))
            if strategy.optimizer.execution_cost is not None
            else (decision.trades.cost or 0.0)) * nav,
        "status": decision.status,
        "funding": funding,
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
