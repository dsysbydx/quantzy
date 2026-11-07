"""The daily production job: what should the book hold today, and what to trade to get there.

    load the saved strategy's data -> strategy.decide(current book) -> orders

This is the same :meth:`~quantzy.strategy.Strategy.decide` the backtest calls at every
rebalance, run once on the latest prices.

**When to run it: before the close, with market-on-close orders.** The backtest decides on
the close of day T and holds the result from T's close (its signals and risk model read data
lagged one row, and its fills are at the same close). The job matches that by running about
15 minutes before the close: Yahoo's daily history then ends in a row for today with the
latest prices, the core decides on that row (:func:`with_execution_row`), and the orders are
market-on-close for today. Check your broker's closing-auction cutoff: NYSE 15:50 ET, Nasdaq
15:55, NYSE Arca 15:59. On 60 sessions of 5-minute bars (2026-08..11) the last 10 minutes
carried about 1% of the day's variance with no lean in the day's direction; a production-style
replay with that noise lost 0.006-0.011 Sharpe on the core, 2008-09..2026-11 (1.184 against
1.173-1.178). Run after the close, the job trades at the next close, one day behind the
backtest (core Sharpe 1.148, max drawdown -17.3% against -14.8%). Before 2026-11-07 the job
decided on the row before its last and traded at the next close: two days behind (1.123,
-16.4%).

The live row is not the close: on an ex-dividend day Yahoo adjusts the history to it, the VIX
in it is the 15:45 print rather than the 16:15 settle, and a quote can be stale. The command
prints the time it ran and fills a missing index value in the live row from the previous
close. It does no backtest, no metrics and no parameter
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

**Margin and stress** (:mod:`quantzy.jobs.margin`). The job estimates the target's Reg T
position -- gross, borrowing, maintenance requirement, the cushion above it and the fall in
the long book that would trigger a call -- and replays three past crash weeks on it. If the
target would exceed 1.95x gross (Reg T's 2x initial limit, less a buffer), it scales the
overlays down first, so the broker does not reject or trim the orders. Alerts are printed when
the cushion is thin. The backtests do not apply that scale-back: it binds on 88 of 4,564 days
(2008-09 .. 2026-10), median factor 0.93, and costs the book about 0.003 Sharpe.

**Funding a new account.** With no holdings the job builds the target the way a backtest
starts, without the optimizer's no-trade band, so a new account goes straight to the full
book (about 1.3x). Banded from cash, the penalty on the cash leg would stop it at about 1.0x,
and it would take weeks to drift up. Every later run, with holdings, applies the band.

Run::

    uv run python -m quantzy.jobs.daily --nav 3000000 --holdings positions.csv \\
        [--strategy core_multiasset] [--out orders.csv] [--after-close]

Run on a trading day before the close (16:00 ET, or 13:00 on the early-close days: the Friday
after Thanksgiving, Dec 24 and some Jul 3s), it is a pre-close run (orders for today's
close); otherwise, or with ``--after-close``, the orders are for the next trading day's close.
On an early-close day, schedule the run for about 12:40 ET: at the usual 15:45 the session is
over and the run falls back to after-close, one day behind.
"""

from __future__ import annotations

import argparse
import datetime
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from quantzy.construction import Portfolio
from quantzy.data import YFinanceReader
from quantzy.data.bundle import DataBundle
from quantzy.jobs import margin as mg
from quantzy.strategies import get, overlays

#: Orders smaller than this many dollars are dropped (a minimum commission would dominate).
MIN_ORDER = 100.0
#: Warn when the latest complete close is older than this many business days.
STALE_DAYS = 3
#: Flag the book when its stress vol exceeds the model's ex-ante vol by this factor.
STRESS_RATIO = 1.5
#: A pre-close run should start in this window, in minutes before the session's close: late
#: enough that the live prices are near the close, early enough for the closing-auction
#: cutoff (NYSE 15:50 on a normal day, 12:50 on a 13:00 early close).
PRECLOSE_FROM, PRECLOSE_TO = 30, 10
TZ = "America/New_York"
#: Panels a pre-close run may fill from the previous close: index levels that can lack a live
#: print. Never a price the job trades (the ETFs, SPY, VIXY): those come from
#: :func:`repair_live_row`.
LIVE_FILL = ("vix",)
#: Warn of a stale live row when this many traded ETFs are exactly flat on the day.
STALE_FLAT = 4
#: More tickers than this without a live price, even fetched on their own, means the download
#: failed rather than one quote: the run stops instead of freezing most of the book.
MAX_FROZEN = 2


@dataclass(frozen=True)
class Session:
    """Where a run falls against the exchange's day.

    Attributes:
        today: The ET date of the run.
        close: Today's close (ET), or ``None`` when today is not a trading day.
        preclose: The market is open and the run is a pre-close run (orders for today's close).
        open_now: The market is open now (a run with ``--after-close`` still drops today's
            live row).
        window: The (from, to) times, ``HH:MM`` ET, a pre-close run should start between.
    """

    today: pd.Timestamp
    close: datetime.time | None
    preclose: bool
    open_now: bool
    window: tuple[str, str] | None


def session_state(now: pd.Timestamp, after_close: bool = False) -> Session:
    """Classify a run at ``now`` (tz-aware, or naive ET) against today's NYSE session,
    including the 13:00 early closes."""
    from quantzy.strategies.treasury_month_end import close_time

    if now.tzinfo is not None:
        now = now.tz_convert(TZ).tz_localize(None)
    today = now.normalize()
    close = close_time(today)
    open_now = close is not None and now.time() < close
    window = None
    if close is not None:
        at = datetime.datetime.combine(today.date(), close)
        window = tuple(f"{at - datetime.timedelta(minutes=m):%H:%M}"
                       for m in (PRECLOSE_FROM, PRECLOSE_TO))
    return Session(today=today, close=close, preclose=open_now and not after_close,
                   open_now=open_now, window=window)


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


def all_prices(module, bundle: DataBundle) -> pd.Series:
    """The latest close of everything the job may order: the universe and overlay assets."""
    prices = bundle.prices.iloc[-1].copy()
    for ov in overlays(module):
        if ov.ASSET not in prices.index:
            prices[ov.ASSET] = ov.asset_prices(bundle).ffill().iloc[-1]
    return prices


def with_execution_row(bundle: DataBundle, day: pd.Timestamp) -> DataBundle:
    """``bundle`` plus a placeholder row dated ``day`` (a copy of the last row).

    Signals and the risk model read data lagged one row (``lag=1``): a backtest that decides
    for day ``d`` reads the close of ``d - 1`` and holds the result through ``d``. Deciding on
    ``bundle`` as it stands would read the row *before* its last one. Appending the day the
    book is held through makes the decision read the last real row, exactly as the backtest
    does; the placeholder's values are never read."""
    out = {}
    for k, v in bundle.items():
        extra = v.iloc[[-1]].copy()
        extra.index = pd.DatetimeIndex([pd.Timestamp(day)])
        out[k] = pd.concat([v, extra])
    return DataBundle(out)


def _previous_trading_day(day: pd.Timestamp) -> pd.Timestamp:
    from quantzy.strategies.treasury_month_end import trading_days

    cal = trading_days(day - pd.Timedelta(days=10), day)
    return cal[cal < day][-1]


def fill_live_row(bundle: DataBundle,
                  panels: tuple[str, ...] = LIVE_FILL) -> tuple[DataBundle, list[str]]:
    """Fill gaps in the last row of ``panels`` from the row before (an index without an
    intraday print), for a pre-close run; returns the bundle and the panel/column pairs it
    filled. Prices the job trades are never filled here: see :func:`repair_live_row`."""
    out, filled = {}, []
    for k, v in bundle.items():
        v = v.copy()
        if k in panels and len(v) > 1:
            gap = v.iloc[-1].isna() & v.iloc[-2].notna()
            if gap.any():
                v.iloc[-1] = v.iloc[-1].fillna(v.iloc[-2])
                filled += [f"{k}:{c}" for c in gap[gap].index]
        out[k] = v
    return DataBundle(out), filled


def live_price(ticker: str, day: pd.Timestamp) -> float | None:
    """The latest 1-minute bar's close for ``ticker`` on ``day`` (ET), fetched on its own: the
    fallback when the batch download left a traded ticker without a live price. ``None`` if
    Yahoo has no bar for ``day``."""
    import yfinance as yf

    try:
        bars = yf.Ticker(ticker).history(period="1d", interval="1m")
    except Exception:  # noqa: BLE001 - any failure means no fallback price
        return None
    bars = bars["Close"].dropna() if "Close" in bars else pd.Series(dtype=float)
    if bars.empty:
        return None
    last = bars.index[-1]
    last = last.tz_convert(TZ).tz_localize(None) if last.tzinfo else last
    return float(bars.iloc[-1]) if last.normalize() == pd.Timestamp(day).normalize() else None


def traded_series(module, bundle: DataBundle) -> dict[str, tuple[str, list[str]]]:
    """Where each ticker the job may order lives in ``bundle``: ``{ticker: (panel, columns)}``.
    The ETFs are columns of ``prices``; an overlay asset outside the universe is a panel of its
    own (its ``PANEL``, the same value in every column)."""
    out = {t: ("prices", [t]) for t in module.RISKY}
    for ov in overlays(module):
        if ov.ASSET in out:
            continue
        panel = getattr(ov, "PANEL", "prices")
        out[ov.ASSET] = (panel, [ov.ASSET] if panel == "prices" else list(bundle[panel].columns))
    return out


def repair_live_row(module, bundle: DataBundle, fetch=live_price
                    ) -> tuple[DataBundle, dict[str, float], list[str]]:
    """Give every traded ticker a price in a pre-close run's live row.

    A ticker the batch download left empty is fetched on its own (``fetch``). If that fails
    too, it is marked at its last close and returned in ``frozen``: :func:`run` then holds it
    where it is for the day rather than trading on a price known to be stale.

    Returns:
        The bundle, ``{ticker: price}`` fetched on its own, and the frozen tickers.
    """
    day = bundle.index[-1]
    panels = {k: v.copy() for k, v in bundle.items()}
    fetched, frozen = {}, []
    for t, (panel, cols) in traded_series(module, bundle).items():
        v = panels[panel]
        if v[cols].iloc[-1].notna().all():
            continue
        price = fetch(t, day)
        if price is not None and np.isfinite(price) and price > 0:
            fetched[t] = float(price)
        else:
            price = v[cols].iloc[:-1].ffill().iloc[-1].iloc[0]
            frozen.append(t)
        v.loc[day, cols] = price
    return DataBundle(panels), fetched, frozen


def flat_today(module, bundle: DataBundle, exclude: list[str] = ()) -> list[str]:
    """Traded ETFs whose live price equals the previous close exactly (a stale quote looks
    like this; four or more at once never happened in the closes since 2008)."""
    px = bundle.prices[[t for t in module.RISKY if t not in exclude]]
    if len(px) < 2:
        return []
    return [t for t in px.columns if px[t].iloc[-1] == px[t].iloc[-2]]


def latest_complete(bundle: DataBundle, tickers: list[str]) -> DataBundle:
    """Drop trailing rows where any traded ETF has no close (a holiday, or today's open)."""
    ok = bundle.prices[tickers].notna().all(axis=1)
    last = ok[ok].index[-1]
    return bundle.upto(last)


def run(strategy_name: str = "core_multiasset", holdings: pd.Series | None = None,
        nav: float = 1_000_000.0, bundle: DataBundle | None = None,
        min_order: float = MIN_ORDER,
        execution_day: pd.Timestamp | None = None,
        frozen: list[str] = ()) -> tuple[pd.DataFrame, dict]:
    """Decide today's target book and the orders to reach it.

    Args:
        strategy_name: A saved strategy in :data:`quantzy.strategies.REGISTRY`.
        holdings: Current weights by ticker (fractions of NAV); missing tickers are 0 and the
            remainder is cash. ``None`` means the book is all cash.
        nav: Account value in dollars, to size orders.
        bundle: The strategy's data; ``None`` loads it (from the cache, or the network).
        min_order: Orders below this many dollars are dropped.
        execution_day: The close the orders fill at. The bundle's last day means a pre-close
            run (the last row holds today's live prices, the orders are market-on-close for
            today), which is how the backtest trades; ``None`` means the next trading day (a
            run after the close, one day behind the backtest).
        frozen: Tickers with no trustworthy price today (:func:`repair_live_row`): held where
            they are, core and overlays alike, with the difference in cash. The rest of the
            book is not re-optimized around them.

    Returns:
        A frame with one row per traded asset (price, current and target weight, the order in
        weight, dollars and shares) and a summary dict.
    """
    module = get(strategy_name)
    strategy = module.build()
    cash = module.CASH
    risky = [t for t in strategy.universe.tickers if t != cash]
    bundle = latest_complete(fresh_bundle(module) if bundle is None else bundle, risky)
    asof = bundle.index[-1]
    execution_day = (_next_trading_day(asof) if execution_day is None
                     else pd.Timestamp(execution_day).normalize())
    if execution_day not in (asof, _next_trading_day(asof)):
        raise ValueError(f"execution day {execution_day.date()} must be the data's last day "
                         f"{asof.date()} (pre-close) or the next trading day (after the close)")
    preclose = execution_day == asof
    window = bundle.tail(strategy.min_history + 1)
    # the core decides on the last row, for the book held through the day after the execution
    # close -- the backtest's timing
    decide_on = with_execution_row(window, _next_trading_day(execution_day))

    ovs = overlays(module)
    universe = list(strategy.universe.tickers)
    extra = [ov.ASSET for ov in ovs if ov.ASSET not in universe]  # traded only by overlays
    w = pd.Series(0.0, index=universe + extra)
    if holdings is not None and len(holdings):
        unknown = [t for t in holdings.index if t not in w.index]
        if unknown:
            raise ValueError(f"holdings include tickers outside the strategy: {unknown}")
        w.update(holdings.astype(float))
    # Overlays: the orders execute at ``execution_day``'s close, so an overlay holds from then
    # on what its rule says for that close; what it holds now is what it said for the close
    # before (or, for an asset only an overlay trades, the whole holding). Each rule reads
    # only closes before the one it is for, so a pre-close run never reads today's live row.
    # The core decides on the holdings without the overlays' part.
    held_from = _previous_trading_day(execution_day)
    funding = holdings is None or not len(holdings)
    overlay_now = pd.Series(0.0, index=w.index)
    overlay_next = pd.Series(0.0, index=w.index)
    per_overlay: dict[str, float] = {}  # each overlay's own weight (several may share an asset)
    # An overlay that sizes on the book's equity weight (``USES_EQUITY``) gets the equity the
    # core holds now, as in its backtest; a book funded from cash holds none yet.
    equity = float(w.reindex(getattr(module, "EQUITY", [])).fillna(0.0).sum())
    for ov in ovs:
        kw = {"equity": equity} if getattr(ov, "USES_EQUITY", False) else {}
        if ov.ASSET in extra:
            now = float(w[ov.ASSET])
        else:
            now = 0.0 if funding else ov.target_after_close(held_from, bundle, **kw)
        overlay_now[ov.ASSET] += now
        per_overlay[ov.NAME] = float(ov.target_after_close(execution_day, bundle, now, **kw))
        overlay_next[ov.ASSET] += per_overlay[ov.NAME]
    w[cash] = 1.0 - w.drop(cash).sum()
    w_total = w.copy()
    w = (w - overlay_now).clip(lower=0.0).loc[universe]
    w[cash] = 1.0 - w.drop(cash).sum()
    current = None if funding else Portfolio(weights=w, nav=nav)
    decision = strategy.decide(decide_on, current=current)

    core_target = decision.target.weights.reindex(w_total.index).fillna(0.0)
    model_vol, stress_vol = ex_ante_vols(
        strategy, with_execution_row(bundle, decide_on.index[-1]), core_target, risky)
    # Keep the book under the initial-margin limit by shrinking the overlays, not by letting
    # the broker reject or trim the orders.
    limit = mg.REG_T.max_gross - mg.GROSS_BUFFER
    scale = mg.scale_overlays(core_target.drop(cash), overlay_next.drop(cash), limit)
    overlay_next *= scale
    per_overlay = {k: v * scale for k, v in per_overlay.items()}
    target = core_target + overlay_next
    unknown = [t for t in frozen if t not in w_total.index or t == cash]
    if unknown:
        raise ValueError(f"cannot freeze {unknown}: not a traded ticker of {strategy_name}")
    for t in frozen:  # no price to trade at: hold it
        target[t] = w_total[t]
    target[cash] = 1.0 - target.drop(cash).sum()
    history = bundle.prices.copy()
    for ov in ovs:
        if ov.ASSET not in history:
            history[ov.ASSET] = ov.asset_prices(bundle)
    book_margin = mg.margin(target.drop(cash))
    book_stress = mg.stress(target.drop(cash), history)
    w = w_total
    prices = all_prices(module, window)
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
        "gross_target": float(target.drop(cash).abs().sum()),  # a short adds to gross
        "cash_target": float(target[cash]),
        "turnover": float(0.5 * (target - w).abs().sum()),
        "execution_day": execution_day.date(),
        "preclose": preclose,
        "overlays": per_overlay,
        "estimated_cost": float(strategy.optimizer.execution_cost.estimate(
            (target - w).to_numpy(), tickers=list(w.index))
            if strategy.optimizer.execution_cost is not None
            else (decision.trades.cost or 0.0)) * nav,
        "status": decision.status,
        "funding": funding,
        "frozen": list(frozen),
        "model_vol": model_vol,
        "stress_vol": stress_vol,
        "overlay_scale": scale,
        "margin": book_margin,
        "stress": book_stress,
        "margin_alerts": _margin_alerts(book_margin, book_stress, scale),
    }
    return out.assign(order=out.index.isin(orders.index)), summary


def _margin_alerts(m: dict, s: dict, scale: float) -> list[str]:
    alerts = []
    if scale < 1:
        alerts.append(f"overlays scaled to {scale:.0%} to keep gross under "
                      f"{mg.REG_T.max_gross - mg.GROSS_BUFFER:.2f}x (initial margin)")
    if m["drop_to_call"] < mg.MIN_DROP_TO_CALL:
        alerts.append(f"a {m['drop_to_call']:.0%} fall in the long book would breach maintenance")
    for name, x in s.items():
        if x["cushion"] < mg.MIN_STRESS_CUSHION:
            alerts.append(f"{name} would leave {x['cushion']:.0%} of NAV above maintenance")
    return alerts


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--strategy", default="core_multiasset")
    ap.add_argument("--nav", type=float, required=True, help="account value in dollars")
    ap.add_argument("--holdings", help="CSV with ticker and shares (or weight)")
    ap.add_argument("--out", help="write the orders to this CSV")
    ap.add_argument("--min-order", type=float, default=MIN_ORDER)
    ap.add_argument("--after-close", action="store_true",
                    help="trade at the next close even if the market is open (one day behind "
                         "the backtest)")
    args = ap.parse_args(argv)

    module = get(args.strategy)
    now = pd.Timestamp.now(tz=TZ)
    state = session_state(now, args.after_close)
    today, session = state.today, state.preclose
    if state.close is not None and state.close != datetime.time(16):
        print(f"early close today: {state.close:%H:%M} ET")
    bundle = fresh_bundle(module)
    frozen: list[str] = []
    if session:
        bundle = bundle.upto(today)
        if bundle.index[-1] != today:
            raise SystemExit(f"pre-close run at {now:%H:%M} ET but the data has no row for "
                             f"{today.date()}; rerun, or pass --after-close")
        bundle, filled = fill_live_row(bundle)
        if filled:
            print(f"live row: filled from the previous close: {', '.join(filled)}")
        bundle, fetched, frozen = repair_live_row(module, bundle)
        if fetched:
            print("live row: fetched on their own: " + ", ".join(
                f"{t} {p:,.2f}" for t, p in fetched.items()))
        if len(frozen) > MAX_FROZEN:
            raise SystemExit(f"no live price for {len(frozen)} tickers ({', '.join(frozen)}), "
                             "even fetched on their own: rerun, or pass --after-close")
        if frozen:
            print(f"WARNING: no live price for {', '.join(frozen)}: held unchanged today "
                  "(no orders); marked at the last close in the decision")
        flat = flat_today(module, bundle, exclude=frozen)
        if len(flat) >= STALE_FLAT:
            print(f"WARNING: {len(flat)} ETFs exactly flat on the day ({', '.join(flat)}): "
                  "the live row may be stale; check the quotes before sending")
        lo, hi = state.window
        if not lo <= f"{now:%H:%M}" <= hi:
            print(f"WARNING: pre-close run at {now:%H:%M} ET, outside {lo}-{hi} (close "
                  f"{state.close:%H:%M}): live prices far from the close, or past the MOC "
                  "cutoff")
    else:
        # with --after-close during the session, today's row is live, not a close: drop it
        bundle = bundle.upto(today - pd.Timedelta(days=1) if today in bundle.index
                             and state.open_now else today)
    bundle = latest_complete(bundle, [t for t in module.RISKY])
    if session and bundle.index[-1] != today:
        raise SystemExit("pre-close run, but a traded ETF has no live price today")
    prices = all_prices(module, bundle)
    age = len(pd.bdate_range(bundle.index[-1], today)) - 1
    if age > STALE_DAYS:
        print(f"WARNING: latest complete close is {bundle.index[-1].date()}, "
              f"{age} business days ago")
    holdings = read_holdings(args.holdings, prices, args.nav)
    book, summary = run(args.strategy, holdings, args.nav, bundle, args.min_order,
                        execution_day=today if session else None,
                        frozen=frozen)
    when = (f"PRE-CLOSE run at {now:%H:%M} ET: market-on-close orders for "
            f"{summary['execution_day']}" if session else
            f"after-close run: orders for the close of {summary['execution_day']} "
            "(one day behind the backtest; run before the close to match it)")
    print(when)

    pd.set_option("display.width", 200)
    print(f"{summary['strategy']} as of {summary['asof']} | NAV ${summary['nav']:,.0f} | "
          f"target gross {summary['gross_target']:.2f}x, cash {summary['cash_target']:+.1%} | "
          f"turnover {summary['turnover']:.1%}, est. cost ${summary['estimated_cost']:,.0f} | "
          f"{summary['status']}" + (" | funding from cash (no band)" if summary["funding"]
                                     else ""))
    for name, wt in summary["overlays"].items():
        print(f"overlay {name}: {wt:.1%} of NAV from the close of {summary['execution_day']}")
    flag = ("  <-- leans on unusual correlations" if summary["stress_vol"]
            > STRESS_RATIO * summary["model_vol"] else "")
    print(f"ex-ante vol of the target: {summary['model_vol']:.1%} (risk model), "
          f"{summary['stress_vol']:.1%} (long-run correlations){flag}")
    m = summary["margin"]
    print(f"margin (Reg T estimate): gross {m['gross']:.2f}x (long {m['long']:.2f}, short "
          f"{m['short']:.2f}), borrowed {m['borrowed']:.0%} | maintenance {m['maintenance']:.0%}"
          f", cushion {m['cushion']:.0%} of NAV | margin call if the long book falls "
          f"{m['drop_to_call']:.0%} | initial margin used {m['initial_used']:.0%}")
    print("stress, past weeks replayed on the target: " + "; ".join(
        f"{name} {x['loss']:+.1%} (cushion {x['cushion']:.0%})"
        for name, x in summary["stress"].items()))
    for alert in summary["margin_alerts"]:
        print(f"ALERT: {alert}")
    view = book[["price", "current", "target", "trade_dollars", "trade_shares"]]
    print(view.to_string(formatters={
        "price": "{:,.2f}".format, "current": "{:.1%}".format, "target": "{:.1%}".format,
        "trade_dollars": "{:+,.0f}".format,
        "trade_shares": lambda v: "" if pd.isna(v) else f"{v:+,.0f}"}))
    orders = book[book["order"]]
    if args.out:
        orders.assign(side=np.where(orders["trade_weight"] > 0, "BUY", "SELL"),
                      order_type="MOC", execution_day=summary["execution_day"])[
            ["side", "trade_shares", "trade_dollars", "price", "order_type",
             "execution_day"]].to_csv(args.out)
        print(f"\n{len(orders)} orders written to {args.out}")
    elif len(orders):
        print(f"\n{len(orders)} orders above ${args.min_order:,.0f} (pass --out to save them)")
    else:
        print("\nno orders: the book is within its no-trade band")


if __name__ == "__main__":
    main()
