"""Core multi-asset book: 17 ETFs, class Sharpe priors, a small EMA-200 trend tilt.

What it is
    Long-only mean-variance on 17 liquid ETFs and a T-bill sleeve, rebalanced daily, up to
    1.3x gross. ``mu_i = vol_i * (SR_class + 0.1 * trend_i)``: a judgment-set Sharpe prior
    per asset class, plus the EMA-200 trend score in Sharpe units.

Evidence (``examples/research_trend_*.py``)
    * Risk model: the 5/20/60 EWMA blend forecast vol best (QLIKE); trend-scaled vol made it
      worse, so the risk model carries no alpha.
    * Trend IC vs next-day return / trailing vol, 47-day interleaved folds, Newey-West t:
      +0.008 (t 1.2) on these 17 ETFs, +0.012 (t 1.9) on 45. Positive in 8 of 9 asset
      groups, strongest in commodities and currencies, mostly from before 2010.
    * k = 0.1 matches the IC (Grinold) and beat 0, 0.2 and 0.3 in both train and test.
    * Shorting the trend did not pay on ETFs (short leg Sharpe -0.15), so long-only.
    * Adding EMB/LQD (credit, prior 0.25) or 28 more ETFs did not improve the book.

Where the Sharpe comes from
    About +0.33 over equal weight from risk-based weighting and correlations, ~0 from the
    class priors, about +0.1 from trend. Leverage and lambda set the size, not the Sharpe.
    Much of the diversification came from bonds hedging equities (2008-2021), which has not
    held since 2022; expect a forward Sharpe nearer 0.5-0.7 than the backtest's ~1.

Snapshot (2008-05-12 .. 2026-10-14, 5 bp trades, borrow at T-bill + 50 bp)
    return 10.7%, vol 10.8%, Sharpe 0.99 (train 0.92 / test 1.05, split 2017-07-26),
    max drawdown -25.3%, turnover ~0.8x a year, gross ~1.26x.
    Sharpe is return over vol with no cash deduction.
"""

from __future__ import annotations

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, YFinanceReader
from quantzy.portfolio import PortfolioConfig
from quantzy.signals import Identity, SharpePrior, VolScaledTrend
from quantzy.signals.base import Signal
from quantzy.strategy import Strategy
from quantzy.universe import Asset, Universe

NAME = "core_multiasset"
CASH = "TBILL"
CLASSES = {
    "US sectors": ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"],
    "equity index": ["QQQ", "EFA", "EEM"],
    "Treasuries": ["IEF", "TLT"],
    "gold & commodities": ["GLD", "DBC"],
    "US dollar": ["UUP"],
}
RISKY = [t for ts in CLASSES.values() for t in ts]
UNIVERSE = Universe(
    name=NAME,
    assets=[Asset(t, t, "etf") for t in RISKY]
    + [Asset(CASH, "Synthetic 13-week T-bill (^IRX)", "cash_equivalent")],
)
START = "1998-12-22"  # the sector SPDRs' first day

# Long-run Sharpe priors, set by judgment rather than fitted: equities earn the most per unit
# of risk, Treasuries a little less, gold and commodities less again (commodity futures lose
# roll yield), and a long dollar position has no premium at all.
CLASS_SR = {"US sectors": 0.3, "equity index": 0.3, "Treasuries": 0.25,
            "gold & commodities": 0.2, "US dollar": 0.0}
PRIOR_SR = {t: CLASS_SR[c] for c, ts in CLASSES.items() for t in ts} | {"DBC": 0.15}

#: Seed influence an EMA may keep at its first trusted row; 0.05 needs ~300 rows of history.
EWM_TOL = 0.05

PORTFOLIO = PortfolioConfig(
    risk_aversion=1.5,  # ~10% vol with these alphas; watch the 8-12% band as alphas change
    max_leverage=1.3,
    max_position=0.2,
    position_cap_of="book",
    cost_bps=5.0,
    cost_aversion=48.0,  # set for slow alphas: a trade must pay back within about a week
    borrow_spread_bps=50.0,
    risk_halflives=(5, 20, 60),
)

ALPHAS: dict[str, tuple[Signal, float]] = {
    "prior": (SharpePrior(PRIOR_SR, normalizer=Identity()), 1.0),
    "trend": (VolScaledTrend(200, exclude=(CASH,), ma="ema", ewm_tol=EWM_TOL,
                             normalizer=Identity()), 0.1),
}


def load(start: str = START) -> DataBundle:
    """Prices for the 17 ETFs plus a T-bill sleeve compounded from the ^IRX yield."""
    raw = CachedReader(YFinanceReader()).history([*RISKY, "^IRX"], start=start)
    yld = raw["^IRX"].ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    prices = raw[RISKY].copy()
    prices[CASH] = (1.0 + cash_ret).cumprod()
    return DataBundle.build(prices)


def build(alphas: dict[str, tuple[Signal, float]] | None = None,
          portfolio: PortfolioConfig | None = None) -> Strategy:
    """The saved strategy, or a variant of it.

    Args:
        alphas: ``{name: (signal, weight)}`` to use instead of :data:`ALPHAS`. To test a new
            alpha against this book, pass ``{**ALPHAS, "new": (signal, weight)}``.
        portfolio: Settings to use instead of :data:`PORTFOLIO`. Leave it alone when testing
            an alpha.
    """
    alphas = ALPHAS if alphas is None else alphas
    portfolio = PORTFOLIO if portfolio is None else portfolio
    active = {n: (s, w) for n, (s, w) in alphas.items() if w}
    return Strategy(
        universe=UNIVERSE,
        signals={n: s for n, (s, _) in active.items()},
        weights={n: w for n, (_, w) in active.items()},
        risk_model=portfolio.risk_model(),
        optimizer=portfolio.optimizer(CASH),
        name=NAME,
    )


def warmup(bundle: DataBundle, strategy: Strategy | None = None) -> int:
    """First row at which every ETF has history enough for the signals and the risk model.

    Each ETF enters at its own inception, so this is the latest inception plus warmup.
    """
    strategy = strategy or build()
    first = bundle.prices[RISKY].apply(lambda s: bundle.index.get_loc(s.first_valid_index()))
    return int(first.max()) + strategy.min_history
