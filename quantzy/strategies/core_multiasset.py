"""Core multi-asset book: 17 ETFs, class Sharpe priors, slow-trend and VRP tilts.

What it is
    Long-only mean-variance on 17 liquid ETFs and a T-bill sleeve, rebalanced daily, up to
    1.3x gross. ``mu_i = vol_i * (SR_class + 0.05 * trend_i + 0.037 * ewmac_i + 0.091 *
    vrp)``: a judgment-set Sharpe prior per asset class (for IEF and TLT, half way to the
    term spread over long-run vol); two slow trend scores in Sharpe units,
    price against its 200-day EMA and the 64/256 EMA crossover, splitting one trend budget;
    and, on the 12 equity ETFs only, the variance risk premium (VIX^2 minus SPY's realized
    variance, a 5-year z-score), one market-wide number.

Evidence (``research/research_trend_*.py``)
    * Risk model: the 5/20/60 EWMA blend forecast vol best (QLIKE); trend-scaled vol made it
      worse, so the risk model carries no alpha.
    * Trend IC vs next-day return / trailing vol, 47-day interleaved folds, Newey-West t:
      +0.008 (t 1.2) on these 17 ETFs, +0.012 (t 1.9) on 45. Positive in 8 of 9 asset
      groups, strongest in commodities and currencies, mostly from before 2010.
    * k = 0.1 matches the IC (Grinold) and beat 0, 0.2 and 0.3 in both train and test.
    * EWMAC 64/256 (``research_ewmac_slow.py``): IC +0.018 (t 2.7) on the same cells where
      EMA-200 has +0.006 (t 0.9), on a plateau from 48/192 to 128/512, positive in every
      five-year period. In the book it is no better than EMA-200 (Sharpe 1.14 vs 1.13 from
      2010), so it shares the trend budget rather than replacing it. Faster crossovers and
      acceleration had no IC (``research_ewmac_accel.py``).
    * VRP (``research_reentry.py``, ``research_vrp.py``): 1-day IC on SPY +0.023 (t 2.3,
      held-out folds, 1993-2026), weaker before 2008 (t 1.1). Neither implied nor realized
      variance predicts alone. Uncorrelated with trend (-0.01). Book Sharpe 1.06 -> 1.09,
      flat for weights 0.5x-2x; most of the gain is 2008 and 2021, and it does not fix the
      slow re-entry after a V-shaped crash (VRP was at -2 at the March 2020 low).
    * Treasury carry (``research_fast_valuation.py``, ``research_term_spread_prior.py``): the
      term spread forecasts Treasury excess returns since 1962 (1-month rank IC +0.15, t 3.9;
      positive out of sample). As half the prior it cut bonds in the flat and inverted curve
      of 2018-19 and 2022-24 (18% -> 8% of capital) and lifted test Sharpe 1.07 -> 1.14.
    * Kept at 0.30: the equity prior. 100 years of sector data say 0.38-0.51, but 0.35-0.40
      added risk without Sharpe (``research_prior_check.py``, ``research_equity_prior.py``).
      CAPE-type valuation forecasts only at 5-10 years (``research_shiller_valuation.py``).
    * Trading (``research_execution_cost.py``): the optimizer's 5 bp x 48 penalty, charged on
      cash too, is a smoothing band, not a cost estimate. Trading closer to the daily target
      lowered Sharpe at every looser setting (frictionless: 0.96 at 24x turnover). Costs
      charged to returns use per-ETF estimates instead.
    * Rejected: long-run vol in the risk model, VIX level, relative sector momentum (IC
      t 4.8 but no gain in a long-only book), an expanding-history Sharpe prior (wrong sign).
    * Shorting the trend did not pay on ETFs (short leg Sharpe -0.15), so long-only.
    * Adding EMB/LQD (credit, prior 0.25) or 28 more ETFs did not improve the book.

Where the Sharpe comes from
    About +0.33 over equal weight from risk-based weighting and correlations, ~0 from the
    class priors, about +0.1 from trend. Leverage and lambda set the size, not the Sharpe.
    Much of the diversification came from bonds hedging equities (2008-2021), which has not
    held since 2022; expect a forward Sharpe nearer 0.5-0.7 than the backtest's ~1.

Snapshot (2008-09-10 .. 2026-10-16, trades charged 1-5 bp by ETF, borrow at T-bill + 50 bp)
    return 12.9%, vol 11.2% (train 9.8% / test 12.5%), Sharpe 1.13 (train 1.14 / test 1.14,
    split 2017-07-26), max drawdown -15.9% (Mar-Sep 2022), turnover ~1.0x a year, cost
    ~0.04% a year, gross ~1.28x. Charged a flat 5 bp, as before: Sharpe 1.13 (1.126 against
    1.132), cost 0.10% a year. With a fixed Treasury prior: Sharpe 1.09 (test 1.07). Without
    VRP as well: Sharpe 1.06, drawdown -18.2% (Sep-Oct 2008), turnover 0.7x. The 64/256
    crossover needs history to 2008-09; the EMA-200-only book over the same dates had Sharpe
    1.07 and max drawdown -16.6%, and from 2008-05 Sharpe 0.99, drawdown -25.3%.
    Sharpe is return over vol with no cash deduction.
"""

from __future__ import annotations

import pandas as pd

from quantzy.config import TRADING_DAYS
from quantzy.data import CachedReader, DataBundle, DataReader, YFinanceReader
from quantzy.portfolio import PortfolioConfig
from quantzy.signals import (
    CarrySharpePrior,
    EwmacTrend,
    Identity,
    VarianceRiskPremium,
    VolScaledTrend,
)
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
EQUITY = [*CLASSES["US sectors"], *CLASSES["equity index"]]
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

#: Treasuries' prior moves half way from 0.25 to carry over long-run vol, where carry is the
#: long yield minus the 3-month bill (``research_term_spread_prior.py``).
CARRY_YIELDS = {"IEF": "^TNX", "TLT": "^TYX"}
CARRY_VOL = {"IEF": 0.07, "TLT": 0.14}

#: Seed influence an EMA may keep at its first trusted row; 0.05 needs ~300 rows of history.
EWM_TOL = 0.05

#: One-way cost charged per trade (commission plus half-spread, IBKR-like), in bp. An estimate
#: to check against real fills. The T-bill sleeve is not traded, so it is free.
EXECUTION_COST_BPS = {**{t: 2.0 for t in [*CLASSES["US sectors"], "EFA", "EEM"]},
                      **{t: 1.0 for t in ("QQQ", "GLD", "IEF", "TLT")}, "DBC": 5.0, "UUP": 5.0}

PORTFOLIO = PortfolioConfig(
    risk_aversion=1.5,  # ~10% vol with these alphas; watch the 8-12% band as alphas change
    max_leverage=1.3,
    max_position=0.2,
    position_cap_of="book",
    cost_bps=5.0,
    # cost_bps x cost_aversion is a smoothing penalty, not the cost charged: trading closer to
    # the noisy daily target lowered Sharpe at every looser setting tried.
    cost_aversion=48.0,
    execution_cost_bps=EXECUTION_COST_BPS,
    borrow_spread_bps=50.0,
    risk_halflives=(5, 20, 60),
)

ALPHAS: dict[str, tuple[Signal, float]] = {
    "prior": (CarrySharpePrior(PRIOR_SR, carry_vol=CARRY_VOL, carry_weight=0.5,
                               normalizer=Identity()), 1.0),
    # The trend budget (k = 0.1 for EMA-200 alone) is split in half between price against its
    # 200-day EMA and the slow 64/256 crossover. The EWMAC weight is half of 0.074, which gives
    # its score the same train-window dispersion as EMA-200's at 0.1.
    "trend": (VolScaledTrend(200, exclude=(CASH,), ma="ema", ewm_tol=EWM_TOL,
                             normalizer=Identity()), 0.05),
    "ewmac": (EwmacTrend(64, exclude=(CASH,), ewm_tol=EWM_TOL, normalizer=Identity()), 0.037),
    # The variance risk premium on the equity ETFs; its weight gives the score the same
    # train-window dispersion as the whole trend budget (``research_vrp.py``).
    "vrp": (VarianceRiskPremium(tuple(EQUITY), normalizer=Identity()), 0.091),
}


def etf_trading_days(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Keep only the days on which at least one of ``tickers`` traded.

    Index and yield series (^IRX, ^VIX) can print on US exchange holidays. A row with no ETF
    price makes every ETF's return on it NaN, and a 60-day rolling vol that sees one NaN is
    NaN for the next 60 days, which zeroes every alpha's ``mu`` for three months.
    """
    return raw[raw[tickers].notna().any(axis=1)]


def load(start: str = START, reader: DataReader | None = None) -> DataBundle:
    """Prices for the 17 ETFs plus a T-bill sleeve compounded from the ^IRX yield; the VIX and
    SPY closes the variance risk premium reads (panels, the same in every column); and a
    ``carry`` panel, long Treasury yield minus the 3-month bill for IEF and TLT.

    ``reader`` defaults to a cached Yahoo reader, which never refreshes a request without an
    end date: right for research, wrong for a daily job, which passes a fresh reader.
    """
    reader = CachedReader(YFinanceReader()) if reader is None else reader
    raw = reader.history(
        [*RISKY, "^IRX", "^VIX", "SPY", *CARRY_YIELDS.values()], start=start)
    raw = etf_trading_days(raw, RISKY)
    yld = raw["^IRX"].ffill() / 100.0
    cash_ret = ((1.0 + yld) ** (1.0 / TRADING_DAYS) - 1.0).shift(1).fillna(0.0)
    prices = raw[RISKY].copy()
    prices[CASH] = (1.0 + cash_ret).cumprod()

    def broadcast(s: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({t: s for t in prices.columns}, index=prices.index)

    carry = pd.DataFrame(index=prices.index, columns=prices.columns, dtype=float)
    for t, y in CARRY_YIELDS.items():
        carry[t] = (raw[y].ffill() - raw["^IRX"].ffill()) / 100.0
    return DataBundle.build(prices, vix=broadcast(raw["^VIX"]), market=broadcast(raw["SPY"]),
                            carry=carry)


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
    # Market-wide inputs (VIX, SPY) start with the bundle, not with the youngest ETF, so a
    # signal reading them only needs its per-asset history after that ETF's inception.
    per_asset = max([strategy.risk_model.min_history]
                    + [getattr(s, "asset_history", s.min_history) + s.lag + 1
                       for s in strategy.signals.values()])
    return int(max(first.max() + per_asset, strategy.min_history))
