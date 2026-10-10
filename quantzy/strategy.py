"""The strategy seam: one decision function, shared by every runner.

A :class:`Strategy` bundles what defines *what you trade* -- the universe, the signals and
their model weights, the risk model, the optimizer -- and exposes the same three read paths
a :class:`~quantzy.signals.base.Signal` does, for the same reason:

- :meth:`Strategy.mu_panel` -- the blended score panel over all history, for research.
- :meth:`Strategy.decide` with an ``asof`` -- one decision at a past date, for the backtest.
- :meth:`Strategy.decide` with no ``asof`` -- today's decision, for the production job.

The last two are the *same method*. A backtest calls it once per rebalance date; a live run
calls it once, with ``asof`` left at ``None``. There is no second implementation to drift,
which is the entire point: the only way to make research and production agree is to give
them nothing to disagree about.

What differs between the two worlds sits strictly on either side of this call:

    ===========  ==============================  ============================
                 backtest                        live
    ===========  ==============================  ============================
    clock        iterate historical dates        now
    bundle       full history                    a warmup window (``min_history``)
    current      simulated book                  the real book
    **decide**   **Strategy.decide**             **Strategy.decide**
    output       apply fills, accrue P&L         emit orders
    ===========  ==============================  ============================

Nothing here imports from :mod:`quantzy.backtest`, and nothing here knows whether it is
being simulated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd

from quantzy.construction.portfolio import Portfolio
from quantzy.construction.trades import TradeList
from quantzy.data.bundle import DataBundle, as_bundle

if TYPE_CHECKING:
    from quantzy.optimize.base import BaseOptimizer
    from quantzy.risk.base import RiskModel
    from quantzy.signals.base import Signal
    from quantzy.universe import Universe


@dataclass(frozen=True, slots=True)
class Decision:
    """Everything a strategy concluded at one instant, and why.

    This is the unit of output in both worlds. A backtest collects a list of them; a live run
    produces one per day and hands :attr:`trades` to execution. Keeping the inputs (``mu``,
    ``sigma``) beside the output makes a decision self-explaining after the fact -- you can
    answer "why did it buy that?" without re-running anything.

    Attributes:
        asof: The instant the decision was made.
        target: The portfolio the strategy wants to hold.
        trades: The diff from the book it was given to :attr:`target`, with estimated cost.
        mu: Blended expected-return scores that drove it.
        sigma: Covariance matrix used.
        status: Solver status reported by the optimizer.
        diagnostics: Free-form extras (strategy name, objective value, rows visible).
    """

    asof: pd.Timestamp
    target: Portfolio
    trades: TradeList
    mu: pd.Series
    sigma: pd.DataFrame
    status: str = "optimal"
    diagnostics: dict = field(default_factory=dict)

    @property
    def weights(self) -> pd.Series:
        """Target weights, for callers that want only the answer."""
        return self.target.weights

    @property
    def turnover(self) -> float:
        """One-way turnover required to reach the target from the book it was given."""
        return self.trades.turnover

    def to_frame(self) -> pd.DataFrame:
        """Per-asset summary: target weight, the score behind it, and the trade."""
        out = pd.DataFrame({"mu": self.mu, "target_weight": self.target.weights})
        trades = self.trades.to_frame()
        out = out.join(trades[["side", "delta_weight", "delta_notional"]], how="left")
        out["side"] = out["side"].fillna("hold")
        out["delta_weight"] = out["delta_weight"].fillna(0.0)
        return out


@dataclass
class Strategy:
    """A complete, runnable strategy: universe + weighted signals + risk model + optimizer.

    Args:
        universe: The assets this strategy may hold. Validated against the data on every
            decision -- a live feed silently dropping a ticker would otherwise look like a
            legitimate zero weight rather than a broken input.
        signals: Mapping of name -> Signal, or a list (names taken from each ``.name``).
            Their z-unit scores are blended into ``mu``; this replaces the old
            ``AlphaCombiner``, so the blend is a property of the strategy rather than of
            whichever script assembled it.
        risk_model: Produces the covariance the optimizer trades off against.
        optimizer: Carries its own constraints and cost model, reused unchanged at every
            decision -- so the constraint set is part of the strategy, not of the caller.
            Previously the example applied a 5% SGOV floor and the tuner did not, which meant
            the strategy being tuned was not the strategy being run.
        weights: Model weight per signal name; missing names default to 1.0.
        mu_scale: Multiplies the blend. Signals are in z-units, and the optimizer's risk
            aversion is calibrated against expected-return units, so this is the one dial
            that sets how hard the whole blend pushes.
        name: Label used in diagnostics and result objects.
    """

    universe: Universe
    signals: dict[str, Signal] | list[Signal]
    risk_model: RiskModel
    optimizer: BaseOptimizer
    weights: dict[str, float] = field(default_factory=dict)
    mu_scale: float = 1.0
    name: str = "strategy"
    _cache: tuple[tuple, pd.DataFrame] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if isinstance(self.signals, list):
            self.signals = {s.name: s for s in self.signals}
        if not self.signals:
            raise ValueError("a Strategy needs at least one signal")
        unknown = set(self.weights) - set(self.signals)
        if unknown:
            raise ValueError(
                f"weights name signals that are not in this strategy: {sorted(unknown)}; "
                f"available: {sorted(self.signals)}"
            )

    # --- research path -------------------------------------------------------

    def mu_panel(self, data: DataBundle | pd.DataFrame) -> pd.DataFrame:
        """The blended score panel over all history (dates x tickers).

        Memoized on the bundle's fingerprint, and each signal memoizes its own panel, so a
        backtest over hundreds of rebalances computes this once rather than once per date.
        """
        bundle = as_bundle(data)
        key = bundle.fingerprint()
        if self._cache is not None and self._cache[0] == key:
            return self._cache[1]

        combined: pd.DataFrame | None = None
        for name, signal in self.signals.items():
            panel = signal.scores(bundle) * self.weights.get(name, 1.0)
            combined = panel if combined is None else combined.add(panel, fill_value=0.0)
        assert combined is not None  # guaranteed non-empty by __post_init__
        panel = (combined * self.mu_scale).reindex(columns=self.universe.tickers)

        self._cache = (key, panel)
        return panel

    def mu(
        self, data: DataBundle | pd.DataFrame, asof: pd.Timestamp | None = None
    ) -> pd.Series:
        """One blended score row -- as of ``asof``, or the last row when ``None``."""
        panel = self.mu_panel(data)
        if asof is None:
            return panel.iloc[-1]
        window = panel.loc[:asof]
        if window.empty:
            raise ValueError(f"no signal history at or before {pd.Timestamp(asof).date()}")
        return window.iloc[-1]

    # --- the seam ------------------------------------------------------------

    def decide(
        self,
        data: DataBundle | pd.DataFrame,
        asof: pd.Timestamp | None = None,
        current: Portfolio | None = None,
    ) -> Decision:
        """Decide the target book, given what is currently held.

        Args:
            data: A bundle (or bare price panel) covering at least :attr:`min_history` rows
                up to ``asof``.
            asof: Point in time to decide at. ``None`` means the last row in ``data`` -- the
                production call. A backtest passes each rebalance date.
            current: The book held coming into this instant. ``None`` means flat, which is
                the honest starting state for a fresh backtest but almost never right live.

        Returns:
            A :class:`Decision` with the target, the trades to reach it, and the ``mu`` and
            ``sigma`` that produced them.

        Raises:
            ValueError: If the data is missing universe assets, or holds too few rows at
                ``asof`` for the signals to have converged.
        """
        bundle = as_bundle(data)
        self._validate(bundle, asof)

        mu = self.mu(bundle, asof)
        sigma = self.risk_model.covariance(bundle, asof)

        if current is None:
            current = Portfolio.empty(self.universe.tickers)
        result = self.optimizer.optimize(mu, sigma, current=current)

        visible = bundle.index if asof is None else bundle.index[bundle.index <= asof]
        return Decision(
            asof=bundle.index[-1] if asof is None else pd.Timestamp(asof),
            target=result.target,
            trades=result.trades,
            mu=mu,
            sigma=sigma,
            status=result.status,
            diagnostics={
                "strategy": self.name,
                "objective_value": result.objective_value,
                "rows_visible": len(visible),
            },
        )

    # --- contract ------------------------------------------------------------

    @property
    def min_history(self) -> int:
        """Rows of history this strategy needs before a decision is trustworthy.

        The binding constraint across every signal (each already accounting for its own lag
        and, for recursive filters, its convergence warmup) and the risk model. This is the
        number a production job uses to size the window it pulls -- pass fewer rows and the
        last row of a converging filter is silently wrong rather than absent.
        """
        signal_need = max(s.min_history + s.lag + 1 for s in self.signals.values())
        return max(signal_need, self.risk_model.min_history)

    def _validate(self, bundle: DataBundle, asof: pd.Timestamp | None) -> None:
        missing = [t for t in self.universe.tickers if t not in bundle.tickers]
        if missing:
            raise ValueError(
                f"data is missing {len(missing)} universe asset(s): {missing}; "
                f"available: {list(bundle.tickers)}"
            )
        visible = len(bundle.index if asof is None else bundle.index[bundle.index <= asof])
        if visible < self.min_history:
            where = "" if asof is None else f" as of {pd.Timestamp(asof).date()}"
            raise ValueError(
                f"{self.name} needs >= {self.min_history} rows{where} for its signals and "
                f"risk model to have converged, got {visible}"
            )

    def params(self) -> dict[str, Any]:
        """Flat parameter map for sweep tables and run labelling."""
        out: dict[str, Any] = {"mu_scale": self.mu_scale}
        for name, signal in self.signals.items():
            out[f"{name}.weight"] = self.weights.get(name, 1.0)
            for k, v in signal.params().items():
                out[f"{name}.{k}"] = v
        return out
