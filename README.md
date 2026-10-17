# quantzy

A composable quant-investing library covering the full pipeline:

**data → signals (μ) → risk (Σ) → portfolio optimization → optimal trade**

The pipeline is composed in exactly one place — `Strategy.decide()` — which the backtest
engine and a production job both call. Everything else is arranged around that call.

## The one idea

Research and production disagree when they are written twice. So they are written once:

```
                        ┌──────────────────────────────┐
   research  ──────────▶│                              │
   (IC, sweeps)         │    Strategy.decide(...)      │
   backtest  ──────────▶│                              │
   (walk forward)       │  data → μ → Σ → optimize     │
   production ─────────▶│         → target + trades    │
   (daily job)          └──────────────────────────────┘
```

|            | backtest                        | production                  |
|------------|---------------------------------|-----------------------------|
| clock      | iterate historical dates        | now                         |
| data       | full history                    | a `min_history` window      |
| book       | simulated (`SimulatedBook`)     | the real one                |
| **decide** | **`Strategy.decide`**           | **`Strategy.decide`**       |
| output     | apply fills, accrue P&L         | emit orders                 |

Only the two ends differ. `tests/test_strategy.py::TestBacktestLiveParity` asserts that the
middle does not: `decide(all_history, asof=T)` must equal `decide(history_up_to_T)`.

### Why this holds

A `Signal` author writes one method, `raw(data)`, which must be **causal** — row *t* may not
depend on any row after *t*. The base class then offers the same computation three ways:

```python
signal.scores(data)            # the whole panel        → research
signal.forecast(data, asof=T)  # one row, as of T       → backtest
signal.latest(window)          # the last row           → production
```

`forecast` and `latest` are *slices of the same panel*, never a separate calculation — so
they cannot drift. `tests/test_signals.py` asserts causality for every signal rather than
trusting the author, including that corrupting all data after *T* leaves the score at *T*
untouched.

Two things make the production slice trustworthy:

- **`lag`** — rows between observing data and being able to trade on it. Default `1`: a
  score computed from the close of day *t* sizes the book that earns day *t+1*. The risk
  model carries the same lag, so μ and Σ rest on identical information.
- **`min_history`** — how much warmup a short production window needs. For a recursive
  filter this is set by *convergence*, not by the span: an EWM never fully forgets its seed,
  so `span=60` needs ~622 rows, not 60. Pass fewer and the last row is silently wrong rather
  than absent. `Strategy.min_history` takes the binding constraint across every component.

## Layout

| Package                | Role       | Responsibility |
|------------------------|------------|----------------|
| `quantzy.strategy`     | **shared** | `Strategy.decide()` → `Decision`. The seam. |
| `quantzy.data`         | shared     | `DataReader`, `YFinanceReader`, `CachedReader`, `DataBundle` |
| `quantzy.universe`     | shared     | `Asset` / `Universe` |
| `quantzy.signals`      | shared     | `Signal` base; trend (`VolScaledTrend`, `EwmacTrend`), priors (`SharpePrior`, `CarrySharpePrior`), `VarianceRiskPremium`, others; normalizers, indicators |
| `quantzy.risk`         | shared     | `SampleCovariance`, `ewma_blend`, `LedoitWolfCovariance`, `StateScaledRisk` → Σ |
| `quantzy.optimize`     | shared     | `MeanVariance` / `MinVariance` / `MaxSharpe`, constraints, cost models |
| `quantzy.construction` | shared     | `Portfolio`, `Trade`, target-vs-current diff |
| `quantzy.metrics`      | shared     | Sharpe, drawdown, turnover — scores any return series |
| `quantzy.backtest`     | *tooling*  | `WalkForwardBacktester`, `SimulatedBook` |
| `quantzy.research`     | *tooling*  | `information_coefficient`, `ic_decay`, `holdout_tune` |
| `quantzy.portfolio`    | shared     | `PortfolioConfig`: book-level settings shared by every alpha |
| `quantzy.strategies`   | **saved**  | Strategies a production job may run, e.g. `core_multiasset` |
| `quantzy.jobs`         | production | `daily`: run a saved strategy against the real book, emit orders |

`backtest` and `research` are tooling: nothing in them is on the path a live run takes.
Production imports `strategy`, `data`, `signals`, `risk`, `optimize`, `construction`,
`portfolio` and `strategies` — and nothing else.

Outside the package:

| Folder           | What it is |
|------------------|------------|
| `research/`      | Records of past studies, with an [index of results](research/README.md). Not maintained against the current code. |
| `tools/tearsheet/` | `export.py` builds the `core_multiasset` backtest tear-sheet (one HTML page). |
| `tests/`         | Synthetic data, no network. |

## Quick start

```bash
uv sync                                      # create .venv from pyproject + uv.lock
uv run pytest                                # synthetic data, no network

# production: today's target book and the orders to reach it
uv run python -m quantzy.jobs.daily --nav 3000000 --holdings positions.csv --out orders.csv

# the backtest tear-sheet for the saved strategy
uv run python tools/tearsheet/export.py      # -> .cache/tearsheet/core_multiasset.html
```

`positions.csv` holds `ticker,shares` (or `ticker,weight`). With no holdings file the job
funds the account from cash. It fetches fresh prices; research code reads through
`CachedReader`, which caches to `.cache/` (parquet) and never refreshes a request without an
end date.

The job, the tear-sheet and every research script build the strategy from the same module,
`quantzy/strategies/core_multiasset.py`: the strategy being researched is the strategy being
backtested is the strategy being traded.

## Saved strategies

A saved strategy lives in `quantzy/strategies/<name>.py` and is the only thing a production
job runs. It keeps two kinds of decision apart:

```python
from quantzy.strategies import core_multiasset as core

core.ALPHAS      # {name: (signal, weight)}: what to own, blended into mu
core.PORTFOLIO   # PortfolioConfig: risk aversion, leverage, caps, costs, risk model
core.build()     # the Strategy; core.load() fetches its data
```

Portfolio settings are chosen once for the book. Alphas come and go. To test a candidate,
keep `PORTFOLIO` fixed and compare against the saved book:

```python
candidate = core.build(alphas={**core.ALPHAS, "value": (Value(window=756), 0.1)})
```

Judge it on its IC (interleaved folds, `chunk_folds` + `newey_west_t`) and on the Sharpe it
adds over `core.build()`, not on the book's total. Promote it by editing `ALPHAS`, in its own
commit, and update the snapshot in the module docstring.

## Writing a signal

```python
from dataclasses import dataclass
from typing import ClassVar

from quantzy.signals import Signal
from quantzy.signals.indicators import sma

@dataclass                                     # required: params() drops non-fields
class PriceToMA(Signal):
    name: ClassVar[str] = "price_to_ma"
    window: int = 200

    def raw(self, data):                       # must be causal
        return -(data.prices / sma(data.prices, self.window) - 1.0)

    @property
    def min_history(self) -> int:              # override; the default of 0 claims no memory
        return self.window
```

Add it to `ALL_SIGNALS` in `tests/test_signals.py` and the causality suite covers it.

Three things are easy to get wrong, so each one fails loudly rather than silently:

| Mistake | What happens |
|---|---|
| forgetting `@dataclass` | `params()` raises — otherwise the parameter vanishes from sweeps and the causality test would rebuild a default-valued object while appearing to pass |
| a non-causal `raw` | `tests/test_signals.py` fails — including a probe that corrupts all data after *T* |
| leaving `min_history` at `0` | `latest()` accepts a window too short to converge, and returns a wrong last row |

## Status

Fully wired: signals → shrinkage covariance → mean-variance optimization → trades, driven by
`Strategy.decide` and exercised end to end by the walk-forward engine, the holdout tuner and
the IC tooling.

Not yet built:

- **Broker integration.** `quantzy.jobs.daily` reads the book from a file and writes orders to
  a file; there is no broker API connection, order execution or run-state persistence.
- **Cache freshness in research.** `CachedReader` keys on `(tickers, start, end, field)` with
  no TTL, so an `end=None` request caches forever. The daily job bypasses it and warns on a
  stale close; research code does not. Delete `.cache/prices` to refresh.
- **Point-in-time fundamentals.** `DataBundle` carries arbitrary panels, but there is no
  vintage handling yet (what a filing said when published, vs as later restated).
- **A factor risk model** for large universes.
