# quantzy

A small, composable quant-investing library covering the full pipeline:

**data → alpha (μ) → risk/variance (Σ) → portfolio optimization → optimal trade**

It is built around typed dataclasses for domain objects and `typing.Protocol`s for
pluggable components, so you can swap a data source, an alpha signal, a risk model, or an
optimizer without touching the rest of the stack.

## Pipeline

```
DataReader ──prices──▶ Alpha ──┐
                               ├─▶ AlphaCombiner ─μ─┐
DataReader ──prices──▶ Alpha ──┘                    ├─▶ Optimizer ─▶ target weights + TradeList
                                                    │
returns ─▶ RiskModel ─Σ─────────────────────────────┘     (against current holdings,
                                                            net of transaction cost)
```

## Quick start

```bash
uv sync                              # create .venv from pyproject + uv.lock (incl. dev tools)
uv run python examples/end_to_end.py # QQQ / VOO / URTH / SGOV -> target weights + trades
uv run pytest                        # runs on synthetic data, no network
```

`uv sync` is reproducible: it installs the exact versions pinned in `uv.lock`. After
changing dependencies in `pyproject.toml`, run `uv lock` to refresh the lockfile. Dev tools
(pytest, ruff) live in the `dev` dependency group and are installed by `uv sync` by default
(use `uv sync --no-dev` to skip them).

## Running the end-to-end example

From the repository root (`quantzy/`):

```bash
uv run python examples/end_to_end.py
```

That's the only command you need — `uv run` automatically creates/syncs `.venv` from
`uv.lock` first if it is missing, so you don't have to run `uv sync` separately or activate
the environment.

Notes:
- **Run it from the repo root** so the `examples/` path and the local `.cache/` directory resolve.
- **The first run needs internet** — it fetches prices from Yahoo Finance and caches them to
  `.cache/` (parquet). Later runs read the cache and work offline. Delete `.cache/` to refresh.
- It prints the expected-return scores (μ), annualized vols from Σ, the optimized target
  weights, and the resulting trade list with estimated transaction cost.

Prefer to activate the environment and use plain `python`? Equivalent:

```bash
uv sync
source .venv/bin/activate            # Windows: .venv\Scripts\activate
python examples/end_to_end.py
```

## Layout

| Package            | Responsibility |
|--------------------|----------------|
| `quantzy.data`     | `DataReader` protocol, `YFinanceReader`, parquet `CachedReader` |
| `quantzy.universe` | `Asset` / `Universe` definitions (starts with QQQ, VOO, URTH, SGOV) |
| `quantzy.alpha`    | `Momentum`, `Value`, `AlphaCombiner` → expected-return forecast μ |
| `quantzy.risk`     | `SampleCovariance`, `LedoitWolfCovariance` → covariance Σ (factor model stubbed) |
| `quantzy.optimize` | `BaseOptimizer` (cvxpy template) + `MeanVariance` / `MinVariance` / `MaxSharpe`, constraints, `CostModel` |
| `quantzy.construction` | `Portfolio`, `Trade`, target-vs-current diff |
| `quantzy.backtest` | metrics (Sharpe, drawdown, turnover); walk-forward engine stubbed |

## Status

This is an initial scaffold: the momentum/value → shrinkage covariance → mean-variance
path is fully wired; the factor risk model, walk-forward backtester, and additional alphas
are stubs to be filled in.
