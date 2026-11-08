# quantzy — audit brief

A self-contained description of the strategy, the framework around it, how it was
researched, what the numbers are, and where it can be wrong. Written for an auditor (human
or AI) arriving cold. State as of **2026-11-07**, prices through 2026-11-05.

Read this first, then the code it points to. Where this file and the code disagree, the code
is right and this file is stale. Please report it.

---

## 1. What this is

- **A long-only, daily-rebalanced, mean-variance ETF book.** 17 ETFs plus a T-bill sleeve, up
  to 1.3× gross, at about 10% vol. Five rule-based overlays run on top.
- **One saved strategy:** `quantzy/strategies/core_multiasset.py`. Everything else is the
  library it is built from, research records, or tooling.
- **Status: backtested, not yet traded live.** A production job (`quantzy/jobs/daily.py`)
  reads holdings from a CSV and writes orders to a CSV. There is no broker connection, no
  order execution and no run-state persistence. "Production" in the period table means the
  book as saved for trading from 2026-11-01. It is not a live track record.
- **Owner's working assumption:** the backtest Sharpe of about 1.4 is optimistic. The module
  docstring says to expect a forward Sharpe nearer 0.5–0.7, because much of the history's
  diversification came from bonds hedging equities (2008–2021), which has not held since 2022.

## 2. Headline numbers

All figures come from the saved book with every overlay, net of per-ETF trading cost and of
borrowing at T-bill + 50 bp. Ann. ret is CAGR. Sharpe = annualized mean daily return /
annualized vol, **with no cash deduction**, so it is not an excess-return Sharpe.

| Period | Data | Ann. ret | Vol | Sharpe | Max DD | Worst 5d |
|---|---|---|---|---|---|---|
| History 1995-10 → 2008-09 | proxies (§7) | 13.8% | 11.1% | 1.22 | −21.2% | −12.2% |
| Train 2008-09 → 2017-07 | ETFs | 13.2% | 9.2% | 1.40 | −15.1% | −8.3% |
| Test 2017-07 → 2026-10 | ETFs | 15.7% | 10.2% | 1.48 | −13.9% | −9.0% |
| Full 2008-09 → 2026-11-05 | ETFs | 14.1% | 9.7% | 1.40 | −15.1% | −9.0% |
| Full 1995-10 → 2026-11-05 | proxies + ETFs | 13.9% | 10.3% | 1.31 | −21.2% | −12.2% |
| 2026 to date | ETFs | +17.4% (total) | 10.7% | 1.83 | −5.4% | −5.4% |

Benchmarks over 2008-09 → 2026-11-05: QQQ Sharpe 0.82, equal-weight of the 17 ETFs 0.72,
the same book with priors only (no tilts) 1.06.

The interactive tear-sheet is built by `tools/tearsheet/export.py` and published at
<https://claude.ai/artifact/KagCis5282eJCN3qJmnim9>. It has all of the above, with charts,
weights and signals by date.

## 3. The framework, in one idea

```
data → signals (mu) → risk (Sigma) → mean-variance optimizer → target weights + trades
                     └──────────── Strategy.decide(bundle, asof, current) ────────────┘
```

Research, backtest and production all call the **same** `Strategy.decide`
(`quantzy/strategy.py`). Only the clock, the data window and the book differ. This is the
main defence against research/production drift. Three properties are asserted by tests:

| Property | Where enforced |
|---|---|
| Signals are **causal**: row *t* uses data through *t* only. A probe corrupts all data after *T* and checks the score at *T* is unchanged | `tests/test_signals.py` (every signal in `ALL_SIGNALS`) |
| **Backtest/live parity**: `decide(all_history, asof=T)` equals `decide(history_up_to_T)` | `tests/test_strategy.py::TestBacktestLiveParity` |
| `min_history` is honest: a short production window converges to the same last row | `tests/test_signals.py` |

`uv run pytest` runs 353 tests on synthetic data, with no network.

Package map (`quantzy/`): `strategy` (the seam), `data` (Yahoo reader, parquet cache, proxy
splicing), `signals`, `risk`, `optimize` (cvxpy mean-variance and composable constraints),
`portfolio` (`PortfolioConfig`: book-level settings), `construction`, `metrics`,
`strategies` (saved strategy and overlays), `jobs` (daily job, margin). `backtest` and
`research` are tooling. Nothing in them is on the live path.

## 4. The core strategy (`quantzy/strategies/core_multiasset.py`)

### 4.1 Universe

| Class | ETFs |
|---|---|
| US sectors | XLB XLE XLF XLI XLK XLP XLU XLV XLY |
| Equity index | QQQ EFA EEM |
| Treasuries | IEF TLT |
| Gold & commodities | GLD DBC |
| US dollar | UUP |
| Cash | TBILL: a synthetic sleeve compounded from the ^IRX yield |

### 4.2 Expected return (μ), in Sharpe units × each ETF's vol

```
mu_i = vol_i × ( SR_prior_i
               + 0.05  × trend_i      price vs its 200-day EMA, tanh score in (-1, 1)
               + 0.037 × ewmac_i      EMA 64/256 crossover, Carver forecast / 10, capped ±2
               + 0.091 × vrp          equity ETFs only: VIX² − SPY 21d realized var, 5y z, cap ±2
               + 0.10  × dollar_value UUP only: −log(DXY / 5y mean), 10y z, cap ±2 )
```

- **Priors (judgment, not fitted):** equities 0.30, GLD 0.20, DBC 0.15, UUP 0. IEF/TLT use
  0.5 × 0.25 + 0.5 × (long yield − 3m bill) / long-run vol (7% / 14%).
- **Tilt sizes:** each tilt weight was set so the score's train-window dispersion matches a
  budget. Trend at k = 0.1 was matched to its IC (Grinold). The tilts are small next to the
  priors.
- **Code:** `ALPHAS` in `core_multiasset.py`. Signal classes are in `quantzy/signals/`
  (`trend.py`, `prior.py`, `vrp.py`, `panel.py`).

### 4.3 Risk (Σ)

An equal-weight blend of EWMA covariances with halflives of 5, 20 and 60 days (lookback 250),
lagged like the signals (`quantzy/risk/blend.py`). There is no factor model and no shrinkage
toward long-run correlations. **Known weakness (§9):** in the worst months, realized book vol
was 1.3× forecast (`research/research_downturn_corr.py`).

### 4.4 Optimizer and constraints (`PortfolioConfig` in `quantzy/portfolio.py`)

Maximize `mu'w − 2 · w'Σw − 48 · 5bp · |Δw|`, subject to:

- long-only (cash may go negative, i.e. borrowing);
- gross of risky assets ≤ 1.3× capital;
- each ETF ≤ 20% of the *invested book* (26% of capital at 1.3×).

The 5 bp × 48 term is a **no-trade band, not a cost estimate**. Trading closer to the daily
target lowered Sharpe at every looser setting tested. Costs actually charged to returns are
per ETF, one way: 1 bp (QQQ, GLD, IEF, TLT), 2 bp (sectors, EFA, EEM), 5 bp (DBC, UUP).
Borrowing is charged at T-bill + 50 bp.

Optional constraints exist but are **off** in the saved book: `group_caps` (`GroupCap`,
`constraints.max_group`) and `group_floors` (`GroupFloor`, `constraints.min_group`). Both
were tested and not adopted (§8).

### 4.5 Where the Sharpe comes from

Roughly +0.33 over equal weight comes from risk weighting across classes. The core is 0.80
correlated with the priors' own portfolio. Trend adds about +0.1, the other tilts about
+0.02–0.05 each. The overlays add about +0.24 (core alone 1.17, with overlays 1.40, over
2008-09 → 2026-11-05). So the book is
mostly a **risk-parity-like allocation with small tilts**, not a signal-driven book.

## 5. Overlays

Rule-based positions **outside** the optimizer. Each is a module in `quantzy/strategies/`
listed in `core_multiasset.OVERLAYS`. Each has `target_after_close` (used by the job) and
`backtest_returns` (used by the tear-sheet and research), and returns are added to the core's.

| Overlay | Rule | Size | Evidence / status |
|---|---|---|---|
| `treasury_month_end` | Long TLT over the last 2 trading days of each month (bond index extension) | 0.5× NAV | Persistent since 1977, fading by period (overlay Sharpe 1.54 / 0.82 / 0.43) |
| `pension_rebalance` | SPY against month-to-date stock−bond drift over the same 2 days: `−0.25 × clip(z/2, ±1)` | ≤0.25× | Published 2025; Sharpe by period 0.91 / 0.77 / 0.35, half the tested size |
| `treasury_auction_10y` / `_30y` | Short IEF / TLT over the 5 trading days into each 10y / 30y auction | 0.125× each | t at the bar, chosen after a failed full-cycle test, 2002–08 the other way; half the tested size |
| `vix_hedge` | VIXY = max(0.05 while the VIX regime is calm [on at ≤13, off at ≥30], 0.10 × (equity weight − 0.75)); nothing bought at VIX ≥ 30 | ~0.05× | **Insurance, not an alpha** (missed the research bar). Exposure part added 2026-11-07 after the 2026-11-05 loss (§10) |

The overlays can take gross to about 2.2× on month-end days. The daily job shrinks them to
stay under 1.95× (Reg T, `quantzy/jobs/margin.py`). **The backtest does not apply that
shrink.**

## 6. Timing conventions (read carefully)

| Component | Information | Trade | Earns |
|---|---|---|---|
| Core, backtest (`quantzy/backtest/engine.py`) | close of *t* | at close *t* (implicitly) | day *t*+1 |
| Core, daily job, **pre-close run** (the intended mode) | live prices about 15 minutes before close *t* | market-on-close at close *t* | day *t*+1 |
| Core, daily job, after-close run (fallback) | close of *t* | close of *t*+1 | day *t*+2 |
| Overlays, backtest and job | through close of *t* | close of *t*+1 | day *t*+2 |

Signals and Σ carry `lag=1`, so they do not look ahead: the engine's decision for day *d*
reads the close of *d*−1 and holds the result through *d*, i.e. it fills at the very close
the signals read. **That is only achievable live by deciding just before the close and
sending market-on-close orders**, which is what the job now does (`quantzy/jobs/daily.py`,
since 2026-11-07): during the session Yahoo's daily history ends in a live row for today, the
job appends a placeholder row for the next day (`with_execution_row`) so the lag lands on the
live row, and the orders are for today's close. The overlays' rules each read only closes
before the one they trade at, so the live row never reaches them.

Measured, core only, before borrowing, 2008-09 .. 2026-11, a production-style daily replay
(decide on true history plus one row, fill, drift):

| Execution | Sharpe | Test half | Max drawdown |
|---|---|---|---|
| Same close (= the engine; the replay reproduces it exactly) | 1.184 | 1.136 | −14.8% |
| Pre-close, calibrated noise (3 seeds) | 1.173–1.178 | 1.121–1.134 | −14.6% to −15.2% |
| Pre-close, stress (7× the measured noise) | 1.162 | 1.109 | −14.9% |
| After-close run (one day behind) | 1.148 | 1.065 | −17.3% |
| The job before 2026-11-07 (two days behind: it decided on the row before its last) | 1.123 | 1.071 | −16.4% |

The noise is calibrated on 60 sessions of 5-minute bars (2026-08-14 .. 11-06): the move from
15:50 to the close carried a median 1.1% of the day's variance, with no lean in the day's
direction (mean correlation with the day so far +0.04) and a cross-ETF correlation of 0.20.
Sixty sessions is a short sample; it did include 2026-11-05 (QQQ −57 bp of −671 in the last
10 minutes). Residual live-row issues: the VIX in it is an intraday print, not the 16:15
settle; on ex-dividend days the history is re-adjusted; a stale quote is only warned of (four
or more ETFs exactly flat on the day, which never happened in the closes since 2008). Only the
VIX is filled from the previous close when it has no live print. A traded ticker (ETF, SPY,
VIXY) without a live price is fetched on its own; failing that it is held unchanged for the
day, and with more than two such tickers the run stops (`repair_live_row`).

## 7. Data

- **Prices:** Yahoo Finance via `YFinanceReader` (adjusted closes), cached as parquet by
  `CachedReader` in `.cache/prices`. **The cache never refreshes a request without an end
  date.** Refresh by moving or deleting `.cache/prices`. The daily job bypasses the cache.
- **Also used:** ^IRX (cash), ^TNX / ^TYX (carry), ^VIX, SPY (VRP), DX-Y.NYB (dollar value),
  VIXY (hedge, from 2011), and Treasury's tentative auction schedule (`.cache/auctions*`).
- **Proxy history (1995-10 → 2008-09)** in `quantzy/strategies/core_proxies.py`: each ETF is
  spliced back with an index fund, index or futures series so the levels meet. Proxies track
  their ETFs at 0.84–0.99 correlation. **Gold before 2000-08 is weakest (0.57):** gold miners
  hedged of the S&P, vol-scaled and demeaned, which assumes no gold trend in 1995–2000.
  History is a sanity check only, never used to choose settings.
- **Free of survivorship bias:** ETFs, not stocks, and no point-in-time fundamentals are used.

## 8. How it was researched

- **Process:** about 100 studies in `research/`, each pre-specifying candidates and an
  adoption rule in its docstring and writing a **Result** paragraph. Index:
  `research/README.md`. Synthesis: `research/SYNTHESIS.md`. Backlog and lessons:
  `research/IDEAS.md`. The studies are records run against the code at the listed commit.
  Many will not run unchanged today.
- **Signal protocol** (`research/README.md`, "Exploring a new signal"):
  1. Pooled time-series IC vs next-day return / 60d vol, on 47-day interleaved folds with
     held-out standardization and Newey–West t.
  2. Score distribution.
  3. Tail treatment (clip ±2 / ±3 / causal winsorize, compared by Pearson IC).
  4. In the book with `PORTFOLIO` fixed, judged train and test separately. The usual bar is
     +0.02 Sharpe with neither half lower.
- **Periods:** history (proxies) 1995-10 → 2008-09, train 2008-09-10 → 2017-07-25, test
  2017-07-26 → 2026-10-30, production from 2026-11-01.
- **Selection bias:** about 40 configurations were tried before the saved core, and about 100
  studies run in total. Several overlays were adopted at half the tested size because their
  t-stats sat at the bar. The train/test split is not a clean holdout: the test period was
  visible during later studies.
- **Notable rejections** (all documented): relative momentum (IC artifact), equity value,
  implied vol in risk, long-run vol in risk, 28 more ETFs, credit and breadth, skew, curve
  inversion, rebound triggers, insider buying (wrong sign), class caps and floors.

## 9. Known risks and open issues (ranked)

1. **Execution timing (§6).** The backtest fills at the close its signals read. The job
   matches it only when run before the close with market-on-close orders (since 2026-11-07;
   about −0.01 Sharpe from the gap between 15:45 prices and the close). Run after the close
   it is one day behind (about −0.04 Sharpe, max drawdown −17.3%); before 2026-11-07 it was
   two days behind (−0.06). The pre-close path is unit-tested against the engine's decision
   but had not yet been run in a live session at the time of writing.
2. **Concentration and pro-cyclical leverage.** Nothing limits a class. Equity weight runs
   −0.63 with the VIX. In 2026 the book reached 125% of capital in equities (long-run mean
   71%), with no Treasuries from March and its top four ETFs at 99%. On 2026-11-05 (QQQ
   −6.5%) it lost 4.41%, an 8σ day on its trailing vol and the 2nd-worst day since 2008. A
   class cap was tested and rejected as the user's call (`research_exposure_controls.py`):
   free over 30 years, but it costs strong equity years. The exposure-based hedge was
   adopted instead. It covers only about a quarter of such a shock.
3. **The stock–bond hedge has broken.** Much of the 2008–21 Sharpe came from bonds hedging
   equities. Since 2022 that correlation is about 0 to positive, and the optimizer has
   dropped bonds. The backtest's ~1.4 is not a forward estimate.
4. **Risk model too slow and too calm at turns.** EWMA 5/20/60 understated realized vol 1.3×
   in the worst months. Correlations jump at the onset of a crash and are not forecast.
5. **Priors are judgment.** Sensitivity: random errors of 0.10 in the priors cost a median
   0.21 Sharpe; inverted priors cost 0.41 (`research_prior_sensitivity.py`).
6. **Overlays are decaying anomalies.** The month-end and pension overlays have falling
   Sharpe by period, and the auction overlay is marginal. Review rules are in
   `research/IDEAS.md` § Monitoring.
7. **Costs:** a linear spread estimate per ETF with no market-impact model. Retail borrowing
   would cost 1–1.5% over T-bills, against the 0.5% assumed (about −0.3%/yr).
8. **Leverage and margin:** the overlay shrink to stay under Reg T is not in the backtest.
   Replaying the Oct 2008 crash week on today's book loses about 22%.
9. **Data:** Yahoo adjusted closes, single source, no cross-check. The cache must be cleared
   by hand.
10. **Bugs found and fixed recently** (each now has a regression test):
    - The month-end window treated the last two days of an *unfinished* month as month-end,
      inflating every mid-month backtest refresh (`treasury_month_end.in_window`, fixed
      2026-11-06).
    - The auction overlays' backtest missed a pre-auction window whose auction fell after the
      last data row, so a backtest ending inside one held no short while the job did
      (`treasury_auction.in_pre_window`, fixed 2026-11-07). It cost the tear-sheet's
      2026-11-04/05 about −0.1% of NAV (11-05 −3.27%, not −3.2%).
    - The job treated any run before 16:00 as pre-close, so on a 13:00 early close a 15:45 run
      would send market-on-close orders for a session that had ended
      (`treasury_month_end.close_time`, `jobs.daily.session_state`, fixed 2026-11-07).
    - The live-row fill also filled traded ETF prices, so a missing quote was traded at the
      previous close (`jobs.daily.fill_live_row`, fixed 2026-11-07).
    - Under pandas 3 a missing price lost the move across it (both returns NaN, booked as 0).
      The engine and overlays now mark a gap at the last price (`returns.holding_returns`).
      No current result changed: the data has no gaps.
    - A book funded from cash was built with the no-trade band, so it started at about 1.0x
      and took weeks to reach 1.3x, while the job's docstring said it started unbanded. Now
      `Strategy.decide(current=None)` builds the first target without the band, in the job
      and the backtest's first day alike (fixed 2026-11-07). Backtest: Sep-Dec 2008 +2.1%
      against +3.0%, train Sharpe 1.402 against 1.412, full 1.403 against 1.408, test
      unchanged. On the proxy history the band kept the two paths apart until 2000-04:
      history Sharpe 1.224 against 1.197. (The table's earlier worst 5 days, −12.9%, did not
      reproduce with the old start either: −12.4%.)
    - Insider research data held typo filings worth up to $2.1e16, which drove a false
      lead. Cleaned 2026-11-05; that was research only.

## 10. Recent change log (2026-11)

| Date | Change | Files |
|---|---|---|
| 11-01 | Treasury auction overlays adopted | `treasury_auction*.py` |
| 11-06 | Month-end window fix for incomplete months | `treasury_month_end.py`, `tests/test_jobs.py` |
| 11-07 | `GroupCap` / `max_group`, `GroupFloor` / `min_group` added, both off | `portfolio.py`, `optimize/constraints.py`, `tests/test_optimizer.py` |
| 11-07 | VIX hedge exposure part ("H2") adopted: Sharpe 1.399 → 1.408, 2026-11-05 −4.4% → −3.2% | `vix_hedge.py`, `jobs/daily.py`, `tools/tearsheet/export.py`, `strategies/__init__.py` |
| 11-07 | Daily job decides on the last row and runs pre-close with market-on-close orders (was two days behind the backtest) | `jobs/daily.py`, `tests/test_jobs.py` |
| 11-07 | Early closes in the job's clock; auction overlay window at the end of the data; live prices fetched or held, not filled; P&L across price gaps; a new book funded at full size, unbanded | `treasury_month_end.py`, `treasury_auction.py`, `jobs/daily.py`, `returns.py`, `backtest/engine.py`, overlays, `tests/test_jobs.py` |
| 11-05..07 | Studies: skew, curve inversion, rebound, insider ×2, exposure controls, class floors (none adopted except H2) | `research/research_*.py` |

As of writing, the 11-05 to 11-07 changes are **uncommitted** in the working tree.

## 11. Audit checklist

**Reproduce:**

```bash
uv sync && uv run pytest                          # 353 tests, synthetic, no network
mv .cache/prices .cache/prices.bak                # force fresh prices (cache never refreshes)
uv run python tools/tearsheet/export.py           # backtests + page -> .cache/tearsheet/
uv run python -m quantzy.jobs.daily --nav 1000000 --out orders.csv   # today's target book
```

**Check specifically:**

1. **Look-ahead:**
   - the engine timing in `quantzy/backtest/engine.py` (rebalance-then-accrue, `lag`);
   - `Signal.lag` and `min_history` in `quantzy/signals/base.py`;
   - the causality probes in `tests/test_signals.py`;
   - that `PanelScore` panels built in `core.load()` (carry, dollar value) use only past data.
2. **Execution timing (§6):** confirm that `run(..., execution_day=t)` on data through *t*
   gives the engine's `decide(asof=t+1)` (`tests/test_jobs.py::TestExecutionTiming`), and
   that the first live pre-close runs fill near the prices they decided on.
3. **Costs and borrowing:** `PortfolioConfig.net_of_borrowing` and `execution_cost_bps`.
   Check that cost is charged on the core's trades and on each overlay's trades.
4. **Overlays:** each `backtest_returns` against its `target_after_close` on the same dates.
   The job and backtest must agree (`tests/test_jobs.py`).
5. **Proxies:** splice levels and tracking stats (tear-sheet §12, `quantzy/data/splice.py`).
   Expect gold before 2000 to be weak.
6. **Selection bias:** read `research/README.md` top to bottom. Count the trials behind each
   adopted piece and compare the train-only result with the test result.
7. **Sharpe definition:** no cash deduction. Recompute on excess returns over TBILL for a
   conventional figure.
8. **Stress:** the 2026 concentration (`research_exposure_controls.py`). Check equity weight
   and top-4 share through time in the tear-sheet's Positioning section.

## 12. File map

| Path | What |
|---|---|
| `quantzy/strategies/core_multiasset.py` | The saved strategy: universe, priors, alphas, `PORTFOLIO`, overlays, `load`, `build`. Docstring has the evidence and snapshots |
| `quantzy/strategies/{treasury_month_end,pension_rebalance,treasury_auction*,vix_hedge}.py` | Overlays |
| `quantzy/strategies/core_proxies.py` | Proxy history 1995 onwards |
| `quantzy/strategy.py` | `Strategy.decide`: the shared seam |
| `quantzy/signals/` | Signal base (causality, lag, `min_history`) and implementations |
| `quantzy/risk/blend.py` | EWMA blend risk model |
| `quantzy/optimize/` | Mean-variance (cvxpy), constraints, cost models |
| `quantzy/portfolio.py` | `PortfolioConfig`, `GroupCap`, `GroupFloor` |
| `quantzy/backtest/engine.py` | Walk-forward engine (timing, §6) |
| `quantzy/jobs/daily.py`, `margin.py` | Production job, Reg T margin and stress |
| `tools/tearsheet/` | Tear-sheet export and page template |
| `research/` | Study records (`README.md` index, `SYNTHESIS.md`, `IDEAS.md`) |
| `tests/` | 353 tests, synthetic data |
| `.cache/` | Price, auction, research and insider caches (not in git) |

## 13. Glossary

- **IC:** correlation of a score with forward vol-scaled returns.
- **Held-out t:** t-stat on folds standardized with the other fold's statistics.
- **Unit score:** a signal before multiplying by vol, in Sharpe units per unit of weight.
- **k:** the weight of a tilt in μ.
- **Invested book:** the sum of non-cash weights.
- **Overlay:** a rule-based position outside the optimizer.
- **Proxy history:** ETF returns extended back with stand-in series.
- **H2:** the VIX hedge's exposure part.
