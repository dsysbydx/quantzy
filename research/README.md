# Research records

Each script here is a study that was run to decide something about a saved strategy. They are
**records, not maintained code**: each ran against the library and saved book as they were at
the commit listed, and later changes (the carry panel, the holiday-row fix, the split between
charged and optimizer cost, deleted helpers) mean many will not run unchanged today. To
reproduce one, check out its commit:

```bash
git checkout <commit> -- research/ quantzy/   # or: git worktree add ../q-<commit> <commit>
uv run python research/<script>.py
```

Most write their conclusion into a **Result** paragraph at the top of the docstring. Studies
import one another from this directory, so run them as files (`python research/x.py`).

Conventions they share: Sharpe is return over vol with no cash deduction; IC is tested on
next-day returns over trailing vol with 47-day interleaved folds and Newey-West t-stats;
candidates are pre-specified before they are run and judged with `PORTFOLIO` held fixed.

## Index

| Study | Question | Result | Adopted | Commit |
|---|---|---|---|---|
| `research_qqq_timing` | Does a trend or vol-target overlay beat buy-and-hold QQQ? | Compared against a static QQQ/cash mix at the same exposure; led to the optimizer-based versions below | — | 91db396 |
| `research_qqq_optimizer` | QQQ timing as a daily mean-variance optimization | Trend had no IC for returns but carried information about risk ahead | — | 91db396 |
| `research_qqq_volmodel` | Can a better vol model replace trend in QQQ's mu? | The 5/20/60 EWMA blend forecast vol best; trend-scaled vol made forecasts worse | `ewma_blend(5, 20, 60)` as the shared risk model | 91db396 |
| `research_qqq_mu` | Trend in mu, with the best vol forecaster as Sigma | The framework for trend as a view in mu | — | 91db396 |
| `research_qqq_tune` | Two-stage tuning of the QQQ overlay | Superseded by the multi-asset book | — | 91db396 |
| `research_pullback` | Long-term trend bought on short-term dips, across sectors | No result recorded | No | 91db396 |
| `research_trend_breadth` | Does trend predict across 17 ETFs? | EMA-200 1-day IC +0.008 (t 1.2); class priors plus trend at k = 0.1; the 17-ETF book | The saved book's base | 91db396 |
| `research_trend_global` | Trend on 45 ETFs | IC +0.012-0.014 (t 1.9-2.3), positive in 8 of 9 groups; the book was worse (EM overloaded) | No | 91db396 |
| `research_trend_sleeve` | Trend as a long/short sleeve | L/S Sharpe 0.53, long leg 0.78, short leg -0.15; no gain as an overlay | No (stay long-only) | 91db396 |
| `research_trend_plus` | Add EMB, LQD or FXY to the 17 | About 2% weight, +0.01 Sharpe | No | 91db396 |
| `research_valuation_priors` | Building-block (yield + growth) priors for equities and bonds | Sharpe 0.98 against 0.99; international dividend yield wrong sign | No | fb9529c |
| `research_ewmac_accel` | Carver EWMAC at four speeds; trend acceleration | Only slow speeds have IC; acceleration none | No | f6e61eb |
| `research_ewmac_slow` | Is EWMAC 64/256 a real edge? | IC plateau 48/192-128/512 (~+0.017, t 2.4-2.7) against +0.006 for EMA-200 | Yes, half the trend budget | 76b9cfe |
| `research_reentry` | Faster re-entry after drawdowns: long-run vol (A), VIX-based view (B) | A re-enters faster but de-risks slower; B (VRP) works | VRP onward | d24d7f9 |
| `research_sector_priors` | Relative sector momentum (C); expanding-history Sharpe prior (D) | C: IC t 4.8 but no book gain; D: wrong sign | No | d24d7f9 |
| `research_vrp` | VRP robustness before promoting it | IC +0.023 (t 2.3) on SPY since 1993; neither part alone predicts; Sharpe 1.06 -> 1.09 | Yes, `vrp` alpha at 0.091 | d24d7f9 |
| `research_prior_check` | Each judgment prior against realized Sharpe on the longest proxy | Equities earned 0.38-0.51 over 100 years against 0.30; others match | — | 8935db6 |
| `research_equity_prior` | Equity prior 0.35 or 0.40 | Adds risk, not Sharpe | No (keep 0.30) | 8935db6 |
| `research_shiller_valuation` | Does CAPE-type valuation forecast US equity returns (1871-2024)? | Yes at 5-10 years, near zero at 1-12 months; levels drift out of sample | No | 4752ec2 |
| `research_fast_valuation` | Market-implied value and carry for bonds, gold, commodities, dollar | Only the Treasury term spread forecasts (1-month IC +0.15, t 3.9 since 1962) | Term spread onward | 4752ec2 |
| `research_term_spread_prior` | Treasury prior half way to term-spread carry | Test Sharpe 1.07 -> 1.13, mainly cutting bonds in the 2022-24 inversion | Yes, `CarrySharpePrior` | b26c32b |
| `research_execution_cost` | Is the cost penalty over-penalizing small trims? | No: it acts as a noise filter; the frictionless target is worst (0.96) | Keep the band; charge per-ETF costs | 46f3739 |
| `research_inflation_regime` | Breakeven momentum tilting real assets and bonds | Zero pooled IC, legs of opposite sign | No | dc4955a |
| `research_long_short` | Lift the long-only rule | Sharpe 1.13 -> 1.06; relative momentum fails in every structure | No | 5cb7e27 |
| `research_broad_universe` | 65 ETFs with equal priors | 0.78-0.88 against 1.17: equal priors made it an EM bet | No | f4208c4 |
| `research_us_tilt` | How much of the Sharpe is a hindsight US tilt? | About 0.09 (world-cap anchor: 1.13 -> 1.04) | No change | f4208c4 |
| `research_fixed_mix_breadth` | 56 ETFs with the class mix held fixed | +0.02 Sharpe, not significant | No | d7d3436 |
| `research_shrinkage` | Correlation shrinkage (0.1, 0.25, Ledoit-Wolf, slow correlations) | Fixes conditioning, not performance | No | d7d3436 |
| `research_corr_anchor` | Blend the risk model's correlations with a 3-5 year window | No Sharpe change; the risk model is well calibrated; today's concentration shrinks only partly | No; stress vol in the daily job instead | 37e1017 |
| `research_risk_budget` | Risk-budget books (equal class risk; class risk ~ view) against mean-variance | 0.82 / 0.90 against 1.13; far lower stress vol today (9.5-11.6% against 21%) | No | 37e1017 |
| `research_calendar_flows` | Pre-FOMC drift, turn of the month, Treasury month-end as a mostly-cash sleeve | FOMC and turn-of-month faded after publication; Treasury month-end persists (t 4.2 TLT, 5.1 since 1977), corr with core +0.05 | Yes, `treasury_month_end` overlay at 0.5x | 4be863e |
| `research_cross_asset_mom` | Bond momentum for equities, equity momentum for bonds | No IC since 1994 for equities; weak for bonds; no book gain | No | 4be863e |
| `research_bitcoin` | IBIT (BTC-USD before 2024) at a zero prior, trend only | Sharpe 1.16 -> 1.24, but ~70% from 2017; test half +0.02 | No | 8d858ef |
| `research_multi_horizon` | 1/3/6/12-month TSMOM, per class | Slow trend is already best; 1-month is reversal on sectors; no book gain | No | 8d858ef |
| `research_short_reversal` | Short-term reversal: sector and country cross-section (5/21 days), time series, pullbacks | Weekly reversal faded since 2017; equity 5-day time-series reversal and country 21-day cross-sectional reversal survive | To `research_reversal_sleeves` | 187f6f3 |
| `research_reversal_sleeves` | Equity bounce-back overlay; country 21-day reversal (long/short, hedged) | Bounce-back is more equity beta (corr 0.44); country reversal adds +0.01 and faded since 2017 | No | 5ebc437 |
| `research_commodities` | 13 commodity ETFs at a zero prior; trend and continuation around EIA, FOMC and payroll days | Nothing enters or helps; event days are bigger but not more predictable | No | e68e1ec |
| `research_value` | Value tilts per class: CAPE-based ERP, sector and regional reversal, real yield, real gold, DBC and dollar reversal | Equity value hurts (0.80 all-in); dollar 5-year reversal is the one clear signal (252d IC +0.43) | Dollar value tilt at 0.10 | 9a35c5d |
| `research_trend_sleeve_broad` | Extras at a zero prior; a separate long-only trend sleeve | Extras barely enter; the sleeve is 0.6-0.7 correlated with the core and lowers Sharpe | No | 01e0c81 |
