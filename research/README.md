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

## Exploring a new signal

Work on the **unit score**: the signal before any times-vol, in Sharpe units per unit of
weight (`signal.raw(bundle) / realized_vol(prices, signal.vol_window, annualize=True)` for
the return-unit signals), NaN until each asset has warmed up. In order:

1. **IC on the score as designed.** `pooled_time_series_ic` (or `information_coefficient` for
   a cross-sectional signal), next-day return over 60-day vol, `chunk_folds(index, 47)` with
   each fold standardized on the other, `newey_west_t`; then five-year periods and longer
   horizons. Fix the candidate settings and the adoption rule before running it.
2. **Distribution.** `describe_scores(unit, by_asset=True)`: moments, the 1/5/95/99th
   percentiles, `max_abs`, the share beyond 2 and 3. Excess kurtosis well above 0, a
   `max_abs` far past the 99th percentile, or more than ~0.3% beyond 3 means fat tails. A
   mean far from 0 is a standing position, not timing. Mass piled at the bounds (a saturated
   `tanh`) is a scaling problem, not a tail problem.
3. **Tail treatment.** Build the score *without* any cap of its own, then
   `tail_treatments(unit)` gives: none, clip +/-2, clip +/-3, and winsorize at the 1st / 99th
   percentiles. Compare them with `compare_tail_treatments(variants, prices)` on the lagged
   scores (`v.shift(signal.lag)`). Two rules:
   - Use **Pearson** IC here (the default). Clipping and winsorizing keep the order of the
     values, so a rank IC cannot tell them apart; print it only as a check (it should move
     by less than ~0.001).
   - Winsorize **causally**. `winsorize_scores` takes each asset's percentiles from its own
     history through the current row; full-sample percentiles use the future.

   Choose on robustness, not level: the treatment with the best held-out t (`held_out_t`)
   and the best `worst_period`. Keep the simpler fixed clip unless another treatment wins on
   both; a gain only on all dates is fitting the tails it can see.
4. **Book.** Put the chosen treatment into the signal (a `cap` field, as `EwmacTrend` and
   `VarianceRiskPremium` have), add it to `core.build(alphas={**core.ALPHAS, ...})` with
   `PORTFOLIO` fixed, and judge the Sharpe it adds, train and test halves separately.

`research_tail_treatment.py` is the worked example, run on the four alphas in the saved book.

Ideas not yet tested, open portfolio questions and lessons so far are in
[IDEAS.md](IDEAS.md).

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
| `research_implied_erp` | Damodaran's implied ERP as a US equity value tilt | Forecasts a year ahead since 1961 (rank corr +0.28), but lowers the book's Sharpe (1.05-1.06) | No | c5c0205 |
| `research_ma_band_reversion` | Trade re-entry from +/-2 bands of a normalized EMA(x) - EMA(200) spread | The OU look is mechanical (a random walk shows it); no edge; long/short loses | No | 46f8220 |
| `research_bounce_filtered` | Equity bounce-back overlay with VRP, VIX and trend crash filters | Trend filter removes crash losses (overlay Sharpe 0.60) but adds +0.01 to the book, none in the test half | No | ddd9284 |
| `research_spread_trades` | Treasury curve slopes (5s10s, 10s30s) and tied ETF pairs, z-score reversion | No faster than a random walk; curve reversion ended around 1990; some pairs trend | No | 1eae436 |
| `research_rsi` | RSI(2) and RSI(14) dip-buying on SPY with a trend filter | RSI(2) works alone (Sharpe ~0.5) but matches the 5-day z and adds nothing to the book; RSI(14) rarely triggers | No | e0d033e |
| `research_credit_breadth` | Fear & Greed parts: 21d credit momentum, 252d credit contrarian, sector breadth vs SPY | None pass; breadth is trend; the 252d credit signal works with the reverse sign (t -3.4, VWEHX 1993 on) | No | 660f780 |
| `research_credit_momentum` | 252d credit momentum tilt on equities, capped (C3) and z-scored (C4) | C3 is a standing equity bet (1.23 -> 1.06); C4 centred still costs 0.03-0.05 (test -0.07+): overlaps trend, late in fast reversals | No | 27b91fb |
| `research_tail_hedge` | VIX-regime SPX puts (on at low VIX, sold into spikes), VIXY, put-write after spikes | Low VIX is not cheap; the rule makes the hedge ~free and lifts the book +0.02-0.06 on simulated prices, within pricing error; VIXY +0.013 | No | b5767f4 |
| `research_vixy_hedge` | The VIX-regime rule on VIXY (real prices): grid of thresholds and sizes | Main version +0.011 full / +0.066 test, misses the +0.02 bar; Calmar down in every cell; 13/30 at 0.05x adopted as insurance by choice | Yes (overlay) | e04f46d |
| `research_fx_sleeve` | G10 currency carry, value and momentum, long/short at 5% vol, FRED data 1980 on | Sharpe 1.07 in 1980-2007, 0.08 since 2008; +0.15 corr with the book; lowers it 1.27 -> 1.13 | No | 9af3709 |
| `research_trend_sleeve_broad` | Extras at a zero prior; a separate long-only trend sleeve | Extras barely enter; the sleeve is 0.6-0.7 correlated with the core and lowers Sharpe | No | 01e0c81 |
| `research_kmlm` | KMLM (managed futures) as an ETF in the book, prior 0.2, no trend on it; checked first against DBMF/CTA and our trend sleeves | Uncorrelated (+0.03 with the book), but in the optimizer 1.07 -> 0.94 since 2021-12 at every prior 0.1-0.25: its weight swings between ~0 and the cap with the short covariance; at a fixed 20% share 1.19 -> 1.22, but lower without 2022 (1.61 -> 1.57); closed | No | 03c32ca |
| `research_qual` | QUAL (US quality factor ETF) as an equity ETF, prior 0.30 (0.35 as a check), trend/EWMAC/VRP on it | Never enters at 0.30 (spanned by the sector SPDRs, corr 0.68 with the book); at 0.35 takes 13.5% for Sharpe 1.12 -> 1.11 | No | d8e826c |
| `research_prior_sensitivity` | The core book under other prior sets: flat, hindsight, inverted, random errors of 0.10, level x0.5 / x1.5 | Flat = saved (1.16 vs 1.15); hindsight +0.18, inverted -0.41, random errors median -0.21; level x0.5 1.31 (both halves), x1.5 1.00, a lead only | — | 2bb024e |
| `research_corr_block` | Correlations shrunk toward class blocks (0.25, 0.5, 1) or identity, or from slower EWMAs (halflife 120-500), on today's book | Saved 1.12; block 1.02-1.11, identity 1.03-1.08, slow 1.05-1.06 with drawdowns to -20%; better conditioning, lower Sharpe | No | 4214792 |
| `research_prior_weight` | The prior's weight in mu, 1.0 to 0.1, picked on the train half; at 0.5 and 0.25 also at the same book size | x0.5 1.30 against 1.15 (test 1.31), but at the same size 1.15 / 1.08: the gain is a smaller book under the 1.3x cap, not more weight on the other alphas | No (a sizing question) | 2e4b947 |
| `research_size_grid` | Risk aversion 1.5-4 x leverage cap 1.3-2x, core plus overlays | A higher cap adds return, not Sharpe, and breaks Reg T with the month-end overlay; Sharpe rises with lambda (1.27 / 1.33 / 1.40 / 1.46) as return falls (14.3% -> 10.3%) | Yes, lambda 2 (cap stays 1.3x), b07be57 | 3a40160 |
| `research_vol_scaling` | Risk aversion 3 and 4 levered back to 10-12% vol (constant k), against 2 unscaled, with the overlays | At ~10% vol 1.34-1.36 against 1.33, at 1.5-1.6x gross and 6% of days above Reg T 2x; at 11-12% Sharpe falls and drawdowns reach -20% | No (stay at lambda 2) | dc951cb |
| `research_tail_treatment` | Each saved alpha's unit-score distribution; no cap, clip +/-2 (saved), +/-3, causal winsor 1/99 by Pearson IC | VRP has the only real tails (excess kurtosis 63) and needs +/-2 (held-out t 0.15 uncapped -> 0.67); EWMAC best at +/-2; trend is a bounded tanh; the dollar's alternatives pass on IC but lower the book (1.334-1.337 against 1.340) | No (keep the saved caps) | 150979e |
| `research_corr_factor` | Off-diagonal noise in the 48-day effective risk model; eigenvalue clipping (RMT) and 1-3 factor PCA correlations, vols kept | Only 1-2 eigenvalues clear the noise edge, but the small ones are real near-duplicates (QQQ vs XLK/XLY, TLT vs IEF, EFA vs EEM; same on 17 years of data); min-variance vol 2.7% saved vs 3.2-3.8% cleaned; book 1.21 vs 1.12-1.22, pca 2 up in train, down in test | No | 10af29e |
| `research_downturn_corr` | Are correlations underestimated in downturns? Predicted vs realized over the next 21 days, ex post (SPY's worst 10%) and ex ante (below EMA-200, VIX > 25, drawdown > 10%) | Ex post equity correlations +0.11 above forecast and book vol 1.31x predicted (1.28x vols, 1.07x correlations; stock-bond hedge weaker than forecast since 2022); ex ante no extra gap (t <= 0.9), so no state-based fix | — | 10af29e |
| `research_implied_vol_risk` | VIX (equities) and MOVE (Treasuries) in the risk model's vols, premium-adjusted to each ETF's 1-year realized vol, weight 0.5 / 1, plus a raw VIX check | Better forecasts (equity QLIKE in SPY's worst months 0.13 vs 0.35) but a worse book: 1.12-1.16 vs 1.21, up in train, down in test (t -2.2 to -2.7); raw 1.23 only in train | No | 7557cde |
| `research_implied_vol_mu` | The implied-vol candidate with `mu` scaled by the same factor as Sigma's vols, so the Sharpe view is unchanged and implied vol only resizes | 1.15 vs 1.21 (test 1.09, t -3.0); average equity weight restored but re-entry still slower (38% vs 46% three months after the 2020 low): implied-vol sizing sells the premium VRP buys | No (idea closed) | 2b37215 |
| `research_trend_skip` | Skip the last 1-21 days in the EMA-200 and EWMAC trend alphas (12-1 style), all assets or equities only; pre-specified 21, then a 5-20 sweep | Equity IC rises with the skip (EMA-200 t 0.6 -> 2.8) but the book falls (1.24 -> 1.18-1.23, test 1.20 -> 1.09-1.19, max DD -14.8% -> -16.9%): later exits in fast crashes (2020, 2025) | No | 06cf8e9 |
| `research_crash_split` | Do the "good IC, worse book" variants (trend skip, implied vol) win on ordinary days and lose in fast crashes, so a regime gate could rescue them? | No: the skips gain ~0 on normal days (+2 to +11 bp/yr, t <= 0.7); a perfect hindsight gate gives 1.243-1.245 vs 1.241; implied vol loses on normal days and gains in crashes | — (closes the asymmetric skip and crash gating) | ac089f0 |
| `research_month_end_flows` | Month-end duration extension in IEF and LQD; pension rebalancing (signed SPY over the last two days, by SPY vs IEF month to date); quarter-ends | IEF passes (1.37 vs 1.34) but is 0.92 correlated with the TLT overlay (a sizing question); LQD fails the test half; pension 1.41 but gone with a one-day-older signal (1.34); its month-to-date slope is real (t -4.0 excluding the entry day) | No; magnitude follow-up | 847f27d |
| `research_month_end_followups` | Month-end Treasury risk split TLT / IEF at the same risk; pension rebalancing sized by the month-to-date SPY-IEF deviation through t-1, capped at 0.5x SPY | Split +0.003 (no); pension 1.396 vs 1.340 (train 1.410 / test 1.388), overlay t 2.96, corr +0.04, robust to t-2 / t-3 (1.40) and without 2008 (1.39); concentrated (best 5 months 58%) and fading since 2017 (overlay Sharpe 0.35) | Yes, `pension_rebalance` overlay at 0.25x | c63dfaf |
| `research_vrp_multi` | The saved VRP signal on Treasuries (MOVE vs TLT), gold (GVZ vs GLD) and oil (OVX vs USO, on DBC), at the equity VRP's weight | IC bonds t 0.8, gold wrong sign, oil t 1.5; book 1.16 vs 1.21 (all three), each alone 1.18-1.20; the saved equity VRP's 1-day IC is ~0 since 2017 (t 0.2) | No | 03f9c58 |
| `research_vrp_review` | Does the saved equity VRP still add to the book, given its ~0 1-day IC since 2017? Retire only if <= 0 since 2017 and < +0.02 overall | Keep: +0.042 Sharpe (t 2.4) overall, +0.048 (t 2.1) since 2017, positive in every five-year period; its edge is at weeks, not days | Kept | e188f3b |
