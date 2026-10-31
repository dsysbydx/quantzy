# Ideas backlog

Ideas not yet tested, or tested only in part, with the reasoning behind them. When one is
run it becomes a study in [README.md](README.md) and leaves this file (its row's commit
links the two). Order within a section is priority. Designs here are drafts: the version
fixed before a test goes in that study's docstring.

What the book is now (2026-10-19): long-only mean-variance on 17 ETFs. Its Sharpe comes
mostly from risk weighting across classes: the core is 0.80 correlated with the prior's own
portfolio. On top of that sit trend (EMA-200 and EWMAC, 0.74-0.80 correlated, so one bet),
VRP and dollar value (each 0.1-0.26 against everything), and two overlays: Treasury month-end
(about 0 against everything) and the VIX hedge (-0.44 against the book).

## Next up

Ranked 2026-10-31 by expected gain x odds x cost. The lessons below rule out more
covariance tweaks (four studies), new long-only assets and leverage changes.

### 1. Futures hybrid, step 2: tracking on real futures data
- **Why:** `research_implementation_cost.py` (branch `implementation-cost`) puts a futures
  hybrid at ~50 bp/yr over US ETFs at IBKR for a Thai or Singapore holder (+0.05 Sharpe),
  from withholding, borrowing and fees; a cost, not an alpha, so the odds are high.
- **Draft:** per contract, excess return over T-bills against the ETF's: tracking error,
  implied financing spread, DV01 ratio for ZN/IEF and ZB/TLT. Then re-run step 1 with the
  measured spreads.
- **Needs:** data. IBKR API (~2 years; exchange subscriptions needed to trade anyway) or
  Databento (15+ years of CME, CBOT, COMEX; free credit likely covers it).

### 2. Implied vol in the risk model -- closed 2026-10-31
- Sigma only (`research_implied_vol_risk.py`): better forecasts, worse book (1.16 vs 1.21).
  Sigma and `mu` consistently (`research_implied_vol_mu.py`): 1.15. Implied-vol sizing cuts
  exposure after selloffs, selling the premium the `vrp` alpha buys.

### 3. VRP on more assets
- **Why:** VRP is the book's strongest uncorrelated alpha, but it is one equity-market
  number. The same premium exists in Treasuries (MOVE), gold (GVZ), oil (OVX) and emerging
  markets (VXEEM).
- **Draft:** implied minus realized variance per asset class, 5-year z-score capped at +/-2,
  on that class's ETFs; IC first, then the book with `PORTFOLIO` fixed.

### 4. More calendar flows, and overlay sizing -- tested 2026-10-31
- `research_month_end_flows.py`: IEF month-end passes but duplicates the TLT overlay
  (corr 0.92); LQD fails the test half; quarter-ends are not special. Pension rebalancing:
  the sign rule's gain needs the same-close signal (gone one day earlier), but the
  month-to-date deviation predicts the window (t -4.0, every period).
- Pension by magnitude (`research_month_end_followups.py`): passes (1.396 vs 1.340, overlay
  t 2.96, robust to the signal's timing); a candidate third overlay, pending a decision on
  size given its concentration and fade since 2017. Shorts need an ETF short or futures.
- TLT / IEF split at the same risk: +0.003, no change; revisit IEF with futures.
- Still open: the TLT overlay's size once futures margin replaces Reg T.

### 5. Sector and country relative momentum, long/short
- **Why:** relative sector momentum had the strongest IC we found (t 4.8) but could not be
  used long-only; the gain is in shorting the losers. Industry and country momentum is
  well documented (Moskowitz-Grinblatt 1999; Asness, Moskowitz and Pedersen 2013) and,
  beta-hedged, should be close to independent of the book.
- **Doubt:** `research_long_short` found relative momentum failing in every structure inside
  the book; a separate beta-hedged sleeve at its own vol is a different test, but the prior
  is weaker than the IC suggests.
- **Draft:** 12-1 month return (also 6-1), ranked within sectors (9 SPDRs) and within ~15
  country ETFs; long top third, short bottom third, beta-neutral to SPY on 1-year betas,
  5% vol, monthly; 2 bp a trade plus 0.5%/yr borrow on shorts. Long check: 12 Fama-French
  industries from 1927.
- **Watch:** momentum crashes (2009, Nov 2020); overlap with the trend portfolio.
- **Bar:** sleeve Sharpe > 0.3 since 2008; +0.02 to the book's full and test-half Sharpe.

### 6. Cross-sectional seasonality
- **Why:** sectors and countries that did well in a calendar month tend to repeat in that
  month (Keloharju, Linnainmaa and Nyberg 2016); documented as independent of momentum.
- **Draft:** on the universe of idea 5, rank by the average return in the same calendar month
  over the past 10-20 years; long/short, beta-neutral, 5% vol. Nearly free once 5 is built.

### 7. Commodity carry (futures-curve basis), long/short
- **Why:** strong evidence (Gorton-Rouwenhorst; Koijen et al. 2018) and independent of the
  book; our commodity tests used ETFs and trend, not carry.
- **Needs:** futures curves. Pairs with buying futures data for idea 1.

### 8. Universe cleanup
- **Why:** the 17 overlap: five sector SPDRs explain 96.5% of QQQ's variance; EFA overlaps
  EEM and XLI (`research_corr_factor.py`). Dropping near-duplicates may steady the
  concentration swings.
- **Doubt:** four covariance studies say conditioning is not what costs the book.

### 9. Tail hedge on real option prices
- **Why:** `research_tail_hedge.py` priced SPX puts off the VIX and found the regime rule
  within pricing error; real end-of-day option prices would settle it.
- **Needs:** Cboe DataShop or ORATS.

### 10. Low-beta versus high-beta sectors, beta-neutral
- **Why:** betting against beta (Frazzini-Pedersen 2014). **Doubt:** it partly overlaps the
  risk weighting the book already does.

## Needs data first

- **Put/call ratio** (a Fear & Greed component). Check Cboe data access first; the other two
  F&G parts we tested (credit, breadth) did not add to trend and VRP.
- Ideas 1, 7 and 9 above also need data.

## Portfolio decisions (not alphas)

- **Target vol 15% / leverage cap.** Answered 2026-10-27 (`research_size_grid.py`): a higher
  cap adds return but not Sharpe, deepens drawdowns and, with the month-end overlay, passes
  Reg T's 2x; the cap stays at 1.3x and risk aversion moved 1.5 -> 2 (vol ~10%). A 15% vol
  target would need the higher cap. Stress vol under long-run correlations is ~20% against
  8.8% under the model. Portfolio settings are global, not tuned to an alpha.
- **Target vol by scaling** (answered 2026-10-27, `research_vol_scaling.py`): risk aversion 3
  or 4 levered back to ~10% vol gives 1.34-1.36 against 1.33 at 2, at 1.5-1.6x gross and
  6% of days above Reg T's 2x. The higher-lambda Sharpe was mostly holding and borrowing
  less. 11-12% vol would need lambda ~1.5-1.75 and a smaller month-end overlay.

## Parked (user's call, 2026-10)

- Backtest before 2000 ("ignore the long backtest for now").
- Futures / FX direction trading ("not asking for futures/FX direction yet"); FX premia since
  tested as a sleeve (`research_fx_sleeve.py`, decayed since 2008).
- IBIT / Bitcoin (prior Sharpe 0 if added; "leave IBIT for now").
- Realized-vol drift outside 8-12% ("leave it for now").

## Monitoring

- **VIX hedge review** (adopted 2026-10-19 as insurance by choice): revisit if over about
  three years it costs more than 1.5% of NAV a year without paying off in a spike.

## Lessons so far

- Adding assets to the long-only book rarely helps: new assets barely enter (broad universe,
  FX ETFs, commodities, Bitcoin), and long-only trend on more assets is 0.6-0.7 correlated
  with the core. A new source of return needs its own sleeve.
- A signal that is mostly relative (cross-sectional) cannot be used long-only; test it
  long/short.
- Managed futures (KMLM) is uncorrelated with the book but earned about nothing outside
  2022 (2020-26): in the optimizer at a fixed prior its weight swings with the short
  covariance; at a fixed share it is 2022 insurance (`research_kmlm.py`, closed).
- Classic cheap premia have decayed since 2008 (G10 currency carry, value and momentum:
  Sharpe 1.07 before, 0.08 after), and published anomalies weaken (Treasury month-end).
- Check a score's mean before using it as a tilt: an off-centre score is a standing
  position, not timing (credit momentum C3).
- Check a score's tails before trusting its IC, and compare tail treatments by Pearson IC
  (rank IC cannot see them). Uncapped, the VRP z-score is noise; capped at +/-2 it is a
  signal (`research_tail_treatment.py`). A cap that clips a thin-tailed score often (the
  dollar, 9% of days) costs a little, but not enough to matter in the book.
- A high condition number is not proof of estimation noise: on these 17 ETFs it is overlap
  (QQQ vs XLK/XLY, TLT vs IEF), present on 17 years of data too. Check the smallest
  eigenvectors on a long sample before cleaning a covariance (`research_corr_factor.py`).
- A better vol forecast is not a better book. Implied vol forecast crash months better but
  sized the book worse: it de-risks after selloffs, when forward returns are high
  (`research_implied_vol_risk.py`, `research_implied_vol_mu.py`).
- IC on next-day returns misses crash timing. Skipping the last month raised the trend
  alphas' equity IC (t 0.6 -> 2.8) and lowered the book (1.24 -> 1.18-1.23): the tilt pays
  in exits from fast selloffs, a few days the IC barely weights (`research_trend_skip.py`).
- But a regime gate would not have rescued it: a perfect hindsight crash gate adds +0.004
  (`research_crash_split.py`). An IC gain of ~0.015 on a tilt of k ~0.05 is worth a few bp a
  year. Refining existing tilts has run out; new Sharpe needs a return source of its own.
- Lagged market-state signals (credit, breadth, VIX exits) help in slow bear markets, which
  trend already covers, and hurt in fast V-shaped reversals.
