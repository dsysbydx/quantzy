# Ideas backlog

Ideas not yet tested, or tested only in part, with the reasoning behind them. When one is
run it becomes a study in [README.md](README.md) and leaves this file (its row's commit
links the two). Order within a section is priority. Designs here are drafts: the version
fixed before a test goes in that study's docstring.

What the book is now (2026-11-01): long-only mean-variance on 17 ETFs, Sharpe 1.44 (train
1.41 / test 1.47) with its overlays. Its Sharpe comes mostly from risk weighting across
classes: the core is 0.80 correlated with the prior's own portfolio. On top of that sit trend
(EMA-200 and EWMAC, 0.74-0.80 correlated, so one bet), VRP and dollar value (each 0.1-0.26
against everything), and four overlay rules: Treasury month-end (about 0 against
everything), pension rebalancing (+0.04 against the book), the Treasury auction shorts
(-0.11) and the VIX hedge (-0.44 against the book).

## Next up

Ranked 2026-11-01 by expected gain x odds x cost, after this run's lessons: a new mechanism
or a real cost saving has paid; refining the existing tilts, more covariance work, new
long-only assets and trend breadth have not. Numbers are stable ids, not ranks.

| Rank | # | Idea | Why this rank |
|---|---|---|---|
| 1 | 1 | Futures hybrid, step 2 | Cost, not alpha: ~50 bp/yr, ~+0.05 Sharpe, high odds. Blocked on the data choice (IBKR ~2y, or Databento 15y+). Also unlocks sizing the month-end overlays on margin. |
| — | 14 | ~~FX month-end hedge rebalancing~~ (closed: wrong sign at daily closes) | New flow, the pension overlay's family: foreign holders reset dollar hedges at the month-end fix, sized by the month's US vs foreign equity move (Melvin and Prins 2015); free data. Must add beyond pension. |
| 3 | 13 | Forced-flow effects (auctions, reconstitution) | New. The only mechanism type that has paid lately; free daily data; one study each. |
| 4 | 11 | Forward valuation tilt | Different information (analysts' expectations, not price); a coarse free test on Damodaran's annual data (~1 day of mapping). Modest prior: price-based value failed and sector priors are sensitive. |
| 5 | 15 | Options-expiration week | Dealer gamma hedging around the third Friday; free daily data, untested here. Medium. |
| 6 | 7 | Commodity carry, long/short | A different, well-documented premium, independent of the book. Needs futures curves: buy with #1's data. |
| 7 | 16 | Commodity index roll ("Goldman roll") | Index funds roll on business days 5-9 each month; front-run the calendar spread (Mou 2011). Needs front and second-month futures: buy with #1 / #7's data. |
| 8 | 6 | Cross-sectional seasonality | A calendar mechanism, free, independent of momentum in the literature. Low-medium odds. |
| 9 | 5 | Relative momentum, long/short | Weaker now: trend's breadth paid ~0 since 2017 (#12) and relative momentum failed inside the book. |
| 10 | 12 | Breadth trend sleeve, step 2 (futures) | Step 1 failed (Sharpe 0.13, ~0 since 2017). Only alongside #1 and #7 on the same data. |
| 11 | 9 | Tail hedge on real options | Insurance, not alpha; paid options data. |
| 12 | 10 | Low-beta vs high-beta sectors | Low: overlaps the book's risk weighting. |

### 1. Futures hybrid, step 2: tracking on real futures data
- **Why:** `research_implementation_cost.py` (branch `implementation-cost`) puts a futures
  hybrid at ~50 bp/yr over US ETFs at IBKR for a Thai or Singapore holder (+0.05 Sharpe),
  from withholding, borrowing and fees; a cost, not an alpha, so the odds are high.
- **Draft:** per contract, excess return over T-bills against the ETF's: tracking error,
  implied financing spread, DV01 ratio for ZN/IEF and ZB/TLT. Then re-run step 1 with the
  measured spreads.
- **Needs:** data. IBKR API (~2 years; exchange subscriptions needed to trade anyway) or
  Databento (15+ years of CME, CBOT, COMEX; free credit likely covers it).

### 13. Other forced-flow effects: Treasury auction cycle, index reconstitution
- **Why:** the one lasting gain in the 2026-10/11 run came from a new *mechanism*, a
  forced flow (pension rebalancing), on top of the earlier one (Treasury month-end). Others
  are documented and run on free daily data. The Treasury auction cycle: dealers absorb new
  supply, so Treasuries cheapen in the days before an auction and recover after (Lou, Yan
  and Zhang, 2013); auction dates are public (TreasuryDirect). Russell reconstitution: index
  funds trade on the last Friday of June, with predictable pressure on small caps (IWM).
- **Draft:** event windows around 10- and 30-year auctions in IEF / TLT (short before, long
  after, or flat-then-long), and around reconstitution day in IWM; overlay-style, like the
  month-end overlays; effect by period first (publication decay), then the book.
- **Watch:** decay since publication; overlap with the month-end windows (auctions cluster
  mid-month, so probably little).
- **Auction cycle tested 2026-11-01** (`research_auction_cycle.py`): fails. The recovery
  after auctions is not reliable; the book falls to 1.361 at much higher vol. The
  *concession before* auctions is significant since 2017 (IEF t -2.8, TLT t -2.4).
- **Short-only follow-up passes** (`research_auction_preshort.py`): book 1.371 -> 1.489, overlay
  t 2.02, timing confirmed by a placebo; caveats: wrong sign in 2002-08, post-hoc, t at the
  bar. **Adopted 2026-11-01** as `treasury_auction_10y` / `_30y` at 0.125x (book 1.37 -> 1.44).
- **Still open:** Russell reconstitution (IWM, last Friday of June): ~25 events since 2000,
  low power.

### 14. FX month-end hedge rebalancing -- closed 2026-11-01
- `research_fx_hedge_rebalance.py`: fails. At daily closes the dollar *strengthened* into
  month-end after US equities led, before 2017 (t +1.6, +1.4); the overlay lost (t -2.1).
  The effect lives in minutes around the 4 pm fix, which daily data cannot isolate.

### 15. Options-expiration week
- **Why:** dealers hedge their option books, and around the monthly expiry (third Friday)
  that hedging has been linked to positive equity returns in expiration week and weaker ones
  the week after. Free daily data; the dates follow a rule.
- **Draft:** SPY excess return in expiration week against other weeks, by period; an overlay
  long in expiration week if it holds.

### 16. Commodity index roll front-running ("Goldman roll")
- **Why:** the large commodity indexes roll from the front contract to the next on business
  days 5-9 each month, a predictable flow that cheapens the front against the next (Mou
  2011). Sell the spread ahead of the roll window.
- **Needs:** front- and second-month futures prices (with ideas 1 and 7's data).

### Lower priority flow ideas (2026-11-01)
- Leveraged-ETF rebalancing into the close (needs intraday data; the daily reversal is the
  known short-term reversal); vol-target and risk-parity deleveraging after vol spikes (hard
  to separate from crash and rebound dynamics); turn of the year in small caps (decayed).

### 11. Forward valuation tilt across sectors and regions (needs data)
- **Why (the user's idea, 2026-11-01):** tilt toward cheap expected earnings with limited
  downside, the way a fundamental investor does: forward earnings yield (1 / forward P/E)
  and PEG, relative across the sectors and between US, developed ex-US (EFA) and emerging
  (EEM). Forward measures use analysts' expectations, unlike what failed before.
- **Prior evidence:** price-based value lowered Sharpe (`research_value`: CAPE ERP, sector
  and regional reversal); CAPE forecasts only at 5-10 years (`research_shiller_valuation`);
  the implied ERP lowered the book (`research_implied_erp`). And the sector priors are
  sensitive: random errors of 0.10 cost a median -0.21 (`research_prior_sensitivity`), so a
  noisy tilt does real damage. Modest prior.
- **Data, point in time (the binding constraint):** Damodaran's annual industry and regional
  datasets (free; forward P/E, PEG, margins; each January; US from the late 1990s, global
  regions from ~2007) -- the first test, annual tilt. Yardeni's weekly sector and MSCI-country
  forward P/E (free, PDFs to scrape). Proper point-in-time I/B/E/S or MSCI (paid).
- **Data check (2026-11-01):** Damodaran's archive (`pc/archives/pedataYY.xls`, US;
  `peEuropeYY.xls` etc., regions) has a forward P/E column for the US every January
  2004-2025 (22 snapshots; PEG 1999 and 2012-2025) and for Europe, Japan, emerging and global
  2012-2025 (14). About 95-101 Damodaran industries, renamed around 2014, to map by hand
  onto the nine SPDR sectors; its forward P/E is a simple average across firms (outliers:
  Advertising 2025 73.7 against an aggregate P/E of 34.7), so a sector P/E needs industry
  market caps from another of his files. Roughly point in time (published early January).
  Enough for a coarse, low-power first test (~21 annual sector cross-sections); only a
  clear result would justify Yardeni's weekly series or paid I/B/E/S.
- **Draft:** sector prior ``SR_i = 0.30 + k * z_i``, ``z`` the cross-sectional z-score of
  forward earnings yield relative to each sector's own trailing 10-year mean (cheap against
  its own history, not just against peers), capped +/-2; PEG and a downside penalty (downside
  beta, or analysts' earnings dispersion) as variants. IC on 12-month sector excess returns
  first, then the book at a ``k`` set from the IC (Grinold), ``PORTFOLIO`` fixed.
- **Watch:** analyst optimism differs by sector; GICS changes (2018) shift sector P/Es with
  no change in value; restated or back-filled estimates leak the future.

### 7. Commodity carry (futures-curve basis), long/short
- **Why:** strong evidence (Gorton-Rouwenhorst; Koijen et al. 2018) and independent of the
  book; our commodity tests used ETFs and trend, not carry.
- **Needs:** futures curves. Pairs with buying futures data for idea 1.

### 6. Cross-sectional seasonality
- **Why:** sectors and countries that did well in a calendar month tend to repeat in that
  month (Keloharju, Linnainmaa and Nyberg 2016); documented as independent of momentum.
- **Draft:** on the universe of idea 5, rank by the average return in the same calendar month
  over the past 10-20 years; long/short, beta-neutral, 5% vol. Nearly free once 5 is built.

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

### 12. Breadth trend sleeve, long/short (the user's push for breadth, 2026-11-01)
- **Why:** IR ~ IC x sqrt(breadth) applies to a strategy whose risk the signal drives, not to
  a small tilt (k ~0.05) in a long-only, prior-dominated book; adding assets there barely
  moved it (`research_trend_global`, `research_broad_universe`). The literature's breadth is
  long/short trend across many *uncorrelated* markets at equal risk (Moskowitz, Ooi and
  Pedersen 2012; Hurst, Ooi and Pedersen 2017) -- a sleeve, combined at the book level.
  Expect a modest post-2009 Sharpe (trend's lost decade; `research_kmlm`: KMLM earned ~0
  outside 2022, yet +0.03 to the book at a fixed 20% share).
- **Step 1 (free):** ~40 ETFs across country equity, bonds, commodities, currencies and
  real estate; EMA-200 and EWMAC 64/256 as in the book; vol-targeted per market, equal risk
  per class, long/short, weekly; costs, short borrow, financing. Standalone Sharpe by period,
  correlation with the book (also in its worst months), and the book plus the sleeve at
  fixed risk shares.
- **Step 1 done 2026-11-01** (`research_breadth_sleeve.py`): fails. Sharpe 0.13 since 2008-09
  (-0.06 since 2017), net long and +0.32 correlated with the book, lowers it at every share;
  ~1.5%/yr of ETF costs and financing. Trend's IC across these markets is ~0 since 2017.
- **Step 2 (data):** the same on ~60-100 futures markets back to 1980 (Norgate, ~$30/month).
  Futures cut most of the drag and add pre-2009 history, but the recent decade is the
  question, and step 1 says it is ~0; worth running only alongside ideas 1 and 7, on the
  same data, not on its own.

### 9. Tail hedge on real option prices
- **Why:** `research_tail_hedge.py` priced SPX puts off the VIX and found the regime rule
  within pricing error; real end-of-day option prices would settle it.
- **Needs:** Cboe DataShop or ORATS.

### 10. Low-beta versus high-beta sectors, beta-neutral
- **Why:** betting against beta (Frazzini-Pedersen 2014). **Doubt:** it partly overlaps the
  risk weighting the book already does.

## Tested and closed (2026-10/11)

### 2. Implied vol in the risk model -- closed 2026-10-31
- Sigma only (`research_implied_vol_risk.py`): better forecasts, worse book (1.16 vs 1.21).
  Sigma and `mu` consistently (`research_implied_vol_mu.py`): 1.15. Implied-vol sizing cuts
  exposure after selloffs, selling the premium the `vrp` alpha buys.

### 3. VRP on more assets -- closed 2026-10-31
- `research_vrp_multi.py`: Treasuries (MOVE), gold (GVZ) and oil (OVX) have no usable IC
  (gold the wrong sign); the book falls to 1.16 against 1.21. VXEEM has no history on Yahoo.

### 4. More calendar flows, and overlay sizing -- tested 2026-10-31
- `research_month_end_flows.py`: IEF month-end passes but duplicates the TLT overlay
  (corr 0.92); LQD fails the test half; quarter-ends are not special. Pension rebalancing:
  the sign rule's gain needs the same-close signal (gone one day earlier), but the
  month-to-date deviation predicts the window (t -4.0, every period).
- Pension by magnitude (`research_month_end_followups.py`): passes (1.396 vs 1.340, overlay
  t 2.96, robust to the signal's timing). **Adopted 2026-10-31** as the `pension_rebalance`
  overlay at 0.25x (half the tested size: lumpy, fading), shorting SPY in the ETF account;
  book 1.34 -> 1.37.
- TLT / IEF split at the same risk: +0.003, no change; revisit IEF with futures.
- Still open: the TLT overlay's size once futures margin replaces Reg T.

### 8. Universe: drop QQQ, or complete the sector set -- closed 2026-11-01
- `research_universe_qqq.py`: no change. Without QQQ 1.208 vs 1.211; with XLC and XLRE in
  2019-26, 1.062 vs 1.078. QQQ stays (free, and the MNQ futures leg).

## Needs data first

- **Put/call ratio** (a Fear & Greed component). Check Cboe data access first; the other two
  F&G parts we tested (credit, breadth) did not add to trend and VRP.
- Ideas 1, 7, 9 and 12 (step 2) above also need data; 11 needs a day of data work.

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

- **Treasury auction overlays review** (adopted 2026-11-01 at 0.125x, half the tested size):
  revisit the size from live Sharpe; retire if the trailing three years turn negative or if
  the tentative schedule stops being published. Refresh: the job refetches it weekly.
- **Pension rebalancing review** (adopted 2026-10-31 at 0.25x): its Sharpe has fallen by
  period (0.91 / 0.77 / 0.35) since publication; retire it if the trailing three years turn
  negative.
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
- The 1-day IC can fail both ways: the equity VRP's is ~0 since 2017, yet it added +0.048
  Sharpe (t 2.1) there; its edge is at weeks. Judge a tilt on the horizon it is held for
  and on the book (`research_vrp_review.py`).
- No crowd pushes through the 200-day line: SMA crosses follow big moves and reverse, and
  the drop-off "peek" is ~0.1% a day (`research_sma_frontrun.py`).
- Per-asset Sharpe priors make diversification look like alpha: an index (SPY, ACWI) is never
  held next to its constituents, QQQ only ~3%, and the book leans to the sectors
  (`research_universe_broad.py`). An equilibrium class prior fixes that but not the book's
  concentration (eff N 4.3 either way) and loses in the test half (`research_class_prior.py`).
- Lagged market-state signals (credit, breadth, VIX exits) help in slow bear markets, which
  trend already covers, and hurt in fast V-shaped reversals.
