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

### 1. Sector and country relative momentum, long/short
- **Why:** relative sector momentum had the strongest IC we found (t 4.8) but could not be
  used long-only; the gain is in shorting the losers. Industry and country momentum is
  well documented (Moskowitz-Grinblatt 1999; Asness, Moskowitz and Pedersen 2013) and,
  beta-hedged, should be close to independent of the book.
- **Draft:** 12-1 month return (also 6-1), ranked within sectors (9 SPDRs) and within ~15
  country ETFs; long top third, short bottom third, beta-neutral to SPY on 1-year betas,
  5% vol, monthly; 2 bp a trade plus 0.5%/yr borrow on shorts. Long check: 12 Fama-French
  industries from 1927.
- **Watch:** momentum crashes (2009, Nov 2020); overlap with the trend portfolio.
- **Bar:** sleeve Sharpe > 0.3 since 2008; +0.02 to the book's full and test-half Sharpe.

### 2. Cross-sectional seasonality
- **Why:** sectors and countries that did well in a calendar month tend to repeat in that
  month (Keloharju, Linnainmaa and Nyberg 2016); documented as independent of momentum.
- **Draft:** on the universe of idea 1, rank by the average return in the same calendar month
  over the past 10-20 years; long/short, beta-neutral, 5% vol. Nearly free once 1 is built.

### 3. Low-beta versus high-beta sectors, beta-neutral
- **Why:** betting against beta (Frazzini-Pedersen 2014). **Doubt:** it partly overlaps the
  risk weighting the book already does.

## Needs data first

- **Commodity carry (futures-curve basis), long/short.** Strong evidence (Gorton-Rouwenhorst;
  Koijen et al. 2018) and independent, but needs futures curves.
- **Tail hedge on real option prices.** `research_tail_hedge.py` priced SPX puts off the VIX
  and found the regime rule within pricing error; real end-of-day option prices (Cboe
  DataShop, ORATS) would settle it.
- **Put/call ratio** (a Fear & Greed component). Check Cboe data access first; the other two
  F&G parts we tested (credit, breadth) did not add to trend and VRP.

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
- Lagged market-state signals (credit, breadth, VIX exits) help in slow bear markets, which
  trend already covers, and hurt in fast V-shaped reversals.
