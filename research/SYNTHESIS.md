# What 80 studies say (2026-11-02)

A step back over every study in [README.md](README.md), the ones adopted and the ones not,
asking whether good ideas were lost to the framework rather than to the evidence. Open
follow-ups are ideas 19-21 in [IDEAS.md](IDEAS.md).

## 1. What worked, and what it shares

| Adopted | Kind |
|---|---|
| Risk weighting across classes (most of the Sharpe; the core is 0.80 correlated with the prior's own book) | Core construction |
| Trend (EMA-200, EWMAC), VRP, dollar value, Treasury carry prior | **Slow** tilts inside the optimizer |
| Treasury month-end, pension rebalancing, Treasury auction concession | **Overlays outside the optimizer** |
| VIX hedge | Insurance overlay |

Since the first week every gain has come from an overlay: its own size, its own timing, its
own trading, never competing inside the optimizer. The book went from 1.27 to 1.44 that way.

## 2. Strong signals left unused

| Signal | Strength | Why it was dropped | The signal or the framework? |
|---|---|---|---|
| Relative sector momentum (`research_sector_priors`, `research_long_short`) | 1-day IC t 4.7, the strongest found | its edge lasts days (21-day t 0.9); the book trades ~once a year (cost band 5 bp x 48) | framework: horizon |
| Country 21-day reversal, long/short (`research_short_reversal`, `research_reversal_sleeves`) | net Sharpe 0.34, corr +0.04 | weak since 2017; shorting ~8 thin country ETFs | both |
| Dividend-calendar run-up (`research_dividend_calendar`) | pooled t 2.2, 12 of 16 markets | 1.8x monthly turnover in country ETFs took the edge | framework: execution |
| Halloween across countries (`research_halloween`, `research_seasonal_prior`) | 27 of 28 markets, +134 bp a month | the book is US equity, where it has faded; the caps absorbed the summer cut | framework: vehicle |
| Equity 5-day reversal (`research_short_reversal`, `research_bounce_filtered`) | t 2.6-2.8 | as an overlay it is more equity beta | needs a hedge |
| Trend skip on equities (`research_trend_skip`) | IC t 0.6 -> 2.8 | later exits in fast crashes | a real tradeoff |

## 3. Four framework choices that block good ideas

1. **One optimizer for everything.** Every alpha must fit a long-only, 1.3x-capped,
   slow-trading, prior-dominated mean-variance book. It cannot use, by construction: fast
   signals (the cost band filters them), relative or short signals (half their information
   is the short side), or new assets without a long premium (they barely enter;
   `research_universe_broad`, `research_class_prior`). Overlays escaped this. The standard
   structure is a core book plus independent sleeves, each with its own horizon, cost model
   and risk budget, combined by risk budget at the top; the overlays are that, built piecemeal.
2. **Every idea was judged alone, paying full costs.** Seasonality, the dividend calendar,
   country reversal and relative momentum each failed as a standalone sleeve. Combined in one
   cross-sectional sleeve, independent signals add (IR ~ sqrt(sum IR_i^2)) and their trades
   partly net, so costs grow less than linearly. Multi-factor managers work this way; it was
   never tried here.
3. **Long/short sleeves were executed naively.** The short leg was an equal-weight basket of
   thin ETFs (15-20 bp a trade, 1% borrow) rather than one liquid hedge (EFA / EEM / ACWI, or
   futures); rebalances were full, with no buffer or turnover penalty; financing assumed ETF
   margin. That may be enough to turn the dividend-calendar and breadth sleeves' positive
   gross edges negative net (`research_breadth_sleeve`: ~1.5% a year of drag).
4. **The screening horizon did not match the holding horizon.** A 1-day IC misled both ways:
   the equity VRP's is ~0 since 2017 yet it added +0.048 Sharpe there (its edge is weeks);
   the trend skip's improved while the book got worse (`research_vrp_review`,
   `research_trend_skip`, `research_crash_split`).

Also standing: covariance changes do not help this universe (five studies; its ill-conditioning
is overlap, not noise); refining existing small tilts is worth a few bp; published calendar
effects decay (turn of month, pre-FOMC, options expiration, Halloween in the US, FX hedges).

## 4. Breadth and the small universe

The universe is small not for want of inefficiency but because a single long-only optimizer
rewards only assets with a *long premium* not already spanned. More assets add independent
bets only inside sleeves that can short and trade at their own horizon. The large pool of
inefficiency is single stocks (thousands of names, strong cross-sectional effects): a
different project, with point-in-time fundamentals. ETFs cap the breadth this repo can reach.

## 5. Next, in order

1. **A multi-sleeve harness** (idea 19): combine the core, the overlays and N sleeves by risk
   budget, each sleeve with its own cost model, turnover control and hedge leg. Everything
   below depends on it.
2. **A combined cross-sectional sleeve** (idea 20): relative momentum (sectors, ~5-day hold),
   country 21-day reversal, dividend calendar and country Halloween timing at equal risk,
   index-hedged, buffered. One pre-registered candidate with fixed weights and a strict bar,
   since each signal has been seen.
3. **Relative sector momentum as a fast sleeve** (idea 21): does the strongest IC found
   survive realistic costs at its own horizon? A quick probe.
4. **Futures data** (ideas 1, 7, 16): cheaper long/short legs, commodity carry and the roll.
