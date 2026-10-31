"""Implementation cost: US-listed ETFs against futures (and UCITS ETFs), for a $3M IBKR account.

The backtest holds 17 US-listed ETFs plus two overlays (0.5x TLT over month-end, 0.05x VIXY).
Its returns come from adjusted closes, so they are already net of each ETF's expense ratio,
but they are optimistic for this account in three ways: dividends are reinvested gross (no US
withholding for a non-US holder), borrowing costs T-bill + 0.5% (IBKR charges about +1% at
this size), and the overlays are funded at T-bill flat. This re-prices the saved book's own
daily weights under four implementations, for a Thai and a Singapore holder:

* ``ETF (backtest)``: as backtested; the reference.
* ``ETF (IBKR)``: the same ETFs, with IBKR's borrow spread on the whole book (overlays
  included) and US dividend withholding (Thailand 15% under its treaty; Singapore 30%, no
  treaty). Bond ETFs' interest-related dividends are taken as exempt.
* ``hybrid``: futures wherever a clean contract exists (IEF -> ZN, TLT -> ZB, QQQ -> NQ/MNQ,
  EFA/EEM -> ICE MSCI EAFE/EM, GLD -> GC/MGC, UUP -> DX, VIXY -> VX/VXM, the month-end TLT
  overlay -> ZB); the nine sector SPDRs and DBC stay US-listed ETFs. A futures position gives
  back the ETF's expense ratio and avoids withholding (index futures price dividends gross),
  and pays a financing spread over T-bills, rolls and its own trading cost. Cash not used by
  the ETF sleeve is held as T-bills, so nothing is borrowed while the ETF sleeve is below 1x.
* ``hybrid + UCITS``: as ``hybrid``, with the sectors in Irish-domiciled UCITS ETFs: a higher
  fee and wider spreads, but 15% withholding at fund level for any investor, and no US
  estate-tax exposure (see below).

Whole contracts at ``NAV``: each futures position is held at an integer number of contracts,
changed only when the target moves a whole contract away (no flip-flopping at x.5). Reported:
the tracking error that rounding adds and the extra trading.

Not modelled, and possibly larger than everything here: **US estate tax**. A non-resident
alien's US-situs assets above $60k (US-listed ETFs included) can be taxed at up to 40% on
death, and neither Thailand nor Singapore has an estate-tax treaty with the US. UCITS ETFs
are not US-situs; the situs of futures is generally treated as non-US but is not settled.
Also not modelled: DBC and UUP are publicly traded partnerships (K-1s; possible 10% withholding
on sale proceeds under section 1446(f) unless a qualified notice applies), futures/ETF basis
and duration mismatch (ZN is not IEF; size by DV01), and IBKR's cash-interest haircut (hold
T-bills directly). This is a cost model, not tax advice: check the tax lines with an adviser.

Every number in ``ASSUMPTIONS`` is an estimate to check (expense ratios and yields as of
2025-26, IBKR rates, futures spreads); the conclusions are only as good as they are.

**Result (2026-10-31): futures are worth ~50 bp a year, mostly from tax and borrowing, not
fees.** Against the backtest, the same ETFs at IBKR lose 41 bp/yr for a Thai holder (21 bp
withholding, 20 bp more borrowing) and 62 bp for a Singapore holder (42 bp withholding). The
hybrid gains back 21 bp of fees and 13 bp of borrowing (the ETF sleeve stays below 1x, so
nothing is borrowed), pays ~6 bp of futures carry and rolls, and nets +10 bp against the
backtest in Thailand (+51 over ETFs at IBKR) and -10 bp in Singapore (+52). UCITS sectors
cost ~8 bp more in fees and spreads; in Thailand they save no withholding (15% either way),
in Singapore they halve it, so hybrid + UCITS is best there (+2 against the backtest, +64
over ETFs). That is +0.05-0.06 Sharpe at the book's 10% vol. Whole contracts at $3M add
~40 bp/yr of tracking error (zero mean; ~0.1% of the book's variance), mostly from EFA and
EEM, whose positions are smaller than one MSCI contract, and slightly *reduce* trading.
For both countries the larger, unmodelled reason to leave US-listed ETFs is estate tax.
Next: check futures-against-ETF tracking on real futures data, and size Treasuries by DV01.

Usage:
    uv run python research/research_implementation_cost.py [--nav 3000000]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "tearsheet"))
import export  # noqa: E402  (the tear-sheet's run of the saved book)

from quantzy.config import TRADING_DAYS  # noqa: E402
from quantzy.data import CachedReader, YFinanceReader  # noqa: E402
from quantzy.strategies import core_multiasset as core  # noqa: E402
from quantzy.strategies import treasury_month_end, vix_hedge  # noqa: E402

NAV = 3_000_000.0
SECTORS = core.CLASSES["US sectors"]

ASSUMPTIONS = {
    # expense ratio, %/yr
    "er": {**{t: 0.08 for t in SECTORS}, "QQQ": 0.20, "EFA": 0.33, "EEM": 0.70, "IEF": 0.15,
           "TLT": 0.15, "GLD": 0.40, "DBC": 0.85, "UUP": 0.77, "VIXY": 0.85},
    # trailing dividend yield, %/yr, subject to US withholding (bond ETFs: interest, exempt)
    "div": {"XLB": 1.9, "XLE": 3.3, "XLF": 1.4, "XLI": 1.4, "XLK": 0.7, "XLP": 2.6, "XLU": 3.0,
            "XLV": 1.6, "XLY": 0.8, "QQQ": 0.6, "EFA": 3.0, "EEM": 2.4},
    "withholding": {"Thailand": 0.15, "Singapore": 0.30},  # on US-listed ETF dividends
    "ucits_withholding": 0.15,  # Irish fund on US dividends, any investor
    "ucits_er": 0.15,  # %/yr, sector UCITS ETFs
    "ucits_trade_bps": 4.0,  # one way, LSE/Xetra listings
    "borrow_spread_ibkr": 1.0,  # %/yr over T-bill, blended IBKR Pro tiers at ~$0.8M borrowed
    # futures: (contract, financing spread over T-bill %/yr, rolls/yr, roll bps, trade bps)
    "futures": {
        "IEF": ("ZN", 0.00, 4, 1.0, 0.9), "TLT": ("ZB", 0.00, 4, 1.0, 0.9),
        "QQQ": ("MNQ", 0.30, 4, 0.8, 0.6), "EFA": ("MFS", 0.30, 4, 1.5, 2.0),
        "EEM": ("MME", 0.30, 4, 1.5, 2.0), "GLD": ("MGC", 0.10, 6, 0.8, 0.6),
        "UUP": ("DX", 0.00, 4, 1.5, 1.5), "VIXY": ("VXM", 0.00, 0, 0.0, 3.0),
    },
}
#: ICE MSCI index level per ETF dollar, approximate (EFA ~ MXEA / 29.5, EEM ~ MXEF / 24.5).
MSCI_PER_ETF = {"EFA": 29.5, "EEM": 24.5}


def contract_notionals(prices: pd.DataFrame) -> dict[str, float]:
    """Dollar notional of one contract at the latest close (some levels approximate)."""
    r = CachedReader(YFinanceReader())
    last = {t: float(r.history([t], start="2026-09-01")[t].dropna().iloc[-1])
            for t in ("ZN=F", "ZB=F", "MNQ=F", "MGC=F", "DX-Y.NYB", "^VIX")}
    return {"IEF": 1000 * last["ZN=F"], "TLT": 1000 * last["ZB=F"], "QQQ": 2 * last["MNQ=F"],
            "GLD": 10 * last["MGC=F"], "UUP": 1000 * last["DX-Y.NYB"],
            "EFA": 50 * MSCI_PER_ETF["EFA"] * float(prices["EFA"].dropna().iloc[-1]),
            "EEM": 50 * MSCI_PER_ETF["EEM"] * float(prices["EEM"].dropna().iloc[-1]),
            "VIXY": 100 * last["^VIX"]}


def overlay_weights(bundle, idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Daily overlay holdings (fraction of NAV), ignoring VIXY's drift inside its band."""
    tlt = treasury_month_end.in_window(idx).astype(float) * treasury_month_end.SIZE
    on = vix_hedge.regime(vix_hedge._vix(bundle)).shift(1, fill_value=False)
    vixy = on.reindex(idx).fillna(False).astype(float) * vix_hedge.SIZE
    return pd.DataFrame({"TLT_ME": tlt, "VIXY": vixy}, index=idx)


def rounded(w: pd.Series, notional: float, nav: float) -> pd.Series:
    """Holdings in whole contracts, changed only when the target is a whole contract away."""
    target = (w * nav / notional).to_numpy()
    n, out = 0.0, np.zeros(len(target))
    for i, x in enumerate(target):
        if abs(x - n) >= 1.0 or (x == 0 and n != 0):
            n = float(np.round(x))
        out[i] = n
    return pd.Series(out * notional / nav, index=w.index)


def main(nav: float) -> None:
    a = ASSUMPTIONS
    bundle = core.load()
    r, w, _, _ = export.run("strategy")
    w = w.reindex(r.index).fillna(0.0)
    ov = overlay_weights(bundle, r.index)
    rets = bundle.prices.pct_change(fill_method=None).reindex(r.index).fillna(0.0)
    years = len(r) / TRADING_DAYS
    tbill_yr = float(rets[core.CASH].mean() * TRADING_DAYS)

    # Positions by instrument, overlays included (month-end TLT is separate: own futures leg).
    pos = w[core.RISKY].copy()
    pos["VIXY"] = ov["VIXY"]
    pos["TLT_ME"] = ov["TLT_ME"]
    turn = pos.diff().abs().fillna(0.0).sum() / years  # one-way, x NAV per year
    avg = pos.mean()
    trade_bps = {**core.EXECUTION_COST_BPS, "VIXY": vix_hedge.COST_BPS,
                 "TLT_ME": treasury_month_end.COST_BPS}

    def wht(t: str, rate: float) -> float:  # bp/yr of NAV
        return avg.get(t, 0) * a["div"].get(t, 0) * rate * 100

    risky_etf = pos[core.RISKY].sum(axis=1)
    rows: dict[str, dict[str, float]] = {}
    for country, rate in a["withholding"].items():
        # ETF (IBKR): borrowing on everything above 1x NAV, at the realistic spread.
        gross_all = risky_etf + pos["VIXY"] + pos["TLT_ME"]
        borrowed_bt = (risky_etf - 1).clip(lower=0).mean()
        borrowed_all = (gross_all - 1).clip(lower=0).mean()
        borrow_ibkr = borrowed_all * a["borrow_spread_ibkr"] * 100 - borrowed_bt * 50
        wht_etf = sum(wht(t, rate) for t in a["div"])
        rows[f"ETF (IBKR), {country}"] = {
            "fees": 0.0, "withholding": -wht_etf, "borrow": -borrow_ibkr,
            "futures carry+roll": 0.0, "trading": 0.0}

        for ucits in (False, True):
            keep = [*SECTORS, "DBC"]
            etf_sleeve = pos[keep].sum(axis=1)
            borrowed_h = (etf_sleeve - 1).clip(lower=0).mean()
            fees = carry = trading = 0.0
            for t, (_, spread, rolls, roll_bps, fut_bps) in a["futures"].items():
                legs = [t, "TLT_ME"] if t == "TLT" else [t]
                for leg in legs:
                    fees += avg[leg] * a["er"][t] * 100 if leg != "TLT_ME" else 0.0
                    carry -= avg[leg] * (spread * 100 + rolls * roll_bps)
                    trading += turn[leg] * (trade_bps[leg] - fut_bps)
            withholding = -sum(wht(t, rate) for t in a["div"] if t in SECTORS)
            if ucits:
                withholding = -sum(wht(t, a["ucits_withholding"]) for t in SECTORS)
                fees -= avg[SECTORS].sum() * (a["ucits_er"] - a["er"]["XLK"]) * 100
                trading -= turn[SECTORS].sum() * (a["ucits_trade_bps"] - 2.0)
            rows[f"hybrid{' + UCITS' if ucits else ''}, {country}"] = {
                "fees": fees, "withholding": withholding,
                "borrow": borrowed_bt * 50 - borrowed_h * a["borrow_spread_ibkr"] * 100,
                "futures carry+roll": carry, "trading": trading}

    t = pd.DataFrame(rows).T
    t["net vs backtest"] = t.sum(axis=1)
    pd.set_option("display.width", 220)
    print(f"saved book 2008-09 .. {r.index[-1].date()}, NAV ${nav:,.0f}; T-bill averaged "
          f"{tbill_yr:.2%}; bp/yr of NAV, + = better than the backtest")
    print("\naverage holdings (x NAV) and one-way turnover a year (x NAV)")
    print(pd.DataFrame({"avg": avg, "turnover": turn}).round(3).T.to_string())
    print("\n1. COST AGAINST THE BACKTEST (bp/yr)")
    print(t.round(1).to_string())
    for c in a["withholding"]:
        e, h, u = (t.loc[f"{k}, {c}", "net vs backtest"]
                   for k in ("ETF (IBKR)", "hybrid", "hybrid + UCITS"))
        print(f"   {c}: hybrid saves {h - e:+.0f} bp/yr over ETFs at IBKR; "
              f"hybrid + UCITS {u - e:+.0f}")

    notional = contract_notionals(bundle.prices)
    print(f"\n2. WHOLE CONTRACTS at ${nav:,.0f} (notional per contract, % of NAV)")
    out = {}
    te = pd.Series(0.0, index=r.index)
    for t, (contract, *_rest) in a["futures"].items():
        legs = ["TLT", "TLT_ME"] if t == "TLT" else [t]
        tgt = pos[legs].sum(axis=1)
        held = rounded(tgt, notional[t], nav)
        asset_ret = (bundle[vix_hedge.PANEL].iloc[:, 0].pct_change(fill_method=None)
                     .reindex(r.index).fillna(0.0) if t == "VIXY" else rets[t])
        err = (held - tgt) * asset_ret
        te = te + err
        out[f"{t} -> {contract}"] = {
            "contract % NAV": notional[t] / nav * 100,
            "avg contracts": float((held.abs() * nav / notional[t]).mean()),
            "TE bp/yr": float(err.std() * np.sqrt(TRADING_DAYS) * 1e4),
            "extra turnover x/yr": float((held.diff().abs().sum() - tgt.diff().abs().sum())
                                         / years),
        }
    print(pd.DataFrame(out).T.round(2).to_string())
    print(f"   total rounding tracking error {te.std() * np.sqrt(TRADING_DAYS) * 1e4:.0f} bp/yr "
          f"(against book vol ~{r.std() * np.sqrt(TRADING_DAYS):.1%})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nav", type=float, default=NAV)
    main(ap.parse_args().nav)
