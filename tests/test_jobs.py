"""The daily job: decide the target book from the current one and turn it into orders."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.jobs.daily import latest_complete, read_holdings, run
from quantzy.strategies import core_multiasset as core


@pytest.fixture
def bundle(core_bundle):
    return core_bundle


def test_from_cash_buys_to_target(bundle):
    book, summary = run("core_multiasset", None, 3_000_000.0, bundle)
    assert summary["status"].startswith("optimal")
    assert book.loc[core.CASH, "current"] == pytest.approx(1.0)
    orders = book[book["order"]]
    assert len(orders) and (orders["trade_dollars"] > 0).all()
    risky = book.drop(index=core.CASH)
    assert risky["target"].abs().sum() == pytest.approx(summary["gross_target"])
    # the core is funded to its full size at once (no band from cash); overlays sit outside
    # its leverage cap
    assert risky.loc[core.RISKY, "target"].sum() == pytest.approx(
        core.PORTFOLIO.max_leverage, abs=1e-4)


def test_reports_model_and_stress_vol(bundle):
    _, summary = run("core_multiasset", None, 3_000_000.0, bundle)
    assert 0 < summary["model_vol"] < 1 and 0 < summary["stress_vol"] < 1


def test_reports_margin_and_stress(bundle):
    _, summary = run("core_multiasset", None, 3_000_000.0, bundle)
    m = summary["margin"]
    assert m["gross"] == pytest.approx(summary["gross_target"])
    assert 0 < m["maintenance"] < m["cushion"] < 1 and m["drop_to_call"] > 0
    # the fixture covers 2015-2020: only the 2020 week is replayed, none it does not reach
    assert set(summary["stress"]) == {"2020-03 crash week"}


def test_overlays_shrink_to_keep_gross_under_the_initial_limit(bundle, monkeypatch):
    from quantzy.jobs import margin as mg
    from quantzy.strategies import treasury_month_end as tme

    monkeypatch.setattr(tme, "target_after_close", lambda day, bundle=None, current=0.0: 1.2)
    book, summary = run("core_multiasset", None, 3_000_000.0, bundle)
    assert summary["overlay_scale"] < 1
    assert summary["gross_target"] <= mg.REG_T.max_gross - mg.GROSS_BUFFER + 1e-6
    scaled = 1.2 * summary["overlay_scale"]
    assert summary["overlays"]["treasury_month_end"] == pytest.approx(scaled)
    assert any("scaled" in a for a in summary["margin_alerts"])


def test_holding_the_target_trades_little(bundle):
    first, _ = run("core_multiasset", None, 3_000_000.0, bundle)
    held = first["target"].drop(core.CASH)
    again, summary = run("core_multiasset", held, 3_000_000.0, bundle)
    assert summary["turnover"] < 0.01
    assert again["order"].sum() <= 2


def test_trailing_incomplete_rows_are_dropped(bundle):
    px = bundle.prices.copy()
    extra = px.index[-1] + pd.offsets.BDay(1)
    px.loc[extra] = np.nan
    px.loc[extra, core.CASH] = px[core.CASH].iloc[-1]
    b = bundle.__class__.build(px, **{k: bundle[k].reindex(px.index) for k in
                                      ("vix", "market", "carry")})
    assert latest_complete(b, core.RISKY).index[-1] == bundle.index[-1]


def test_holdings_file_by_shares(tmp_path):
    prices = pd.Series({"XLK": 200.0, "GLD": 300.0})
    f = tmp_path / "pos.csv"
    f.write_text("ticker,shares\nxlk,100\nGLD,50\n")
    w = read_holdings(f, prices, 100_000.0)
    assert w["XLK"] == pytest.approx(0.2) and w["GLD"] == pytest.approx(0.15)


def test_unknown_ticker_is_rejected(bundle):
    with pytest.raises(ValueError, match="outside the strategy"):
        run("core_multiasset", pd.Series({"TSLA": 0.1}), 1e6, bundle)


class TestTreasuryMonthEnd:
    def test_window_is_last_two_trading_days(self):
        from quantzy.strategies import treasury_month_end as tme

        # October 2026: Thu 29 and Fri 30 are the last two trading days
        assert tme.target_after_close("2026-10-27") == 0.0  # next day Wed 28: outside
        assert tme.target_after_close("2026-10-28") == tme.SIZE  # holds through Thu 29
        assert tme.target_after_close("2026-10-29") == tme.SIZE  # holds through Fri 30
        assert tme.target_after_close("2026-10-30") == 0.0  # next day Mon Nov 2
        # Christmas Friday 2026: the last two trading days of December are 30 and 31
        assert tme.target_after_close("2026-12-29") == tme.SIZE
        assert tme.target_after_close("2026-12-28") == 0.0

    def test_nyse_holidays(self):
        from quantzy.strategies.treasury_month_end import trading_days

        days = trading_days("2026-01-01", "2026-12-31")
        for h in ("2026-01-01", "2026-04-03", "2026-06-19", "2026-07-03", "2026-11-26",
                  "2026-12-25"):
            assert pd.Timestamp(h) not in days
        assert len(days) == 251

    def test_excess_returns_hold_size_in_window(self):
        from quantzy.strategies import treasury_month_end as tme

        idx = pd.bdate_range("2024-01-01", "2024-03-31")
        r = pd.Series(0.001, index=idx)
        ex = tme.excess_returns(r, pd.Series(0.0, index=idx))
        inside = ex[ex > 0]  # the day after a window carries only the exit cost
        assert len(inside) == 3 * tme.DAYS
        assert inside.max() == pytest.approx(tme.SIZE * 0.001)
        assert np.allclose(ex[ex < 0], -tme.SIZE * tme.COST_BPS / 1e4)

    def test_window_skips_an_incomplete_last_month(self):
        """Data ending mid-month must not mark its latest days as the month's last days."""
        from quantzy.strategies import treasury_month_end as tme

        idx = pd.bdate_range("2026-10-01", "2026-11-05")
        w = tme.in_window(idx)
        assert not w.loc["2026-11"].any()
        assert list(w[w].index.strftime("%Y-%m-%d")) == ["2026-10-29", "2026-10-30"]

    def test_job_adds_the_overlay(self, bundle):
        book, summary = run("core_multiasset", None, 3_000_000.0, bundle)
        assert set(summary["overlays"]) == {"treasury_month_end", "vix_hedge",
                                            "pension_rebalance", "treasury_auction_10y",
                                            "treasury_auction_30y"}


def _vix_bundle(vix: list[float], vixy_ret: float = 0.0):
    from quantzy.data.bundle import DataBundle

    idx = pd.bdate_range("2024-01-01", periods=len(vix))
    px = pd.DataFrame({"TBILL": 1.0}, index=idx)
    v = pd.DataFrame({"TBILL": vix}, index=idx, dtype=float)
    vixy = pd.DataFrame({"TBILL": 20 * (1 + vixy_ret) ** np.arange(len(vix))}, index=idx)
    return DataBundle.build(px, vix=v, vixy=vixy)


class TestVixHedge:
    def test_regime_switches_with_hysteresis(self):
        from quantzy.strategies import vix_hedge as vh

        on = vh.regime(pd.Series([20, 12, 20, 29, 31, 20, 14, 13.0]))
        assert on.tolist() == [False, True, True, True, False, False, False, True]

    def test_band_keeps_a_drifted_holding(self):
        from quantzy.strategies import vix_hedge as vh

        assert vh.rebalance(vh.SIZE, vh.SIZE * 1.2) == pytest.approx(vh.SIZE * 1.2)
        assert vh.rebalance(vh.SIZE, vh.SIZE * 1.3) == vh.SIZE
        assert vh.rebalance(vh.SIZE, 0.0) == vh.SIZE
        assert vh.rebalance(0.0, vh.SIZE) == 0.0

    def test_target_uses_the_vix_before_the_day(self):
        from quantzy.strategies import vix_hedge as vh

        b = _vix_bundle([20, 20, 12, 20])
        d = b.index
        assert vh.target_after_close(d[2], b) == 0.0  # the VIX through d[1] is 20
        assert vh.target_after_close(d[3], b) == vh.SIZE  # through d[2]: 12, on
        assert vh.target_after_close(d[3] + pd.Timedelta(days=1), b, 0.055) == 0.055

    def test_backtest_earns_two_days_after_the_signal(self):
        from quantzy.strategies import vix_hedge as vh

        r = vh.backtest_returns(_vix_bundle([20, 20, 12, 20, 20, 20], vixy_ret=0.01))
        # VIX 12 at close 2 -> bought at close 3 (cost) -> first return on day 4
        assert (r.iloc[:3] == 0).all()
        assert r.iloc[3] == pytest.approx(-vh.SIZE * vh.COST_BPS / 1e4)
        assert r.iloc[4] == pytest.approx(vh.SIZE * 0.01)

    def test_exposure_part_scales_with_equity_and_stops_at_high_vix(self):
        from quantzy.strategies import vix_hedge as vh

        assert vh.target(False, 20.0, 0.70) == 0.0  # below the 75% floor
        assert vh.target(False, 20.0, 1.25) == pytest.approx(0.05)
        assert vh.target(False, 20.0, 1.30) == pytest.approx(0.055)
        assert vh.target(True, 12.0, 0.90) == vh.SIZE  # the regime's size is larger
        assert vh.target(False, 31.0, 1.30) == 0.0  # nothing bought at VIX >= 30
        b = _vix_bundle([20, 20, 20, 20])
        d = b.index
        assert vh.target_after_close(d[2], b) == 0.0  # no equity given: regime part only
        assert vh.target_after_close(d[2], b, equity=1.25) == pytest.approx(0.05)

    def test_backtest_exposure_part_uses_the_equity_held(self):
        from quantzy.strategies import vix_hedge as vh

        b = _vix_bundle([20, 20, 20, 20, 20], vixy_ret=0.01)
        eq = pd.Series([0.5, 0.5, 1.25, 1.25, 1.25], index=b.index)
        r = vh.backtest_returns(b, equity=eq)
        # equity 1.25 held on day 2 -> 0.05 bought at close 2 (cost) -> earns day 3
        assert (r.iloc[:2] == 0).all()
        assert r.iloc[2] == pytest.approx(-0.05 * vh.COST_BPS / 1e4)
        assert r.iloc[3] == pytest.approx(0.05 * 0.01)
        assert (vh.backtest_returns(b) == 0).all()  # regime off and no equity: no hedge

    def test_job_trades_vixy_outside_the_universe(self, bundle):
        from quantzy.data.bundle import DataBundle
        from quantzy.strategies import vix_hedge as vh

        panels = {k: bundle[k] for k in ("market", "carry", "dollar_value", "vixy")}
        b = DataBundle.build(bundle.prices, vix=bundle["vix"] * 0 + 12.0, **panels)  # calm
        book, summary = run("core_multiasset", None, 3_000_000.0, b)
        assert summary["overlays"]["vix_hedge"] == vh.SIZE
        assert book.loc[vh.ASSET, "target"] == pytest.approx(vh.SIZE)
        assert book.loc[vh.ASSET, "order"]
        # already holding a drifted 5.5%: inside the band, no VIXY order
        book2, _ = run("core_multiasset", pd.Series({vh.ASSET: 0.055}), 3_000_000.0, b)
        assert book2.loc[vh.ASSET, "target"] == pytest.approx(0.055)
        assert not book2.loc[vh.ASSET, "order"]


def _pension_bundle(spy_oct: float = 0.10, end: str = "2026-10-30"):
    """Two years of quiet SPY and IEF, then SPY up ``spy_oct`` over October 2026."""
    from quantzy.data.bundle import DataBundle
    from quantzy.strategies.treasury_month_end import trading_days

    idx = trading_days("2024-09-01", end)
    rng = np.random.default_rng(3)
    spy = pd.Series(100 * np.cumprod(1 + rng.normal(0, 0.005, len(idx))), index=idx)
    ief = pd.Series(100 * np.cumprod(1 + rng.normal(0, 0.003, len(idx))), index=idx)
    october = idx >= "2026-10-01"
    spy[october] *= np.linspace(1, 1 + spy_oct, october.sum())
    ief[october] = ief[~october].iloc[-1]  # flat bonds in October
    px = pd.DataFrame({"IEF": ief, "TBILL": 1.0}, index=idx)
    return DataBundle.build(px, market=pd.DataFrame({"IEF": spy, "TBILL": spy}, index=idx))


class TestPensionRebalance:
    def test_weight_is_signed_and_capped(self):
        from quantzy.strategies import pension_rebalance as pr

        assert pr.weight(4.0) == -pr.SIZE  # stocks led: short, capped
        assert pr.weight(-4.0) == pr.SIZE  # stocks lagged: long
        assert pr.weight(1.0) == pytest.approx(-pr.SIZE / 2)
        assert pr.weight(float("nan")) == 0.0

    def test_signal_is_causal(self):
        from quantzy.strategies import pension_rebalance as pr

        b = _pension_bundle()
        z = pr.z_after_close(pr.asset_prices(b), b.prices[pr.BOND])
        spy, ief = pr.asset_prices(b).copy(), b.prices[pr.BOND].copy()
        spy.iloc[-5:] *= 3.0  # corrupt the last week
        z2 = pr.z_after_close(spy, ief)
        pd.testing.assert_series_equal(z.iloc[:-5], z2.iloc[:-5])

    def test_window_and_measurement_day(self):
        from quantzy.strategies import pension_rebalance as pr

        b = _pension_bundle(0.10)
        # October 2026: window Thu 29, Fri 30; entry close Wed 28; measured at Tue 27's close
        assert pr.target_after_close("2026-10-27", b) == 0.0
        held = pr.target_after_close("2026-10-28", b)
        assert held == -pr.SIZE  # SPY +10% against flat bonds: short at full size
        assert pr.target_after_close("2026-10-29", b) == held  # held through the window
        assert pr.target_after_close("2026-10-30", b) == 0.0
        # the entry day's own close is not used
        moved = _pension_bundle(0.10)
        moved["market"].loc["2026-10-28"] *= 0.5
        assert pr.target_after_close("2026-10-28", moved) == held

    def test_long_when_stocks_lag(self):
        from quantzy.strategies import pension_rebalance as pr

        assert pr.target_after_close("2026-10-28", _pension_bundle(-0.10)) == pr.SIZE

    def test_backtest_holds_only_in_the_window(self):
        from quantzy.strategies import pension_rebalance as pr

        b = _pension_bundle(0.10)
        r = pr.backtest_returns(b)
        oct_ = r.loc["2026-10"]
        assert -pr.SIZE * pr.COST_BPS / 1e4 <= oct_.iloc[0] <= 0  # 1 Oct: September's exit cost
        assert (oct_.loc["2026-10-02":"2026-10-28"] == 0).all()
        spy_r = pr.asset_prices(b).pct_change().loc["2026-10-30"]
        assert oct_.loc["2026-10-30"] == pytest.approx(-pr.SIZE * spy_r)

    def test_job_orders_a_short(self, bundle):
        from quantzy.data.bundle import DataBundle
        from quantzy.strategies import pension_rebalance as pr
        from quantzy.strategies.treasury_month_end import trading_days

        # End the data on the close before an entry close, with SPY far ahead of IEF that month.
        days = trading_days(bundle.index[0], bundle.index[-1])
        month = days[days.to_period("M") == days[-60].to_period("M")]
        asof = month[-4]  # the job's orders execute at month[-3], the entry close
        b = bundle.upto(asof)
        market = b["market"].copy()
        this_month = market.index.to_period("M") == asof.to_period("M")
        market.loc[this_month] *= np.linspace(1, 1.15, this_month.sum())[:, None]
        panels = {k: b[k] for k in ("vix", "carry", "dollar_value", "vixy")}
        b = DataBundle.build(b.prices, market=market, **panels)
        book, summary = run("core_multiasset", None, 3_000_000.0, b)
        # a fully funded 1.3x core plus the month-end overlays passes 1.95x gross, so the
        # overlays are scaled back (Reg T)
        size = pr.SIZE * summary["overlay_scale"]
        assert summary["overlay_scale"] <= 1.0
        assert summary["overlays"]["pension_rebalance"] == pytest.approx(-size)
        assert book.loc[pr.ASSET, "target"] == pytest.approx(-size)
        assert book.loc[pr.ASSET, "order"]
        assert summary["gross_target"] >= book.drop(index=core.CASH)["target"].sum() + size


class TestTreasuryAuction:
    # A 10-year auction on Tue 2026-11-10: the window earns the returns of Wed 4 .. Tue 10
    AUCTION = pd.Timestamp("2026-11-10")

    def _set(self, ten=(), thirty=()):
        from quantzy.strategies import treasury_auction as ta

        ta.set_dates({"10-Year": list(ten), "30-Year": list(thirty)})

    def test_pre_window_is_the_five_days_into_the_auction(self):
        from quantzy.strategies import treasury_auction as ta
        from quantzy.strategies.treasury_month_end import trading_days

        days = trading_days("2026-10-26", "2026-11-20")
        w = ta.in_pre_window(days, pd.DatetimeIndex([self.AUCTION]))
        assert list(w[w].index.strftime("%m-%d")) == ["11-04", "11-05", "11-06", "11-09",
                                                       "11-10"]

    def test_target_shorts_from_the_entry_close_to_the_auction_close(self):
        from quantzy.strategies import treasury_auction_10y as a10

        self._set(ten=[self.AUCTION])
        assert a10.target_after_close("2026-11-02") == 0.0  # next day Tue 3: outside
        assert a10.target_after_close("2026-11-03") == -a10.SIZE  # entry close
        assert a10.target_after_close("2026-11-09") == -a10.SIZE  # holds into the auction
        assert a10.target_after_close("2026-11-10") == 0.0  # exits at the auction close

    def test_terms_are_separate(self):
        from quantzy.strategies import treasury_auction_10y as a10
        from quantzy.strategies import treasury_auction_30y as a30

        self._set(thirty=[self.AUCTION])
        assert a10.target_after_close("2026-11-03") == 0.0
        assert a30.target_after_close("2026-11-03") == -a30.SIZE

    def test_stale_schedule_is_refused(self, monkeypatch):
        from quantzy.strategies import treasury_auction as ta

        ta.set_dates(None)
        monkeypatch.setattr(ta, "schedule_end", lambda: pd.Timestamp("2026-11-15"))
        with pytest.raises(RuntimeError, match="tentative auction schedule ends"):
            ta.target_after_close("2026-11-03", "10-Year")

    def test_backtest_earns_the_short_in_the_window(self):
        from quantzy.data.bundle import DataBundle
        from quantzy.strategies import treasury_auction_10y as a10
        from quantzy.strategies.treasury_month_end import trading_days

        idx = trading_days("2026-10-01", "2026-11-30")
        px = pd.DataFrame({"IEF": 100 * 1.001 ** np.arange(len(idx)), "TBILL": 1.0}, index=idx)
        self._set(ten=[self.AUCTION])
        r = a10.backtest_returns(DataBundle.build(px))
        inside = r.loc["2026-11-04":"2026-11-10"]
        assert len(inside) == 5 and (inside < 0).all()  # short a rising ETF
        assert (r.loc[:"2026-11-03"] == 0).all()
        # Veterans Day (Wed 11) is an NYSE trading day: the exit cost lands on it
        assert r.loc["2026-11-11"] < 0 and r.loc["2026-11-12":].eq(0).all()

    def test_data_ending_inside_a_window_holds_the_short(self):
        """The bug: with data ending on Thu 2026-11-05, before the 11-10 auction, the backtest
        held nothing on 11-04 and 11-05 while the live rule was short."""
        from quantzy.data.bundle import DataBundle
        from quantzy.strategies import treasury_auction as ta
        from quantzy.strategies import treasury_auction_10y as a10
        from quantzy.strategies.treasury_month_end import trading_days

        idx = trading_days("2026-10-01", "2026-11-05")
        self._set(ten=[self.AUCTION])
        flags = ta.in_pre_window(idx, ta.auction_dates("10-Year"))
        assert flags.loc["2026-11-04":].all() and not flags.loc[:"2026-11-03"].any()
        px = pd.DataFrame({"IEF": 100 * 1.001 ** np.arange(len(idx)), "TBILL": 1.0}, index=idx)
        r = a10.backtest_returns(DataBundle.build(px))
        assert (r.loc["2026-11-04":] < 0).all()
        # the backtest's position on each day is the job's target from the close before it
        for prev, day in zip(idx[:-1], idx[1:], strict=True):
            assert a10.target_after_close(prev) == (-ta.SIZE if flags[day] else 0.0)

    def test_job_adds_the_short_to_the_core(self, bundle):
        from quantzy.strategies import treasury_auction_10y as a10
        from quantzy.strategies.treasury_month_end import trading_days

        cal = trading_days(bundle.index[-1], bundle.index[-1] + pd.Timedelta(days=20))
        auction = cal[cal > bundle.index[-1]][4]  # the job executes at cal[1], the entry close
        self._set(ten=[auction])
        book, summary = run("core_multiasset", None, 3_000_000.0, bundle)
        assert summary["overlays"]["treasury_auction_10y"] == pytest.approx(-a10.SIZE)
        assert summary["overlays"]["treasury_auction_30y"] == 0.0

    def test_summary_attributes_each_overlay_on_a_shared_asset(self, bundle, monkeypatch):
        """TLT carries the month-end overlay and the 30-year auction overlay: the summary must
        report each one's own weight, not their sum under both names."""
        from quantzy.strategies import treasury_month_end as tme

        monkeypatch.setattr(tme, "target_after_close", lambda day, bundle=None, current=0.0: 0.5)
        book, summary = run("core_multiasset", None, 3_000_000.0, bundle)
        assert summary["overlays"]["treasury_month_end"] == 0.5
        assert summary["overlays"]["treasury_auction_30y"] == 0.0


class TestExecutionTiming:
    """The job must decide on the same data the backtest does for the day it trades."""

    def test_preclose_run_matches_the_backtest_decision(self, core_bundle, monkeypatch):
        """Run before the close of T with T's prices, fill at T's close: the core book is the
        one the backtest holds through T+1 (``decide(asof=T+1)``, which reads T's close)."""
        monkeypatch.setattr(core, "OVERLAYS", ())
        day, nxt = core_bundle.index[-2], core_bundle.index[-1]
        book, summary = run("core_multiasset", None, 3_000_000.0, core_bundle.upto(day),
                            execution_day=day)
        assert summary["preclose"] and summary["execution_day"] == day.date()
        engine = core.build().decide(core_bundle, asof=nxt).target.weights
        got = book["target"].reindex(engine.index)
        # the job reads a shorter window than the backtest, so slow EWMAs differ in the 5th place
        assert np.allclose(got.to_numpy(), engine.to_numpy(), atol=1e-4)

    def test_decision_reads_the_last_row(self, core_bundle, monkeypatch):
        """Moving only the last row's prices must move the target (the old job read the row
        before it, two closes behind the fill)."""
        monkeypatch.setattr(core, "OVERLAYS", ())
        day = core_bundle.index[-1]
        base, _ = run("core_multiasset", None, 3e6, core_bundle, execution_day=day)
        px = core_bundle.prices.copy()
        px.loc[day, core.RISKY] *= np.where(np.arange(len(core.RISKY)) % 2, 1.08, 0.92)
        moved = core_bundle.__class__({**core_bundle, "prices": px})
        shifted, _ = run("core_multiasset", None, 3e6, moved, execution_day=day)
        assert (base["target"] - shifted["target"]).abs().max() > 1e-3

    def test_after_close_run_executes_next_day(self, core_bundle):
        _, summary = run("core_multiasset", None, 3e6, core_bundle)
        assert not summary["preclose"] and summary["execution_day"] > core_bundle.index[-1].date()

    def test_other_execution_days_are_rejected(self, core_bundle):
        with pytest.raises(ValueError, match="execution day"):
            run("core_multiasset", None, 3e6, core_bundle,
                execution_day=core_bundle.index[-1] + pd.Timedelta(days=10))

    def test_preclose_overlays_hold_from_the_previous_close(self, core_bundle, monkeypatch):
        """Pre-close, the overlay's 'now' is its target for yesterday's close and its order
        is for today's; each is asked for exactly those closes."""
        from quantzy.strategies import treasury_month_end as tme

        asked = []
        monkeypatch.setattr(tme, "target_after_close",
                            lambda day, bundle=None, current=0.0: asked.append(day) or 0.0)
        day = core_bundle.index[-1]
        run("core_multiasset", pd.Series({"XLK": 0.1}), 3e6, core_bundle, execution_day=day)
        assert [pd.Timestamp(d) for d in asked] == [core_bundle.index[-2], day]


def test_fill_live_row_fills_only_the_last_row(core_bundle):
    from quantzy.jobs.daily import fill_live_row

    vix = core_bundle["vix"].copy()
    vix.iloc[-1] = np.nan
    vix.iloc[-3, 0] = np.nan  # an older gap stays
    b, filled = fill_live_row(core_bundle.__class__({**core_bundle, "vix": vix}))
    assert b["vix"].iloc[-1].equals(core_bundle["vix"].iloc[-2])
    assert np.isnan(b["vix"].iloc[-3, 0]) and all(f.startswith("vix:") for f in filled)


class TestSessionClock:
    """The job's pre-close / after-close call follows the NYSE close, including half days."""

    @pytest.mark.parametrize("day, close", [
        ("2026-11-27", "13:00"),  # Friday after Thanksgiving
        ("2026-12-24", "13:00"),  # Thursday
        ("2025-07-03", "13:00"),  # Jul 4 a Friday
        ("2023-07-03", "13:00"),  # Jul 4 a Tuesday
        ("2026-07-02", "16:00"),  # Jul 3 2026 is the observed holiday, no early close
        ("2027-07-02", "16:00"),  # Jul 4 a Sunday
        ("2026-11-30", "16:00"),
        ("2026-12-23", "16:00"),
    ])
    def test_close_time(self, day, close):
        from quantzy.strategies.treasury_month_end import close_time

        assert f"{close_time(day):%H:%M}" == close

    def test_no_close_on_a_holiday_or_weekend(self):
        from quantzy.strategies.treasury_month_end import close_time

        assert close_time("2026-11-26") is None  # Thanksgiving
        assert close_time("2026-12-25") is None
        assert close_time("2026-07-03") is None  # observed Independence Day
        assert close_time("2026-11-28") is None  # Saturday

    def test_half_day_before_its_close_is_preclose_with_an_early_window(self):
        from quantzy.jobs.daily import session_state

        s = session_state(pd.Timestamp("2026-11-27 12:40", tz="America/New_York"))
        assert s.preclose and s.window == ("12:30", "12:50")

    def test_half_day_after_its_close_is_after_close(self):
        """The bug: 15:45 on a 13:00 day was treated as pre-close, sending MOC orders for a
        session that had ended."""
        from quantzy.jobs.daily import session_state

        s = session_state(pd.Timestamp("2026-11-27 15:45", tz="America/New_York"))
        assert not s.preclose and not s.open_now

    def test_normal_day(self):
        from quantzy.jobs.daily import session_state

        s = session_state(pd.Timestamp("2026-11-30 15:45", tz="America/New_York"))
        assert s.preclose and s.window == ("15:30", "15:50")
        assert not session_state(pd.Timestamp("2026-11-30 16:05", tz="America/New_York")).open_now

    def test_after_close_flag_and_other_timezones(self):
        from quantzy.jobs.daily import session_state

        s = session_state(pd.Timestamp("2026-11-30 15:45", tz="America/New_York"), True)
        assert s.open_now and not s.preclose
        # 20:45 UTC is 15:45 ET in November
        assert session_state(pd.Timestamp("2026-11-30 20:45", tz="UTC")).preclose

    def test_closed_day(self):
        from quantzy.jobs.daily import session_state

        s = session_state(pd.Timestamp("2026-11-26 15:45", tz="America/New_York"))
        assert not s.preclose and s.close is None and s.window is None


class TestLivePrices:
    """A pre-close run fills only index levels; a traded ticker without a live price is
    fetched on its own, else held unchanged for the day."""

    def _drop_live(self, b, panel, cols):
        v = b[panel].copy()
        v.iloc[-1, [v.columns.get_loc(c) for c in cols]] = np.nan
        return b.__class__({**b, panel: v})

    def test_fill_live_row_leaves_prices_alone(self, core_bundle):
        from quantzy.jobs.daily import fill_live_row

        b, filled = fill_live_row(self._drop_live(core_bundle, "prices", ["XLK"]))
        assert np.isnan(b.prices["XLK"].iloc[-1]) and not filled

    def test_missing_etf_is_fetched_on_its_own(self, core_bundle):
        from quantzy.jobs.daily import repair_live_row

        b = self._drop_live(core_bundle, "prices", ["XLK"])
        asked = []
        fixed, fetched, frozen = repair_live_row(
            core, b, fetch=lambda t, day: asked.append((t, day)) or 123.0)
        assert asked == [("XLK", b.index[-1])] and fetched == {"XLK": 123.0} and not frozen
        assert fixed.prices["XLK"].iloc[-1] == 123.0

    def test_overlay_asset_panel_is_repaired_in_every_column(self, core_bundle):
        from quantzy.jobs.daily import repair_live_row

        b = self._drop_live(core_bundle, "market", list(core_bundle["market"].columns))
        fixed, fetched, _ = repair_live_row(core, b, fetch=lambda t, day: 500.0)
        assert fetched == {"SPY": 500.0} and (fixed["market"].iloc[-1] == 500.0).all()

    def test_unfetchable_ticker_is_frozen_at_the_last_close(self, core_bundle):
        from quantzy.jobs.daily import repair_live_row

        b = self._drop_live(core_bundle, "prices", ["DBC"])
        fixed, fetched, frozen = repair_live_row(core, b, fetch=lambda t, day: None)
        assert frozen == ["DBC"] and not fetched
        assert fixed.prices["DBC"].iloc[-1] == core_bundle.prices["DBC"].iloc[-2]

    def test_run_holds_a_frozen_ticker_and_trades_the_rest(self, core_bundle):
        held = pd.Series({"XLK": 0.12, "DBC": 0.03, "IEF": 0.1})
        day = core_bundle.index[-1]
        free, _ = run("core_multiasset", held, 3e6, core_bundle, execution_day=day)
        book, summary = run("core_multiasset", held, 3e6, core_bundle, execution_day=day,
                            frozen=["XLK"])
        assert abs(free.loc["XLK", "target"] - 0.12) > 1e-3  # it would have traded
        assert book.loc["XLK", "target"] == pytest.approx(0.12) and not book.loc["XLK", "order"]
        assert summary["frozen"] == ["XLK"]
        assert book["target"].sum() == pytest.approx(1.0)  # the difference sits in cash
        assert book.drop(index="XLK")["order"].any()

    def test_freezing_an_unknown_ticker_is_refused(self, core_bundle):
        with pytest.raises(ValueError, match="cannot freeze"):
            run("core_multiasset", None, 3e6, core_bundle, frozen=["ZZZ"])

    def test_flat_today_flags_unchanged_prices(self, core_bundle):
        from quantzy.jobs.daily import flat_today

        px = core_bundle.prices.copy()
        for t in ("XLB", "XLE", "XLF", "XLI"):
            px.loc[px.index[-1], t] = px[t].iloc[-2]
        b = core_bundle.__class__({**core_bundle, "prices": px})
        assert flat_today(core, b) == ["XLB", "XLE", "XLF", "XLI"]
        assert flat_today(core, b, exclude=["XLB"]) == ["XLE", "XLF", "XLI"]


class TestHoldingReturns:
    def test_gap_defers_the_move_instead_of_losing_it(self):
        from quantzy.returns import holding_returns

        r = holding_returns(pd.Series([np.nan, 100.0, np.nan, 110.0, np.nan]))
        assert np.isnan(r.iloc[1])  # before the first price: no return
        assert r.iloc[2] == 0.0 and r.iloc[3] == pytest.approx(0.10)
        assert np.isnan(r.iloc[4])  # after the last price: not filled

    def test_engine_books_the_whole_move_across_a_gap(self, core_bundle):
        """A held asset with a one-day gap earns the same over the gap as without it."""
        from quantzy.backtest import WalkForwardBacktester
        from quantzy.data.bundle import DataBundle

        def go(p):
            data = DataBundle({**{k: v.iloc[:1400] for k, v in core_bundle.items()},
                               "prices": p})
            return WalkForwardBacktester(rebalance_every=10_000, warmup=1300).run(
                data, core.build())

        px = core_bundle.prices.iloc[:1400].copy()
        base = go(px)
        held = base.weights.iloc[-6].drop(core.CASH).idxmax()  # the largest holding
        assert base.weights[held].iloc[-6] > 0.05
        gap = px.copy()
        gap.iloc[-5, gap.columns.get_loc(held)] = np.nan
        assert (1 + base.returns).prod() == pytest.approx((1 + go(gap).returns).prod(),
                                                           rel=1e-12)
