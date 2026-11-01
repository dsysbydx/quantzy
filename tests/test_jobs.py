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
    assert risky["target"].sum() <= core.PORTFOLIO.max_leverage + 1e-6


def test_reports_model_and_stress_vol(bundle):
    _, summary = run("core_multiasset", None, 3_000_000.0, bundle)
    assert 0 < summary["model_vol"] < 1 and 0 < summary["stress_vol"] < 1


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
        assert summary["overlays"]["pension_rebalance"] == pytest.approx(-pr.SIZE)
        assert book.loc[pr.ASSET, "target"] == pytest.approx(-pr.SIZE)
        assert book.loc[pr.ASSET, "order"]
        assert summary["gross_target"] >= book.drop(index=core.CASH)["target"].sum() + pr.SIZE


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

    def test_job_adds_the_short_to_the_core(self, bundle):
        from quantzy.strategies import treasury_auction_10y as a10
        from quantzy.strategies.treasury_month_end import trading_days

        cal = trading_days(bundle.index[-1], bundle.index[-1] + pd.Timedelta(days=20))
        auction = cal[cal > bundle.index[-1]][4]  # the job executes at cal[1], the entry close
        self._set(ten=[auction])
        book, summary = run("core_multiasset", None, 3_000_000.0, bundle)
        assert summary["overlays"]["treasury_auction_10y"] == pytest.approx(-a10.SIZE)
        assert summary["overlays"]["treasury_auction_30y"] == 0.0
