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
    assert risky["target"].sum() == pytest.approx(summary["gross_target"])
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
        assert set(summary["overlays"]) == {"treasury_month_end"}
