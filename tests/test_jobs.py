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
