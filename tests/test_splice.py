"""splice / SplicedReader: young ETFs extended back with proxy returns."""

from __future__ import annotations

import numpy as np
import pandas as pd

from quantzy.data.splice import SplicedReader, splice

IDX = pd.bdate_range("2020-01-01", periods=6)


def test_splice_keeps_the_etf_and_rebuilds_levels_from_proxy_returns():
    etf = pd.Series([np.nan, np.nan, np.nan, 100.0, 101.0, 102.0], index=IDX)
    proxy = pd.Series([np.nan, 0.10, -0.05, 0.02, 0.5, 0.5], index=IDX)
    out = splice(etf, proxy)
    pd.testing.assert_series_equal(out.loc[IDX[3]:], etf.dropna())
    # Day 3's close is 100 after a 2% day, day 2's after a -5% day.
    assert np.isclose(out[IDX[2]], 100 / 1.02)
    assert np.isclose(out[IDX[1]], 100 / 1.02 / 0.95)
    assert np.isclose(out[IDX[0]], 100 / 1.02 / 0.95 / 1.10)
    assert np.allclose(out.pct_change().iloc[1:4], [0.10, -0.05, 0.02])


def test_a_chain_fills_only_before_what_is_built_so_far():
    etf = pd.Series([np.nan, np.nan, np.nan, np.nan, 10.0, 11.0], index=IDX)
    fund = pd.Series([np.nan, np.nan, 0.5, 0.0, 0.0, 9.9], index=IDX)
    index = pd.Series([0.0, 1.0, 9.9, 9.9, 9.9, 9.9], index=IDX)
    out = splice(etf, fund, index)
    r = out.pct_change()
    assert np.isclose(r[IDX[1]], 1.0) and np.isclose(r[IDX[2]], 0.5)
    assert out[IDX[4]] == 10.0 and len(out) == 6


class _Reader:
    def history(self, tickers, start=None, end=None, field="adj_close"):
        px = {"ETF": [np.nan, np.nan, 50.0, 51.0, 52.0, 53.0],
              "^IDX": [1.0, 2.0, np.nan, 4.0, 5.0, np.nan]}  # an index printing on a holiday
        return pd.DataFrame({t: px[t] for t in tickers}, index=IDX)


def test_reader_splices_and_carries_a_proxy_holiday_flat():
    proxy = pd.Series([np.nan, 0.01, 0.02, 0.03, 0.04, 0.05], index=IDX).drop(IDX[3])
    r = SplicedReader(_Reader(), {"ETF": [lambda start, end: proxy]})
    out = r.history(["ETF", "^IDX"])
    assert out["ETF"].notna().all()
    assert out.loc[IDX[2]:, "ETF"].tolist() == [50.0, 51.0, 52.0, 53.0]
    assert np.isclose(out["ETF"][IDX[1]], 50 / 1.02)
    assert np.isnan(out["^IDX"][IDX[2]]), "tickers without a proxy pass through unchanged"
