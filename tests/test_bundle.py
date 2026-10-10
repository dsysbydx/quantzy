"""DataBundle tests: alignment, point-in-time truncation, and the fingerprint."""

from __future__ import annotations

import pandas as pd
import pytest

from quantzy.data.bundle import DataBundle, as_bundle


class TestBuild:
    def test_requires_a_price_panel(self, panel):
        with pytest.raises(ValueError, match="needs a 'prices' panel"):
            DataBundle({"volume": panel})

    def test_aligns_extra_panels_onto_prices(self, panel):
        volume = panel.iloc[::2] * 10.0  # coarser index, same columns
        bundle = DataBundle.build(panel, volume=volume)
        assert bundle["volume"].index.equals(bundle.prices.index)
        assert list(bundle["volume"].columns) == list(bundle.prices.columns)

    def test_alignment_does_not_fill(self, panel):
        """Reindexing must leave gaps visible rather than forward-filling them silently."""
        sparse = panel.iloc[::2]
        bundle = DataBundle.build(panel, eps=sparse)
        assert bundle["eps"].isna().any().any()

    def test_rejects_prices_as_a_keyword(self, panel):
        with pytest.raises(ValueError, match="passed positionally"):
            DataBundle.build(panel, prices=panel)

    def test_unknown_panel_names_itself_in_the_error(self, bundle):
        with pytest.raises(KeyError, match="no panel 'eps'"):
            bundle["eps"]


class TestPointInTime:
    def test_upto_truncates_every_panel(self, panel):
        bundle = DataBundle.build(panel, volume=panel * 2.0)
        asof = panel.index[500]
        clipped = bundle.upto(asof)
        assert clipped.index[-1] == asof
        assert clipped["volume"].index[-1] == asof

    def test_upto_is_inclusive(self, bundle):
        asof = bundle.index[500]
        assert asof in bundle.upto(asof).index

    def test_tail_takes_the_last_rows_of_every_panel(self, panel):
        bundle = DataBundle.build(panel, volume=panel * 2.0)
        windowed = bundle.tail(100)
        assert len(windowed.index) == 100
        assert len(windowed["volume"]) == 100
        assert windowed.index[-1] == bundle.index[-1]


class TestFingerprint:
    def test_differs_after_truncation(self, bundle):
        assert bundle.fingerprint() != bundle.upto(bundle.index[500]).fingerprint()

    def test_differs_when_a_panel_is_added(self, panel):
        assert (
            DataBundle.build(panel).fingerprint()
            != DataBundle.build(panel, volume=panel).fingerprint()
        )

    def test_matches_for_an_equivalent_rebuild(self, panel):
        assert DataBundle.build(panel).fingerprint() == DataBundle.build(panel).fingerprint()

    def test_differs_when_columns_are_reordered(self, panel):
        reordered = panel[list(panel.columns)[::-1]]
        assert DataBundle.build(panel).fingerprint() != DataBundle.build(reordered).fingerprint()


class TestAsBundle:
    def test_passes_a_bundle_through(self, bundle):
        assert as_bundle(bundle) is bundle

    def test_wraps_a_bare_frame(self, panel):
        wrapped = as_bundle(panel)
        assert isinstance(wrapped, DataBundle)
        pd.testing.assert_frame_equal(wrapped.prices, panel)

    def test_is_a_mapping(self, bundle):
        assert len(bundle) == 1
        assert list(bundle) == ["prices"]
