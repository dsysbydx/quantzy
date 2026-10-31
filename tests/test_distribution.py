"""Score-distribution and tail-treatment tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.research import (
    clip_scores,
    compare_tail_treatments,
    describe_scores,
    tail_treatments,
    winsorize_scores,
)


@pytest.fixture
def fat_tailed():
    """Student-t(3) scores for three assets: fat tails, unit-ish scale."""
    rng = np.random.default_rng(7)
    idx = pd.bdate_range("2010-01-01", periods=1500)
    return pd.DataFrame(rng.standard_t(3, size=(len(idx), 3)), index=idx, columns=list("ABC"))


class TestDescribe:
    def test_reports_fat_tails(self, fat_tailed):
        d = describe_scores(fat_tailed, by_asset=True)
        assert list(d.index) == ["pooled", "A", "B", "C"]
        assert d.loc["pooled", "ex_kurt"] > 1.0
        assert d.loc["pooled", "share_gt3"] > 0.003  # more than a normal's
        assert d.loc["pooled", "n"] == fat_tailed.size

    def test_ignores_no_view_cells(self, fat_tailed):
        s = fat_tailed.copy()
        s.iloc[:100] = np.nan
        assert describe_scores(s).loc["pooled", "n"] == fat_tailed.size - 300


class TestClip:
    def test_bounds_the_score(self, fat_tailed):
        c = clip_scores(fat_tailed, 2.0)
        assert c.abs().max().max() == pytest.approx(2.0)
        inside = fat_tailed.abs() <= 2.0
        pd.testing.assert_frame_equal(c[inside], fat_tailed[inside])

    def test_rejects_a_non_positive_cap(self, fat_tailed):
        with pytest.raises(ValueError, match="k > 0"):
            clip_scores(fat_tailed, 0.0)


class TestWinsorize:
    def test_is_causal(self, fat_tailed):
        """Corrupting every row after T must leave rows through T untouched."""
        t = 900
        w = winsorize_scores(fat_tailed, 0.01, min_periods=252)
        bad = fat_tailed.copy()
        bad.iloc[t + 1:] = 1e6
        w_bad = winsorize_scores(bad, 0.01, min_periods=252)
        pd.testing.assert_frame_equal(w.iloc[: t + 1], w_bad.iloc[: t + 1])

    def test_leaves_the_warmup_alone(self, fat_tailed):
        w = winsorize_scores(fat_tailed, 0.01, min_periods=252)
        pd.testing.assert_frame_equal(w.iloc[:251], fat_tailed.iloc[:251])

    def test_clips_about_the_tail_share(self, fat_tailed):
        w = winsorize_scores(fat_tailed, 0.01, min_periods=252)
        changed = (w != fat_tailed).iloc[252:].to_numpy().mean()
        assert 0.01 < changed < 0.04  # ~2% expected, both tails

    def test_rejects_a_bad_quantile(self, fat_tailed):
        with pytest.raises(ValueError, match="0 < q < 0.5"):
            winsorize_scores(fat_tailed, 0.5)


class TestCompare:
    @pytest.fixture
    def setup(self, fat_tailed):
        """Returns driven by the clipped score, so clipping should help a Pearson IC."""
        rng = np.random.default_rng(11)
        signal = fat_tailed.clip(-2, 2)
        ret = 0.004 * signal + rng.normal(0, 0.01, size=fat_tailed.shape)
        prices = 100 * (1 + ret.shift(1).fillna(0)).cumprod()  # day t+1 earns ret[t]
        variants = tail_treatments(fat_tailed, clips=(2.0,), winsor=(0.01,), min_periods=252)
        return variants, prices

    def test_names_every_variant(self, setup):
        variants, _ = setup
        assert list(variants) == ["none", "clip +/-2", "winsor 1/99"]

    def test_pearson_sees_the_clip(self, setup):
        variants, prices = setup
        t = compare_tail_treatments(variants, prices, vol_window=None)
        assert t.loc["clip +/-2", "ic"] > t.loc["none", "ic"]
        assert t.loc["none", "altered"] == 0.0
        assert t.loc["clip +/-2", "altered"] == pytest.approx(
            (variants["none"].abs() > 2).to_numpy().mean())

    def test_rank_ic_barely_moves(self, setup):
        """Why the comparison defaults to Pearson: clipping keeps the order of the values."""
        variants, prices = setup
        r = compare_tail_treatments(variants, prices, method="spearman", vol_window=None)
        p = compare_tail_treatments(variants, prices, method="pearson", vol_window=None)
        gap_rank = abs(r.loc["clip +/-2", "ic"] - r.loc["none", "ic"])
        gap_pearson = abs(p.loc["clip +/-2", "ic"] - p.loc["none", "ic"])
        assert gap_rank < gap_pearson / 3
