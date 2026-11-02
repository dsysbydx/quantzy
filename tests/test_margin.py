"""Margin and stress checks on a target book."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantzy.jobs import margin as mg


class TestMargin:
    def test_a_levered_long_book(self):
        m = mg.margin(pd.Series({"A": 1.3}))
        assert m["gross"] == pytest.approx(1.3) and m["borrowed"] == pytest.approx(0.3)
        assert m["maintenance"] == pytest.approx(0.325)
        assert m["cushion"] == pytest.approx(0.675)
        assert m["drop_to_call"] == pytest.approx(0.675 / (1.3 * 0.75))
        assert m["initial_used"] == pytest.approx(0.65)

    def test_the_drop_to_call_exhausts_the_cushion(self):
        m = mg.margin(pd.Series({"A": 1.8}))
        x = m["drop_to_call"]
        equity, req = 1 - 1.8 * x, 0.25 * 1.8 * (1 - x)
        assert equity == pytest.approx(req)

    def test_shorts_carry_their_own_requirement(self):
        m = mg.margin(pd.Series({"A": 1.0, "B": -0.5}))
        assert m["gross"] == pytest.approx(1.5) and m["net"] == pytest.approx(0.5)
        assert m["maintenance"] == pytest.approx(0.25 + 0.15)


class TestStress:
    def test_replays_the_weeks_return(self):
        idx = pd.bdate_range("2008-09-29", periods=10)
        px = pd.DataFrame({"A": 100.0, "B": 100.0}, index=idx)
        px.loc[idx[-5]:, "A"] = 80.0  # -20% over the week ending on the last day
        s = mg.stress(pd.Series({"A": 1.0, "B": 0.5}), px,
                      weeks={"test week": str(idx[-1].date())})["test week"]
        assert s["loss"] == pytest.approx(-0.20)
        assert s["cushion"] == pytest.approx(0.80 - 0.25 * (0.8 + 0.5))

    def test_assets_without_history_take_no_shock(self):
        idx = pd.bdate_range("2008-09-29", periods=10)
        px = pd.DataFrame({"A": np.linspace(100, 90, 10)}, index=idx)
        s = mg.stress(pd.Series({"A": 1.0, "NEW": 1.0}), px,
                      weeks={"w": str(idx[-1].date())})["w"]
        assert s["loss"] == pytest.approx(px["A"].iloc[-1] / px["A"].iloc[-6] - 1)


    def test_weeks_the_data_does_not_reach_are_skipped(self):
        idx = pd.bdate_range("2015-01-01", periods=300)
        px = pd.DataFrame({"A": np.linspace(100, 120, 300)}, index=idx)
        assert mg.stress(pd.Series({"A": 1.0}), px) == {}  # 2008 too early, 2020-22 too late


class TestScaleOverlays:
    core = pd.Series({"A": 1.3, "B": 0.0})

    def test_fits_already(self):
        assert mg.scale_overlays(self.core, pd.Series({"A": 0.0, "B": 0.5}), 1.95) == 1.0

    def test_shrinks_to_the_limit(self):
        s = mg.scale_overlays(self.core, pd.Series({"A": 0.0, "B": 0.8}), 1.95)
        assert s == pytest.approx(0.65 / 0.8, abs=1e-9)

    def test_an_offsetting_overlay_is_left_alone(self):
        assert mg.scale_overlays(self.core, pd.Series({"A": -0.3, "B": 0.0}), 1.95) == 1.0

    def test_core_alone_over_the_limit(self):
        assert mg.scale_overlays(pd.Series({"A": 2.5}), pd.Series({"A": 0.5}), 1.95) == 0.0
