"""CachedReader: a disk cache that parallel processes can share."""

from __future__ import annotations

import pandas as pd

from quantzy.data.cache import CachedReader


class _Counting:
    def __init__(self) -> None:
        self.calls = 0

    def history(self, tickers, start=None, end=None, field="adj_close"):
        self.calls += 1
        idx = pd.bdate_range("2020-01-01", periods=3)
        return pd.DataFrame({t: [1.0, 2.0, 3.0] for t in tickers}, index=idx)


def test_second_call_reads_the_cache(tmp_path):
    src = _Counting()
    r = CachedReader(src, cache_dir=tmp_path)
    a = r.history(["A", "B"])
    b = r.history(["A", "B"])
    pd.testing.assert_frame_equal(a, b, check_freq=False)
    assert src.calls == 1
    assert not list(tmp_path.glob("*.tmp")), "no temporary file is left behind"


def test_truncated_file_is_fetched_again(tmp_path):
    src = _Counting()
    r = CachedReader(src, cache_dir=tmp_path)
    r._key(["A"], None, None, "adj_close").write_bytes(b"PAR1 truncated")
    out = r.history(["A"])
    assert src.calls == 1 and list(out.columns) == ["A"]
    assert len(r.history(["A"])) == 3 and src.calls == 1
