import numpy as np

from quantzy.risk import LedoitWolfCovariance, SampleCovariance, vol_from_cov


class TestShrinkage:
    def test_shrinkage_is_symmetric_psd(self, random_returns):
        sigma = LedoitWolfCovariance(annualize=False).covariance(random_returns)
        arr = sigma.to_numpy()
        # symmetric
        assert np.allclose(arr, arr.T)
        # positive semidefinite
        eigvals = np.linalg.eigvalsh(arr)
        assert eigvals.min() > -1e-10
        # labelled by ticker, square
        assert list(sigma.index) == list(random_returns.columns)
        assert sigma.shape == (4, 4)

    def test_shrinkage_intensity_bounds(self, random_returns):
        # Manual full shrinkage -> diagonal target (zero off-diagonals).
        sigma = LedoitWolfCovariance(annualize=False, shrinkage=1.0).covariance(random_returns)
        off_diag = sigma.to_numpy() - np.diag(np.diag(sigma.to_numpy()))
        assert np.allclose(off_diag, 0.0)


class TestSampleCovariance:
    def test_sample_annualization(self, random_returns):
        daily = SampleCovariance(annualize=False).covariance(random_returns)
        annual = SampleCovariance(annualize=True, trading_days=252).covariance(random_returns)
        assert np.allclose(annual.to_numpy(), daily.to_numpy() * 252)

    def test_vol_from_cov_matches_diagonal(self, random_returns):
        sigma = SampleCovariance(annualize=False).covariance(random_returns)
        vol = vol_from_cov(sigma)
        assert np.allclose(vol.to_numpy(), np.sqrt(np.diag(sigma.to_numpy())))
