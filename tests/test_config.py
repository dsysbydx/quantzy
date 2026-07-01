from quantzy.config import DEFAULT_ALPHA_WEIGHTS, StrategyConfig


class TestStrategyConfig:
    def test_default_alpha_weights(self):
        cfg = StrategyConfig()
        assert cfg.alpha_weights == {"momentum": 1.0, "value": 0.5}

    def test_alpha_weights_not_shared_between_instances(self):
        # Mutable default must not be shared across instances.
        a = StrategyConfig()
        b = StrategyConfig()
        a.alpha_weights["momentum"] = 99.0
        assert b.alpha_weights["momentum"] == 1.0
        assert DEFAULT_ALPHA_WEIGHTS["momentum"] == 1.0  # module-level default untouched

    def test_alpha_weights_override(self):
        cfg = StrategyConfig(alpha_weights={"momentum": 2.0})
        assert cfg.alpha_weights == {"momentum": 2.0}
