"""Unit tests for datasets/generators/config.py."""

from __future__ import annotations

import pytest

from datasets.generators.config import GeneratorConfig, RecordCounts


class TestRecordCounts:
    def test_full_scale_totals(self):
        counts = RecordCounts()
        assert counts.orders == 1_000_000
        assert counts.customers == 500_000
        assert counts.order_lines == 2_500_000

    def test_immutable(self):
        counts = RecordCounts()
        with pytest.raises(Exception):
            counts.orders = 999  # type: ignore[misc]


class TestGeneratorConfigDefaults:
    def test_default_seed(self):
        cfg = GeneratorConfig()
        assert cfg.seed == 42

    def test_default_scale_factor_from_env(self, monkeypatch):
        monkeypatch.setenv("SCALE_FACTOR", "0.5")
        cfg = GeneratorConfig()
        assert cfg.scale_factor == 0.5

    def test_date_range_valid(self):
        cfg = GeneratorConfig()
        assert cfg.start_date < cfg.end_date

    def test_total_days_positive(self):
        cfg = GeneratorConfig()
        assert cfg.total_days > 365

    def test_run_timestamp_format(self):
        cfg = GeneratorConfig()
        ts = cfg.run_timestamp
        assert len(ts) == 15  # YYYYMMDD_HHMMSS
        assert "_" in ts


class TestScaledCount:
    def test_full_scale(self):
        cfg = GeneratorConfig(scale_factor=1.0)
        assert cfg.scaled_count(1_000_000) == 1_000_000

    def test_dev_scale(self):
        cfg = GeneratorConfig(scale_factor=0.01)
        assert cfg.scaled_count(1_000_000) == 10_000

    def test_minimum_floor(self):
        cfg = GeneratorConfig(scale_factor=0.0001)
        assert cfg.scaled_count(1_000) == 10  # minimum = 10

    def test_counts_property_is_scaled(self):
        cfg = GeneratorConfig(scale_factor=0.01)
        assert cfg.counts.customers == max(10, int(500_000 * 0.01))

    def test_zero_scale_factor_hits_floor(self):
        cfg = GeneratorConfig(scale_factor=0.0)
        assert cfg.scaled_count(100_000) == 10


class TestBusinessRuleWeights:
    def test_segment_weights_sum_to_one(self):
        cfg = GeneratorConfig()
        total = sum(cfg.segment_weights.values())
        assert abs(total - 1.0) < 1e-6

    def test_payment_method_weights_sum_to_one(self):
        cfg = GeneratorConfig()
        total = sum(cfg.payment_method_weights.values())
        assert abs(total - 1.0) < 1e-6

    def test_channel_weights_sum_to_one(self):
        cfg = GeneratorConfig()
        total = sum(cfg.channel_weights.values())
        assert abs(total - 1.0) < 1e-6

    def test_return_reason_weights_sum_to_one(self):
        cfg = GeneratorConfig()
        total = sum(cfg.return_reason_weights.values())
        assert abs(total - 1.0) < 1e-6
