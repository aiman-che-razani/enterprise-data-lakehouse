"""`max_null_rates`: per-column null allowances for columns that are legitimately sparse."""

from __future__ import annotations

import pytest

from src.quality.domain_config import (
    BRONZE_CONFIGS,
    GOLD_CONFIGS,
    SILVER_CONFIGS,
    DomainCheckConfig,
)

_ALL = [
    (layer, name, cfg)
    for layer, configs in (
        ("bronze", BRONZE_CONFIGS),
        ("silver", SILVER_CONFIGS),
        ("gold", GOLD_CONFIGS),
    )
    for name, cfg in configs.items()
]


def test_required_optional_and_overridden_columns_are_grouped_by_threshold():
    cfg = DomainCheckConfig(
        required_columns=["id", "store_id"],
        optional_columns=["note", "days"],
        max_null_rates={"store_id": 0.9, "days": 0.4, "other": 0.4},
    )
    plan = {tuple(cols): rate for cols, rate in cfg.completeness_plan(optional_threshold=0.05)}
    assert plan == {("id",): 0.0, ("note",): 0.05, ("days", "other"): 0.4, ("store_id",): 0.9}


def test_an_overridden_column_is_checked_exactly_once():
    cfg = DomainCheckConfig(required_columns=["a"], max_null_rates={"a": 0.5})
    columns = [c for cols, _ in cfg.completeness_plan(0.05) for c in cols]
    assert columns == ["a"]


def test_without_overrides_the_plan_is_the_classic_required_then_optional():
    cfg = DomainCheckConfig(required_columns=["a"], optional_columns=["b"])
    assert cfg.completeness_plan(0.1) == [(["a"], 0.0), (["b"], 0.1)]


@pytest.mark.parametrize(("layer", "name", "cfg"), _ALL, ids=[f"{a}.{b}" for a, b, _ in _ALL])
def test_configured_null_rates_are_valid_probabilities(layer, name, cfg):
    for column, rate in cfg.max_null_rates.items():
        assert 0.0 < rate <= 1.0, f"{layer}.{name}.{column}: {rate}"


@pytest.mark.parametrize(("layer", "name", "cfg"), _ALL, ids=[f"{a}.{b}" for a, b, _ in _ALL])
def test_overrides_only_relax_columns_that_were_already_checked(layer, name, cfg):
    """An override must not introduce a brand-new check by accident: it loosens an existing one."""
    checked = set(cfg.required_columns) | set(cfg.optional_columns)
    assert (
        set(cfg.max_null_rates) <= checked
    ), f"{layer}.{name}: {set(cfg.max_null_rates) - checked}"


@pytest.mark.parametrize(
    ("configs", "column"),
    [
        (BRONZE_CONFIGS, "_ingested_at"),
        (SILVER_CONFIGS, "_silver_processed_at"),
        (GOLD_CONFIGS, "_gold_processed_at"),
    ],
    ids=["bronze", "silver", "gold"],
)
def test_freshness_measures_when_the_pipeline_last_wrote_the_layer(configs, column):
    """An SLA on an event date (order_date, hire_date, ...) can never pass for historical data."""
    assert {name: cfg.timestamp_col for name, cfg in configs.items()} == dict.fromkeys(
        configs, column
    )
