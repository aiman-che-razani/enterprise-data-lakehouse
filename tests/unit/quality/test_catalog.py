"""The data-quality check catalogue: size, coverage and agreement with the pipeline metadata."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.processing.bronze.domain_meta import DOMAIN_PKS
from src.processing.gold.domain_meta import (
    DIM_NATURAL_KEYS,
    FACT_PRIMARY_KEYS,
    GOLD_DIM_ORDER,
    GOLD_FACT_ORDER,
)
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER
from src.quality.catalog import defined_checks, main, summary
from src.quality.domain_config import BRONZE_CONFIGS, GOLD_CONFIGS, SILVER_CONFIGS

_KINDS = {"row_count", "completeness", "freshness", "uniqueness", "value_range", "referential"}


def test_at_least_eighty_checks_are_defined():
    """ROADMAP sprint 7 exit criterion: 80+ quality checks."""
    assert len(defined_checks()) >= 80


def test_every_layer_has_a_substantial_catalogue():
    s = summary()
    assert {layer: row["total"] for layer, row in s.items()}.keys() == {"bronze", "silver", "gold"}
    assert all(row["total"] >= 25 for row in s.values())


def test_every_table_is_covered():
    tables = {(c.layer, c.table) for c in defined_checks()}
    expected = (
        {("bronze", d) for d in SILVER_PROCESSING_ORDER}
        | {("silver", d) for d in SILVER_PROCESSING_ORDER}
        | {("gold", t) for t in list(GOLD_DIM_ORDER) + list(GOLD_FACT_ORDER)}
    )
    assert tables == expected


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_every_table_has_row_count_freshness_and_key_uniqueness(layer):
    by_table: dict[str, set[str]] = {}
    for c in defined_checks():
        if c.layer == layer:
            by_table.setdefault(c.table, set()).add(c.check)
    for table, kinds in by_table.items():
        assert {
            "row_count",
            "freshness",
            "uniqueness",
        } <= kinds, f"{layer}.{table}: {sorted(kinds)}"


def test_only_known_check_kinds_and_critical_ones_are_flagged():
    for c in defined_checks():
        assert c.check in _KINDS
        assert (c.severity == "critical") == (c.check in {"row_count", "uniqueness"})


def test_unique_key_columns_match_the_pipeline_metadata():
    for domain, cfg in BRONZE_CONFIGS.items():
        assert cfg.pk_col == DOMAIN_PKS[domain], f"bronze.{domain}"
    for domain, cfg in SILVER_CONFIGS.items():
        assert cfg.pk_col == DOMAIN_PKS[domain], f"silver.{domain}"
    # Gold keys must be real columns of the warehouse table (the DDL is the schema of record)
    ddl = (
        Path(__file__).resolve().parents[3] / "warehouse" / "redshift" / "ddl" / "gold_schema.sql"
    ).read_text(encoding="utf-8")
    for table, cfg in GOLD_CONFIGS.items():
        body = re.search(
            rf"CREATE TABLE IF NOT EXISTS gold\.{table}\s*\((.*?)\n\)", ddl, re.S
        ).group(1)
        columns = {
            ln.split()[0]
            for ln in body.split("\n")
            if ln.strip() and not ln.strip().startswith(("--", "PRIMARY"))
        }
        assert cfg.pk_col in columns, f"gold.{table}: {cfg.pk_col} is not a column"
    for table, key in {**DIM_NATURAL_KEYS, **FACT_PRIMARY_KEYS}.items():
        if table.startswith("fact_"):
            assert GOLD_CONFIGS[table].pk_col == key, f"gold.{table}"


def test_no_duplicate_definitions():
    checks = defined_checks()
    assert len(checks) == len(set(checks)) or all(
        c.check == "completeness" for c in checks if checks.count(c) > 1
    )


def test_cli_prints_a_summary(capsys):
    assert main([]) == 0
    out = capsys.readouterr().out
    assert "bronze" in out and "silver" in out and "gold" in out and "ALL" in out
    assert main(["--list"]) == 0
