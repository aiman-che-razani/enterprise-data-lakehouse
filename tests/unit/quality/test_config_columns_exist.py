"""Every column a DQ config names must exist in the table it checks.

Completeness and range checks silently skip a column that is absent, so a typo or a stale name in the
config means the check simply never runs. ~35 such columns once sat in the configs (value-range checks on
``revenue`` and ``unit_price`` included) while the quality reports stayed green.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.quality.domain_config import BRONZE_CONFIGS, GOLD_CONFIGS, DomainCheckConfig

pytest.importorskip("pyspark")

from spark.schemas.bronze_schemas import BRONZE_SCHEMAS

_DDL = (
    Path(__file__).resolve().parents[3] / "warehouse" / "redshift" / "ddl" / "gold_schema.sql"
).read_text(encoding="utf-8")


def _referenced(cfg: DomainCheckConfig) -> set[str]:
    cols = set(cfg.required_columns) | set(cfg.optional_columns) | set(cfg.numeric_ranges)
    cols |= set(cfg.max_null_rates)
    cols |= {fk for fk, _parent, _pk in cfg.fk_checks}
    if cfg.pk_col:
        cols.add(cfg.pk_col)
    if cfg.timestamp_col:
        cols.add(cfg.timestamp_col)
    return cols


def _gold_columns(table: str) -> set[str]:
    body = re.search(rf"CREATE TABLE IF NOT EXISTS gold\.{table}\s*\((.*?)\n\)", _DDL, re.S)
    assert body, f"gold.{table} is missing from the DDL"
    lines = body.group(1).split("\n")
    return {
        ln.split()[0] for ln in lines if ln.strip() and not ln.strip().startswith(("--", "PRIMARY"))
    }


@pytest.mark.parametrize("domain", sorted(BRONZE_CONFIGS))
def test_bronze_config_columns_exist_in_the_bronze_schema(domain):
    # _ingested_at is the audit column the ingestor adds to every Bronze table
    actual = {f.name for f in BRONZE_SCHEMAS[domain].fields} | {"_ingested_at"}
    missing = sorted(_referenced(BRONZE_CONFIGS[domain]) - actual)
    assert not missing, f"bronze.{domain} checks columns that do not exist: {missing}"


@pytest.mark.parametrize("table", sorted(GOLD_CONFIGS))
def test_gold_config_columns_exist_in_the_warehouse_ddl(table):
    missing = sorted(_referenced(GOLD_CONFIGS[table]) - _gold_columns(table))
    assert not missing, f"gold.{table} checks columns that do not exist: {missing}"
