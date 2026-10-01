"""
Unit tests for Gold domain metadata consistency.
"""

from __future__ import annotations

from src.processing.gold.domain_meta import (
    DIM_NATURAL_KEYS,
    DIM_SK_COL,
    DIM_SOURCE_MAP,
    FACT_PRIMARY_KEYS,
    FACT_ZORDER_COLS,
    FULL_REFRESH_DIMS,
    GOLD_DIM_ORDER,
    GOLD_FACT_ORDER,
    SCD2_DIM_TABLES,
)


class TestDimMetaConsistency:
    def test_all_dims_have_sk_col(self):
        for dim in GOLD_DIM_ORDER:
            assert dim in DIM_SK_COL, f"{dim} missing from DIM_SK_COL"

    def test_all_dims_have_natural_key(self):
        for dim in GOLD_DIM_ORDER:
            assert dim in DIM_NATURAL_KEYS, f"{dim} missing from DIM_NATURAL_KEYS"

    def test_scd2_dims_are_subset_of_gold_dims(self):
        assert SCD2_DIM_TABLES.issubset(set(GOLD_DIM_ORDER))

    def test_full_refresh_dims_cover_all(self):
        assert set(GOLD_DIM_ORDER) == FULL_REFRESH_DIMS

    def test_8_dim_tables(self):
        assert len(GOLD_DIM_ORDER) == 8

    def test_6_fact_tables(self):
        assert len(GOLD_FACT_ORDER) == 6

    def test_dim_source_map_domains_valid(self):
        from src.processing.bronze.domain_meta import BATCH_INGESTION_ORDER

        for domain in DIM_SOURCE_MAP:
            assert domain in BATCH_INGESTION_ORDER, f"{domain} not a valid Silver domain"


class TestFactMetaConsistency:
    def test_all_facts_have_pk(self):
        for fact in GOLD_FACT_ORDER:
            assert fact in FACT_PRIMARY_KEYS, f"{fact} missing from FACT_PRIMARY_KEYS"

    def test_all_facts_have_zorder(self):
        for fact in GOLD_FACT_ORDER:
            assert fact in FACT_ZORDER_COLS, f"{fact} missing from FACT_ZORDER_COLS"
            assert len(FACT_ZORDER_COLS[fact]) > 0

    def test_pk_names_follow_nk_convention(self):
        for fact, pk in FACT_PRIMARY_KEYS.items():
            assert pk.startswith("nk_"), f"{fact} PK '{pk}' must start with 'nk_'"
