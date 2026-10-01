"""
Unit tests for Silver domain metadata consistency.
"""

from __future__ import annotations

from src.processing.bronze.domain_meta import BATCH_INGESTION_ORDER
from src.processing.silver.domain_meta import (
    DOMAIN_TIMESTAMP_COLS,
    SILVER_PII_DOMAINS,
    SILVER_PII_HANDLING,
    SILVER_PROCESSING_ORDER,
    SILVER_SCD2_DOMAINS,
    SILVER_SCD_COLS,
)


class TestSilverProcessingOrder:
    def test_all_bronze_domains_present(self):
        missing = set(BATCH_INGESTION_ORDER) - set(SILVER_PROCESSING_ORDER)
        assert not missing, f"Domains in Bronze but not Silver order: {missing}"

    def test_no_extra_domains(self):
        extra = set(SILVER_PROCESSING_ORDER) - set(BATCH_INGESTION_ORDER)
        assert not extra, f"Domains in Silver order but not Bronze: {extra}"

    def test_16_domains(self):
        assert len(SILVER_PROCESSING_ORDER) == 16


class TestScd2Metadata:
    def test_all_scd2_domains_have_scd_cols(self):
        for domain in SILVER_SCD2_DOMAINS:
            assert domain in SILVER_SCD_COLS, f"{domain} missing from SILVER_SCD_COLS"
            assert len(SILVER_SCD_COLS[domain]) > 0

    def test_scd2_domains_are_dimensions(self):
        expected = {"customers", "products", "suppliers", "stores", "employees"}
        assert expected == SILVER_SCD2_DOMAINS


class TestPiiMetadata:
    def test_pii_domains_subset_of_processing_order(self):
        assert SILVER_PII_DOMAINS.issubset(set(SILVER_PROCESSING_ORDER))

    def test_all_pii_domains_have_handling_rules(self):
        for domain in SILVER_PII_DOMAINS:
            assert domain in SILVER_PII_HANDLING, f"{domain} missing from SILVER_PII_HANDLING"

    def test_valid_strategies(self):
        valid = {"hash", "drop", "age_bucket"}
        for domain, rules in SILVER_PII_HANDLING.items():
            for col, strategy in rules.items():
                assert strategy in valid, f"{domain}.{col} has unknown strategy '{strategy}'"

    def test_customers_email_hashed(self):
        assert SILVER_PII_HANDLING["customers"]["email"] == "hash"

    def test_customers_phone_dropped(self):
        assert SILVER_PII_HANDLING["customers"]["phone"] == "drop"

    def test_customers_dob_age_bucketed(self):
        assert SILVER_PII_HANDLING["customers"]["date_of_birth"] == "age_bucket"


class TestTimestampCols:
    def test_orders_has_created_at(self):
        assert "created_at" in DOMAIN_TIMESTAMP_COLS.get("orders", [])

    def test_no_empty_lists(self):
        # Every domain that has an entry must have at least one timestamp col
        for domain, cols in DOMAIN_TIMESTAMP_COLS.items():
            assert len(cols) > 0, f"{domain} has empty timestamp col list"
