"""
Unit tests for Silver PII tokenisation (pure Python helpers, no PySpark required).
"""

from __future__ import annotations

import hashlib
from datetime import date

from src.processing.silver.pii_tokenizer import age_bucket, sha256_email


class TestSha256Email:
    def test_deterministic(self):
        assert sha256_email("user@example.com") == sha256_email("user@example.com")

    def test_normalises_case(self):
        assert sha256_email("User@Example.COM") == sha256_email("user@example.com")

    def test_trims_whitespace(self):
        assert sha256_email("  user@example.com  ") == sha256_email("user@example.com")

    def test_returns_64_char_hex(self):
        result = sha256_email("user@example.com")
        assert len(result) == 64
        int(result, 16)  # raises if not valid hex

    def test_different_emails_produce_different_hashes(self):
        assert sha256_email("alice@example.com") != sha256_email("bob@example.com")

    def test_matches_stdlib(self):
        raw = "user@example.com"
        expected = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        assert sha256_email(raw) == expected


class TestAgeBucket:
    def _dob_for_age(self, age: int) -> date:
        today = date.today()
        return today.replace(year=today.year - age)

    def test_unknown_for_none(self):
        assert age_bucket(None) == "Unknown"

    def test_invalid_string_returns_unknown(self):
        assert age_bucket("not-a-date") == "Unknown"

    def test_under_18(self):
        assert age_bucket(self._dob_for_age(10)) == "0-17"

    def test_young_adult(self):
        assert age_bucket(self._dob_for_age(22)) == "18-24"

    def test_25_to_34(self):
        assert age_bucket(self._dob_for_age(30)) == "25-34"

    def test_35_to_44(self):
        assert age_bucket(self._dob_for_age(40)) == "35-44"

    def test_senior(self):
        assert age_bucket(self._dob_for_age(80)) == "75+"

    def test_accepts_iso_string(self):
        dob = self._dob_for_age(30)
        result = age_bucket(dob.isoformat())
        assert result == "25-34"

    def test_accepts_date_object(self):
        dob = self._dob_for_age(30)
        assert age_bucket(dob) == "25-34"
