"""The synthetic data generator must emit exactly the columns, in the order, that Bronze reads.

Spark reads the generated CSV files positionally against the Bronze StructType, so a renamed, extra or
re-ordered column does not fail loudly - it lands values in the wrong fields and (with full parsing)
turns every row into a dead-letter record. This test caught four domains drifting apart.
"""

from __future__ import annotations

import csv
import gzip
import json

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("faker")
pytest.importorskip("numpy")

from datasets.generators.__main__ import main as generate
from spark.schemas.bronze_schemas import BRONZE_SCHEMAS


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    import os

    cwd = os.getcwd()
    os.chdir(out)  # the generator writes its registry checkpoint relative to the cwd
    try:
        assert generate(["--scale-factor", "0.0001", "--output-dir", str(out), "--no-summary"]) == 0
    finally:
        os.chdir(cwd)
    return out


def _columns(root, domain):
    for fmt in ("csv", "json", "parquet"):
        folder = root / fmt / domain
        if not folder.exists():
            continue
        first = sorted(folder.glob("*"))[0]
        if first.suffix == ".csv":
            with open(first, encoding="utf-8", newline="") as f:
                return next(csv.reader(f))
        if first.suffix == ".gz":
            with gzip.open(first, "rt", encoding="utf-8") as f:
                return list(json.loads(f.readline()))
        if first.suffix == ".json":
            with open(first, encoding="utf-8") as f:
                return list(json.loads(f.readline()))
        pytest.skip(f"{domain}: {first.suffix} output needs pandas to inspect")
    pytest.fail(f"no generated files for {domain}")


@pytest.mark.parametrize("domain", sorted(BRONZE_SCHEMAS))
def test_generated_columns_match_the_bronze_schema(generated, domain):
    expected = [f.name for f in BRONZE_SCHEMAS[domain].fields]
    assert _columns(generated, domain) == expected
