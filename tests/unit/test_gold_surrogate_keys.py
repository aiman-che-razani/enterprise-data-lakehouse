"""
Unit tests for Gold surrogate key generation (requires PySpark).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src.processing.gold.surrogate_keys import add_sk, date_to_sk


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_gold_sk")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


class TestAddSk:
    def _df(self, spark, rows, cols):
        schema = StructType([StructField(c, StringType()) for c in cols])
        return spark.createDataFrame(rows, schema)

    def test_single_col_sk_is_long(self, spark):
        df = self._df(spark, [("abc",)], ["customer_id"])
        result = add_sk(df, "sk_customer_id", "customer_id")
        sk_type = result.schema["sk_customer_id"].dataType
        assert sk_type == LongType()

    def test_deterministic_same_input(self, spark):
        df1 = self._df(spark, [("cust-001",)], ["customer_id"])
        df2 = self._df(spark, [("cust-001",)], ["customer_id"])
        sk1 = add_sk(df1, "sk", "customer_id").first()["sk"]
        sk2 = add_sk(df2, "sk", "customer_id").first()["sk"]
        assert sk1 == sk2

    def test_different_inputs_produce_different_sks(self, spark):
        df = self._df(spark, [("a",), ("b",)], ["id"])
        result = add_sk(df, "sk", "id").collect()
        assert result[0]["sk"] != result[1]["sk"]

    def test_composite_key(self, spark):
        schema = StructType(
            [
                StructField("city", StringType()),
                StructField("state", StringType()),
                StructField("country", StringType()),
            ]
        )
        df = spark.createDataFrame([("Seattle", "WA", "US")], schema)
        result = add_sk(df, "sk_location_id", "city", "state", "country")
        assert result.schema["sk_location_id"].dataType == LongType()
        assert result.first()["sk_location_id"] is not None

    def test_null_natural_key_produces_sk(self, spark):
        df = self._df(spark, [(None,)], ["id"])
        result = add_sk(df, "sk", "id")
        # xxhash64(null) is not null in Spark — it hashes the string "null"
        assert result.first()["sk"] is not None


class TestDateToSk:
    def test_date_sk_format(self, spark):
        from pyspark.sql.types import DateType

        schema = StructType([StructField("order_date", DateType())])
        import datetime

        df = spark.createDataFrame([(datetime.date(2025, 6, 1),)], schema)
        result = df.withColumn("sk", date_to_sk("order_date"))
        sk_val = result.first()["sk"]
        assert sk_val == 20250601

    def test_date_sk_is_int(self, spark):
        import datetime

        from pyspark.sql.types import DateType

        schema = StructType([StructField("dt", DateType())])
        df = spark.createDataFrame([(datetime.date(2020, 1, 1),)], schema)
        result = df.withColumn("sk", date_to_sk("dt"))
        assert result.schema["sk"].dataType == IntegerType()
        assert result.first()["sk"] == 20200101
