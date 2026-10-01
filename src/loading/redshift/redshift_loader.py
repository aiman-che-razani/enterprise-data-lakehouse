"""
Redshift warehouse loader.

Uses the community spark-redshift connector to write Gold Delta data to Redshift.
Connector coordinates: io.github.spark_redshift_community:spark-redshift_2.12:5.1.0

Write strategy:
  - Dimensions (full-refresh): TRUNCATE + append via preactions
  - Facts: append only — Redshift MERGE (upsert) is handled separately via
    a stored procedure invoked via JDBC after the COPY completes.

Credentials: REDSHIFT_HOST, REDSHIFT_USER, REDSHIFT_PASSWORD (or IAM role in prod)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.loading.base_loader import BaseWarehouseLoader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RedshiftLoader(BaseWarehouseLoader):
    target = "redshift"

    def _write(self, df: DataFrame, table_name: str, write_mode: str) -> None:
        wh = self.wh
        full_table = f"{wh.redshift_schema}.{table_name}"

        pre_action = f"TRUNCATE TABLE {full_table};" if write_mode == "overwrite" else ""

        (
            df.write.format("io.github.spark_redshift_community.spark.redshift")
            .option("url", wh.redshift_jdbc_url)
            .option("dbtable", full_table)
            .option("tempdir", wh.redshift_temp_s3_prefix)
            .option("aws_iam_role", wh.redshift_iam_role or "")
            .option("preactions", pre_action)
            .option("extracopyoptions", "TIMEFORMAT 'auto' TRUNCATECOLUMNS")
            .option("tempformat", "CSV GZIP")
            .mode("append")
            .save()
        )

    def transform(self, df: DataFrame, table_name: str) -> DataFrame:
        # Redshift VARCHAR columns have byte limits — truncate very long strings
        # that could exceed column widths defined in DDL.
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        str_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]
        for col in str_cols:
            if col in ("description",):
                df = df.withColumn(col, F.substring(F.col(col), 1, 500))
            elif col in ("nk_location_key",):
                df = df.withColumn(col, F.substring(F.col(col), 1, 300))
        return df
