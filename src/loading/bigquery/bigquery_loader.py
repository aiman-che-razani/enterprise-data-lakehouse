"""
BigQuery warehouse loader.

Uses the Google Cloud Spark-BigQuery connector.
Connector coordinates: com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0

Write strategy:
  - Dimensions: WRITE_TRUNCATE (full refresh; replaces partition or table)
  - Facts: WRITE_APPEND (incremental; BigQuery handles dedup via partition keys)

Authentication: GOOGLE_APPLICATION_CREDENTIALS env var (service account JSON key)
  In GCP environments (Cloud Dataproc, Vertex AI) uses ADC (Workload Identity).

Temporary GCS bucket: required by the connector for staging Parquet files
  before the BigQuery load job.

Credentials: GOOGLE_APPLICATION_CREDENTIALS, BQ_PROJECT, GCS_TEMP_BUCKET
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.loading.base_loader import BaseWarehouseLoader


class BigQueryLoader(BaseWarehouseLoader):
    target = "bigquery"

    def _write(self, df: DataFrame, table_name: str, write_mode: str) -> None:
        wh = self.wh
        bq_table = f"{wh.bq_project}.{wh.bq_dataset}.{table_name}"
        write_disposition = "WRITE_TRUNCATE" if write_mode == "overwrite" else "WRITE_APPEND"

        write_opts = {
            "table": bq_table,
            "temporaryGcsBucket": wh.gcs_temp_bucket,
            "writeDisposition": write_disposition,
            "createDisposition": "CREATE_IF_NEEDED",
            "partitionField": "_gold_date",
            "partitionType": "DAY",
        }

        if wh.google_application_credentials:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = wh.google_application_credentials

        (
            df.write.format("bigquery")
            .options(**write_opts)
            .mode("overwrite" if write_mode == "overwrite" else "append")
            .save()
        )

    def transform(self, df: DataFrame, table_name: str) -> DataFrame:
        # BigQuery uses INT64 for all integer types; cast surrogate keys explicitly
        sk_cols = [c for c in df.columns if c.startswith("sk_")]
        for col in sk_cols:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(LongType()))
        return df
