"""
Bronze batch ingestor — reads raw synthetic data and writes Delta Lake tables.

Design principles:
  - Idempotent: every run produces identical results for the same input.
    A _batch_id column lets consumers detect and skip re-ingested rows.
  - Schema-enforced: uses pre-defined StructType schemas; corrupt records
    go to a per-domain dead-letter path.
  - Append-only: Bronze is never mutated after write (immutable raw layer).
  - Auditable: _ingested_at, _source_file, _batch_id, _pipeline_version
    columns are added to every row.
  - Change Data Feed: enabled on every table so Silver jobs can read only
    new rows via tableChanges().
  - Partitioned by _ingestion_date (date of the run) for fast pruning.
  - Watermarks: updates pipeline.watermarks in PostgreSQL after each domain.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg2
from delta import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark.schemas.bronze_schemas import get_bronze_schema
from src.common.config.settings import Settings, get_settings
from src.common.logging import bind_context, get_logger
from src.processing.bronze.domain_meta import (
    BATCH_INGESTION_ORDER,
    DOMAIN_FORMATS,
    DOMAIN_PKS,
    DOMAIN_ZORDER,
)
from src.processing.bronze.quality_gates import BronzeQualityGates

_PIPELINE_VERSION = "3.0.0"
_DEAD_LETTER_SUFFIX = "_dlq"


class BronzeBatchIngestor:
    """
    Ingests one or more domains from raw files into Bronze Delta tables.

    Usage:
        spark = create_bronze_spark_session()
        ingestor = BronzeBatchIngestor(spark)
        ingestor.ingest_domain("orders")
        ingestor.ingest_all()
    """

    def __init__(
        self,
        spark: SparkSession,
        settings: Settings | None = None,
        raw_data_dir: str | None = None,
        batch_id: str | None = None,
    ) -> None:
        self.spark = spark
        self.settings = settings or get_settings()
        # A plain path or a URI (s3a://...); Path() would collapse the "//" of a URI.
        self.raw_data_dir = str(raw_data_dir or self.settings.raw_path).rstrip("/")
        self.batch_id = batch_id or str(uuid.uuid4())
        self.run_timestamp = datetime.now(UTC)
        self.ingestion_date = self.run_timestamp.date().isoformat()
        self.log = get_logger(__name__)
        self.quality = BronzeQualityGates()

        bind_context(batch_id=self.batch_id, pipeline="bronze")

    # ─── Public API ───────────────────────────────────────────────────────────

    def ingest_all(self, domains: list[str] | None = None) -> dict[str, dict[str, Any]]:
        """Ingest all (or a subset of) domains in dependency order."""
        targets = domains or BATCH_INGESTION_ORDER
        ordered = [d for d in BATCH_INGESTION_ORDER if d in targets]

        results: dict[str, dict[str, Any]] = {}
        for domain in ordered:
            try:
                results[domain] = self.ingest_domain(domain)
            except Exception as exc:
                self.log.error(
                    "domain_ingestion_failed",
                    domain=domain,
                    error=str(exc),
                    exc_info=True,
                )
                results[domain] = {"status": "failed", "error": str(exc)}
        return results

    def ingest_domain(self, domain: str) -> dict[str, Any]:
        """
        Read raw source files for a single domain and write to Bronze Delta.

        Returns a dict with ingestion statistics.
        """
        bind_context(domain=domain)
        self.log.info("bronze_ingest_start", domain=domain, batch_id=self.batch_id)

        schema = get_bronze_schema(domain)
        source_format = DOMAIN_FORMATS[domain]
        source_path = self._source_path(domain, source_format)
        bronze_path = self.settings.s3_path("bronze", domain)
        dlq_path = self.settings.s3_path("bronze", domain + _DEAD_LETTER_SUFFIX)

        # ── Read raw ──────────────────────────────────────────────────────────
        raw_df = self._read_raw(source_path, source_format, schema, domain)
        if raw_df is None:
            self.log.warning("no_source_files", domain=domain, path=source_path)
            return {"status": "skipped", "rows_written": 0}

        try:
            # ── Skip files an earlier run already ingested ────────────────────────
            raw_df = self._exclude_ingested_files(raw_df, bronze_path)

            # ── Validate + split good / bad ───────────────────────────────────────
            good_df, bad_df = self.quality.split_valid_records(raw_df, schema, domain)

            if bad_df is not None:
                bad_count = bad_df.count()
                if bad_count > 0:
                    self._write_dead_letter(bad_df, dlq_path, domain)
                    self.log.warning("dead_letter_records", domain=domain, count=bad_count)

            # ── Add metadata columns ──────────────────────────────────────────────
            enriched_df = self._add_metadata(good_df, source_path)

            # ── Write Delta ───────────────────────────────────────────────────────
            rows_written = self._write_delta(enriched_df, bronze_path, domain)

            # ── Post-write: OPTIMIZE + ZORDER ─────────────────────────────────────
            self._optimize(bronze_path, domain)

            # ── Update watermark ──────────────────────────────────────────────────
            self._update_watermark(domain, rows_written)

            result = {
                "status": "success",
                "domain": domain,
                "rows_written": rows_written,
                "batch_id": self.batch_id,
                "bronze_path": bronze_path,
            }
            self.log.info("bronze_ingest_complete", **result)
            return result
        finally:
            # The quality gate caches the parsed frame; Spark's cache is keyed by plan, so a leftover entry
            # would be substituted into the next run's scan of the same folder (and hide its new files).
            raw_df.unpersist()

    # ─── Private: read ────────────────────────────────────────────────────────

    def _source_path(self, domain: str, fmt: str) -> str:
        """Resolve the raw source directory for a domain."""
        return f"{self.raw_data_dir}/{fmt}/{domain}"

    def _read_raw(
        self,
        source_path: str,
        fmt: str,
        schema: StructType,
        domain: str,
    ) -> DataFrame | None:
        """Read source files using the pre-defined StructType schema."""
        spark_path = source_path
        if "://" not in spark_path and not Path(spark_path).exists():
            return None

        # StructType.add() mutates in place; adding to the shared schema would break a second read
        with_corrupt = StructType(list(schema.fields)).add(
            "_corrupt_record", "string", nullable=True
        )
        try:
            if fmt == "json":
                return (
                    self.spark.read.option("multiLine", "false")  # newline-delimited JSON
                    .option("mode", "PERMISSIVE")  # bad rows → _corrupt_record
                    .option("columnNameOfCorruptRecord", "_corrupt_record")
                    .schema(with_corrupt)
                    .json(spark_path)
                )
            elif fmt == "csv":
                return (
                    self.spark.read.option("header", "true")
                    .option("mode", "PERMISSIVE")
                    .option("columnNameOfCorruptRecord", "_corrupt_record")
                    .schema(with_corrupt)
                    .csv(spark_path)
                )
            elif fmt == "parquet":
                # Parquet carries physical types (pandas writes INT64 where the contract says int, and Spark's
                # reader refuses that conversion), so read as stored and cast to the contract's types.
                raw = self.spark.read.parquet(spark_path)
                return raw.select(
                    *(
                        (F.col(f.name) if f.name in raw.columns else F.lit(None))
                        .cast(f.dataType)
                        .alias(f.name)
                        for f in schema.fields
                    )
                )
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        except AnalysisException as exc:
            if "PATH_NOT_FOUND" not in str(exc) and "does not exist" not in str(exc):
                raise  # a schema/format problem is a failure, not "no source files"
            self.log.warning("read_failed", domain=domain, path=spark_path, error=str(exc))
            return None

    # ─── Private: validate ────────────────────────────────────────────────────

    def _exclude_ingested_files(self, df: DataFrame, bronze_path: str) -> DataFrame:
        """Tag each row with its source file and drop rows from files Bronze already holds.

        Every run reads the whole raw folder, so without this each daily run would append all of history
        again. Raw files are treated as immutable (new data arrives as new files), the same contract as
        Auto Loader; a re-delivered file under the same name is deliberately not re-ingested.
        """
        df = df.withColumn("_source_file", F.input_file_name())
        if not DeltaTable.isDeltaTable(self.spark, bronze_path):
            return df
        # A collected set + NOT IN (rather than a join): input_file_name() is only reliable while the
        # rows are still in the scan stage, and a join can move the projection above the shuffle.
        ingested = [
            row[0]
            for row in self.spark.read.format("delta")
            .load(bronze_path)
            .select("_source_file")
            .distinct()
            .collect()
        ]
        return df.filter(~F.col("_source_file").isin(ingested)) if ingested else df

    # ─── Private: enrich ──────────────────────────────────────────────────────

    def _add_metadata(self, df: DataFrame, source_path: str) -> DataFrame:
        """Add audit columns required by every Bronze table."""
        return (
            df.withColumn("_ingested_at", F.lit(self.run_timestamp.isoformat()).cast("timestamp"))
            .withColumn("_ingestion_date", F.lit(self.ingestion_date).cast("date"))
            .withColumn("_batch_id", F.lit(self.batch_id))
            .withColumn("_pipeline_version", F.lit(_PIPELINE_VERSION))
        )

    # ─── Private: write ───────────────────────────────────────────────────────

    def _write_delta(self, df: DataFrame, bronze_path: str, domain: str) -> int:
        """
        Append to Delta table, partitioned by _ingestion_date.

        Returns the number of rows written.
        """
        row_count = df.count()
        (
            df.write.format("delta")
            .mode("append")
            .partitionBy("_ingestion_date")
            .option("mergeSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.logRetentionDuration", "interval 2557 days")
            .option("delta.deletedFileRetentionDuration", "interval 2557 days")
            .save(bronze_path)
        )
        self.log.info("delta_write_complete", domain=domain, path=bronze_path, rows=row_count)
        return row_count

    def _write_dead_letter(self, df: DataFrame, dlq_path: str, domain: str) -> None:
        """Write corrupt/invalid records to a DLQ Delta table for alerting."""
        (
            df.withColumn("_dlq_timestamp", F.current_timestamp())
            .withColumn("_batch_id", F.lit(self.batch_id))
            .write.format("delta")
            .mode("append")
            .save(dlq_path)
        )

    # ─── Private: optimize ────────────────────────────────────────────────────

    def _optimize(self, bronze_path: str, domain: str) -> None:
        """Run OPTIMIZE with ZORDER on the Bronze table after write."""
        zorder_cols = DOMAIN_ZORDER.get(domain, [DOMAIN_PKS[domain]])
        zorder_clause = ", ".join(zorder_cols)
        try:
            self.spark.sql(f"OPTIMIZE delta.`{bronze_path}` ZORDER BY ({zorder_clause})")
            self.log.info("optimize_complete", domain=domain, zorder=zorder_cols)
        except Exception as exc:
            # OPTIMIZE failure is non-fatal — data is already written
            self.log.warning("optimize_failed", domain=domain, error=str(exc))

    # ─── Private: watermark ───────────────────────────────────────────────────

    def _update_watermark(self, domain: str, rows_written: int) -> None:
        """Update pipeline.watermarks in PostgreSQL with the latest run info."""
        s = self.settings
        try:
            conn = psycopg2.connect(
                host=s.postgres_host,
                port=s.postgres_port,
                dbname=s.postgres_db,
                user=s.postgres_user,
                password=s.postgres_password.get_secret_value(),
            )
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO pipeline.watermarks
                            (domain, bronze_last_ingest, bronze_watermark,
                             bronze_rows_written, bronze_last_status)
                        VALUES (%s, NOW(), %s, %s, 'success')
                        ON CONFLICT (domain) DO UPDATE
                            SET bronze_last_ingest  = EXCLUDED.bronze_last_ingest,
                                bronze_watermark    = EXCLUDED.bronze_watermark,
                                bronze_rows_written = EXCLUDED.bronze_rows_written,
                                bronze_last_status  = EXCLUDED.bronze_last_status
                        """,
                    (
                        domain,
                        self.run_timestamp.isoformat(),
                        rows_written,
                    ),
                )
            conn.close()
        except Exception as exc:
            # Watermark failure is non-fatal — Bronze data is still written
            self.log.warning("watermark_update_failed", domain=domain, error=str(exc))
