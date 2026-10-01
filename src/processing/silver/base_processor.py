"""
Base Silver processor — orchestrates the full Bronze → Silver pipeline per domain.

Pipeline stages (in order):
  1. Read Bronze via Change Data Feed (only new Bronze versions since last run)
  2. Deduplicate by PK (keep latest _ingested_at per PK)
  3. Cleanse (cast types, trim, lowercase enums, nullify empties, clip ranges)
  4. Validate business rules (route failures to Silver DLQ)
  5. Tokenise PII (hash email, drop phone/address, bucket DOB)
  6. Domain transform (override hook for domain-specific logic)
  7. Validate the Silver data contract (DATA_CONTRACT_MODE: off | warn | enforce)
  8. Write Silver via SCD2 MERGE (dimensions) or MERGE-by-PK (facts)
  9. Update pipeline.watermarks with Bronze version checkpoint
"""

from __future__ import annotations

import traceback
from abc import ABC
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logging import get_logger
from src.common.utils.delta import merge_update_set
from src.common.utils.errors import WatermarkError
from src.processing.bronze.domain_meta import DOMAIN_PKS
from src.processing.silver.cleanser import SilverCleanser
from src.processing.silver.deduplicator import SilverDeduplicator
from src.processing.silver.domain_meta import (
    SILVER_PII_DOMAINS,
    SILVER_SCD2_DOMAINS,
    SILVER_SCD_COLS,
)
from src.processing.silver.pii_tokenizer import DEV_HASH_KEY, PiiTokenizer
from src.processing.silver.scd_handler import ScdType2Handler
from src.processing.silver.validator import SilverValidator
from src.quality.contracts import ContractEnforcer

if TYPE_CHECKING:
    from src.common.config import Settings

logger = get_logger(__name__)


def _pii_key(settings: Settings) -> str:
    """PII hash key from Settings (a SecretStr); falls back to the dev key for stub settings."""
    secret: Any = getattr(settings, "pii_hash_key", None)
    value: Any = secret.get_secret_value() if hasattr(secret, "get_secret_value") else None
    return value if isinstance(value, str) and value else DEV_HASH_KEY


class BaseSilverProcessor(ABC):
    """
    Abstract base for per-domain Silver processors.

    Subclasses may override:
      - ``domain_transform(df)``     — domain-specific column derivations
      - ``extra_metadata(df)``       — additional Silver metadata columns

    Subclasses must set:
      - ``domain: str``              — e.g. "customers"
    """

    domain: str  # must be set by subclass

    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.settings = settings
        self._cleanser = SilverCleanser()
        self._deduplicator = SilverDeduplicator()
        self._validator = SilverValidator()
        self._tokenizer = PiiTokenizer(_pii_key(settings))
        self._scd_handler = ScdType2Handler(spark)
        self._contract_enforcer = ContractEnforcer(settings)
        self.log = get_logger(f"{__name__}.{self.domain}")

    # ─── Public entry point ───────────────────────────────────────────────────

    def process(self, batch_id: str | None = None) -> dict[str, Any]:
        """
        Run the full Bronze → Silver pipeline for this domain.

        Returns a result dict with keys:
          status, domain, rows_read, rows_written, dlq_rows, bronze_version, duration_s
        """
        start = datetime.now(UTC)
        result: dict[str, Any] = {
            "domain": self.domain,
            "status": "success",
            "rows_read": 0,
            "rows_written": 0,
            "dlq_rows": 0,
            "bronze_version": None,
        }

        try:
            # ── 1. Read Bronze CDF ────────────────────────────────────────────
            bronze_df, bronze_version = self._read_bronze_cdf()
            if bronze_df is None:
                result["status"] = "no_new_data"
                self.log.info("no_new_bronze_data", domain=self.domain)
                return result

            result["bronze_version"] = bronze_version
            rows_read = bronze_df.count()
            result["rows_read"] = rows_read
            self.log.info(
                "bronze_cdf_read", domain=self.domain, rows=rows_read, version=bronze_version
            )

            # ── 2. Deduplicate ────────────────────────────────────────────────
            pk_col = DOMAIN_PKS[self.domain]
            deduped_df = self._deduplicator.deduplicate(bronze_df, pk_col)

            # ── 3. Cleanse ────────────────────────────────────────────────────
            cleansed_df = self._cleanser.cleanse(deduped_df, self.domain)

            # ── 4. Validate ───────────────────────────────────────────────────
            valid_df, invalid_df = self._validator.validate(cleansed_df, self.domain)
            if invalid_df is not None:
                dlq_rows = self._write_silver_dlq(invalid_df)
                result["dlq_rows"] = dlq_rows

            # ── 5. Tokenise PII ───────────────────────────────────────────────
            if self.domain in SILVER_PII_DOMAINS:
                valid_df = self._tokenizer.tokenize(valid_df, self.domain)

            # ── 6. Domain transform ───────────────────────────────────────────
            transformed_df = self.domain_transform(valid_df)

            # ── 7. Add Silver metadata columns ────────────────────────────────
            transformed_df = self._add_silver_metadata(transformed_df, bronze_version, batch_id)
            transformed_df = self.extra_metadata(transformed_df)

            # ── 7b. Validate the published Silver data contract ───────────────
            # warn (default): log + persist violations; enforce: fail the run.
            self._contract_enforcer.check(transformed_df, self.domain, run_id=batch_id)

            # ── 8. Write Silver ───────────────────────────────────────────────
            silver_path = self.settings.s3_path("silver", self.domain)
            rows_written = self._write_silver(transformed_df, silver_path, pk_col)
            result["rows_written"] = rows_written

            # ── 9. Update watermark ───────────────────────────────────────────
            self._update_watermark(bronze_version, rows_written)

            elapsed = (datetime.now(UTC) - start).total_seconds()
            result["duration_s"] = round(elapsed, 2)
            self.log.info("silver_done", **result)

        except Exception as exc:
            result["status"] = "failed"
            result["error"] = str(exc)
            result["traceback"] = traceback.format_exc()
            self.log.error("silver_failed", domain=self.domain, error=str(exc))
            raise

        return result

    # ─── Override hooks ───────────────────────────────────────────────────────

    def domain_transform(self, df: DataFrame) -> DataFrame:
        """Optional domain-specific transformations. Override in subclasses."""
        return df

    def extra_metadata(self, df: DataFrame) -> DataFrame:
        """Optional extra metadata columns. Override in subclasses."""
        return df

    # ─── Private pipeline stages ──────────────────────────────────────────────

    def _read_bronze_cdf(self) -> tuple[DataFrame | None, int | None]:
        """
        Read new records from Bronze using Delta Change Data Feed.

        Returns (DataFrame, latest_bronze_version) or (None, None) if no new data.
        The starting version comes from pipeline.watermarks.silver_bronze_version.
        """
        bronze_path = self.settings.s3_path("bronze", self.domain)
        last_version = self._get_last_bronze_version()

        try:
            latest_version = self._get_bronze_latest_version(bronze_path)
        except Exception:
            return None, None

        if last_version is not None and last_version >= latest_version:
            return None, None

        read_opts: dict[str, Any] = {
            "readChangeFeed": "true",
            "startingVersion": (last_version + 1) if last_version is not None else 0,
        }

        df = (
            self.spark.read.format("delta")
            .options(**read_opts)
            .load(bronze_path)
            # CDF adds _change_type, _commit_version, _commit_timestamp
            # Keep only INSERT and UPDATE_AFTER rows (ignore DELETE / UPDATE_BEFORE)
            .filter(F.col("_change_type").isin("insert", "update_postimage"))
            .drop("_change_type", "_commit_version", "_commit_timestamp")
        )

        return df, latest_version

    def _get_bronze_latest_version(self, bronze_path: str) -> int:
        """Return the latest Delta version for a Bronze table."""
        history = (
            self.spark.sql(f"DESCRIBE HISTORY delta.`{bronze_path}`")
            .orderBy(F.col("version").desc())
            .limit(1)
            .select("version")
            .collect()
        )
        if not history:
            raise RuntimeError(f"No Delta history found at {bronze_path}")
        return int(history[0]["version"])

    def _write_silver(
        self,
        df: DataFrame,
        silver_path: str,
        pk_col: str,
    ) -> int:
        """Write Silver records via SCD2 MERGE (dimensions) or upsert MERGE (facts)."""
        if self.domain in SILVER_SCD2_DOMAINS:
            scd_cols = SILVER_SCD_COLS.get(self.domain, [])
            return self._scd_handler.merge(df, silver_path, pk_col, scd_cols)
        else:
            return self._merge_fact(df, silver_path, pk_col)

    def _merge_fact(self, df: DataFrame, silver_path: str, pk_col: str) -> int:
        """
        MERGE fact records into Silver by PK.

        Existing rows are replaced (UPDATE); new rows are inserted.
        This is NOT SCD2 — facts are mutable (e.g. order status changes).
        """
        from delta import DeltaTable

        table_exists = self._scd_handler._table_exists(silver_path)

        if not table_exists:
            df.write.format("delta").partitionBy("_silver_date").mode("overwrite").save(silver_path)
            count = df.count()
            self.log.info("fact_initial_write", path=silver_path, rows=count)
            return count

        dt = DeltaTable.forPath(self.spark, silver_path)
        {c: f"source.{c}" for c in df.columns}

        (
            dt.alias("target")
            .merge(df.alias("source"), f"target.{pk_col} = source.{pk_col}")
            .whenMatchedUpdate(set=merge_update_set(df.columns, ["_silver_date"]))
            .whenNotMatchedInsertAll()
            .execute()
        )

        return df.count()

    def _write_silver_dlq(self, df: DataFrame) -> int:
        """Route validation failures to the Silver DLQ Delta table."""
        dlq_path = self.settings.s3_path("silver_dlq", self.domain)
        (
            df.withColumn("_dlq_timestamp", F.current_timestamp())
            .withColumn("_dlq_domain", F.lit(self.domain))
            .write.format("delta")
            .mode("append")
            .save(dlq_path)
        )
        count = df.count()
        self.log.warning("silver_dlq_written", domain=self.domain, rows=count)
        return count

    def _add_silver_metadata(
        self,
        df: DataFrame,
        bronze_version: int | None,
        batch_id: str | None,
    ) -> DataFrame:
        """Add standard Silver metadata columns."""
        now = F.current_timestamp()
        return (
            df.withColumn("_bronze_version", F.lit(bronze_version))
            .withColumn("_silver_processed_at", now)
            .withColumn("_silver_date", F.to_date(now))
            .withColumn("_batch_id", F.lit(batch_id or ""))
            .withColumn("_pipeline_version", F.lit(self.settings.pipeline_version))
        )

    # ─── Watermark helpers (PostgreSQL pipeline.watermarks) ───────────────────

    def _get_last_bronze_version(self) -> int | None:
        """Read silver_bronze_version checkpoint from pipeline.watermarks."""
        try:
            conn = psycopg2.connect(self.settings.postgres_dsn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT silver_bronze_version
                    FROM pipeline.watermarks
                    WHERE domain = %s
                    """,
                    (self.domain,),
                )
                row = cur.fetchone()
            conn.close()
            return row[0] if row and row[0] is not None else None
        except Exception as exc:
            # Returning None here would silently reprocess from Bronze version 0.
            self.log.error("watermark_read_failed", domain=self.domain, error=str(exc))
            raise WatermarkError(f"cannot read silver checkpoint for {self.domain}: {exc}") from exc

    def _update_watermark(self, bronze_version: int | None, rows_written: int) -> None:
        """Upsert silver_bronze_version into pipeline.watermarks."""
        if bronze_version is None:
            return
        try:
            conn = psycopg2.connect(self.settings.postgres_dsn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline.watermarks
                        (domain, silver_bronze_version, silver_last_run, silver_rows_written)
                    VALUES (%s, %s, NOW(), %s)
                    ON CONFLICT (domain) DO UPDATE
                        SET silver_bronze_version = EXCLUDED.silver_bronze_version,
                            silver_last_run       = EXCLUDED.silver_last_run,
                            silver_rows_written   = EXCLUDED.silver_rows_written
                    """,
                    (self.domain, bronze_version, rows_written),
                )
                conn.commit()
            conn.close()
        except Exception as exc:
            # The Silver MERGE already succeeded and is idempotent: fail the task so it is
            # retried and alerted, rather than leaving a stale checkpoint behind.
            self.log.error("watermark_write_failed", domain=self.domain, error=str(exc))
            raise WatermarkError(
                f"cannot write silver checkpoint for {self.domain}: {exc}"
            ) from exc
