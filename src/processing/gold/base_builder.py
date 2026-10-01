"""
Base Gold builder — orchestrates Silver → Gold transformation per table.

Architecture:
  - Dimensions: full OVERWRITE on each run (Silver is the SOR; Gold is current-state snapshot)
  - Facts: MERGE on natural PK (upsert semantics; handles late-arriving Silver updates)

Subclasses implement:
  - ``build_dim(self) -> DataFrame``  for dimension tables
  - ``build_fact(self) -> DataFrame`` for fact tables
"""

from __future__ import annotations

import traceback
from abc import ABC
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg2
from delta import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logging import get_logger
from src.common.utils.delta import merge_update_set
from src.common.utils.errors import WatermarkError
from src.processing.gold.domain_meta import (
    FACT_PARTITION_COLS,
    FACT_PRIMARY_KEYS,
    FACT_STABLE_PARTITION,
    FACT_ZORDER_COLS,
    PARTITION_COL,
)

if TYPE_CHECKING:
    from src.common.config import Settings


class BaseGoldBuilder(ABC):
    """
    Abstract base for per-table Gold builders.

    Subclasses set:
      table_name: str   — e.g. "dim_customer" or "fact_sales"
      is_dim: bool      — True for dimensions, False for facts

    Subclasses implement either ``build_dim()`` or ``build_fact()``.
    """

    table_name: str
    is_dim: bool = True

    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.settings = settings
        self.log = get_logger(f"{__name__}.{self.table_name}")

    # ─── Public entry point ───────────────────────────────────────────────────

    def build(self, batch_id: str | None = None) -> dict[str, Any]:
        start = datetime.now(UTC)
        result: dict[str, Any] = {
            "table": self.table_name,
            "status": "success",
            "rows_written": 0,
        }
        try:
            df = self.build_dim() if self.is_dim else self.build_fact()
            df = self._add_gold_metadata(df, batch_id)

            gold_path = self.settings.s3_path("gold", self.table_name)

            # Every dimension is a full refresh (FULL_REFRESH_DIMS lists them all); facts MERGE on PK.
            if self.is_dim:
                rows = self._write_overwrite(df, gold_path)
            else:
                rows = self._write_merge(df, gold_path)

            self._optimize(gold_path)
            self._update_watermark(rows)

            result["rows_written"] = rows
            result["duration_s"] = round((datetime.now(UTC) - start).total_seconds(), 2)
            self.log.info("gold_build_done", **result)

        except Exception as exc:
            result["status"] = "failed"
            result["error"] = str(exc)
            result["traceback"] = traceback.format_exc()
            self.log.error("gold_build_failed", table=self.table_name, error=str(exc))
            raise

        return result

    # ─── Abstract methods ─────────────────────────────────────────────────────

    def build_dim(self) -> DataFrame:
        raise NotImplementedError(f"{self.__class__.__name__} must implement build_dim()")

    def build_fact(self) -> DataFrame:
        raise NotImplementedError(f"{self.__class__.__name__} must implement build_fact()")

    # ─── Silver reader helpers ─────────────────────────────────────────────────

    def read_silver(self, domain: str) -> DataFrame:
        """Read current Silver Delta table for a domain."""
        path = self.settings.s3_path("silver", domain)
        return self.spark.read.format("delta").load(path)

    def read_silver_current(self, domain: str) -> DataFrame:
        """Read only is_current=True rows from a Silver SCD2 dimension."""
        return self.read_silver(domain).filter(F.col("is_current") == True)  # noqa: E712

    def read_gold(self, table_name: str) -> DataFrame:
        """Read an existing Gold Delta table (used by facts to join dim SKs)."""
        path = self.settings.s3_path("gold", table_name)
        return self.spark.read.format("delta").load(path)

    # ─── Write strategies ─────────────────────────────────────────────────────

    def _write_overwrite(self, df: DataFrame, path: str) -> int:
        count = df.count()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        self.log.info("gold_overwrite", path=path, rows=count)
        return count

    def _write_merge(self, df: DataFrame, path: str) -> int:
        pk_col = FACT_PRIMARY_KEYS.get(self.table_name)
        partition_cols = FACT_PARTITION_COLS.get(self.table_name, [PARTITION_COL])
        df = self._with_partition_columns(df)

        if not self._table_exists(path):
            count = df.count()
            df.write.format("delta").partitionBy(*partition_cols).mode("overwrite").save(path)
            self.log.info("gold_initial_write", path=path, rows=count)
            return count

        self._repartition_if_needed(path, partition_cols)

        condition = f"target.{pk_col} = source.{pk_col}"
        stable = self.table_name in FACT_STABLE_PARTITION
        if stable:
            # the key's month never changes: prune the merge to the months present in this batch
            condition += f" AND target.{PARTITION_COL} = source.{PARTITION_COL}"
        # _gold_date means "the day this row first landed", so a match never rewrites it; the partition
        # column is only protected when it is stable (a moving date must be able to move the row).
        keep = ["_gold_date", *(partition_cols if stable else [])]

        dt = DeltaTable.forPath(self.spark, path)
        (
            dt.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdate(set=merge_update_set(df.columns, keep))
            .whenNotMatchedInsertAll()
            .execute()
        )
        count = df.count()
        self.log.info("gold_merge", path=path, rows=count)
        return count

    def _with_partition_columns(self, df: DataFrame) -> DataFrame:
        """Add the business-month partition column (yyyymm) from sk_date_id when it is missing."""
        if PARTITION_COL in df.columns or "sk_date_id" not in df.columns:
            return df
        return df.withColumn(PARTITION_COL, (F.col("sk_date_id") / 100).cast("int"))

    def _repartition_if_needed(self, path: str, partition_cols: list[str]) -> None:
        """One-off migration: rewrite a table that is still partitioned by the old processing-day column."""
        current = DeltaTable.forPath(self.spark, path).detail().select("partitionColumns").first()
        if current is None or list(current[0]) == partition_cols:
            return
        self.log.warning("gold_repartition", path=path, was=list(current[0]), now=partition_cols)
        existing = self._with_partition_columns(self.spark.read.format("delta").load(path))
        (
            existing.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(*partition_cols)
            .save(path)
        )

    def _optimize(self, path: str) -> None:
        zorder_cols = FACT_ZORDER_COLS.get(self.table_name)
        try:
            if zorder_cols:
                cols_str = ", ".join(zorder_cols)
                self.spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols_str})")
            else:
                self.spark.sql(f"OPTIMIZE delta.`{path}`")
        except Exception as exc:
            self.log.warning("gold_optimize_failed", path=path, error=str(exc))

    def _add_gold_metadata(self, df: DataFrame, batch_id: str | None) -> DataFrame:
        return (
            df.withColumn("_gold_processed_at", F.current_timestamp())
            .withColumn("_gold_date", F.to_date(F.current_timestamp()))
            .withColumn("_batch_id", F.lit(batch_id or ""))
            .withColumn("_pipeline_version", F.lit(self.settings.pipeline_version))
        )

    def _table_exists(self, path: str) -> bool:
        try:
            self.spark.read.format("delta").load(path).limit(0).collect()
            return True
        except AnalysisException:
            return False

    # ─── Watermark ────────────────────────────────────────────────────────────

    def _update_watermark(self, rows_written: int) -> None:
        try:
            conn = psycopg2.connect(self.settings.postgres_dsn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline.watermarks (domain, gold_last_run, gold_rows_written)
                    VALUES (%s, NOW(), %s)
                    ON CONFLICT (domain) DO UPDATE
                        SET gold_last_run      = EXCLUDED.gold_last_run,
                            gold_rows_written  = EXCLUDED.gold_rows_written
                    """,
                    (self.table_name, rows_written),
                )
                conn.commit()
            conn.close()
        except Exception as exc:
            # The Gold write is idempotent (overwrite / MERGE): fail so the task is retried.
            self.log.error("watermark_write_failed", table=self.table_name, error=str(exc))
            raise WatermarkError(
                f"cannot write gold watermark for {self.table_name}: {exc}"
            ) from exc
