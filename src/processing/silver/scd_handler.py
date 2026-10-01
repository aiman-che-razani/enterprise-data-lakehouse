"""
SCD Type 2 handler for Silver dimension tables.

Implementation uses a two-pass Delta MERGE strategy:

Pass 1 — Expire changed records:
    MERGE target USING source
    ON target.pk = source.pk AND target.is_current = true AND target._scd_hash != source._scd_hash
    WHEN MATCHED → UPDATE: set is_current=false, effective_to=now

Pass 2 — Insert new records (net-new + changed versions):
    MERGE target USING (source WHERE no matching current row OR hash changed)
    WHEN NOT MATCHED → INSERT

This approach handles:
  - Net-new PKs → new current record
  - Changed attributes → close old, open new version
  - Unchanged records → no-op
  - Late arrivals → treated as new if hash differs from current
"""

from __future__ import annotations

from datetime import UTC, datetime

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, TimestampType

from src.common.logging import get_logger

logger = get_logger(__name__)


class ScdType2Handler:
    """
    Applies SCD Type 2 MERGE to a Silver dimension table.

    Usage:
        handler = ScdType2Handler(spark)
        rows_written = handler.merge(
            updates_df=new_data,
            silver_path="s3a://retailnexus-lakehouse/silver/customers",
            pk_col="customer_id",
            scd_cols=["segment", "loyalty_tier", "is_active"],
        )
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.log = get_logger(__name__)

    def merge(
        self,
        updates_df: DataFrame,
        silver_path: str,
        pk_col: str,
        scd_cols: list[str],
        now: datetime | None = None,
    ) -> int:
        """
        Perform SCD2 MERGE.

        Args:
            updates_df:  Incoming records from Silver processing pipeline.
            silver_path: S3/local path to the Silver Delta table.
            pk_col:      Primary key column name.
            scd_cols:    Columns whose changes trigger a new SCD version.
            now:         Effective timestamp (default: current UTC time).

        Returns:
            Number of rows written (inserts + updates).
        """
        effective_ts = now or datetime.now(UTC)
        effective_ts_str = effective_ts.isoformat()

        # Compute a stable hash of the SCD columns for change detection
        updates_df = self._add_scd_hash(updates_df, scd_cols)
        updates_df = self._add_scd_columns(updates_df, effective_ts_str)

        silver_exists = self._table_exists(silver_path)

        if not silver_exists:
            # First load — write directly without MERGE
            updates_df.write.format("delta").mode("overwrite").save(silver_path)
            count = updates_df.count()
            self.log.info("scd2_initial_load", path=silver_path, rows=count)
            return count

        dt = DeltaTable.forPath(self.spark, silver_path)

        # ── Pass 1: Expire changed current records ─────────────────────────
        (
            dt.alias("target")
            .merge(
                updates_df.alias("source"),
                f"""
                target.{pk_col} = source.{pk_col}
                AND target.is_current = true
                AND target._scd_hash != source._scd_hash
                """,
            )
            .whenMatchedUpdate(
                set={
                    "is_current": F.lit(False),
                    "effective_to": F.lit(effective_ts_str).cast(TimestampType()),
                    "_silver_updated_at": F.current_timestamp(),
                }
            )
            .execute()
        )

        # ── Pass 2: Insert new versions (net-new + changed) ────────────────
        # "New" = no matching current record with the same hash
        current_target = (
            self.spark.read.format("delta")
            .load(silver_path)
            .filter(F.col("is_current") == True)  # noqa: E712
            .select(pk_col, "_scd_hash")
        )

        source_to_insert = updates_df.alias("src").join(
            current_target.alias("tgt"),
            on=(F.col(f"src.{pk_col}") == F.col(f"tgt.{pk_col}"))
            & (F.col("src._scd_hash") == F.col("tgt._scd_hash")),
            how="left_anti",  # rows NOT in current target with same hash
        )

        insert_count = source_to_insert.count()
        if insert_count > 0:
            source_to_insert.write.format("delta").mode("append").save(silver_path)
            self.log.info("scd2_inserts", path=silver_path, rows=insert_count)

        return insert_count

    # ─── Private helpers ──────────────────────────────────────────────────────

    def _add_scd_hash(self, df: DataFrame, scd_cols: list[str]) -> DataFrame:
        """Compute SHA-256 hash of SCD column values for change detection."""
        available = [c for c in scd_cols if c in df.columns]
        hash_input = F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in available],
        )
        return df.withColumn("_scd_hash", F.sha2(hash_input, 256))

    def _add_scd_columns(self, df: DataFrame, effective_ts_str: str) -> DataFrame:
        """Add SCD2 bookkeeping columns if not already present."""
        if "is_current" not in df.columns:
            df = df.withColumn("is_current", F.lit(True).cast(BooleanType()))
        if "effective_from" not in df.columns:
            df = df.withColumn(
                "effective_from",
                F.lit(effective_ts_str).cast(TimestampType()),
            )
        if "effective_to" not in df.columns:
            df = df.withColumn("effective_to", F.lit(None).cast(TimestampType()))
        if "_silver_updated_at" not in df.columns:
            df = df.withColumn("_silver_updated_at", F.current_timestamp())
        return df

    def _table_exists(self, path: str) -> bool:
        from pyspark.errors import AnalysisException

        try:
            self.spark.read.format("delta").load(path).limit(0).collect()
            return True
        except AnalysisException:
            return False
