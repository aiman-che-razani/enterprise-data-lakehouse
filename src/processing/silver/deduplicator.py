"""
Deduplication for the Silver layer.

Bronze is append-only and idempotent — running the same ingestion twice
appends duplicate rows. Silver deduplicates using a window function that
keeps the latest row per primary key (by _ingested_at timestamp).

For high-cardinality fact tables (clickstream, iot_sensors), deduplication
uses event_id / reading_id directly since they are UUIDs.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.logging import get_logger

logger = get_logger(__name__)


class SilverDeduplicator:
    """
    Removes duplicate rows from a Bronze-sourced DataFrame.

    Strategy:
        Window over (pk_col) ordered by _ingested_at DESC, then keep row_number == 1.
        This preserves the most recently ingested version of each record.
    """

    def deduplicate(
        self,
        df: DataFrame,
        pk_col: str,
        order_col: str = "_ingested_at",
    ) -> DataFrame:
        """
        Return a DataFrame with one row per unique pk_col value
        (the row with the latest order_col value).

        Args:
            df:        Input DataFrame (from Bronze CDF read).
            pk_col:    Primary key column to deduplicate on.
            order_col: Column to use for ordering within duplicates (latest wins).
        """
        if pk_col not in df.columns:
            logger.warning(
                "dedup_pk_missing",
                pk_col=pk_col,
                available=df.columns,
            )
            return df

        order_by_col = F.col(order_col).desc() if order_col in df.columns else F.lit(1)

        window = Window.partitionBy(pk_col).orderBy(order_by_col)
        deduped = (
            df.withColumn("_dedup_rn", F.row_number().over(window))
            .filter(F.col("_dedup_rn") == 1)
            .drop("_dedup_rn")
        )

        return deduped

    def count_duplicates(self, df: DataFrame, pk_col: str) -> int:
        """Return the number of duplicate rows (total rows - distinct PKs)."""
        if pk_col not in df.columns:
            return 0
        total = df.count()
        distinct = df.select(pk_col).distinct().count()
        return total - distinct
