"""
Base warehouse loader — orchestrates Gold Delta → warehouse for one target.

Subclasses implement:
  ``_write(df, table_name, write_mode)``  — target-specific write

The base class handles:
  - Reading Gold Delta tables via Spark
  - Dropping internal Delta metadata columns before writing
  - Choosing write mode (overwrite for dims, append for facts)
  - Logging + timing
  - Updating pipeline.watermarks.{target}_last_load
"""

from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg2

from src.common.logging import get_logger
from src.loading.warehouse_config import WarehouseSettings
from src.processing.gold.domain_meta import FULL_REFRESH_DIMS, GOLD_DIM_ORDER, GOLD_FACT_ORDER

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from src.common.config import Settings

# Columns produced by Delta Lake internals that must not land in the warehouse
_DROP_COLS: frozenset[str] = frozenset(
    {
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "_scd_hash",
        "_silver_updated_at",
        "_bronze_version",
        "_silver_processed_at",
        "_silver_date",
        "_date_month",  # Gold Delta partition column; not part of the warehouse schema
    }
)


class BaseWarehouseLoader(ABC):
    """Abstract base for Redshift / Snowflake / BigQuery loaders."""

    target: str  # "redshift" / "snowflake" / "bigquery"

    def __init__(
        self,
        spark: SparkSession,
        settings: Settings,
        wh_settings: WarehouseSettings | None = None,
    ) -> None:
        self.spark = spark
        self.settings = settings
        self.wh = wh_settings or WarehouseSettings()
        self.log = get_logger(f"{__name__}.{self.target}")

    # ─── Public entry points ──────────────────────────────────────────────────

    def load_all(self, batch_id: str | None = None) -> dict[str, Any]:
        """Load all 14 Gold tables into this warehouse target."""
        results: list[dict[str, Any]] = []
        errors = 0
        for table in GOLD_DIM_ORDER + GOLD_FACT_ORDER:
            result = self.load_table(table, batch_id=batch_id)
            results.append(result)
            if result["status"] == "failed":
                errors += 1
        return {
            "target": self.target,
            "tables": len(results),
            "success": len(results) - errors,
            "failed": errors,
            "results": results,
        }

    def load_table(
        self,
        table_name: str,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Load a single Gold table into this warehouse target."""
        start = datetime.now(UTC)
        result: dict[str, Any] = {
            "target": self.target,
            "table": table_name,
            "status": "success",
            "rows": 0,
        }
        try:
            df = self._read_gold(table_name)
            df = self._drop_internal_cols(df)
            df = self.transform(df, table_name)

            write_mode = "overwrite" if table_name in FULL_REFRESH_DIMS else "append"
            self._write(df, table_name, write_mode)

            result["rows"] = df.count()
            result["write_mode"] = write_mode
            result["duration_s"] = round((datetime.now(UTC) - start).total_seconds(), 2)
            self._update_watermark(table_name)
            self.log.info("warehouse_load_done", **result)

        except Exception as exc:
            result["status"] = "failed"
            result["error"] = str(exc)
            result["traceback"] = traceback.format_exc()
            self.log.error(
                "warehouse_load_failed", target=self.target, table=table_name, error=str(exc)
            )

        return result

    # ─── Abstract ─────────────────────────────────────────────────────────────

    @abstractmethod
    def _write(self, df: DataFrame, table_name: str, write_mode: str) -> None:
        """Write DataFrame to the warehouse table."""

    # ─── Override hooks ───────────────────────────────────────────────────────

    def transform(self, df: DataFrame, table_name: str) -> DataFrame:
        """Optional warehouse-specific column transforms. Override in subclasses."""
        return df

    # ─── Private helpers ──────────────────────────────────────────────────────

    def _read_gold(self, table_name: str) -> DataFrame:
        path = self.settings.s3_path("gold", table_name)
        return self.spark.read.format("delta").load(path)

    def _drop_internal_cols(self, df: DataFrame) -> DataFrame:
        cols_to_drop = [c for c in df.columns if c in _DROP_COLS]
        return df.drop(*cols_to_drop) if cols_to_drop else df

    def _update_watermark(self, table_name: str) -> None:
        col = f"{self.target}_last_load"
        try:
            conn = psycopg2.connect(self.settings.postgres_dsn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO pipeline.watermarks (domain, {col})
                    VALUES (%s, NOW())
                    ON CONFLICT (domain) DO UPDATE
                        SET {col} = EXCLUDED.{col}
                    """,
                    (table_name,),
                )
                conn.commit()
            conn.close()
        except Exception as exc:
            # Not fatal: the load succeeded, and re-raising would make Airflow re-run an append-mode
            # fact load (duplicates). Logged at error level so the log-based alert picks it up.
            self.log.error("watermark_update_failed", table=table_name, error=str(exc))
