"""
Delta Lake table catalog manager.

Provides a programmatic interface to:
  - Register Bronze/Silver/Gold tables in catalog.tables (PostgreSQL)
  - Query table metadata (row counts, last updated, schema)
  - Read Delta table history for audit and lineage
  - Verify table health (exists, non-empty, not corrupted)
"""

from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras

from src.common.config.settings import Settings, get_settings
from src.common.logging import get_logger

logger = get_logger(__name__)


class DeltaCatalogManager:
    """
    Manages the lakehouse table catalog stored in PostgreSQL (catalog schema).

    All three layers (Bronze / Silver / Gold) are registered here so that
    the API, Airflow, and quality framework have a single lookup point.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.log = get_logger(__name__)

    # ─── Registration ─────────────────────────────────────────────────────────

    def register_table(
        self,
        layer: str,
        domain: str,
        table_path: str,
        row_count: int = 0,
        schema_json: str | None = None,
    ) -> None:
        """Upsert a table entry in catalog.tables."""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO catalog.tables
                        (schema_name, table_name, layer, domain, s3_location,
                         row_count, schema_json, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (schema_name, table_name) DO UPDATE SET
                        row_count   = EXCLUDED.row_count,
                        s3_location = EXCLUDED.s3_location,
                        schema_json = COALESCE(EXCLUDED.schema_json, catalog.tables.schema_json),
                        updated_at  = NOW()
                    """,
                (
                    layer,
                    f"{layer}.{domain}",
                    layer,
                    domain,
                    table_path,
                    row_count,
                    schema_json,
                ),
            )

    def update_row_count(self, layer: str, domain: str, row_count: int) -> None:
        """Update just the row_count and updated_at for an existing table entry."""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE catalog.tables
                    SET row_count = %s, updated_at = NOW()
                    WHERE table_name = %s
                    """,
                (row_count, f"{layer}.{domain}"),
            )

    # ─── Queries ──────────────────────────────────────────────────────────────

    def get_table(self, layer: str, domain: str) -> dict[str, Any] | None:
        """Return catalog entry for a table, or None if not registered."""
        with (
            self._conn() as conn,
            conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,
        ):
            cur.execute(
                "SELECT * FROM catalog.tables WHERE table_name = %s",
                (f"{layer}.{domain}",),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def list_tables(self, layer: str | None = None) -> list[dict[str, Any]]:
        """Return all registered tables, optionally filtered by layer."""
        with (
            self._conn() as conn,
            conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur,
        ):
            if layer:
                cur.execute(
                    "SELECT * FROM catalog.tables WHERE layer = %s ORDER BY domain",
                    (layer,),
                )
            else:
                cur.execute("SELECT * FROM catalog.tables ORDER BY layer, domain")
            return [dict(r) for r in cur.fetchall()]

    def get_bronze_tables(self) -> list[dict[str, Any]]:
        return self.list_tables(layer="bronze")

    # ─── Delta-native metadata (requires PySpark) ─────────────────────────────

    @staticmethod
    def get_delta_history(spark: Any, table_path: str, limit: int = 10) -> list[dict[str, Any]]:
        """Return the last `limit` operations from Delta table history."""
        from delta import DeltaTable

        dt = DeltaTable.forPath(spark, table_path)
        history_df = dt.history(limit)
        return [row.asDict(recursive=True) for row in history_df.collect()]

    @staticmethod
    def get_delta_details(spark: Any, table_path: str) -> dict[str, Any]:
        """Return DESCRIBE DETAIL output for a Delta table."""
        df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
        row = df.first()
        return dict(row.asDict(recursive=True)) if row else {}

    @staticmethod
    def count_rows(spark: Any, table_path: str) -> int:
        """Return the current row count of a Delta table."""
        return int(spark.read.format("delta").load(table_path).count())

    # ─── Health checks ────────────────────────────────────────────────────────

    @staticmethod
    def table_exists(spark: Any, table_path: str) -> bool:
        """Return True if the Delta table exists and is readable."""
        from pyspark.errors import AnalysisException

        try:
            spark.read.format("delta").load(table_path).limit(0).collect()
            return True
        except AnalysisException:
            return False

    # ─── Private ──────────────────────────────────────────────────────────────

    def _conn(self) -> Any:
        s = self.settings
        return psycopg2.connect(
            host=s.postgres_host,
            port=s.postgres_port,
            dbname=s.postgres_db,
            user=s.postgres_user,
            password=s.postgres_password.get_secret_value(),
        )
