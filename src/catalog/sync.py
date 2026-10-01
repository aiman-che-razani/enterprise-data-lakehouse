"""
Catalog synchroniser — reflects the real Delta tables into ``catalog.tables`` /
``catalog.columns`` so the API (`/catalog/*`) and lineage views describe what is
actually on storage, not what was last registered by hand.

Run via ``scripts/maintenance/sync_catalog.py`` (also wired into ``make sync-catalog``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import psycopg2
import psycopg2.extras

from src.common.logging import get_logger
from src.processing.bronze.domain_meta import DOMAIN_PKS
from src.processing.gold.domain_meta import (
    DIM_NATURAL_KEYS,
    FACT_PRIMARY_KEYS,
    GOLD_DIM_ORDER,
    GOLD_FACT_ORDER,
)
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pyspark.sql import SparkSession

    from src.common.config.settings import Settings

logger = get_logger(__name__)

# Column names that carry personal / payment data wherever they appear.
PII_COLUMNS: frozenset[str] = frozenset(
    {
        "email",
        "phone",
        "address_line1",
        "date_of_birth",
        "first_name",
        "last_name",
        "contact_email",
        "contact_phone",
        "ip_address",
        "gender",
        "anonymous_id",
        "address",
    }
)
PCI_COLUMNS: frozenset[str] = frozenset({"card_last4", "card_exp", "card_network"})


@dataclass(frozen=True)
class TableRef:
    layer: str
    name: str
    primary_key: str | None

    @property
    def qualified(self) -> str:
        return f"{self.layer}.{self.name}"


def all_tables(layers: Iterable[str] = ("bronze", "silver", "gold")) -> list[TableRef]:
    """Every table the platform is expected to publish, per layer."""
    refs: list[TableRef] = []
    layers = tuple(layers)
    for layer in ("bronze", "silver"):
        if layer in layers:
            refs += [TableRef(layer, d, DOMAIN_PKS.get(d)) for d in SILVER_PROCESSING_ORDER]
    if "gold" in layers:
        refs += [TableRef("gold", t, DIM_NATURAL_KEYS.get(t)) for t in GOLD_DIM_ORDER]
        refs += [TableRef("gold", t, FACT_PRIMARY_KEYS.get(t)) for t in GOLD_FACT_ORDER]
    return refs


def column_rows(fields: Iterable[tuple[str, str, bool]]) -> list[dict[str, Any]]:
    """Convert ``(name, type, nullable)`` triples to ``catalog.columns`` row dicts."""
    return [
        {
            "column_name": name,
            "data_type": dtype[:64],
            "ordinal_position": i,
            "is_nullable": bool(nullable),
            "is_pii": name in PII_COLUMNS,
            "is_pci": name in PCI_COLUMNS,
        }
        for i, (name, dtype, nullable) in enumerate(fields, start=1)
    ]


class CatalogSynchronizer:
    def __init__(self, spark: SparkSession, settings: Settings, count_rows: bool = False) -> None:
        self.spark = spark
        self.settings = settings
        self.count_rows = count_rows

    def sync_all(
        self, layers: Iterable[str] = ("bronze", "silver", "gold")
    ) -> dict[str, list[str]]:
        """Sync every expected table; returns ``{"synced": [...], "missing": [...]}``."""
        synced: list[str] = []
        missing: list[str] = []
        conn = psycopg2.connect(self.settings.postgres_dsn)
        try:
            for ref in all_tables(layers):
                try:
                    ok = self._sync_one(conn, ref)
                except Exception as exc:  # — one bad table must not stop the sweep
                    conn.rollback()
                    logger.error("catalog_sync_failed", table=ref.qualified, error=str(exc))
                    missing.append(ref.qualified)
                    continue
                (synced if ok else missing).append(ref.qualified)
            conn.commit()
        finally:
            conn.close()
        logger.info("catalog_sync_done", synced=len(synced), missing=len(missing))
        return {"synced": synced, "missing": missing}

    # ------------------------------------------------------------------
    def _sync_one(self, conn: Any, ref: TableRef) -> bool:
        path = self.settings.s3_path(ref.layer, ref.name)
        try:
            df = self.spark.read.format("delta").load(path)
        except Exception as exc:  # — table not created yet
            logger.info(
                "catalog_table_absent", table=ref.qualified, path=path, error=type(exc).__name__
            )
            return False

        detail = self.spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
        size_bytes = (
            int(detail["sizeInBytes"]) if detail and detail["sizeInBytes"] is not None else None
        )
        partitions = list(detail["partitionColumns"]) if detail else []
        row_count = df.count() if self.count_rows else None
        columns = column_rows(
            (f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields
        )
        schema_json = json.dumps(
            [{"name": c["column_name"], "type": c["data_type"]} for c in columns]
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO catalog.tables
                    (schema_name, table_name, layer, domain, s3_location, format,
                     partition_columns, primary_key_columns, row_count, size_bytes,
                     schema_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'delta', %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (schema_name, table_name) DO UPDATE SET
                    s3_location         = EXCLUDED.s3_location,
                    partition_columns   = EXCLUDED.partition_columns,
                    primary_key_columns = EXCLUDED.primary_key_columns,
                    row_count           = COALESCE(EXCLUDED.row_count, catalog.tables.row_count),
                    size_bytes          = EXCLUDED.size_bytes,
                    schema_json         = EXCLUDED.schema_json,
                    updated_at          = NOW()
                RETURNING table_id
                """,
                (
                    ref.layer,
                    ref.qualified,
                    ref.layer,
                    ref.name,
                    path,
                    partitions,
                    [ref.primary_key] if ref.primary_key else [],
                    row_count,
                    size_bytes,
                    schema_json,
                ),
            )
            table_id = cur.fetchone()[0]
            cur.execute("DELETE FROM catalog.columns WHERE table_id = %s", (table_id,))
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO catalog.columns
                    (table_id, column_name, data_type, ordinal_position, is_nullable, is_pii, is_pci)
                VALUES %s
                """,
                [
                    (
                        table_id,
                        c["column_name"],
                        c["data_type"],
                        c["ordinal_position"],
                        c["is_nullable"],
                        c["is_pii"],
                        c["is_pci"],
                    )
                    for c in columns
                ],
            )
        logger.info(
            "catalog_table_synced", table=ref.qualified, columns=len(columns), size_bytes=size_bytes
        )
        return True
