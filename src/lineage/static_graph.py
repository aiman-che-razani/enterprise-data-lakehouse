"""
Declared lineage for the whole platform, derived from the pipeline metadata.

    kafka topic ─▶ bronze ─▶ silver ─▶ gold ─▶ redshift | snowflake | bigquery

Bronze→Silver is 1:1 per domain. Silver→Gold comes from ``DIM_SOURCE_MAP`` plus
``GOLD_FACT_SOURCES`` below, which mirrors the ``read_silver()`` calls in
``src/processing/gold`` (a unit test fails if the two drift apart).
"""

from __future__ import annotations

from src.processing.bronze.domain_meta import DOMAIN_KAFKA_TOPICS
from src.processing.gold.domain_meta import DIM_SOURCE_MAP, GOLD_DIM_ORDER, GOLD_FACT_ORDER
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

from .models import LineageEdge

WAREHOUSE_TARGETS: tuple[str, ...] = ("redshift", "snowflake", "bigquery")

# Silver domains read by each Gold fact builder.
GOLD_FACT_SOURCES: dict[str, tuple[str, ...]] = {
    "fact_sales": ("orders", "order_lines"),
    "fact_inventory": ("inventory",),
    "fact_marketing": ("campaigns", "clickstream"),
    "fact_returns": ("returns", "orders"),
    "fact_shipments": ("shipments", "orders"),
    "fact_finance": ("finance",),
}

# Dimensions with more than one (or a non-mapped) Silver source.
GOLD_DIM_EXTRA_SOURCES: dict[str, tuple[str, ...]] = {
    "dim_location": ("stores", "customers"),
}


def gold_sources() -> dict[str, tuple[str, ...]]:
    """Gold table → Silver domains it is built from (dim_date has none)."""
    out: dict[str, tuple[str, ...]] = {gold: (silver,) for silver, gold in DIM_SOURCE_MAP.items()}
    out.update(GOLD_DIM_EXTRA_SOURCES)
    out.update(GOLD_FACT_SOURCES)
    return out


def build_static_edges() -> list[LineageEdge]:
    edges: list[LineageEdge] = []

    for domain, topic in DOMAIN_KAFKA_TOPICS.items():
        edges.append(
            LineageEdge(
                f"kafka.{topic}", f"bronze.{domain}", "stream_ingest", f"spark.streaming.{domain}"
            )
        )

    for domain in SILVER_PROCESSING_ORDER:
        edges.append(
            LineageEdge(
                f"bronze.{domain}", f"silver.{domain}", "silver_transform", f"silver.{domain}"
            )
        )

    for gold_table, silver_domains in gold_sources().items():
        for domain in silver_domains:
            edges.append(
                LineageEdge(
                    f"silver.{domain}", f"gold.{gold_table}", "gold_build", f"gold.{gold_table}"
                )
            )

    for gold_table in list(GOLD_DIM_ORDER) + list(GOLD_FACT_ORDER):
        for target in WAREHOUSE_TARGETS:
            edges.append(
                LineageEdge(
                    f"gold.{gold_table}",
                    f"{target}.{gold_table}",
                    "warehouse_load",
                    f"load.{target}",
                )
            )

    # Deterministic, de-duplicated
    return sorted(set(edges), key=lambda e: (e.target_table, e.source_table, e.job_name))
