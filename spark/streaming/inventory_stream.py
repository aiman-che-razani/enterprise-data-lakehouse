"""Bronze streaming consumer for retailnexus.inventory."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas.bronze_schemas import INVENTORY_SCHEMA
from spark.streaming.base_stream import BaseStreamConsumer
from src.common.logging import setup_logging


class InventoryStreamConsumer(BaseStreamConsumer):
    topic = "retailnexus.inventory"
    domain = "inventory"
    schema = INVENTORY_SCHEMA
    event_time_col = "updated_at"
    starting_offsets = "latest"

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "updated_at",
            F.to_timestamp(F.col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ).withColumn(
            "snapshot_date",
            F.to_date(F.col("snapshot_date"), "yyyy-MM-dd"),
        )


if __name__ == "__main__":
    setup_logging(service_name="inventory-stream")
    InventoryStreamConsumer().start()
