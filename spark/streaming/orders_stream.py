"""Bronze streaming consumer for retailnexus.orders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas.bronze_schemas import ORDERS_SCHEMA
from spark.streaming.base_stream import BaseStreamConsumer
from src.common.logging import setup_logging


class OrdersStreamConsumer(BaseStreamConsumer):
    topic = "retailnexus.orders"
    domain = "orders"
    schema = ORDERS_SCHEMA
    event_time_col = "created_at"
    starting_offsets = "latest"

    def transform(self, df: DataFrame) -> DataFrame:
        # Cast created_at string to proper timestamp for watermarking
        return df.withColumn(
            "created_at",
            F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )


if __name__ == "__main__":
    setup_logging(service_name="orders-stream")
    consumer = OrdersStreamConsumer()
    consumer.start()
