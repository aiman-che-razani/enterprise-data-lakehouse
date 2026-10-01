"""Bronze streaming consumer for retailnexus.payments."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas.bronze_schemas import PAYMENTS_SCHEMA
from spark.streaming.base_stream import BaseStreamConsumer
from src.common.logging import setup_logging


class PaymentsStreamConsumer(BaseStreamConsumer):
    topic = "retailnexus.payments"
    domain = "payments"
    schema = PAYMENTS_SCHEMA
    event_time_col = "created_at"
    starting_offsets = "latest"

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "created_at",
            F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )


if __name__ == "__main__":
    setup_logging(service_name="payments-stream")
    PaymentsStreamConsumer().start()
