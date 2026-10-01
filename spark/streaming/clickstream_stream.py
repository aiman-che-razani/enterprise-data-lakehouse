"""
Bronze streaming consumer for retailnexus.clickstream.

Highest-volume stream (~5M events/day). Uses maxOffsetsPerTrigger=500_000
and a shorter trigger interval to sustain throughput.

Session events arriving > 10 min late are watermarked out; this is acceptable
since the Silver layer uses a wider session reconstruction window.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.schemas.bronze_schemas import CLICKSTREAM_SCHEMA
from spark.streaming.base_stream import BaseStreamConsumer
from src.common.logging import setup_logging

if TYPE_CHECKING:
    from src.common.config.settings import Settings


class ClickstreamStreamConsumer(BaseStreamConsumer):
    topic = "retailnexus.clickstream"
    domain = "clickstream"
    schema = CLICKSTREAM_SCHEMA
    event_time_col = "created_at"
    starting_offsets = "latest"

    def __init__(self, spark: SparkSession | None = None, settings: Settings | None = None) -> None:
        # Higher per-trigger offset limit for clickstream volume
        super().__init__(spark, settings)
        self._max_offsets = 500_000

    def _read_kafka(self) -> DataFrame:
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", self._max_offsets)
            .load()
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn(
                "created_at",
                F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
            # Partition by event_type so downstream Silver can prune by funnel stage
            .withColumn("event_date", F.to_date(F.col("created_at")))
        )


if __name__ == "__main__":
    setup_logging(service_name="clickstream-stream")
    ClickstreamStreamConsumer().start()
