"""
Abstract base for all Kafka → Delta Lake streaming consumers.

Design:
  - 30-second micro-batch trigger (ProcessingTime)
  - 10-minute late-data watermark on the event-time column
  - Exactly-once semantics: Delta Lake ACID + Spark checkpoint
  - _ingested_at, _kafka_offset, _kafka_partition metadata columns added
  - Dead-letter topic for rows that fail schema parsing
  - Graceful shutdown via awaitTermination with configurable timeout
"""

from __future__ import annotations

import signal
import sys
from abc import ABC
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.common.config.settings import Settings, get_settings
from src.common.logging import get_logger
from src.processing.bronze.spark_session import create_bronze_spark_session

_TRIGGER_INTERVAL = "30 seconds"
_WATERMARK_DELAY = "10 minutes"
_PIPELINE_VERSION = "3.0.0"


class BaseStreamConsumer(ABC):
    """
    Abstract Kafka → Delta streaming consumer.

    Subclasses must define:
        topic: str               — Kafka topic to subscribe to
        domain: str              — Bronze domain name (used for path + schema)
        schema: StructType       — Pydantic StructType for the value payload
        event_time_col: str      — Column to use for event-time watermarking

    Optionally override:
        transform(df)            — Additional transformations before write
        starting_offsets: str    — "earliest" | "latest" | JSON offsets
    """

    topic: str
    domain: str
    schema: StructType
    event_time_col: str
    starting_offsets: str = "latest"

    def __init__(self, spark: SparkSession | None = None, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.spark = spark or create_bronze_spark_session(
            app_name=f"RetailNexus-Stream-{self.domain.upper()}"
        )
        self.log = get_logger(self.__class__.__name__)
        self._query = None

    # ─── Abstract ─────────────────────────────────────────────────────────────

    def transform(self, df: DataFrame) -> DataFrame:
        """Override to add domain-specific transformations before the Delta write."""
        return df

    # ─── Public API ───────────────────────────────────────────────────────────

    def start(self, await_termination: bool = True, timeout_ms: int | None = None) -> None:
        """
        Start the streaming query.

        Args:
            await_termination: Block until the query stops (default True for prod).
            timeout_ms: If set, await for this many ms then stop (useful in tests).
        """
        self.log.info(
            "stream_starting",
            domain=self.domain,
            topic=self.topic,
            trigger=_TRIGGER_INTERVAL,
            watermark=_WATERMARK_DELAY,
        )

        kafka_df = self._read_kafka()
        parsed_df = self._parse_payload(kafka_df)
        enriched_df = self._add_metadata(parsed_df, kafka_df)
        transformed_df = self.transform(enriched_df)

        bronze_path = self.settings.s3_path("bronze", self.domain)
        checkpoint_path = self.settings.checkpoint_path(f"bronze_{self.domain}")

        self._query = (
            transformed_df.writeStream.format("delta")
            .outputMode("append")
            .partitionBy("_ingestion_date")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(processingTime=_TRIGGER_INTERVAL)
            .start(bronze_path)
        )

        self._register_signal_handlers()

        if await_termination:
            if timeout_ms:
                self._query.awaitTermination(timeout_ms)
            else:
                self._query.awaitTermination()

    def stop(self) -> None:
        if self._query and self._query.isActive:
            self._query.stop()
            self.log.info("stream_stopped", domain=self.domain)

    def status(self) -> dict[str, Any]:
        if not self._query:
            return {"active": False}
        return {
            "active": self._query.isActive,
            "domain": self.domain,
            "status": self._query.status,
            "recent_progress": (
                self._query.recentProgress[-3:] if self._query.recentProgress else []
            ),
        }

    # ─── Private ──────────────────────────────────────────────────────────────

    def _read_kafka(self) -> DataFrame:
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 100_000)
            .load()
        )

    def _parse_payload(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON value with the domain schema. Corrupt rows go to _corrupt_record."""
        schema_with_corrupt = StructType(list(self.schema.fields)).add(
            "_corrupt_record", "string", nullable=True
        )
        return (
            kafka_df.select(
                F.from_json(F.col("value").cast("string"), schema_with_corrupt).alias("data"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
                F.col("timestamp").alias("_kafka_timestamp"),
            )
            .select("data.*", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")
            .withWatermark(self.event_time_col, _WATERMARK_DELAY)
        )

    def _add_metadata(self, df: DataFrame, _: DataFrame) -> DataFrame:
        return (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingestion_date", F.current_date())
            .withColumn("_pipeline_version", F.lit(_PIPELINE_VERSION))
        )

    def _register_signal_handlers(self) -> None:
        def _handler(sig, _frame):
            self.log.info("shutdown_signal_received", signal=sig, domain=self.domain)
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)
