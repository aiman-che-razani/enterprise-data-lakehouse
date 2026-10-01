"""
Bronze streaming consumer for retailnexus.iot-sensors.

IoT readings have a tight SLA: alerts must appear in Bronze within 2 minutes.
We use a 5-second trigger interval and 5-minute watermark (sensors can buffer
readings during network blips, but data > 5 min late is anomalous).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas.bronze_schemas import IOT_SENSORS_SCHEMA
from spark.streaming.base_stream import BaseStreamConsumer
from src.common.logging import setup_logging

_SENSOR_TRIGGER = "5 seconds"
_SENSOR_WATERMARK = "5 minutes"


class SensorsStreamConsumer(BaseStreamConsumer):
    topic = "retailnexus.iot-sensors"
    domain = "iot_sensors"
    schema = IOT_SENSORS_SCHEMA
    event_time_col = "reading_at"
    starting_offsets = "latest"

    def _parse_payload(self, kafka_df: DataFrame) -> DataFrame:
        """Override: use tighter watermark for sensor low-latency SLA."""
        schema_with_corrupt = self.schema.add("_corrupt_record", "string", nullable=True)
        return (
            kafka_df.select(
                F.from_json(F.col("value").cast("string"), schema_with_corrupt).alias("data"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
                F.col("timestamp").alias("_kafka_timestamp"),
            )
            .select("data.*", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")
            .withColumn(
                "reading_at",
                F.to_timestamp(F.col("reading_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
            .withWatermark("reading_at", _SENSOR_WATERMARK)
        )

    def start(self, await_termination: bool = True, timeout_ms: int | None = None) -> None:
        """Override to use faster trigger interval for low-latency alerts."""
        self.log.info(
            "sensor_stream_starting",
            domain=self.domain,
            trigger=_SENSOR_TRIGGER,
            watermark=_SENSOR_WATERMARK,
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
            .trigger(processingTime=_SENSOR_TRIGGER)
            .start(bronze_path)
        )

        self._register_signal_handlers()
        if await_termination:
            if timeout_ms:
                self._query.awaitTermination(timeout_ms)
            else:
                self._query.awaitTermination()

    def transform(self, df: DataFrame) -> DataFrame:
        # Flag alerts for downstream real-time alerting consumers
        return df.withColumn(
            "_alert_priority",
            F.when(F.col("alert_triggered") & F.col("is_anomaly"), F.lit("critical"))
            .when(F.col("alert_triggered"), F.lit("warning"))
            .otherwise(F.lit("normal")),
        )


if __name__ == "__main__":
    setup_logging(service_name="sensors-stream")
    SensorsStreamConsumer().start()
