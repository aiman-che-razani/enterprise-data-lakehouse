"""
Silver processor for the IoT sensor readings fact table.

Derived Silver columns:
  is_alert_active   — alert_triggered AND reading is outside safe_min/safe_max
  reading_band      — "below_min" / "normal" / "above_max"
  alert_priority    — "critical" (anomaly + alert) / "warning" (alert only) / "normal"
                      Mirrors _alert_priority from the streaming consumer for batch parity.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class IotSensorsSilverProcessor(BaseSilverProcessor):
    domain = "iot_sensors"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        has_reading = "sensor_value" in df.columns
        has_safe = "safe_min" in df.columns and "safe_max" in df.columns
        has_alert = "alert_triggered" in df.columns
        has_anomaly = "is_anomaly" in df.columns

        if has_reading and has_safe:
            df = df.withColumn(
                "reading_band",
                F.when(F.col("sensor_value") < F.col("safe_min"), "below_min")
                .when(F.col("sensor_value") > F.col("safe_max"), "above_max")
                .otherwise("normal"),
            )

        if has_alert and has_anomaly:
            df = df.withColumn(
                "alert_priority",
                F.when(
                    F.col("alert_triggered") & F.col("is_anomaly"),
                    F.lit("critical"),
                )
                .when(F.col("alert_triggered"), F.lit("warning"))
                .otherwise(F.lit("normal")),
            )

        return df
