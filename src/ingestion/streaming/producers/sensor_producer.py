"""
Kafka producer for the `retailnexus.iot-sensors` topic.

IoT sensor readings — 2M records, time-ordered per sensor.
Key: sensor_id (keeps time-series for a sensor co-located for TSDB writes).
Alert-triggered readings are sent with higher priority by producing to a
separate header field (consumed by the alerting consumer group).
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path
from typing import Any

from src.common.logging import get_logger, setup_logging
from src.ingestion.streaming.producers.base_producer import BaseKafkaProducer

TOPIC = "retailnexus.iot-sensors"
NUM_PARTITIONS = 24


class SensorProducer(BaseKafkaProducer):
    topic_name = TOPIC

    def __init__(self) -> None:
        super().__init__(batch_size=20_000, linger_ms=10, compression="lz4")
        self._alert_count = 0

    def message_key(self, record: dict[str, Any]) -> str | None:
        return record.get("sensor_id")

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        record.setdefault("_schema_version", "iot_sensors/v1")
        if record.get("alert_triggered"):
            record["_alert"] = True
            self._alert_count += 1
        return record

    def stats(self) -> dict[str, Any]:
        base = super().stats()
        base["alerts_triggered"] = self._alert_count
        return base


def produce_from_files(data_dir: Path) -> None:
    log = get_logger(__name__)
    pattern = str(data_dir / "json" / "iot_sensors" / "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("no_sensor_files_found", pattern=pattern)
        return

    SensorProducer.ensure_topic(TOPIC, num_partitions=NUM_PARTITIONS)

    with SensorProducer() as producer:
        for path in files:
            with open(path, encoding="utf-8") as f:
                records = [json.loads(line) for line in f if line.strip()]
            producer.produce_batch(records)

    s = producer.stats()
    log.info("sensor_producer_done", **s)


if __name__ == "__main__":
    setup_logging(service_name="sensor-producer")
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets/raw")
    produce_from_files(data_dir)
