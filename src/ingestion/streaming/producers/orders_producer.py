"""
Kafka producer for the `retailnexus.orders` topic.

Reads from datasets/raw/json/orders/ and streams to Kafka.
Key: order_id (routes all events for an order to the same partition).
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path
from typing import Any

from src.common.logging import get_logger, setup_logging
from src.ingestion.streaming.producers.base_producer import BaseKafkaProducer

TOPIC = "retailnexus.orders"
NUM_PARTITIONS = 12


class OrdersProducer(BaseKafkaProducer):
    topic_name = TOPIC

    def message_key(self, record: dict[str, Any]) -> str | None:
        return record.get("order_id")

    def transform(self, record: dict[str, Any]) -> dict[str, Any]:
        record.setdefault("_producer_version", "1.0")
        record.setdefault("_schema_version", "orders/v1")
        return record


def produce_from_files(data_dir: Path, batch_size: int = 5_000) -> None:
    log = get_logger(__name__)
    pattern = str(data_dir / "json" / "orders" / "*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("no_order_files_found", pattern=pattern)
        return

    OrdersProducer.ensure_topic(TOPIC, num_partitions=NUM_PARTITIONS)

    with OrdersProducer(batch_size=batch_size) as producer:
        for path in files:
            log.info("producing_file", file=path)
            with open(path, encoding="utf-8") as f:
                records = [json.loads(line) for line in f if line.strip()]
            producer.produce_batch(records)
            log.info("file_produced", file=path, count=len(records))

    log.info("orders_producer_done")


if __name__ == "__main__":
    setup_logging(service_name="orders-producer")
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("datasets/raw")
    produce_from_files(data_dir)
